from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional
import re
import json
import ast

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Request, Query
from sqlmodel import select

from ..db import get_session, engine
from ..models import Client, Item, Order, Payment, ImportRun, ImportRow, StockMovement, Product, OrderItem
from ..services.importer import read_bizim_file, read_kargo_file, read_returns_file
from ..services.importer.committers import process_kargo_row, process_bizim_row
from ..schemas import BizimRow, KargoRow, ReturnsRow, BIZIM_ALLOWED_KEYS, KARGO_ALLOWED_KEYS, RETURNS_ALLOWED_KEYS
from ..services.matching import find_order_by_tracking, find_client_candidates
from ..services.matching import find_order_by_client_and_date, find_recent_placeholder_kargo_for_client
from ..utils.hashing import compute_row_hash
from ..utils.normalize import client_unique_key, legacy_client_unique_key, normalize_phone, normalize_text
from ..utils.slugify import slugify
from ..services.cache import bump_namespace
from ..services.inventory import adjust_stock
from ..services.shipping import compute_shipping_fee
from ..services.mapping import resolve_mapping
from fastapi.responses import HTMLResponse, FileResponse

router = APIRouter(prefix="")

# Project root is two levels up from this file: app/routers/importer.py -> app/ -> project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BIZIM_DIR = PROJECT_ROOT / "bizimexcellerimiz"
KARGO_DIR = PROJECT_ROOT / "kargocununexcelleri"
IADE_DIR = PROJECT_ROOT / "iadeler"


def _enrich_duplicate_row(rec: dict, run: ImportRun, existing_ir: ImportRow | None, session, source: str) -> None:
	"""
	If a row is skipped as duplicate, try to hydrate missing payment/total/dates on the matched order.
	Safe/idempotent: only fills empty fields or adds a payment when not already present.
	"""
	try:
		order = None
		if existing_ir and existing_ir.matched_order_id:
			order = session.get(Order, existing_ir.matched_order_id)
		if (order is None) and rec.get("tracking_no"):
			order = session.exec(select(Order).where(Order.tracking_no == rec.get("tracking_no"))).first()
		if order is None:
			return

		pdate = rec.get("delivery_date") or rec.get("shipment_date") or run.data_date
		amt = rec.get("payment_amount") or 0.0
		if amt and amt > 0 and pdate:
			existing_pay = session.exec(
				select(Payment).where(
					Payment.order_id == order.id,
					Payment.amount == float(amt),
					Payment.date == pdate,
				)
			).first()
			if existing_pay is None:
				fee_kom = rec.get("fee_komisyon") or 0.0
				fee_hiz = rec.get("fee_hizmet") or 0.0
				fee_iad = rec.get("fee_iade") or 0.0
				fee_eok = rec.get("fee_erken_odeme") or 0.0
				fee_kar = compute_shipping_fee(float(amt))
				net = round(float(amt) - sum([fee_kom, fee_hiz, fee_kar, fee_iad, fee_eok]), 2)
				pmt = Payment(
					client_id=order.client_id,
					order_id=order.id,
					amount=float(amt),
					date=pdate,
					payment_date=pdate,
					method=rec.get("payment_method") or (source if source in ("kargo", "bizim") else "import"),
					reference=rec.get("tracking_no"),
					fee_komisyon=fee_kom,
					fee_hizmet=fee_hiz,
					fee_kargo=fee_kar,
					fee_iade=fee_iad,
					fee_erken_odeme=fee_eok,
					net_amount=net,
				)
				session.add(pmt)

		if (order.total_amount is None) or (float(order.total_amount or 0.0) == 0.0):
			if rec.get("total_amount") is not None:
				order.total_amount = rec.get("total_amount")
			elif amt and amt > 0:
				order.total_amount = float(amt)

		if (order.quantity or 0) and (not order.unit_price) and order.total_amount is not None:
			try:
				order.unit_price = round(float(order.total_amount) / float(order.quantity), 2)
			except Exception:
				pass

		if rec.get("shipment_date") and not order.shipment_date:
			order.shipment_date = rec.get("shipment_date")
		if rec.get("delivery_date") and not getattr(order, "delivery_date", None):
			order.delivery_date = rec.get("delivery_date")
	except Exception:
		return


def _format_size(num: int) -> str:
	"""Human-readable file size."""
	size = float(num)
	for unit in ("B", "KB", "MB", "GB", "TB"):
		if size < 1024.0 or unit == "TB":
			return f"{int(size)} {unit}" if unit == "B" else f"{size:.1f} {unit}"
		size /= 1024.0
	return f"{size:.1f} TB"


@router.get("/uploads", response_class=HTMLResponse)
def list_uploaded_excels(request: Request):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	sources: list[dict[str, Any]] = [
		{"key": "bizim", "label": "Bizim Excel", "folder": BIZIM_DIR},
		{"key": "kargo", "label": "Kargo Excel", "folder": KARGO_DIR},
		{"key": "returns", "label": "İade Excel", "folder": IADE_DIR},
	]
	for src in sources:
		folder = src["folder"]
		files: list[dict[str, Any]] = []
		if folder.exists():
			paths = sorted(folder.glob("*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
			for p in paths:
				try:
					st = p.stat()
				except FileNotFoundError:
					continue
				files.append({
					"name": p.name,
					"size_bytes": st.st_size,
					"size_human": _format_size(st.st_size),
					"modified": datetime.fromtimestamp(st.st_mtime),
				})
		src["files"] = files
		src["folder_str"] = str(folder)
	total_files = sum(len(s["files"]) for s in sources)
	templates = request.app.state.templates
	return templates.TemplateResponse(
		"import_uploads.html",
		{
			"request": request,
			"sources": sources,
			"total_files": total_files,
		},
	)


@router.get("/uploads/download")
def download_uploaded_excel(source: str, filename: str, request: Request):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	if source not in ("bizim", "kargo", "returns"):
		raise HTTPException(status_code=400, detail="source must be 'bizim', 'kargo' or 'returns'")
	folder = BIZIM_DIR if source == "bizim" else (KARGO_DIR if source == "kargo" else IADE_DIR)
	safe_name = Path(filename).name
	file_path = folder / safe_name
	if not file_path.exists():
		raise HTTPException(status_code=404, detail="File not found")
	if file_path.suffix.lower() != ".xlsx":
		raise HTTPException(status_code=400, detail="Only .xlsx files are allowed")
	return FileResponse(
		path=str(file_path),
		filename=file_path.name,
		media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	)


def parse_item_details(text: str | None) -> tuple[str, int | None, int | None, list[str]]:
	"""Extract base item name, height(cm), weight(kg), and extra notes.

	Handles nested parentheses like "(178,80(KENDİSİ))":
	- First top-level parentheses: parse two numbers as height/weight if present.
	- Any nested parentheses anywhere are treated as notes.
	- Additional top-level parentheses after the first are also notes.
	"""
	if not text:
		return "Genel Ürün", None, None, []

	def split_top_level(s: str) -> list[str]:
		parts: list[str] = []
		depth = 0
		buf: list[str] = []
		for ch in s:
			if ch == '(':
				if depth > 0:
					buf.append(ch)
				depth += 1
				if depth == 1:
					buf = []
			elif ch == ')':
				if depth > 1:
					buf.append(ch)
				depth -= 1
				if depth == 0:
					parts.append(''.join(buf).strip())
		# if string ended but we are still inside a paren block, accept incomplete as a part
		if depth > 0 and buf:
			parts.append(''.join(buf).strip())
		return parts

	parts = split_top_level(text)
	# base name: everything before the first '('
	idx = text.find('(')
	base = (text[:idx] if idx != -1 else text).strip()

	height: int | None = None
	weight: int | None = None
	notes: list[str] = []

	if parts:
		# check first top-level part for two numbers
		nums = re.findall(r"\d{2,3}", parts[0])
		if len(nums) >= 2:
			try:
				height = int(nums[0])
				weight = int(nums[1])
			except Exception:
				pass
		# gather nested notes from the first part
		for inner in re.findall(r"\(([^()]*)\)", parts[0]):
			inner = inner.strip()
			if inner:
				notes.append(inner)
		# remaining top-level parts are notes (and their nested contents)
		for p in parts[1:]:
			added_inner = False
			for inner in re.findall(r"\(([^()]*)\)", p):
				inner = inner.strip()
				if inner:
					notes.append(inner)
					added_inner = True
			p_clean = re.sub(r"\([^()]*\)", "", p).strip()
			if p_clean and not added_inner:
				notes.append(p_clean)

	# de-duplicate notes while preserving order
	seen = set()
	unique_notes: list[str] = []
	for n in notes:
		if n not in seen:
			seen.add(n)
			unique_notes.append(n)

	return base, height, weight, unique_notes



@router.get("/runs")
def list_runs():
	with get_session() as session:
		runs = session.exec(select(ImportRun).order_by(ImportRun.id.desc())).all()
		return [
			{
				"id": r.id or 0,
				"source": r.source,
				"filename": r.filename,
				"row_count": r.row_count,
				"created_clients": r.created_clients,
				"updated_clients": r.updated_clients,
				"created_items": r.created_items,
				"created_orders": r.created_orders,
				"created_payments": r.created_payments,
				"unmatched_count": r.unmatched_count,
			}
			for r in runs
		]


@router.post("/preview")
def preview_import(body: dict, request: Request):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	source = body.get("source")
	filename = body.get("filename")
	if source not in ("bizim", "kargo", "returns"):
		raise HTTPException(status_code=400, detail="source must be 'bizim', 'kargo' or 'returns'")
	folder = BIZIM_DIR if source == "bizim" else (KARGO_DIR if source == "kargo" else IADE_DIR)
	if not folder.exists():
		raise HTTPException(status_code=400, detail=f"Folder not found: {folder}")
	if filename:
		file_path = folder / filename
		if not file_path.exists():
			raise HTTPException(status_code=404, detail="File not found")
	else:
		candidates = sorted(folder.glob("*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
		if not candidates:
			raise HTTPException(status_code=404, detail="No .xlsx files found")
		file_path = candidates[0]

	if source == "bizim":
		records = read_bizim_file(str(file_path))
	elif source == "kargo":
		records = read_kargo_file(str(file_path))
	else:
		records = read_returns_file(str(file_path))
	# enforce per-source whitelist and annotate record_type
	filtered: list[dict] = []
	if source == "bizim":
		allowed = BIZIM_ALLOWED_KEYS
	elif source == "kargo":
		allowed = KARGO_ALLOWED_KEYS
	else:
		allowed = RETURNS_ALLOWED_KEYS
	for r in records:
		# attach record_type for debugging
		r["record_type"] = source
		# drop unknown keys
		r2 = {k: v for k, v in r.items() if k in allowed}
		# map any stray item_name for kargo to notes just in case
		if source == "kargo" and r.get("item_name"):
			val = r.get("item_name")
			r2["notes"] = f"{r2.get('notes')} | {val}" if r2.get("notes") else val
		filtered.append(r2)
	records = filtered
	# DEBUG: echo headers and first few raw/mapped rows to server logs for troubleshooting
	try:
		from ..services.importer.common import read_sheet_rows
		hdrs, raw_rows = read_sheet_rows(str(file_path))
		print("[IMPORT DEBUG] headers:", hdrs)
		if raw_rows:
			print("[IMPORT DEBUG] first row raw:", raw_rows[0])
			print("[IMPORT DEBUG] first row mapped:", records[0] if records else {})
			# also log number of non-empty fields in first mapped row
			print("[IMPORT DEBUG] first mapped keys:", [k for k,v in (records[0] or {}).items() if v not in (None, '', 0)])
			print(f"[IMPORT DEBUG] total mapped records: {len(records)}")
	except Exception as _e:
		print("[IMPORT DEBUG] header probe failed:", _e)
	return {
		"source": source,
		"filename": file_path.name,
		"row_count": len(records),
		"sample": records[:5],
	}


@router.post("/preview-map")
def preview_map(body: dict, request: Request):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	source = body.get("source")
	filename = body.get("filename")
	filenames = body.get("filenames")  # optional list for multi-file aggregation
	exclude_generic = bool(body.get("exclude_generic", False))
	return_rows = bool(body.get("return_rows", False))
	rows_limit = int(body.get("rows_limit", 1000))
	if source != "bizim":
		raise HTTPException(status_code=400, detail="source must be 'bizim' and filename(s) required")
	folder = BIZIM_DIR
	# unify single vs multi
	file_list: list[str] = []
	if filenames and isinstance(filenames, list) and filenames:
		file_list = [str(x) for x in filenames]
	elif filename:
		# support comma-separated list in filename as a convenience
		parts = [p for p in str(filename).split(",") if p.strip()]
		file_list = parts if len(parts) > 1 else [filename]
	else:
		raise HTTPException(status_code=400, detail="filename or filenames required")
	# pre-read files list for per-file iteration
	file_records: list[tuple[str, list[dict]]]=[]
	for fn in file_list:
		file_path = folder / fn
		if not file_path.exists():
			raise HTTPException(status_code=404, detail=f"File not found: {fn}")
		recs = read_bizim_file(str(file_path))
		file_records.append((fn, recs))
	from ..services.mapping import resolve_mapping
	unmatched: dict[str, dict] = {}
	unmatched_rows: list[dict] = []
	pattern_filter = body.get("pattern")
	for fn, recs in file_records:
		for idx, rec in enumerate(recs):
			item_name_raw = rec.get("item_name") or "Genel Ürün"
			base_name, _h, _w, _notes = parse_item_details(item_name_raw)
			outs, rule = None, None
			try:
				with get_session() as session:
					outs, rule = resolve_mapping(session, base_name)
			except Exception:
				outs, rule = [], None
			if not outs:
				# optionally hide generic placeholder patterns from the list when excluded (still counted)
				lower_base = base_name.strip().lower()
				if exclude_generic and (lower_base in ("genel ürün", "genel urun")):
					if return_rows and len(unmatched_rows) < rows_limit and ((not pattern_filter) or (pattern_filter == base_name)):
						unmatched_rows.append({
							"filename": fn,
							"row_index": idx,
							"item_name": item_name_raw,
							"base": base_name,
							"quantity": rec.get("quantity"),
							"unit_price": rec.get("unit_price"),
							"total_amount": rec.get("total_amount"),
							"name": rec.get("name"),
							"phone": rec.get("phone"),
						})
					# do not add to unmatched patterns list
					continue
				entry = unmatched.get(base_name)
				if not entry:
					entry = {"pattern": base_name, "count": 0, "samples": [], "suggested_price": None}
					unmatched[base_name] = entry
				entry["count"] += 1
				if len(entry["samples"]) < 3:
					entry["samples"].append(item_name_raw)
				# try suggest a unit price from row
				try:
					amt = rec.get("unit_price") if rec.get("unit_price") is not None else rec.get("total_amount")
					qty = rec.get("quantity") or 1
					if amt is not None and (entry.get("suggested_price") is None):
						sp = float(amt) / float(qty or 1)
						entry["suggested_price"] = round(sp, 2)
				except Exception:
					pass
				# capture row details if requested
				if return_rows and len(unmatched_rows) < rows_limit and ((not pattern_filter) or (pattern_filter == base_name)):
					unmatched_rows.append({
						"filename": fn,
						"row_index": idx,
						"item_name": item_name_raw,
						"base": base_name,
						"quantity": rec.get("quantity"),
						"unit_price": rec.get("unit_price"),
						"total_amount": rec.get("total_amount"),
						"name": rec.get("name"),
						"phone": rec.get("phone"),
					})
	return {
		"filename": file_list[0] if len(file_list) == 1 else None,
		"filenames": file_list,
		"unmatched_patterns": sorted(unmatched.values(), key=lambda x: x["count"], reverse=True),
		"total_unmatched": sum(v["count"] for v in unmatched.values()),
		"unmatched_rows": unmatched_rows if return_rows else None,
		"rows_returned": len(unmatched_rows) if return_rows else 0,
	}


@router.get("/map")
def import_map(source: str, filename: str, request: Request):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	if source != "bizim":
		raise HTTPException(status_code=400, detail="Only 'bizim' supported for mapping wizard")
	folder = BIZIM_DIR
	# Allow multi: parse optional comma-separated 'filename' list via query string
	file_list: list[str] = []
	if filename == "last":
		candidates = sorted(folder.glob("*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
		if not candidates:
			raise HTTPException(status_code=404, detail="No .xlsx files found")
		file_list = [candidates[0].name]
	elif filename == "all":
		candidates = sorted(folder.glob("*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
		if not candidates:
			raise HTTPException(status_code=404, detail="No .xlsx files found")
		file_list = [c.name for c in candidates]
	else:
		parts = [p for p in str(filename).split(",") if p.strip()]
		file_list = parts if parts else []
	if not file_list:
		raise HTTPException(status_code=400, detail="No filename(s) provided")
	# validate existence
	for fn in file_list:
		fp = folder / fn
		if not fp.exists():
			raise HTTPException(status_code=404, detail=f"File not found: {fn}")
	# aggregate unmatched via preview_map logic
	preview = preview_map({"source": source, "filenames": file_list}, request)
	# products list for picker
	from ..models import Product as _Product
	with get_session() as session:
		products = session.exec(select(_Product).order_by(_Product.name.asc()).limit(1000)).all()
		prod_rows = [{"id": p.id, "name": p.name, "slug": p.slug, "default_color": p.default_color} for p in products]
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"import_map.html",
			{
				"request": request,
				"source": source,
				"filename": file_list[0] if len(file_list) == 1 else None,
				"filenames": file_list,
				"unmatched_patterns": preview.get("unmatched_patterns") or [],
				"total_unmatched": preview.get("total_unmatched") or 0,
				"products": prod_rows,
			},
		)

@router.post("/commit")
def commit_import(body: dict, request: Request):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	source = body.get("source")
	filename = body.get("filename")
	filenames = body.get("filenames")
	skip_duplicates = body.get("skip_duplicates", True)
	data_date_raw = body.get("data_date")  # ISO YYYY-MM-DD string, may apply to all
	data_dates_map = body.get("data_dates") or {}  # optional per-filename map
	if source not in ("bizim", "kargo", "returns"):
		raise HTTPException(status_code=400, detail="source ('bizim'|'kargo'|'returns') is required")

	# For returns, block direct commit and route users to interactive review first
	if source == "returns":
		fn = None
		if body.get("filename"):
			fn = str(body.get("filename"))
		elif body.get("filenames") and isinstance(body.get("filenames"), list) and body.get("filenames"):
			try:
				fn = str(body.get("filenames")[0])
			except Exception:
				fn = None
		raise HTTPException(status_code=400, detail={
			"error": "returns_interactive_required",
			"next": f"/import/returns/review?filename={fn}" if fn else "/import/returns/review",
		})

	# Preflight mapping guard: block bizim commits when there are unmatched patterns (including generics)
	if source == "bizim":
		file_list_pf: list[str] = []
		if filenames:
			file_list_pf = [str(x) for x in (filenames if isinstance(filenames, list) else str(filenames).split(",")) if x]
		elif filename:
			file_list_pf = [str(filename)]
		else:
			raise HTTPException(status_code=400, detail="filename(s) required")
		try:
			preview = preview_map({"source": "bizim", "filenames": file_list_pf, "exclude_generic": False}, request)
		except HTTPException:
			raise
		except Exception as _e:
			preview = {"total_unmatched": 0, "unmatched_patterns": []}
		if (preview.get("total_unmatched") or 0) > 0:
			from urllib.parse import quote
			joined = ",".join(file_list_pf)
			raise HTTPException(status_code=400, detail={
				"error": "unmatched_mappings",
				"total_unmatched": preview.get("total_unmatched"),
				"unmatched_patterns": (preview.get("unmatched_patterns") or [])[:50],
				"filenames": file_list_pf,
				"next": f"/import/map?source=bizim&filename={quote(joined)}",
			})

	# helper to process a single file name
	def _commit_single(fn: str, dd_raw: str | None, *, skip_duplicates: bool = True) -> dict:
		folder_loc = BIZIM_DIR if source == "bizim" else KARGO_DIR
		if source == "returns":
			folder_loc = IADE_DIR
		file_path_loc = folder_loc / fn
		if not file_path_loc.exists():
			raise HTTPException(status_code=404, detail=f"File not found: {fn}")
		if source == "bizim":
			records_loc = read_bizim_file(str(file_path_loc))
		elif source == "kargo":
			records_loc = read_kargo_file(str(file_path_loc))
		else:
			records_loc = read_returns_file(str(file_path_loc))
		# Diagnostics: log file and records count
		try:
			print("[IMPORT COMMIT] file:", fn, "source:", source, "records:", len(records_loc), "path:", str(file_path_loc))
			if source == "bizim" and records_loc:
				print("[IMPORT COMMIT] first mapped:", records_loc[0])
		except Exception:
			pass
		with get_session() as session:
			run = ImportRun(source=source, filename=fn)
			# set data_date
			if source == "bizim":
				if dd_raw:
					try:
						# Import here to avoid adding a new top-level import
						import datetime as _dt
						run.data_date = _dt.date.fromisoformat(dd_raw)
					except Exception:
						raise HTTPException(status_code=400, detail="Invalid data_date; expected YYYY-MM-DD")
				else:
					# Derive from filename: first 10 characters expected to be YYYY-MM-DD
					try:
						import datetime as _dt
						prefix = str(fn)[:10]
						run.data_date = _dt.date.fromisoformat(prefix)
					except Exception:
						# Leave as None if filename doesn't contain a valid ISO date
						pass
			elif source == "kargo":  # derive from filename (when kargo Excel was imported/received)
				# Extract date from filename (first 10 characters expected to be YYYY-MM-DD)
				try:
					import datetime as _dt
					prefix = str(fn)[:10]
					run.data_date = _dt.date.fromisoformat(prefix)
				except Exception:
					# Leave as None if filename doesn't contain a valid ISO date
					pass
			else:  # returns -> expect dd_raw provided by caller (commit body)
				if dd_raw:
					try:
						import datetime as _dt
						run.data_date = _dt.date.fromisoformat(dd_raw)
					except Exception:
						raise HTTPException(status_code=400, detail="Invalid data_date; expected YYYY-MM-DD")
			session.add(run)
			session.flush()

			# local (non-persisted) counters for detailed summary
			enriched_orders_cnt = 0
			payments_created_cnt = 0
			payments_existing_cnt = 0
			payments_skipped_zero_cnt = 0
			returns_processed_cnt = 0
			returns_skipped_cnt = 0
			returns_unmatched_cnt = 0
			# row-level status counters
			rows_created_cnt = 0
			rows_skipped_cnt = 0
			rows_unmatched_cnt = 0
			rows_error_cnt = 0
			rows_duplicate_cnt = 0

			for idx, rec in enumerate(records_loc):
				# guard and normalize basic fields
				rec_name = (rec.get("name") or "").strip()
				rec_phone = normalize_phone(rec.get("phone"))
				if rec_phone:
					rec["phone"] = rec_phone
				# skip rows that have neither a name nor a phone
				if not (rec_name or rec_phone):
					status = "skipped"
					ir = ImportRow(
						import_run_id=run.id or 0,
						row_index=idx,
						row_hash=compute_row_hash(rec),
						mapped_json=str(rec),
						status=status,  # type: ignore
						message="empty name and phone",
						matched_client_id=None,
						matched_order_id=None,
					)
					session.add(ir)
					run.unmatched_count += 0
					rows_skipped_cnt += 1
					continue
				row_hash = compute_row_hash(rec)
				# idempotency: skip duplicate rows only when a prior run already produced/matched an order
				existing_ir = session.exec(select(ImportRow).where(ImportRow.row_hash == row_hash).order_by(ImportRow.id.desc())).first()
				skip_due_to_duplicate = False
				if existing_ir:
					try:
						prev_status = str(existing_ir.status or "").lower()
						# Skip when a previous import created/updated/merged or matched an order
						skip_due_to_duplicate = (existing_ir.matched_order_id is not None) or (prev_status in ("created","updated","merged"))
					except Exception:
						skip_due_to_duplicate = False
				# For returns, allow reprocessing to fix status/date even if duplicate,
				# BUT do not reuse matched_order_id from previous ImportRow (force fresh match)
				if source == "returns":
					skip_due_to_duplicate = False
					existing_ir = None
				if skip_due_to_duplicate and skip_duplicates:
					# even though we mark skipped, try to enrich the existing order with any new data/payments
					try:
						_enrich_duplicate_row(rec, run, existing_ir, session, source)
					except Exception:
						pass
					ir = ImportRow(
						import_run_id=run.id or 0,
						row_index=idx,
						row_hash=row_hash,
						mapped_json=str(rec),
						status="skipped",  # type: ignore
						message="duplicate row (already processed)",
						matched_client_id=existing_ir.matched_client_id,
						matched_order_id=existing_ir.matched_order_id,
					)
					session.add(ir)
					run.unmatched_count += 0
					rows_duplicate_cnt += 1
					continue
				status = "created"
				message = None
				matched_client_id = None
				matched_order_id = None
				candidates: list = []

				try:
					# DEBUG: log each row minimal mapping state
					if idx < 5 or (idx % 50 == 0):
						print("[ROW DEBUG]", idx, {
							"tracking_no": rec.get("tracking_no"),
							"name": rec.get("name"),
							"record_type": rec.get("record_type"),
							"notes": rec.get("notes"),
							"quantity": rec.get("quantity"),
							"total_amount": rec.get("total_amount"),
							"payment_amount": rec.get("payment_amount"),
							"payment_method": rec.get("payment_method"),
							"shipment_date": rec.get("shipment_date"),
							"delivery_date": rec.get("delivery_date"),
						})
					if source == "bizim":
						item_name_raw = rec.get("item_name") or "Genel Ürün"
						base_name, height_cm, weight_kg, extra_notes = parse_item_details(item_name_raw)
						rec["item_name_base"] = base_name
						status, message, matched_client_id, matched_order_id, candidates = process_bizim_row(session, run, rec)

					# Kargo branch extracted into service function
					elif source == "kargo":
						status, message, matched_client_id, matched_order_id, candidates = process_kargo_row(session, run, rec)
					elif source == "returns":
						# resolve client (best-effort)
						new_uq = client_unique_key(rec.get("name"), rec.get("phone"))
						old_uq = legacy_client_unique_key(rec.get("name"), rec.get("phone"))
						client = None
						if new_uq:
							client = session.exec(select(Client).where(Client.unique_key == new_uq)).first()
						if not client and old_uq:
							client = session.exec(select(Client).where(Client.unique_key == old_uq)).first()
						# fallback: phone substring (last 6)
						if not client and rec.get("phone"):
							try:
								ph = "".join([d for d in str(rec.get("phone")) if d.isdigit()])
								sub = ph[-6:] if len(ph) >= 6 else ph
								if sub:
									cands = session.exec(select(Client).where(Client.phone.ilike(f"%{sub}%"))).all()
									if len(cands) == 1:
										client = cands[0]
							except Exception:
								pass
						matched_client_id = client.id if client and client.id is not None else None

						# find most relevant order for this client (most recent, try to match item base in notes or item name)
						matched_order_id = None
						action = (rec.get("action") or "").strip()  # refund | switch
						base = (rec.get("item_name_base") or rec.get("item_name") or "").strip()
						if client and client.id is not None:
							from sqlmodel import select as _select
							cand = session.exec(
								_select(Order).where(Order.client_id == client.id).order_by(Order.id.desc())
							).all()
							def _order_matches(o: Order) -> bool:
								try:
									if base:
										# match via item name
										if o.item_id:
											it = session.exec(_select(Item).where(Item.id == o.item_id)).first()
											if it and base and (base.lower() in (it.name or '').lower()):
												return True
										# match via notes
										if o.notes and (base.lower() in (o.notes or '').lower()):
											return True
								except Exception:
									pass
								return False
							chosen = None
							for o in cand:
								if (o.status or "") in ("refunded", "switched", "stitched"):
									continue
								if _order_matches(o):
									chosen = o
									break
							if chosen and chosen.id is not None:
								matched_order_id = chosen.id
								# restock items like refund/switch endpoint
								from ..services.inventory import adjust_stock as _adjust_stock
								oitems = session.exec(select(OrderItem).where(OrderItem.order_id == chosen.id)).all()
								# idempotency guard: skip if already has an 'in' movement linked or date set
								already_in = session.exec(
									select(StockMovement).where(StockMovement.related_order_id == chosen.id, StockMovement.direction == "in")
								).first()
								already_has_date = chosen.return_or_switch_date is not None
								already_done = (chosen.status or "") in ("refunded", "switched", "stitched") or already_in is not None

								# If previously partially processed (date set but status empty), allow status fix without restock
								if already_done or already_has_date:
									if (chosen.status or "").lower() not in ("refunded", "switched", "stitched"):
										if action == "refund":
											chosen.status = "refunded"
										elif action == "switch":
											chosen.status = "switched"
										if not chosen.return_or_switch_date:
											ret_date = rec.get("date") or run.data_date
											chosen.return_or_switch_date = ret_date
										status = "updated"
										message = "status_fixed"
									else:
										status = "skipped"
										message = "already_processed"
									returns_skipped_cnt += 1
								else:
									restocked = 0
									for oi in oitems:
										if oi.item_id is None:
											continue
										qty = int(oi.quantity or 0)
										if qty > 0:
											_adjust_stock(session, item_id=int(oi.item_id), delta=qty, related_order_id=chosen.id)
											restocked += qty
								# set status and date
									if action == "refund":
										chosen.status = "refunded"
									elif action == "switch":
										chosen.status = "switched"
								# prefer per-row date, fallback to run date
								ret_date = rec.get("date") or run.data_date
								chosen.return_or_switch_date = ret_date
								# set revenue (Toplam) from provided amount when available (e.g., -88 for refund)
								amt = rec.get("amount")
								try:
									if amt is not None:
										chosen.total_amount = round(float(amt), 2)
								except Exception:
									pass
								# structured debug
								try:
									print("[RETURNS DEBUG]", {
										"client_id": matched_client_id,
										"action": action,
										"chosen_order_id": chosen.id,
										"restocked_total": restocked,
										"date": str(ret_date),
										"amount": amt,
									})
								except Exception:
									pass
									status = "updated"
									message = f"returns:{action},restocked:{restocked}"
									returns_processed_cnt += 1
							else:
								status = "unmatched"
								message = "no_matching_order"
								returns_unmatched_cnt += 1
				except Exception as e:
					status = "error"
					message = str(e)

				if status == "ambiguous":
					ir = ImportRow(
						import_run_id=run.id or 0,
						row_index=idx,
						row_hash=row_hash,
						mapped_json=str(rec),
						status=status,  # type: ignore
						message=message,
						matched_client_id=None,
						matched_order_id=None,
						candidates_json=json.dumps(candidates or []),
					)
					session.add(ir)
					run.unmatched_count += 1
					rows_unmatched_cnt += 1
					continue

				ir = ImportRow(
					import_run_id=run.id or 0,
					row_index=idx,
					row_hash=row_hash,
					mapped_json=str(rec),
					status=status,  # type: ignore
					message=message,
					matched_client_id=matched_client_id,
					matched_order_id=matched_order_id,
				)
				session.add(ir)
				if status == "unmatched":
					run.unmatched_count += 1
				# bump row-level counters
				if status == "created":
					rows_created_cnt += 1
				elif status == "skipped":
					rows_skipped_cnt += 1
				elif status == "unmatched":
					rows_unmatched_cnt += 1
				elif status == "error":
					rows_error_cnt += 1

			run.row_count = len(records_loc)
			# snapshot response before leaving session context to avoid DetachedInstanceError
			summary_loc = {
				"run_id": run.id or 0,
				"row_count": run.row_count,
				"created_orders": run.created_orders,
				"created_clients": run.created_clients,
				"created_items": run.created_items,
				"created_payments": run.created_payments,
				"unmatched": run.unmatched_count,
				"enriched_orders": enriched_orders_cnt,
				"payments_existing": payments_existing_cnt,
				"payments_skipped_zero": payments_skipped_zero_cnt,
				"returns_processed": returns_processed_cnt,
				"returns_skipped": returns_skipped_cnt,
				"returns_unmatched": returns_unmatched_cnt,
				"rows_created": rows_created_cnt,
				"rows_skipped": rows_skipped_cnt,
				"rows_unmatched": rows_unmatched_cnt,
				"rows_error": rows_error_cnt,
				"rows_duplicates": rows_duplicate_cnt,
			}
			try:
				print("[IMPORT COMMIT] summary:", summary_loc)
			except Exception:
				pass
			return summary_loc

	# If multi provided, iterate
	if filenames:
		if isinstance(filenames, str):
			file_list = [p for p in filenames.split(",") if p.strip()]
		else:
			file_list = [str(x) for x in filenames]
		agg = {
			"runs": [],
			"row_count": 0,
			"created_orders": 0,
			"created_clients": 0,
			"created_items": 0,
			"created_payments": 0,
			"unmatched": 0,
			"enriched_orders": 0,
			"payments_existing": 0,
			"payments_skipped_zero": 0,
			"rows_created": 0,
			"rows_skipped": 0,
			"rows_unmatched": 0,
			"rows_error": 0,
			"rows_duplicates": 0,
		}
		for fn in file_list:
			dd = (data_dates_map.get(fn) if isinstance(data_dates_map, dict) else None) or data_date_raw
			res = _commit_single(fn, dd, skip_duplicates=skip_duplicates)
			agg["runs"].append({"filename": fn, **res})
			for k in ("row_count","created_orders","created_clients","created_items","created_payments","unmatched","enriched_orders","payments_existing","payments_skipped_zero","rows_created","rows_skipped","rows_unmatched","rows_error","rows_duplicates"):
				agg[k] += (res.get(k) or 0)
		# Invalidate all cached reads after commit
		bump_namespace()
		return agg

	# single-file fallback (original behavior)
	if not filename:
		raise HTTPException(status_code=400, detail="filename is required for single commit")
	res_single = _commit_single(filename, data_date_raw, skip_duplicates=skip_duplicates)
	# Invalidate all cached reads after commit
	bump_namespace()
	return res_single


@router.post("/reset")
def reset_database(request: Request, preserve_mappings: bool = False):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	# Full DB reset is intentionally disabled in this deployment.
	# Keep the route to avoid breaking old links, but fail explicitly.
	raise HTTPException(status_code=501, detail="Database reset is disabled in this deployment")


@router.post("/upload")
async def upload_excel(
	source: str = Form(..., description="'bizim' or 'kargo'"),
	files: Optional[List[UploadFile]] = File(None),
	file: Optional[UploadFile] = File(None),
	skip_duplicates: bool = Form(default=True, description="If false, reprocess duplicate rows (advanced)"),
	request: Request = None,
):
	# Starlette injects Request when declared as a parameter
	if not request or not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	if source not in ("bizim", "kargo", "returns"):
		raise HTTPException(status_code=400, detail="source must be 'bizim', 'kargo' or 'returns'")
	folder = BIZIM_DIR if source == "bizim" else (KARGO_DIR if source == "kargo" else IADE_DIR)
	folder.mkdir(parents=True, exist_ok=True)

	# unify inputs: accept either 'files' (multiple) or single 'file'
	uploads: List[UploadFile] = []
	if files:
		uploads.extend(files)
	if file:
		uploads.append(file)
	if not uploads:
		raise HTTPException(status_code=400, detail="No files uploaded; send 'files' or 'file'")

	saved: list[dict[str, Any]] = []
	for file in uploads:
		filename = file.filename or "upload.xlsx"
		if not filename.lower().endswith(".xlsx"):
			filename = f"{filename}.xlsx"
		dst = folder / filename
		ctr = 1
		while dst.exists():
			stem = dst.stem
			ext = dst.suffix
			dst = folder / f"{stem}-{ctr}{ext}"
			ctr += 1
		content = await file.read()
		dst.write_bytes(content)
		if source == "bizim":
			records = read_bizim_file(str(dst))
		elif source == "kargo":
			records = read_kargo_file(str(dst))
		else:
			records = read_returns_file(str(dst))
		saved.append({
			"filename": dst.name,
			"row_count": len(records),
			"sample": records[:3],
		})

	# Invalidate all cached reads after new data is uploaded
	bump_namespace()
	return {"status": "ok", "source": source, "files": saved, "skip_duplicates": skip_duplicates}


# ---------- Returns interactive review before commit ----------

@router.get("/returns/review", response_class=HTMLResponse)
def returns_review(filename: str, request: Request):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	folder = IADE_DIR
	file_path = folder / filename
	if not file_path.exists():
		raise HTTPException(status_code=404, detail="File not found")
	records = read_returns_file(str(file_path))
	# build candidates per row
	rows: list[dict] = []
	from sqlmodel import select as _select
	with get_session() as session:
		for idx, rec in enumerate(records):
			# resolve client like in commit
			new_uq = client_unique_key(rec.get("name"), rec.get("phone"))
			old_uq = legacy_client_unique_key(rec.get("name"), rec.get("phone"))
			client = None
			if new_uq:
				client = session.exec(_select(Client).where(Client.unique_key == new_uq)).first()
			if not client and old_uq:
				client = session.exec(_select(Client).where(Client.unique_key == old_uq)).first()
			candidates: list[dict] = []
			base = str(rec.get("item_name_base") or rec.get("item_name") or "").strip().lower()
			row_date = rec.get("date")
			if client and client.id is not None:
				cand = session.exec(_select(Order).where(Order.client_id == client.id).order_by(Order.id.desc())).all()
				for o in cand[:20]:
					itname = None
					if o.item_id:
						it = session.exec(_select(Item).where(Item.id == o.item_id)).first()
						itname = it.name if it else None
					# include client details for clarity
					cname = client.name if client else None
					cphone = client.phone if client else None
					# preselect when base matches item/notes (strict)
					iname_l = (itname or "").lower() if itname else ""
					notes_l = (o.notes or "").lower() if o.notes else ""
					exact = bool(base and (base in iname_l or base in notes_l))
					candidates.append({
						"id": o.id,
						"date": str(o.shipment_date or o.data_date),
						"status": o.status,
						"total": float(o.total_amount or 0.0),
						"item_name": itname,
						"client_name": cname,
						"client_phone": cphone,
						"selected": exact,
					})
			rows.append({"row_index": idx, "record": rec, "candidates": candidates})
	templates = request.app.state.templates
	return templates.TemplateResponse(
		"returns_review.html",
		{"request": request, "filename": filename, "rows": rows},
	)


@router.get("/ambiguous", response_class=HTMLResponse)
@router.get("/importer/ambiguous", response_class=HTMLResponse)
def list_ambiguous(
	request: Request,
	search: str | None = Query(default=None),
	source: str | None = Query(default=None),
	limit: int = Query(default=200, ge=1, le=1000),
):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	with get_session() as session:
		q = (
			select(ImportRow, ImportRun)
			.join(ImportRun, ImportRun.id == ImportRow.import_run_id)
			.where(ImportRow.status == "ambiguous")
			.order_by(ImportRow.id.desc())
			.limit(limit)
		)
		if source in ("bizim", "kargo", "returns"):
			q = q.where(ImportRun.source == source)
		rows = session.exec(q).all()
		items = []
		for ir, run in rows:
			try:
				cands = json.loads(ir.candidates_json or "[]")
			except Exception:
				cands = []
			if search:
				slc = search.lower()
				if slc not in (ir.mapped_json or "").lower() and slc not in (run.filename or "").lower():
					continue
			items.append(
				{
					"import_row": ir,
					"run": run,
					"candidates": cands,
				}
			)
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"import_ambiguous.html",
			{"request": request, "rows": items, "search": search or "", "source": source or ""},
		)


@router.post("/ambiguous/resolve")
@router.post("/importer/ambiguous/resolve")
def resolve_ambiguous(
	import_row_id: int = Form(...),
	client_id: int = Form(...),
):
	with get_session() as session:
		ir = session.get(ImportRow, int(import_row_id))
		if not ir:
			raise HTTPException(status_code=404, detail="ImportRow not found")
		run = session.get(ImportRun, ir.import_run_id)
		if not run:
			raise HTTPException(status_code=404, detail="ImportRun not found")
		try:
			rec = ast.literal_eval(ir.mapped_json)
		except Exception:
			raise HTTPException(status_code=400, detail="mapped_json parse failed")
		if not isinstance(rec, dict):
			raise HTTPException(status_code=400, detail="mapped_json is not a dict")
		status = "error"
		message = None
		matched_client_id = None
		matched_order_id = None
		candidates = []
		if run.source == "kargo":
			status, message, matched_client_id, matched_order_id, candidates = process_kargo_row(
				session, run, rec, force_client_id=int(client_id)
			)
		elif run.source == "bizim":
			status, message, matched_client_id, matched_order_id, candidates = process_bizim_row(
				session, run, rec, force_client_id=int(client_id)
			)
		else:
			raise HTTPException(status_code=400, detail="Only bizim/kargo supported for resolution")
		ir.status = status
		ir.message = message
		ir.matched_client_id = matched_client_id
		ir.matched_order_id = matched_order_id
		if status != "ambiguous":
			ir.candidates_json = None
		session.add(ir)
		session.commit()
		return {
			"import_row_id": ir.id,
			"status": ir.status,
			"matched_client_id": matched_client_id,
			"matched_order_id": matched_order_id,
			"message": ir.message,
		}


@router.get("/map-debug", response_class=HTMLResponse)
@router.get("/import/map-debug", response_class=HTMLResponse)
def map_debug(request: Request, q: str | None = Query(default=None)):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	results = None
	base_name = None
	if q:
		base_name, _, _, _ = parse_item_details(q)
		with get_session() as session:
			try:
				outs, rule = resolve_mapping(session, base_name)
			except Exception as e:
				outs, rule = [], None
				results = {"error": str(e)}
			else:
				results = {
					"base_name": base_name,
					"outputs": outs,
					"rule": rule,
				}
	templates = request.app.state.templates
	return templates.TemplateResponse(
		"import_map_debug.html",
		{"request": request, "q": q or "", "base": base_name, "results": results},
	)


@router.get("/result", response_class=HTMLResponse)
def import_result(run_ids: str, request: Request):
    if not request.session.get("uid"):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        ids = [int(x) for x in (run_ids or "").split(",") if str(x).strip()]
    except Exception:
        raise HTTPException(status_code=400, detail="run_ids must be comma-separated integers")
    if not ids:
        raise HTTPException(status_code=400, detail="run_ids required")
    with get_session() as session:
        runs = session.exec(select(ImportRun).where(ImportRun.id.in_(ids)).order_by(ImportRun.id.desc())).all()
        if not runs:
            raise HTTPException(status_code=404, detail="No runs found")
        totals = {
            "row_count": 0,
            "created_orders": 0,
            "created_clients": 0,
            "created_items": 0,
            "created_payments": 0,
            "unmatched": 0,
            "enriched_orders": 0,
            "payments_existing": 0,
            "payments_skipped_zero": 0,
            "rows_created": 0,
            "rows_skipped": 0,
            "rows_unmatched": 0,
            "rows_error": 0,
            "rows_duplicates": 0,
        }
        for r in runs:
            totals["row_count"] += int(r.row_count or 0)
            totals["created_orders"] += int(r.created_orders or 0)
            totals["created_clients"] += int(r.created_clients or 0)
            totals["created_items"] += int(r.created_items or 0)
            totals["created_payments"] += int(r.created_payments or 0)
            totals["unmatched"] += int(r.unmatched_count or 0)
        # Aggregate row-level counters from ImportRow
        from sqlmodel import select as _select
        rows = session.exec(
            _select(ImportRow).where(ImportRow.import_run_id.in_(ids)).order_by(ImportRow.import_run_id, ImportRow.row_index)
        ).all()
        for ir in rows:
            st = (ir.status or "").lower()
            if st == "created":
                totals["rows_created"] += 1
            elif st == "skipped":
                totals["rows_skipped"] += 1
            elif st == "unmatched":
                totals["rows_unmatched"] += 1
            elif st == "error":
                totals["rows_error"] += 1
            elif st == "duplicate":
                totals["rows_duplicates"] += 1
        # Failures: show errors and unmatched rows (limit 200)
        failures: list[dict] = []
        for ir in rows:
            st = (ir.status or "").lower()
            if st in ("error", "unmatched"):
                if len(failures) >= 200:
                    break
                failures.append({
                    "run_id": ir.import_run_id,
                    "row_index": ir.row_index,
                    "status": ir.status,
                    "message": ir.message,
                    "matched_client_id": ir.matched_client_id,
                    "matched_order_id": ir.matched_order_id,
                    "mapped_json": ir.mapped_json,
                })
        # Detailed per-row view grouped by run for debugging (created/skipped/duplicate etc.)
        rows_by_run: dict[int, list[dict]] = {int(r.id or 0): [] for r in runs}
        max_rows_per_run = 1000
        # Preload clients for quick lookup
        client_ids = sorted({ir.matched_client_id for ir in rows if ir.matched_client_id})
        client_map: dict[int, str] = {}
        if client_ids:
            from ..models import Client as _Client
            crows = session.exec(_select(_Client).where(_Client.id.in_(client_ids))).all()
            for c in crows:
                if c.id is not None:
                    client_map[int(c.id)] = c.name or ""

        for ir in rows:
            rid = int(ir.import_run_id or 0)
            bucket = rows_by_run.setdefault(rid, [])
            if len(bucket) >= max_rows_per_run:
                continue
            mapped_json = ir.mapped_json or ""
            # Extract a few structured hints for payment reconciliation links
            try:
                rec = eval(mapped_json) if mapped_json else {}
            except Exception:
                rec = {}
            payment_amount = rec.get("payment_amount")
            pay_date = rec.get("delivery_date") or rec.get("shipment_date")
            # build optional link to payment reconciliation page
            reconcile_url = None
            if ir.matched_client_id and payment_amount and pay_date:
                try:
                    reconcile_url = f"/reconcile/payments?client_id={int(ir.matched_client_id)}&amount={float(payment_amount)}&date={str(pay_date)}"
                except Exception:
                    reconcile_url = None

            if mapped_json and len(mapped_json) > 600:
                mapped_preview = mapped_json[:580] + "…"
            else:
                mapped_preview = mapped_json
            bucket.append(
                {
                    "run_id": ir.import_run_id,
                    "row_index": ir.row_index,
                    "status": ir.status,
                    "message": ir.message,
                    "matched_client_id": ir.matched_client_id,
                    "matched_order_id": ir.matched_order_id,
                    "matched_client_name": client_map.get(int(ir.matched_client_id)) if ir.matched_client_id else None,
                    "mapped_json": mapped_preview,
                    "payment_amount": payment_amount,
                    "payment_date": str(pay_date) if pay_date is not None else None,
                    "reconcile_url": reconcile_url,
                }
            )
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "import_result.html",
            {
                "request": request,
                "run_ids": ids,
                "runs": [{
                    "id": r.id,
                    "source": r.source,
                    "filename": r.filename,
                    "row_count": r.row_count,
                    "created_clients": r.created_clients,
                    "created_orders": r.created_orders,
                    "created_items": r.created_items,
                    "created_payments": r.created_payments,
                    "unmatched": r.unmatched_count,
                    "data_date": r.data_date,
                } for r in runs],
                "totals": totals,
                "failures": failures,
                "rows_by_run": rows_by_run,
            },
        )


@router.post("/returns/apply")
def returns_apply(body: dict, request: Request):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	filename = body.get("filename")
	data_date_raw = body.get("data_date")
	selections = body.get("selections") or []
	skip_stock = bool(body.get("skip_stock", False))
	if not filename or not isinstance(selections, list):
		raise HTTPException(status_code=400, detail="filename and selections[] required")
	folder = IADE_DIR
	file_path = folder / filename
	if not file_path.exists():
		raise HTTPException(status_code=404, detail="File not found")
	records = read_returns_file(str(file_path))
	# build quick map row_index -> chosen order id
	chosen_map: dict[int, int] = {}
	for sel in selections:
		try:
			ri = int(sel.get("row_index"))
			oid = int(sel.get("order_id"))
		except Exception:
			continue
		chosen_map[ri] = oid
	with get_session() as session:
		run = ImportRun(source="returns", filename=filename)
		if data_date_raw:
			try:
				import datetime as _dt
				run.data_date = _dt.date.fromisoformat(str(data_date_raw))
			except Exception:
				raise HTTPException(status_code=400, detail="Invalid data_date; expected YYYY-MM-DD")
		session.add(run)
		session.flush()
		from sqlmodel import select as _select
		updated = 0
		unmatched = 0
		errors: list[dict] = []
		for idx, rec in enumerate(records):
			row_hash = compute_row_hash(rec)
			chosen_id = chosen_map.get(idx)
			status = "unmatched"
			message = None
			matched_client_id = None
			matched_order_id = None
			# Previously applied rows are allowed to re-run for status/date fix; restock guard below prevents double stock-in.
			existing_applied = session.exec(select(ImportRow).where(ImportRow.row_hash == row_hash, ImportRow.status == "updated")).first()
			# resolve client id for logging
			new_uq = client_unique_key(rec.get("name"), rec.get("phone"))
			old_uq = legacy_client_unique_key(rec.get("name"), rec.get("phone"))
			client = None
			if new_uq:
				client = session.exec(_select(Client).where(Client.unique_key == new_uq)).first()
			if not client and old_uq:
				client = session.exec(_select(Client).where(Client.unique_key == old_uq)).first()
			matched_client_id = client.id if client and client.id is not None else None
			if chosen_id:
				try:
					o = session.exec(_select(Order).where(Order.id == int(chosen_id))).first()
					if not o:
						errors.append({"row_index": idx, "error": "order_not_found", "order_id": chosen_id})
					else:
						# Check if already processed for this order (restock done or date set previously)
						already_in = session.exec(select(StockMovement).where(StockMovement.related_order_id == o.id, StockMovement.direction == "in")).first()
						already_processed = (already_in is not None) or (o.return_or_switch_date is not None)
						# Restock only once: when not previously processed and not explicitly skipped
						if (not skip_stock) and (not already_processed):
							oitems = session.exec(_select(OrderItem).where(OrderItem.order_id == o.id)).all()
							for oi in oitems:
								if oi.item_id is None:
									continue
								qty = int(oi.quantity or 0)
								if qty > 0:
									adjust_stock(session, item_id=int(oi.item_id), delta=qty, related_order_id=o.id)
						# Always update order fields from the spreadsheet (status/date/amount)
						action = (rec.get("action") or "").strip()
						if not action:
							try:
								amt_val = float(rec.get("amount") or 0.0)
								if amt_val < 0:
									action = "refund"
							except Exception:
								pass
						action = (rec.get("action") or "").strip()
						if not action:
							try:
								amt_val = float(rec.get("amount") or 0.0)
								if amt_val < 0:
									action = "refund"
							except Exception:
								pass
						if action == "refund":
							o.status = "refunded"
						elif action == "switch":
							o.status = "switched"
						ret_date = rec.get("date") or run.data_date
						o.return_or_switch_date = ret_date
						try:
							amt = rec.get("amount")
							if amt is not None:
								o.total_amount = round(float(amt), 2)
						except Exception as _e_amt:
							errors.append({"row_index": idx, "error": "invalid_amount", "detail": str(_e_amt)})
						status = "updated"
						matched_order_id = o.id
						updated += 1
						# Inform via message if this was previously processed
						if already_processed and (message is None):
							message = "already_processed"
				except Exception as _e:
					errors.append({"row_index": idx, "error": "apply_failed", "detail": str(_e), "order_id": chosen_id})
			else:
				unmatched += 1
			ir = ImportRow(
				import_run_id=run.id or 0,
				row_index=idx,
				row_hash=row_hash,
				mapped_json=str(rec),
				status=status,  # type: ignore
				message=message,
				matched_client_id=matched_client_id,
				matched_order_id=matched_order_id,
			)
			session.add(ir)
		# Invalidate caches
		bump_namespace()
		# Always return 200 to allow partial success; include errors for client display
		return {
			"status": "ok",
			"run_id": run.id or 0,
			"updated": updated,
			"unmatched": unmatched,
			"errors": errors,
		}