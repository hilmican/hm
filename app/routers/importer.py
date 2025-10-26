from pathlib import Path
from typing import Any, List, Optional
import re

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Request
from sqlmodel import select

from ..db import get_session, reset_db
from ..models import Client, Item, Order, Payment, ImportRun, ImportRow, StockMovement, Product
from ..services.importer import read_bizim_file, read_kargo_file
from ..services.importer.committers import process_kargo_row, process_bizim_row
from ..schemas import BizimRow, KargoRow, BIZIM_ALLOWED_KEYS, KARGO_ALLOWED_KEYS
from ..services.matching import find_order_by_tracking, find_client_candidates
from ..services.matching import find_order_by_client_and_date, find_recent_placeholder_kargo_for_client
from ..utils.hashing import compute_row_hash
from ..utils.normalize import client_unique_key, legacy_client_unique_key, normalize_phone, normalize_text
from ..utils.slugify import slugify

router = APIRouter(prefix="")

# Project root is two levels up from this file: app/routers/importer.py -> app/ -> project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BIZIM_DIR = PROJECT_ROOT / "bizimexcellerimiz"
KARGO_DIR = PROJECT_ROOT / "kargocununexcelleri"
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
	if source not in ("bizim", "kargo"):
		raise HTTPException(status_code=400, detail="source must be 'bizim' or 'kargo'")
	folder = BIZIM_DIR if source == "bizim" else KARGO_DIR
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

	records = read_bizim_file(str(file_path)) if source == "bizim" else read_kargo_file(str(file_path))
	# enforce per-source whitelist and annotate record_type
	filtered: list[dict] = []
	allowed = BIZIM_ALLOWED_KEYS if source == "bizim" else KARGO_ALLOWED_KEYS
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
	data_date_raw = body.get("data_date")  # ISO YYYY-MM-DD string, may apply to all
	data_dates_map = body.get("data_dates") or {}  # optional per-filename map
	if source not in ("bizim", "kargo"):
		raise HTTPException(status_code=400, detail="source ('bizim'|'kargo') is required")

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
	def _commit_single(fn: str, dd_raw: str | None) -> dict:
		folder_loc = BIZIM_DIR if source == "bizim" else KARGO_DIR
		file_path_loc = folder_loc / fn
		if not file_path_loc.exists():
			raise HTTPException(status_code=404, detail=f"File not found: {fn}")
		records_loc = read_bizim_file(str(file_path_loc)) if source == "bizim" else read_kargo_file(str(file_path_loc))
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
			else:  # kargo -> derive from records' shipment_date
				try:
					import datetime as _dt
					dates = [r.get("shipment_date") for r in records_loc if r.get("shipment_date")]
					run.data_date = max(dates) if dates else None
				except Exception:
					pass
			session.add(run)
			session.flush()

			# local (non-persisted) counters for detailed summary
			enriched_orders_cnt = 0
			payments_created_cnt = 0
			payments_existing_cnt = 0
			payments_skipped_zero_cnt = 0

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
					continue
				row_hash = compute_row_hash(rec)
				# idempotency: skip duplicate rows by row_hash
				existing_ir = session.exec(select(ImportRow).where(ImportRow.row_hash == row_hash)).first()
				if existing_ir:
					ir = ImportRow(
						import_run_id=run.id or 0,
						row_index=idx,
						row_hash=row_hash,
						mapped_json=str(rec),
						status="skipped",  # type: ignore
						message="duplicate row",
						matched_client_id=existing_ir.matched_client_id,
						matched_order_id=existing_ir.matched_order_id,
					)
					session.add(ir)
					run.unmatched_count += 0
					continue
				status = "created"
				message = None
				matched_client_id = None
				matched_order_id = None

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
						new_uq = client_unique_key(rec.get("name"), rec.get("phone"))
						old_uq = legacy_client_unique_key(rec.get("name"), rec.get("phone"))
						client = None
						if new_uq:
							client = session.exec(select(Client).where(Client.unique_key == new_uq)).first()
						if not client and old_uq:
							client = session.exec(select(Client).where(Client.unique_key == old_uq)).first()
						if not client:
							client = Client(
								name=rec_name or "",
								phone=rec.get("phone"),
								address=rec.get("address"),
								city=rec.get("city"),
								unique_key=new_uq or None,
							)
							session.add(client)
							session.flush()
							run.created_clients += 1
							# Bizim created client initially missing kargo
							client.status = client.status or "missing-kargo"
						else:
							# migrate legacy key to new format if needed
							if new_uq and client.unique_key != new_uq:
								client.unique_key = new_uq
							updated = False
							for f in ("phone", "address", "city"):
								val = rec.get(f)
								if val:
									setattr(client, f, val)
									updated = True
							if updated:
								run.updated_clients += 1
						matched_client_id = client.id

						item_name_raw = rec.get("item_name") or "Genel Ürün"
						base_name, height_cm, weight_kg, extra_notes = parse_item_details(item_name_raw)
						# update client with parsed metrics if present
						if height_cm is not None:
							client.height_cm = client.height_cm or height_cm
						if weight_kg is not None:
							client.weight_kg = client.weight_kg or weight_kg
						# pass base name explicitly for mapping; keep original for notes
						rec["item_name_base"] = base_name
						# Bizim branch: delegate mapping/stock to committer
						status, message, matched_client_id, matched_order_id = process_bizim_row(session, run, rec)

					# Kargo branch extracted into service function
					elif source == "kargo":
						status, message, matched_client_id, matched_order_id = process_kargo_row(session, run, rec)
				except Exception as e:
					status = "error"
					message = str(e)

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

			run.row_count = len(records_loc)
			# snapshot response before leaving session context to avoid DetachedInstanceError
			summary_loc = {
				"run_id": run.id or 0,
				"created_orders": run.created_orders,
				"created_clients": run.created_clients,
				"created_items": run.created_items,
				"created_payments": run.created_payments,
				"unmatched": run.unmatched_count,
				"enriched_orders": enriched_orders_cnt,
				"payments_existing": payments_existing_cnt,
				"payments_skipped_zero": payments_skipped_zero_cnt,
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
			"created_orders": 0,
			"created_clients": 0,
			"created_items": 0,
			"created_payments": 0,
			"unmatched": 0,
			"enriched_orders": 0,
			"payments_existing": 0,
			"payments_skipped_zero": 0,
		}
		for fn in file_list:
			dd = (data_dates_map.get(fn) if isinstance(data_dates_map, dict) else None) or data_date_raw
			res = _commit_single(fn, dd)
			agg["runs"].append({"filename": fn, **res})
			for k in ("created_orders","created_clients","created_items","created_payments","unmatched","enriched_orders","payments_existing","payments_skipped_zero"):
				agg[k] += (res.get(k) or 0)
		return agg

	# single-file fallback (original behavior)
	if not filename:
		raise HTTPException(status_code=400, detail="filename is required for single commit")
	return _commit_single(filename, data_date_raw)


@router.post("/reset")
def reset_database(request: Request, preserve_mappings: bool = False):
    if not request.session.get("uid"):
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not preserve_mappings:
        reset_db()
        return {"status": "ok"}
    # preserve mappings: backup products + mapping rules/outputs, reset, then restore
    from ..models import ItemMappingRule as _Rule, ItemMappingOutput as _Out, Product as _Prod
    from sqlmodel import select as _select
    rules_dump: list[dict] = []
    products_dump: list[dict] = []
    with get_session() as session:
        # backup all products (keep ids to satisfy mapping outputs product_id)
        prods = session.exec(_select(_Prod)).all()
        for p in prods:
            products_dump.append({
                "id": p.id,
                "name": p.name,
                "slug": p.slug,
                "default_unit": p.default_unit,
                "default_price": p.default_price,
                "created_at": p.created_at,
                "updated_at": p.updated_at,
            })
        rules = session.exec(_select(_Rule)).all()
        for r in rules:
            outs = session.exec(_select(_Out).where(_Out.rule_id == r.id)).all()
            rules_dump.append({
                "rule": {
                    "source_pattern": r.source_pattern,
                    "match_mode": r.match_mode,
                    "priority": r.priority,
                    "notes": r.notes,
                    "is_active": r.is_active,
                },
                "outs": [
                    {
                        "item_id": o.item_id,
                        "product_id": o.product_id,
                        "size": o.size,
                        "color": o.color,
                        "quantity": o.quantity,
                        "unit_price": o.unit_price,
                    }
                    for o in outs
                ],
            })
    # hard reset
    reset_db()
    # restore products, then mappings
    from ..models import ItemMappingRule as _R2, ItemMappingOutput as _O2, Product as _P2
    with get_session() as session:
        # restore products first
        for data in products_dump:
            try:
                session.add(_P2(**data))
            except Exception:
                # fallback by slug if explicit id insertion fails
                slug = data.get("slug")
                existing = session.exec(_select(_P2).where(_P2.slug == slug)).first()
                if not existing:
                    nd = dict(data)
                    nd.pop("id", None)
                    session.add(_P2(**nd))
        for entry in rules_dump:
            rdata = entry.get("rule") or {}
            r = _R2(
                source_pattern=rdata.get("source_pattern"),
                match_mode=rdata.get("match_mode") or "exact",
                priority=int(rdata.get("priority") or 0),
                notes=rdata.get("notes"),
                is_active=bool(rdata.get("is_active")),
            )
            session.add(r)
            session.flush()
            for o in (entry.get("outs") or []):
                session.add(_O2(
                    rule_id=r.id or 0,
                    item_id=o.get("item_id"),
                    product_id=o.get("product_id"),
                    size=o.get("size"),
                    color=o.get("color"),
                    quantity=o.get("quantity") or 1,
                    unit_price=o.get("unit_price"),
                ))
    return {"status": "ok", "restored_rules": len(rules_dump)}


@router.post("/upload")
async def upload_excel(
	source: str = Form(..., description="'bizim' or 'kargo'"),
	files: Optional[List[UploadFile]] = File(None),
	file: Optional[UploadFile] = File(None),
	request: Request = None,
):
	# Starlette injects Request when declared as a parameter
	if not request or not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	if source not in ("bizim", "kargo"):
		raise HTTPException(status_code=400, detail="source must be 'bizim' or 'kargo'")
	folder = BIZIM_DIR if source == "bizim" else KARGO_DIR
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
		records = read_bizim_file(str(dst)) if source == "bizim" else read_kargo_file(str(dst))
		saved.append({
			"filename": dst.name,
			"row_count": len(records),
			"sample": records[:3],
		})

	return {"status": "ok", "source": source, "files": saved}