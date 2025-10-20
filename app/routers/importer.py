from pathlib import Path
from typing import Any, List, Optional
import re

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Request
from sqlmodel import select

from ..db import get_session, reset_db
from ..models import Client, Item, Order, Payment, ImportRun, ImportRow, StockMovement, Product
from ..services.importer.bizim import read_bizim_file
from ..services.importer.kargo import read_kargo_file
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
			else:
				if depth > 0:
					buf.append(ch)
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
	exclude_generic = bool(body.get("exclude_generic", True))
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
	# read and aggregate records from all files
	all_records: list[dict] = []
	for fn in file_list:
		file_path = folder / fn
		if not file_path.exists():
			raise HTTPException(status_code=404, detail=f"File not found: {fn}")
		recs = read_bizim_file(str(file_path))
		all_records.extend(recs)
	records = all_records
	from ..services.mapping import resolve_mapping
	unmatched: dict[str, dict] = {}
	unmatched_rows: list[dict] = []
	for rec in records:
		item_name_raw = rec.get("item_name") or "Genel Ürün"
		base_name, _h, _w, _notes = parse_item_details(item_name_raw)
		outs, rule = None, None
		try:
			with get_session() as session:
				outs, rule = resolve_mapping(session, base_name)
		except Exception:
			outs, rule = [], None
		if not outs:
			# optionally ignore generic placeholder patterns
			if exclude_generic and (base_name.strip().lower() in ("genel ürün", "genel urun")):
				# still count as total_unmatched but do not expose as a pattern when excluded
				if return_rows and len(unmatched_rows) < rows_limit:
					unmatched_rows.append({
						"row_index": len(unmatched_rows),
						"item_name": item_name_raw,
						"base": base_name,
						"quantity": rec.get("quantity"),
						"unit_price": rec.get("unit_price"),
						"total_amount": rec.get("total_amount"),
					})
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
			if return_rows and len(unmatched_rows) < rows_limit:
				unmatched_rows.append({
					"row_index": len(unmatched_rows),
					"item_name": item_name_raw,
					"base": base_name,
					"quantity": rec.get("quantity"),
					"unit_price": rec.get("unit_price"),
					"total_amount": rec.get("total_amount"),
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
		prod_rows = [{"id": p.id, "name": p.name} for p in products]
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"import_map.html",
			{
				"request": request,
				"source": source,
				"filename": file_list[0] if len(file_list) == 1 else None,
				"filenames": file_list,
				"unmatched_patterns": preview.get("unmatched_patterns") or [],
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
						# Resolve mapping to variant(s)
						from ..services.mapping import resolve_mapping, find_or_create_variant
						outputs, matched_rule = resolve_mapping(session, base_name)
						# Pair items with per-output quantities and price overrides
						created_items_local: list[tuple[Item, int, float | None]] = []
						if outputs:
							# Ensure product/variant exists for each output
							for out in outputs:
								it: Item | None = None
								if out.item_id:
									it = session.exec(select(Item).where(Item.id == out.item_id)).first()
								else:
									prod: Product | None = None
									if out.product_id:
										prod = session.exec(select(Product).where(Product.id == out.product_id)).first()
									if not prod and out.product_id:
										continue
									if prod is None:
										# fallback create/find product by base name
										pslug = slugify(base_name)
										prod = session.exec(select(Product).where(Product.slug == pslug)).first()
										if not prod:
											prod = Product(name=base_name, slug=pslug)
											session.add(prod)
											session.flush()
									it = find_or_create_variant(
										session,
										product=prod,  # type: ignore
										size=out.size,
										color=out.color,
										pack_type=out.pack_type,
										pair_multiplier=out.pair_multiplier or 1,
									)
							if it:
								if out.unit_price is not None:
									it.price = out.unit_price
								created_items_local.append((it, int(out.quantity or 1), out.unit_price))
						else:
							# fallback: single generic item by base name, but mark row unmatched; do NOT create stock movements
							item_name = base_name
							sku = slugify(item_name)
							item = session.exec(select(Item).where(Item.sku == sku)).first()
							if not item:
								item = Item(sku=sku, name=item_name)
								session.add(item)
								session.flush()
								run.created_items += 1
							created_items_local.append((item, 1, None))
							status = "unmatched"
							message = f"No mapping rule for '{base_name}'"

						order_notes = rec.get("notes") or None
						if extra_notes:
							joined = ", ".join(extra_notes)
							order_notes = f"{order_notes} | {joined}" if order_notes else joined

						# If a kargo placeholder exists for same client/date, upgrade it instead of creating new
						existing_order = None
						date_hint = rec.get("shipment_date") or run.data_date
						if date_hint:
							existing_order = find_order_by_client_and_date(session, client.id, date_hint)
							try:
								print("[MERGE DEBUG][bizim] date match try:", {
									"client_id": client.id,
									"name": rec.get("name"),
									"shipment_date": rec.get("shipment_date"),
									"data_date_hint": run.data_date,
									"found_order": existing_order.id if existing_order else None,
									"found_source": (existing_order.source if existing_order else None),
								})
							except Exception:
								pass
						if not existing_order:
							# fallback: upgrade most recent kargo placeholder for this client (bizim often lacks date)
							existing_order = find_recent_placeholder_kargo_for_client(session, client.id)
							try:
								print("[MERGE DEBUG][bizim] no date; fallback recent placeholder:", {
									"client_id": client.id,
									"name": rec.get("name"),
									"found_order": existing_order.id if existing_order else None,
									"found_source": (existing_order.source if existing_order else None),
								})
							except Exception:
								pass
							chosen_item_id = (created_items_local[0][0].id if created_items_local else (item.id if 'item' in locals() and item else None))  # type: ignore
							if existing_order:
								# upgrade placeholder kargo order
								if (existing_order.source or "") == "kargo":
									existing_order.item_id = chosen_item_id  # type: ignore
								existing_order.quantity = rec.get("quantity") or existing_order.quantity or 1
								existing_order.unit_price = rec.get("unit_price") or existing_order.unit_price
								existing_order.total_amount = rec.get("total_amount") or existing_order.total_amount
								existing_order.shipment_date = rec.get("shipment_date") or existing_order.shipment_date
								existing_order.data_date = existing_order.data_date or run.data_date
								existing_order.source = "bizim"
								if order_notes:
									cur = existing_order.notes or None
									existing_order.notes = f"{cur} | {order_notes}" if cur else order_notes
								# after bizim details, wait for kargo to merge
								existing_order.status = existing_order.status or "missing-kargo"
								matched_order_id = existing_order.id
								try:
									print("[MERGE DEBUG][bizim] upgraded kargo placeholder -> bizim:", {
										"order_id": existing_order.id,
										"tracking_no": existing_order.tracking_no,
										"client_id": existing_order.client_id,
										"quantity": existing_order.quantity,
										"unit_price": existing_order.unit_price,
										"total_amount": existing_order.total_amount,
									})
								except Exception:
									pass
							else:
								# create a new bizim order
								order = Order(
									tracking_no=rec.get("tracking_no"),
									client_id=client.id,  # type: ignore
									item_id=chosen_item_id,      # type: ignore
									quantity=rec.get("quantity") or 1,
									unit_price=rec.get("unit_price"),
									total_amount=rec.get("total_amount"),
									shipment_date=rec.get("shipment_date"),
									data_date=run.data_date,
									source="bizim",
									notes=order_notes,
								)
								session.add(order)
								session.flush()
								run.created_orders += 1
								try:
									print("[MERGE DEBUG][bizim] created new bizim order:", {
										"order_id": order.id,
										"client_id": order.client_id,
										"shipment_date": order.shipment_date,
										"data_date": order.data_date,
										"total_amount": order.total_amount,
									})
								except Exception:
									pass
								# Bizim order initially missing kargo
								order.status = order.status or "missing-kargo"
								matched_order_id = order.id

						# Create stock movements (out) for mapped variants (only when mapping matched)
						try:
							qty_base = int(rec.get("quantity") or 1)
							if outputs and created_items_local:
								for it, out_qty_each, _price in created_items_local:
									multiplier = int(it.pair_multiplier or 1)
									total_qty = qty_base * int(out_qty_each or 1) * multiplier
									if total_qty > 0:
										mv = StockMovement(item_id=it.id, direction="out", quantity=total_qty, related_order_id=matched_order_id)
										session.add(mv)
						except Exception:
							pass

					else:  # kargo
						# hard guard: never treat any kargo field as item; move any residual item_name into notes
						if rec.get("item_name"):
							itm = str(rec.get("item_name") or "").strip()
							if itm:
								rec["notes"] = (f"{rec.get('notes')} | {itm}" if rec.get("notes") else itm)
							rec.pop("item_name", None)
						order = find_order_by_tracking(session, rec.get("tracking_no"))
						try:
							print("[MERGE DEBUG][kargo] tracking lookup:", {
								"tracking_no": rec.get("tracking_no"),
								"found_order": order.id if order else None,
							})
						except Exception:
							pass
						if order:
							matched_order_id = order.id
							matched_client_id = order.client_id
							# enrich order if missing data
							if rec.get("total_amount") and not order.total_amount:
								order.total_amount = rec.get("total_amount")
							if rec.get("shipment_date") and not order.shipment_date:
								order.shipment_date = rec.get("shipment_date")
							# set data_date from TEFTarih if empty
							if rec.get("shipment_date") and not order.data_date:
								order.data_date = rec.get("shipment_date")
							# append AliciKodu to notes if present
							if rec.get("alici_kodu"):
								cur = order.notes or None
								ak = f"AliciKodu:{rec.get('alici_kodu')}"
								order.notes = f"{cur} | {ak}" if cur else ak
							# do not create or link items from kargo; treat descriptions as notes only
							if rec.get("notes"):
								cur = order.notes or None
								order.notes = f"{cur} | {rec.get('notes')}" if cur else rec.get("notes")
							enriched_orders_cnt += 1
							# payments idempotent per entry: merge by (order_id, date); allow multiple dates
							amt_raw = rec.get("payment_amount")
							pdate = rec.get("delivery_date") or rec.get("shipment_date") or run.data_date
							if (amt_raw or 0.0) > 0 and pdate is not None:
								existing = session.exec(select(Payment).where(Payment.order_id == order.id, Payment.date == pdate)).first()
								fee_kom = rec.get("fee_komisyon") or 0.0
								fee_hiz = rec.get("fee_hizmet") or 0.0
								fee_kar = rec.get("fee_kargo") or 0.0
								fee_iad = rec.get("fee_iade") or 0.0
								fee_eok = rec.get("fee_erken_odeme") or 0.0
								amt = float(amt_raw or 0.0)
								net = (amt or 0.0) - sum([fee_kom, fee_hiz, fee_kar, fee_iad, fee_eok])
								if not existing:
									pmt = Payment(
										client_id=order.client_id,
										order_id=order.id,
										amount=amt,
										date=pdate,
										method=rec.get("payment_method") or "kargo",
										reference=rec.get("tracking_no"),
										fee_komisyon=fee_kom,
										fee_hizmet=fee_hiz,
										fee_kargo=fee_kar,
										fee_iade=fee_iad,
										fee_erken_odeme=fee_eok,
										net_amount=net,
									)
									session.add(pmt)
									run.created_payments += 1
									payments_created_cnt += 1
								else:
									# if later file has bigger amount same date, upgrade existing
									if amt > float(existing.amount or 0.0):
										existing.amount = amt
										existing.method = rec.get("payment_method") or existing.method
										existing.reference = rec.get("tracking_no") or existing.reference
										existing.fee_komisyon = fee_kom
										existing.fee_hizmet = fee_hiz
										existing.fee_kargo = fee_kar
										existing.fee_iade = fee_iad
										existing.fee_erken_odeme = fee_eok
										existing.net_amount = net
									payments_existing_cnt += 1
							else:
								payments_skipped_zero_cnt += 1
							# flip statuses if we have a bizim order
							try:
								from ..models import Client as _Client
								if (order.source or "") == "bizim":
									order.status = "merged"
									cl = session.exec(select(_Client).where(_Client.id == order.client_id)).first()
									if cl:
										cl.status = "merged"
							except Exception:
								pass
						else:
							# no direct order; resolve client then order by date, else create placeholder
							new_uq = client_unique_key(rec.get("name"), rec.get("phone"))
							old_uq = legacy_client_unique_key(rec.get("name"), rec.get("phone"))
							client = None
							if new_uq:
								client = session.exec(select(Client).where(Client.unique_key == new_uq)).first()
							if not client and old_uq:
								client = session.exec(select(Client).where(Client.unique_key == old_uq)).first()
							if not client:
								client = Client(
									name=rec.get("name") or "",
									phone=rec.get("phone"),
									address=rec.get("address"),
									city=rec.get("city"),
									unique_key=new_uq or None,
									status="missing-bizim",
								)
								session.add(client)
								session.flush()
								run.created_clients += 1
							else:
								# backfill client fields if present
								if new_uq and client.unique_key != new_uq:
									client.unique_key = new_uq
								updated = False
								for f in ("phone","address","city"):
									val = rec.get(f)
									if val and not getattr(client, f):
										setattr(client, f, val)
										updated = True
							# try find existing bizim order by client/date
							order = find_order_by_client_and_date(session, client.id, rec.get("shipment_date"))
							try:
								print("[MERGE DEBUG][kargo] client/date lookup:", {
									"client_id": client.id,
									"name": rec.get("name"),
									"shipment_date": rec.get("shipment_date"),
									"found_order": order.id if order else None,
									"found_source": (order.source if order else None),
								})
							except Exception:
								pass
							if order:
								# enrich existing order (usually bizim)
								matched_order_id = order.id
								matched_client_id = client.id
								if rec.get("total_amount") and not order.total_amount:
									order.total_amount = rec.get("total_amount")
								if rec.get("shipment_date") and not order.shipment_date:
									order.shipment_date = rec.get("shipment_date")
								if rec.get("shipment_date") and not order.data_date:
									order.data_date = rec.get("shipment_date")
								if rec.get("alici_kodu"):
									cur = order.notes or None
									ak = f"AliciKodu:{rec.get('alici_kodu')}"
									order.notes = f"{cur} | {ak}" if cur else ak
								if rec.get("notes"):
									cur = order.notes or None
									order.notes = f"{cur} | {rec.get('notes')}" if cur else rec.get("notes")
								# flip statuses if we have a bizim order
								try:
									from ..models import Client as _Client
									if (order.source or "") == "bizim":
										order.status = "merged"
										cl = session.exec(select(_Client).where(_Client.id == order.client_id)).first()
										if cl:
											cl.status = "merged"
								except Exception:
									pass
							else:
								# create placeholder kargo order
								order_notes = rec.get("notes") or None
								if rec.get("alici_kodu"):
									order_notes = f"{order_notes} | AliciKodu:{rec.get('alici_kodu')}" if order_notes else f"AliciKodu:{rec.get('alici_kodu')}"
								order = Order(
									tracking_no=rec.get("tracking_no"),
									client_id=client.id,  # type: ignore
									item_id=None,
									quantity=rec.get("quantity") or 1,
									unit_price=rec.get("unit_price"),
									total_amount=rec.get("total_amount"),
									shipment_date=rec.get("shipment_date"),
									data_date=rec.get("shipment_date") or run.data_date,
									source="kargo",
									notes=order_notes,
									status="placeholder",
								)
								session.add(order)
								session.flush()
								run.created_orders += 1
								matched_order_id = order.id
								matched_client_id = client.id

						# payment for matched/created order (allow multiple dates; merge by date)
						amt_raw = rec.get("payment_amount")
						pdate = rec.get("delivery_date") or rec.get("shipment_date") or run.data_date
						if (amt_raw or 0.0) > 0 and pdate is not None:
							existing = session.exec(select(Payment).where(Payment.order_id == order.id, Payment.date == pdate)).first()
							fee_kom = rec.get("fee_komisyon") or 0.0
							fee_hiz = rec.get("fee_hizmet") or 0.0
							fee_kar = rec.get("fee_kargo") or 0.0
							fee_iad = rec.get("fee_iade") or 0.0
							fee_eok = rec.get("fee_erken_odeme") or 0.0
							amt = float(amt_raw or 0.0)
							net = (amt or 0.0) - sum([fee_kom, fee_hiz, fee_kar, fee_iad, fee_eok])
							if not existing:
								pmt = Payment(
									client_id=order.client_id,
									order_id=order.id,
									amount=amt,
									date=pdate,
									method=rec.get("payment_method") or "kargo",
									reference=rec.get("tracking_no"),
									fee_komisyon=fee_kom,
									fee_hizmet=fee_hiz,
									fee_kargo=fee_kar,
									fee_iade=fee_iad,
									fee_erken_odeme=fee_eok,
									net_amount=net,
								)
								session.add(pmt)
								run.created_payments += 1
								payments_created_cnt += 1
							else:
								if amt > float(existing.amount or 0.0):
									existing.amount = amt
									existing.method = rec.get("payment_method") or existing.method
									existing.reference = rec.get("tracking_no") or existing.reference
									existing.fee_komisyon = fee_kom
									existing.fee_hizmet = fee_hiz
									existing.fee_kargo = fee_kar
									existing.fee_iade = fee_iad
									existing.fee_erken_odeme = fee_eok
									existing.net_amount = net
								payments_existing_cnt += 1
						else:
							payments_skipped_zero_cnt += 1
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
					# Resolve mapping to variant(s)
					from ..services.mapping import resolve_mapping, find_or_create_variant
					outputs, matched_rule = resolve_mapping(session, base_name)
					# Pair items with per-output quantities and price overrides
					created_items_local: list[tuple[Item, int, float | None]] = []
					if outputs:
						# Ensure product/variant exists for each output
						for out in outputs:
							it: Item | None = None
							if out.item_id:
								it = session.exec(select(Item).where(Item.id == out.item_id)).first()
							else:
								prod: Product | None = None
								if out.product_id:
									prod = session.exec(select(Product).where(Product.id == out.product_id)).first()
								if not prod and out.product_id:
									continue
								if prod is None:
									# fallback create/find product by base name
									pslug = slugify(base_name)
									prod = session.exec(select(Product).where(Product.slug == pslug)).first()
									if not prod:
										prod = Product(name=base_name, slug=pslug)
										session.add(prod)
										session.flush()
								it = find_or_create_variant(
									session,
									product=prod,  # type: ignore
									size=out.size,
									color=out.color,
									pack_type=out.pack_type,
									pair_multiplier=out.pair_multiplier or 1,
								)
							if it:
								if out.unit_price is not None:
									it.price = out.unit_price
								created_items_local.append((it, int(out.quantity or 1), out.unit_price))
					else:
						# fallback: single generic item by base name, but mark row unmatched; do NOT create stock movements
						item_name = base_name
						sku = slugify(item_name)
						item = session.exec(select(Item).where(Item.sku == sku)).first()
						if not item:
							item = Item(sku=sku, name=item_name)
							session.add(item)
							session.flush()
							run.created_items += 1
						created_items_local.append((item, 1, None))
						status = "unmatched"
						message = f"No mapping rule for '{base_name}'"

					order_notes = rec.get("notes") or None
					if extra_notes:
						joined = ", ".join(extra_notes)
						order_notes = f"{order_notes} | {joined}" if order_notes else joined

					# If a kargo placeholder exists for same client/date, upgrade it instead of creating new
					existing_order = None
					date_hint = rec.get("shipment_date") or run.data_date
					if date_hint:
						existing_order = find_order_by_client_and_date(session, client.id, date_hint)
						try:
							print("[MERGE DEBUG][bizim] date match try:", {
								"client_id": client.id,
								"name": rec.get("name"),
								"shipment_date": rec.get("shipment_date"),
								"data_date_hint": run.data_date,
								"found_order": existing_order.id if existing_order else None,
								"found_source": (existing_order.source if existing_order else None),
							})
						except Exception:
							pass
					if not existing_order:
						# fallback: upgrade most recent kargo placeholder for this client (bizim often lacks date)
						existing_order = find_recent_placeholder_kargo_for_client(session, client.id)
						try:
							print("[MERGE DEBUG][bizim] no date; fallback recent placeholder:", {
								"client_id": client.id,
								"name": rec.get("name"),
								"found_order": existing_order.id if existing_order else None,
								"found_source": (existing_order.source if existing_order else None),
							})
						except Exception:
							pass
						chosen_item_id = (created_items_local[0][0].id if created_items_local else (item.id if 'item' in locals() and item else None))  # type: ignore
						if existing_order:
							# upgrade placeholder kargo order
							if (existing_order.source or "") == "kargo":
								existing_order.item_id = chosen_item_id  # type: ignore
							existing_order.quantity = rec.get("quantity") or existing_order.quantity or 1
							existing_order.unit_price = rec.get("unit_price") or existing_order.unit_price
							existing_order.total_amount = rec.get("total_amount") or existing_order.total_amount
							existing_order.shipment_date = rec.get("shipment_date") or existing_order.shipment_date
							existing_order.data_date = existing_order.data_date or run.data_date
							existing_order.source = "bizim"
							if order_notes:
								cur = existing_order.notes or None
								existing_order.notes = f"{cur} | {order_notes}" if cur else order_notes
							# after bizim details, wait for kargo to merge
							existing_order.status = existing_order.status or "missing-kargo"
							matched_order_id = existing_order.id
							try:
								print("[MERGE DEBUG][bizim] upgraded kargo placeholder -> bizim:", {
									"order_id": existing_order.id,
									"tracking_no": existing_order.tracking_no,
									"client_id": existing_order.client_id,
									"quantity": existing_order.quantity,
									"unit_price": existing_order.unit_price,
									"total_amount": existing_order.total_amount,
								})
							except Exception:
								pass
						else:
							# create a new bizim order
							order = Order(
								tracking_no=rec.get("tracking_no"),
								client_id=client.id,  # type: ignore
								item_id=chosen_item_id,      # type: ignore
								quantity=rec.get("quantity") or 1,
								unit_price=rec.get("unit_price"),
								total_amount=rec.get("total_amount"),
								shipment_date=rec.get("shipment_date"),
								data_date=run.data_date,
								source="bizim",
								notes=order_notes,
							)
							session.add(order)
							session.flush()
							run.created_orders += 1
							try:
								print("[MERGE DEBUG][bizim] created new bizim order:", {
									"order_id": order.id,
									"client_id": order.client_id,
									"shipment_date": order.shipment_date,
									"data_date": order.data_date,
									"total_amount": order.total_amount,
								})
							except Exception:
								pass
							# Bizim order initially missing kargo
							order.status = order.status or "missing-kargo"
							matched_order_id = order.id

						# Create stock movements (out) for mapped variants (only when mapping matched)
						try:
							qty_base = int(rec.get("quantity") or 1)
							if outputs and created_items_local:
								for it, out_qty_each, _price in created_items_local:
									multiplier = int(it.pair_multiplier or 1)
									total_qty = qty_base * int(out_qty_each or 1) * multiplier
									if total_qty > 0:
										mv = StockMovement(item_id=it.id, direction="out", quantity=total_qty, related_order_id=matched_order_id)
										session.add(mv)
						except Exception:
							pass

				else:  # kargo
					# hard guard: never treat any kargo field as item; move any residual item_name into notes
					if rec.get("item_name"):
						itm = str(rec.get("item_name") or "").strip()
						if itm:
							rec["notes"] = (f"{rec.get('notes')} | {itm}" if rec.get("notes") else itm)
						rec.pop("item_name", None)
					order = find_order_by_tracking(session, rec.get("tracking_no"))
					try:
						print("[MERGE DEBUG][kargo] tracking lookup:", {
							"tracking_no": rec.get("tracking_no"),
							"found_order": order.id if order else None,
						})
					except Exception:
						pass
					if order:
						matched_order_id = order.id
						matched_client_id = order.client_id
						# enrich order if missing data
						if rec.get("total_amount") and not order.total_amount:
							order.total_amount = rec.get("total_amount")
						if rec.get("shipment_date") and not order.shipment_date:
							order.shipment_date = rec.get("shipment_date")
						# set data_date from TEFTarih if empty
						if rec.get("shipment_date") and not order.data_date:
							order.data_date = rec.get("shipment_date")
						# append AliciKodu to notes if present
						if rec.get("alici_kodu"):
							cur = order.notes or None
							ak = f"AliciKodu:{rec.get('alici_kodu')}"
							order.notes = f"{cur} | {ak}" if cur else ak
						# do not create or link items from kargo; treat descriptions as notes only
						if rec.get("notes"):
							cur = order.notes or None
							order.notes = f"{cur} | {rec.get('notes')}" if cur else rec.get("notes")
						enriched_orders_cnt += 1
						# payments idempotent per entry: merge by (order_id, date); allow multiple dates
						amt_raw = rec.get("payment_amount")
						pdate = rec.get("delivery_date") or rec.get("shipment_date") or run.data_date
						if (amt_raw or 0.0) > 0 and pdate is not None:
							existing = session.exec(select(Payment).where(Payment.order_id == order.id, Payment.date == pdate)).first()
							fee_kom = rec.get("fee_komisyon") or 0.0
							fee_hiz = rec.get("fee_hizmet") or 0.0
							fee_kar = rec.get("fee_kargo") or 0.0
							fee_iad = rec.get("fee_iade") or 0.0
							fee_eok = rec.get("fee_erken_odeme") or 0.0
							amt = float(amt_raw or 0.0)
							net = (amt or 0.0) - sum([fee_kom, fee_hiz, fee_kar, fee_iad, fee_eok])
							if not existing:
								pmt = Payment(
									client_id=order.client_id,
									order_id=order.id,
									amount=amt,
									date=pdate,
									method=rec.get("payment_method") or "kargo",
									reference=rec.get("tracking_no"),
									fee_komisyon=fee_kom,
									fee_hizmet=fee_hiz,
									fee_kargo=fee_kar,
									fee_iade=fee_iad,
									fee_erken_odeme=fee_eok,
									net_amount=net,
								)
								session.add(pmt)
								run.created_payments += 1
								payments_created_cnt += 1
							else:
								# if later file has bigger amount same date, upgrade existing
								if amt > float(existing.amount or 0.0):
									existing.amount = amt
									existing.method = rec.get("payment_method") or existing.method
									existing.reference = rec.get("tracking_no") or existing.reference
									existing.fee_komisyon = fee_kom
									existing.fee_hizmet = fee_hiz
									existing.fee_kargo = fee_kar
									existing.fee_iade = fee_iad
									existing.fee_erken_odeme = fee_eok
									existing.net_amount = net
								payments_existing_cnt += 1
						else:
							payments_skipped_zero_cnt += 1
						# flip statuses if we have a bizim order
						try:
							from ..models import Client as _Client
							if (order.source or "") == "bizim":
								order.status = "merged"
								cl = session.exec(select(_Client).where(_Client.id == order.client_id)).first()
								if cl:
									cl.status = "merged"
						except Exception:
							pass
					else:
						# no direct order; resolve client then order by date, else create placeholder
						new_uq = client_unique_key(rec.get("name"), rec.get("phone"))
						old_uq = legacy_client_unique_key(rec.get("name"), rec.get("phone"))
						client = None
						if new_uq:
							client = session.exec(select(Client).where(Client.unique_key == new_uq)).first()
						if not client and old_uq:
							client = session.exec(select(Client).where(Client.unique_key == old_uq)).first()
						if not client:
							client = Client(
								name=rec.get("name") or "",
								phone=rec.get("phone"),
								address=rec.get("address"),
								city=rec.get("city"),
								unique_key=new_uq or None,
								status="missing-bizim",
							)
							session.add(client)
							session.flush()
							run.created_clients += 1
						else:
							# backfill client fields if present
							if new_uq and client.unique_key != new_uq:
								client.unique_key = new_uq
							updated = False
							for f in ("phone","address","city"):
								val = rec.get(f)
								if val and not getattr(client, f):
									setattr(client, f, val)
									updated = True
						# try find existing bizim order by client/date
						order = find_order_by_client_and_date(session, client.id, rec.get("shipment_date"))
						try:
							print("[MERGE DEBUG][kargo] client/date lookup:", {
								"client_id": client.id,
								"name": rec.get("name"),
								"shipment_date": rec.get("shipment_date"),
								"found_order": order.id if order else None,
								"found_source": (order.source if order else None),
							})
						except Exception:
							pass
						if order:
							# enrich existing order (usually bizim)
							matched_order_id = order.id
							matched_client_id = client.id
							if rec.get("total_amount") and not order.total_amount:
								order.total_amount = rec.get("total_amount")
							if rec.get("shipment_date") and not order.shipment_date:
								order.shipment_date = rec.get("shipment_date")
							if rec.get("shipment_date") and not order.data_date:
								order.data_date = rec.get("shipment_date")
							if rec.get("alici_kodu"):
								cur = order.notes or None
								ak = f"AliciKodu:{rec.get('alici_kodu')}"
								order.notes = f"{cur} | {ak}" if cur else ak
							if rec.get("notes"):
								cur = order.notes or None
								order.notes = f"{cur} | {rec.get('notes')}" if cur else rec.get("notes")
							# flip statuses if we have a bizim order
							try:
								from ..models import Client as _Client
								if (order.source or "") == "bizim":
									order.status = "merged"
									cl = session.exec(select(_Client).where(_Client.id == order.client_id)).first()
									if cl:
										cl.status = "merged"
							except Exception:
								pass
						else:
							# create placeholder kargo order
							order_notes = rec.get("notes") or None
							if rec.get("alici_kodu"):
								order_notes = f"{order_notes} | AliciKodu:{rec.get('alici_kodu')}" if order_notes else f"AliciKodu:{rec.get('alici_kodu')}"
							order = Order(
								tracking_no=rec.get("tracking_no"),
								client_id=client.id,  # type: ignore
								item_id=None,
								quantity=rec.get("quantity") or 1,
								unit_price=rec.get("unit_price"),
								total_amount=rec.get("total_amount"),
								shipment_date=rec.get("shipment_date"),
								data_date=rec.get("shipment_date") or run.data_date,
								source="kargo",
								notes=order_notes,
								status="placeholder",
							)
							session.add(order)
							session.flush()
							run.created_orders += 1
							matched_order_id = order.id
							matched_client_id = client.id

						# payment for matched/created order (allow multiple dates; merge by date)
						amt_raw = rec.get("payment_amount")
						pdate = rec.get("delivery_date") or rec.get("shipment_date") or run.data_date
						if (amt_raw or 0.0) > 0 and pdate is not None:
							existing = session.exec(select(Payment).where(Payment.order_id == order.id, Payment.date == pdate)).first()
							fee_kom = rec.get("fee_komisyon") or 0.0
							fee_hiz = rec.get("fee_hizmet") or 0.0
							fee_kar = rec.get("fee_kargo") or 0.0
							fee_iad = rec.get("fee_iade") or 0.0
							fee_eok = rec.get("fee_erken_odeme") or 0.0
							amt = float(amt_raw or 0.0)
							net = (amt or 0.0) - sum([fee_kom, fee_hiz, fee_kar, fee_iad, fee_eok])
							if not existing:
								pmt = Payment(
									client_id=order.client_id,
									order_id=order.id,
									amount=amt,
									date=pdate,
									method=rec.get("payment_method") or "kargo",
									reference=rec.get("tracking_no"),
									fee_komisyon=fee_kom,
									fee_hizmet=fee_hiz,
									fee_kargo=fee_kar,
									fee_iade=fee_iad,
									fee_erken_odeme=fee_eok,
									net_amount=net,
								)
								session.add(pmt)
								run.created_payments += 1
								payments_created_cnt += 1
							else:
								if amt > float(existing.amount or 0.0):
									existing.amount = amt
									existing.method = rec.get("payment_method") or existing.method
									existing.reference = rec.get("tracking_no") or existing.reference
									existing.fee_komisyon = fee_kom
									existing.fee_hizmet = fee_hiz
									existing.fee_kargo = fee_kar
									existing.fee_iade = fee_iad
									existing.fee_erken_odeme = fee_eok
									existing.net_amount = net
								payments_existing_cnt += 1
						else:
							payments_skipped_zero_cnt += 1
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

		run.row_count = len(records)
		# snapshot response before leaving session context to avoid DetachedInstanceError
		summary = {
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
		# Log summary for debugging/observability of imports
		try:
			print("[IMPORT COMMIT] summary:", summary)
		except Exception:
			pass
		return summary


@router.post("/reset")
def reset_database(request: Request):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	reset_db()
	return {"status": "ok"}


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