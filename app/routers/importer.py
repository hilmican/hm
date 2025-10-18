from pathlib import Path
from typing import Any, List, Optional
import re

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Request
from sqlmodel import select

from ..db import get_session, reset_db
from ..models import Client, Item, Order, Payment, ImportRun, ImportRow
from ..services.importer.bizim import read_bizim_file
from ..services.importer.kargo import read_kargo_file
from ..schemas import BizimRow, KargoRow, BIZIM_ALLOWED_KEYS, KARGO_ALLOWED_KEYS
from ..services.matching import find_order_by_tracking, find_client_candidates
from ..services.matching import find_order_by_client_and_date
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


@router.post("/commit")
def commit_import(body: dict, request: Request):
	if not request.session.get("uid"):
		raise HTTPException(status_code=401, detail="Unauthorized")
	source = body.get("source")
	filename = body.get("filename")
	data_date_raw = body.get("data_date")  # ISO YYYY-MM-DD string
	if source not in ("bizim", "kargo") or not filename:
		raise HTTPException(status_code=400, detail="source ('bizim'|'kargo') and filename are required")

	folder = BIZIM_DIR if source == "bizim" else KARGO_DIR
	file_path = folder / filename
	if not file_path.exists():
		raise HTTPException(status_code=404, detail="File not found")

	records = read_bizim_file(str(file_path)) if source == "bizim" else read_kargo_file(str(file_path))

	with get_session() as session:
		run = ImportRun(source=source, filename=filename)
		# set data_date
		if source == "bizim":
			if data_date_raw:
				try:
					# Import here to avoid adding a new top-level import
					import datetime as _dt
					run.data_date = _dt.date.fromisoformat(data_date_raw)
				except Exception:
					raise HTTPException(status_code=400, detail="Invalid data_date; expected YYYY-MM-DD")
		else:  # kargo -> derive from records' shipment_date
			try:
				import datetime as _dt
				dates = [r.get("shipment_date") for r in records if r.get("shipment_date")]
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

		for idx, rec in enumerate(records):
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
					item_name = base_name
					sku = slugify(item_name)
					item = session.exec(select(Item).where(Item.sku == sku)).first()
					if not item:
						item = Item(sku=sku, name=item_name)
						session.add(item)
						session.flush()
						run.created_items += 1

					order_notes = rec.get("notes") or None
					if extra_notes:
						joined = ", ".join(extra_notes)
						order_notes = f"{order_notes} | {joined}" if order_notes else joined
					order = Order(
						tracking_no=rec.get("tracking_no"),
						client_id=client.id,  # type: ignore
						item_id=item.id,      # type: ignore
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
					# Bizim order initially missing kargo
					order.status = order.status or "missing-kargo"
					matched_order_id = order.id

				else:  # kargo
					# hard guard: never treat any kargo field as item; move any residual item_name into notes
					if rec.get("item_name"):
						itm = str(rec.get("item_name") or "").strip()
						if itm:
							rec["notes"] = (f"{rec.get('notes')} | {itm}" if rec.get("notes") else itm)
						rec.pop("item_name", None)
					order = find_order_by_tracking(session, rec.get("tracking_no"))
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
						# payments idempotent
						if rec.get("payment_amount"):
							pdate = rec.get("delivery_date") or rec.get("shipment_date") or run.data_date
							existing = session.exec(select(Payment).where(
								Payment.order_id == order.id,
								Payment.amount == (rec.get("payment_amount") or 0.0),
								Payment.date == pdate,
							)).first()
							if not existing and (rec.get("payment_amount") or 0.0) > 0 and pdate is not None:
								# compute net and fees
								amt = rec.get("payment_amount") or 0.0
								fee_kom = rec.get("fee_komisyon") or 0.0
								fee_hiz = rec.get("fee_hizmet") or 0.0
								fee_kar = rec.get("fee_kargo") or 0.0
								fee_iad = rec.get("fee_iade") or 0.0
								fee_eok = rec.get("fee_erken_odeme") or 0.0
								net = (amt or 0.0) - sum([fee_kom, fee_hiz, fee_kar, fee_iad, fee_eok])
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
							elif existing:
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
						if not order:
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
						# payment for matched/created order
						if rec.get("payment_amount"):
							pdate = rec.get("delivery_date") or rec.get("shipment_date") or run.data_date
							existing = session.exec(select(Payment).where(
								Payment.order_id == order.id,
								Payment.amount == (rec.get("payment_amount") or 0.0),
								Payment.date == pdate,
							)).first()
							if not existing and (rec.get("payment_amount") or 0.0) > 0 and pdate is not None:
								amt = rec.get("payment_amount") or 0.0
								fee_kom = rec.get("fee_komisyon") or 0.0
								fee_hiz = rec.get("fee_hizmet") or 0.0
								fee_kar = rec.get("fee_kargo") or 0.0
								fee_iad = rec.get("fee_iade") or 0.0
								fee_eok = rec.get("fee_erken_odeme") or 0.0
								net = (amt or 0.0) - sum([fee_kom, fee_hiz, fee_kar, fee_iad, fee_eok])
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
							elif existing:
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