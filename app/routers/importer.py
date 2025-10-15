from pathlib import Path
from typing import Any
import re

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from sqlmodel import select

from ..db import get_session, reset_db
from ..models import Client, Item, Order, Payment, ImportRun, ImportRow
from ..services.importer.bizim import read_bizim_file
from ..services.importer.kargo import read_kargo_file
from ..services.matching import find_order_by_tracking, find_client_candidates
from ..utils.hashing import compute_row_hash
from ..utils.normalize import client_unique_key, normalize_phone, normalize_text
from ..utils.slugify import slugify

router = APIRouter(prefix="")

# Project root is two levels up from this file: app/routers/importer.py -> app/ -> project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BIZIM_DIR = PROJECT_ROOT / "bizimexcellerimiz"
KARGO_DIR = PROJECT_ROOT / "kargocununexcelleri"
def parse_item_details(text: str | None) -> tuple[str, int | None, int | None, list[str]]:
	"""Extract base item name, height(cm), weight(kg), and extra notes from parentheses.

	Patterns handled:
	- "NAME (180,75) (NOTE)" -> ("NAME", 180, 75, ["NOTE"]) 
	- If first parentheses isn't two numbers, treat all parentheses as notes.
	"""
	if not text:
		return "Genel Ürün", None, None, []
	# collect parentheses content
	parts = re.findall(r"\(([^()]*)\)", text)
	# base name is text with parentheses removed
	base = re.sub(r"\([^()]*\)", "", text).strip()
	if not base:
		base = text.strip()
	height: int | None = None
	weight: int | None = None
	notes: list[str] = []
	if parts:
		# try to parse first as height/weight
		nums = re.findall(r"\d{2,3}", parts[0])
		if len(nums) >= 2:
			try:
				height = int(nums[0])
				weight = int(nums[1])
			except Exception:
				pass
			# remaining parentheses are notes
			for p in parts[1:]:
				n = p.strip()
				if n:
					notes.append(n)
		else:
			# none numeric, all are notes
			for p in parts:
				n = p.strip()
				if n:
					notes.append(n)
	return base, height, weight, notes



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
def preview_import(body: dict):
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
	return {
		"source": source,
		"filename": file_path.name,
		"row_count": len(records),
		"sample": records[:5],
	}


@router.post("/commit")
def commit_import(body: dict):
	source = body.get("source")
	filename = body.get("filename")
	if source not in ("bizim", "kargo") or not filename:
		raise HTTPException(status_code=400, detail="source ('bizim'|'kargo') and filename are required")

	folder = BIZIM_DIR if source == "bizim" else KARGO_DIR
	file_path = folder / filename
	if not file_path.exists():
		raise HTTPException(status_code=404, detail="File not found")

	records = read_bizim_file(str(file_path)) if source == "bizim" else read_kargo_file(str(file_path))

	with get_session() as session:
		run = ImportRun(source=source, filename=filename)
		session.add(run)
		session.flush()

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
				if source == "bizim":
					uq = client_unique_key(rec.get("name"), rec.get("phone"))
					client = None
					if uq:
						client = session.exec(select(Client).where(Client.unique_key == uq)).first()
					if not client:
						client = Client(
							name=rec_name or "",
							phone=rec.get("phone"),
							address=rec.get("address"),
							city=rec.get("city"),
							unique_key=uq or None,
						)
						session.add(client)
						session.flush()
						run.created_clients += 1
					else:
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
						source="bizim",
						notes=order_notes,
					)
					session.add(order)
					session.flush()
					run.created_orders += 1
					matched_order_id = order.id

				else:  # kargo
					order = find_order_by_tracking(session, rec.get("tracking_no"))
					if order:
						matched_order_id = order.id
						matched_client_id = order.client_id
						if rec.get("payment_amount"):
							pmt = Payment(
								client_id=order.client_id,
								order_id=order.id,
								amount=rec.get("payment_amount") or 0.0,
								date=rec.get("shipment_date"),
								method="kargo",
								reference=rec.get("tracking_no"),
							)
							session.add(pmt)
							run.created_payments += 1
					else:
						status = "unmatched"
						message = "no tracking match"
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
		}
		return summary


@router.post("/reset")
def reset_database():
	reset_db()
	return {"status": "ok"}


@router.post("/upload")
async def upload_excel(
	source: str = Form(..., description="'bizim' or 'kargo'"),
	files: list[UploadFile] = File(...),
):
	if source not in ("bizim", "kargo"):
		raise HTTPException(status_code=400, detail="source must be 'bizim' or 'kargo'")
	folder = BIZIM_DIR if source == "bizim" else KARGO_DIR
	folder.mkdir(parents=True, exist_ok=True)

	saved: list[dict[str, Any]] = []
	for file in files:
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