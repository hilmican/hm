from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import RedirectResponse
from starlette.status import HTTP_303_SEE_OTHER
from typing import Any, Optional
from sqlalchemy import text as _text

from ..db import get_session


router = APIRouter(prefix="/ads", tags=["ads"])


@router.get("/{ad_id}/edit")
def edit_ad(request: Request, ad_id: str):
	"""
	Load ad cache row, existing mapping (if any) and candidate *products* for selection.
	We no longer need per-SKU selection here; ads are linked to a single product.
	"""
	ad_row: dict[str, Any] = {}
	mapping: dict[str, Any] = {}
	products: list[dict[str, Any]] = []
	with get_session() as session:
		# Ad cache row
		try:
			row = session.exec(
				_text(
					"SELECT ad_id, name, image_url, link, fetch_status, fetch_error, updated_at FROM ads WHERE ad_id=:id"
				)
			).params(id=str(ad_id)).first()
		except Exception:
			row = None
		if row:
			ad_row = {
				"ad_id": getattr(row, "ad_id", None) if hasattr(row, "ad_id") else (row[0] if len(row) > 0 else None),
				"name": getattr(row, "name", None) if hasattr(row, "name") else (row[1] if len(row) > 1 else None),
				"image_url": getattr(row, "image_url", None) if hasattr(row, "image_url") else (row[2] if len(row) > 2 else None),
				"link": getattr(row, "link", None) if hasattr(row, "link") else (row[3] if len(row) > 3 else None),
				"fetch_status": getattr(row, "fetch_status", None) if hasattr(row, "fetch_status") else (row[4] if len(row) > 4 else None),
				"fetch_error": getattr(row, "fetch_error", None) if hasattr(row, "fetch_error") else (row[5] if len(row) > 5 else None),
				"updated_at": getattr(row, "updated_at", None) if hasattr(row, "updated_at") else (row[6] if len(row) > 6 else None),
			}
		# Existing mapping for this ad (if any)
		try:
			mp = session.exec(
				_text("SELECT ad_id, product_id, sku FROM ads_products WHERE ad_id=:id LIMIT 1")
			).params(id=str(ad_id)).first()
		except Exception:
			mp = None
		if mp:
			mapping = {
				"ad_id": getattr(mp, "ad_id", None) if hasattr(mp, "ad_id") else (mp[0] if len(mp) > 0 else None),
				"product_id": getattr(mp, "product_id", None) if hasattr(mp, "product_id") else (mp[1] if len(mp) > 1 else None),
				"sku": getattr(mp, "sku", None) if hasattr(mp, "sku") else (mp[2] if len(mp) > 2 else None),
			}
		# Candidate products for combobox-style selection (limit to keep page fast)
		try:
			rows_products = session.exec(
				_text(
					"""
					SELECT id, name
					FROM product
					ORDER BY name
					LIMIT 500
					"""
				)
			).all()
		except Exception:
			rows_products = []
		for r in rows_products:
			try:
				pid = getattr(r, "id", None) if hasattr(r, "id") else (r[0] if len(r) > 0 else None)
				name = getattr(r, "name", None) if hasattr(r, "name") else (r[1] if len(r) > 1 else None)
				if pid is None or not name:
					continue
				products.append({"id": pid, "name": name})
			except Exception:
				continue
	templates = request.app.state.templates
	return templates.TemplateResponse(
		"ad_edit.html",
		{
			"request": request,
			"ad": ad_row,
			"mapping": mapping,
			"products": products,
		},
	)


@router.post("/{ad_id}/save")
def save_ad_mapping(ad_id: str, sku: Optional[str] = Form(default=None), product_id: Optional[int] = Form(default=None)):
	if not ((sku and sku.strip()) or product_id):
		raise HTTPException(status_code=400, detail="SKU veya Product ID giriniz")
	with get_session() as session:
		# Resolve product_id from sku if needed (best-effort)
		pid: Optional[int] = int(product_id) if product_id is not None else None
		sku_clean: Optional[str] = (sku.strip() if isinstance(sku, str) else None)
		if pid is None and sku_clean:
			try:
				rowi = session.exec(_text("SELECT product_id FROM item WHERE sku=:s LIMIT 1")).params(s=str(sku_clean)).first()
				if rowi:
					val = getattr(rowi, "product_id", None) if hasattr(rowi, "product_id") else (rowi[0] if len(rowi) > 0 else None)
					pid = int(val) if val is not None else None
			except Exception:
				pid = None
		# Upsert mapping
		try:
			session.exec(_text(
				"INSERT OR REPLACE INTO ads_products(ad_id, product_id, sku) VALUES(:id, :pid, :sku)"
			)).params(id=str(ad_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
		except Exception:
			# Fallback for MySQL: emulate replace with insert/update
			try:
				session.exec(_text(
					"INSERT INTO ads_products(ad_id, product_id, sku) VALUES(:id, :pid, :sku) ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), sku=VALUES(sku)"
				)).params(id=str(ad_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
			except Exception:
				# Last resort: try separate update/insert
				rowm = session.exec(_text("SELECT ad_id FROM ads_products WHERE ad_id=:id")).params(id=str(ad_id)).first()
				if rowm:
					session.exec(_text("UPDATE ads_products SET product_id=:pid, sku=:sku WHERE ad_id=:id")).params(id=str(ad_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
				else:
					session.exec(_text("INSERT INTO ads_products(ad_id, product_id, sku) VALUES(:id, :pid, :sku)")).params(id=str(ad_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
	# Redirect back to edit page
	return RedirectResponse(url=f"/ads/{ad_id}/edit", status_code=HTTP_303_SEE_OTHER)


