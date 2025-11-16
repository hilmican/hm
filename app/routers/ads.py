from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import RedirectResponse, JSONResponse
from starlette.status import HTTP_303_SEE_OTHER
from typing import Any, Optional
from sqlalchemy import text as _text
import json

from ..db import get_session
from ..services.ai import AIClient
from ..services.prompts import AD_PRODUCT_MATCH_SYSTEM_PROMPT


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
			stmt_ad = _text(
				"SELECT ad_id, name, image_url, link, fetch_status, fetch_error, updated_at FROM ads WHERE ad_id=:id"
			).bindparams(id=str(ad_id))
			row = session.exec(stmt_ad).first()
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
			stmt_mp = _text("SELECT ad_id, product_id, sku, auto_linked FROM ads_products WHERE ad_id=:id LIMIT 1").bindparams(
				id=str(ad_id)
			)
			mp = session.exec(stmt_mp).first()
		except Exception:
			mp = None
		if mp:
			mapping = {
				"ad_id": getattr(mp, "ad_id", None) if hasattr(mp, "ad_id") else (mp[0] if len(mp) > 0 else None),
				"product_id": getattr(mp, "product_id", None) if hasattr(mp, "product_id") else (mp[1] if len(mp) > 1 else None),
				"sku": getattr(mp, "sku", None) if hasattr(mp, "sku") else (mp[2] if len(mp) > 2 else None),
				"auto_linked": bool(getattr(mp, "auto_linked", None) if hasattr(mp, "auto_linked") else (mp[3] if len(mp) > 3 else False)),
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
			"ad_id": ad_id,
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
		# Manual save should clear auto_linked flag (user is correcting/confirming)
		# Resolve product_id from sku if needed (best-effort)
		pid: Optional[int] = int(product_id) if product_id is not None else None
		sku_clean: Optional[str] = (sku.strip() if isinstance(sku, str) else None)
		if pid is None and sku_clean:
			try:
				stmt_item = _text("SELECT product_id FROM item WHERE sku=:s LIMIT 1").bindparams(s=str(sku_clean))
				rowi = session.exec(stmt_item).first()
				if rowi:
					val = getattr(rowi, "product_id", None) if hasattr(rowi, "product_id") else (rowi[0] if len(rowi) > 0 else None)
					pid = int(val) if val is not None else None
			except Exception:
				pid = None
		# Upsert mapping (manual save clears auto_linked flag)
		try:
			stmt_upsert_sqlite = _text(
				"INSERT OR REPLACE INTO ads_products(ad_id, product_id, sku, auto_linked) VALUES(:id, :pid, :sku, 0)"
			).bindparams(
				id=str(ad_id),
				pid=(int(pid) if pid is not None else None),
				sku=(sku_clean or None),
			)
			session.exec(stmt_upsert_sqlite)
		except Exception:
			# Fallback for MySQL: emulate replace with insert/update
			try:
				stmt_upsert_mysql = _text(
					"INSERT INTO ads_products(ad_id, product_id, sku, auto_linked) VALUES(:id, :pid, :sku, 0) "
					"ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), sku=VALUES(sku), auto_linked=0"
				).bindparams(
					id=str(ad_id),
					pid=(int(pid) if pid is not None else None),
					sku=(sku_clean or None),
				)
				session.exec(stmt_upsert_mysql)
			except Exception:
				# Last resort: try separate update/insert
				stmt_sel = _text("SELECT ad_id FROM ads_products WHERE ad_id=:id").bindparams(id=str(ad_id))
				rowm = session.exec(stmt_sel).first()
				if rowm:
					stmt_update = _text(
						"UPDATE ads_products SET product_id=:pid, sku=:sku, auto_linked=0 WHERE ad_id=:id"
					).bindparams(
						id=str(ad_id),
						pid=(int(pid) if pid is not None else None),
						sku=(sku_clean or None),
					)
					session.exec(stmt_update)
				else:
					stmt_insert = _text(
						"INSERT INTO ads_products(ad_id, product_id, sku, auto_linked) VALUES(:id, :pid, :sku, 0)"
					).bindparams(
						id=str(ad_id),
						pid=(int(pid) if pid is not None else None),
						sku=(sku_clean or None),
					)
					session.exec(stmt_insert)
	# Redirect back to edit page
	return RedirectResponse(url=f"/ads/{ad_id}/edit", status_code=HTTP_303_SEE_OTHER)


@router.post("/{ad_id}/ai/suggest")
def ai_suggest_product_for_ad(request: Request, ad_id: str, auto_save: bool = True, min_confidence: float = 0.7):
	"""
	Use AI to suggest which product an ad is about based on ad_title and product list.
	If auto_save is True and confidence >= min_confidence, automatically saves the mapping.
	Similar to the automatic post linking system.
	"""
	ai = getattr(request.app.state, "ai", None)
	if not ai or not getattr(ai, "enabled", False):
		raise HTTPException(status_code=503, detail="AI not configured")
	
	ad_title: Optional[str] = None
	ad_name: Optional[str] = None
	products: list[dict[str, Any]] = []
	
	with get_session() as session:
		# Get ad title from ads table or messages
		try:
			# First try ads table
			stmt_ad = _text(
				"SELECT name FROM ads WHERE ad_id=:id LIMIT 1"
			).bindparams(id=str(ad_id))
			row = session.exec(stmt_ad).first()
			if row:
				ad_name = getattr(row, "name", None) if hasattr(row, "name") else (row[0] if len(row) > 0 else None)
		except Exception:
			pass
		
		# Also try to get ad_title from messages (more reliable, includes ads_context_data)
		try:
			stmt_msg = _text(
				"SELECT ad_title, referral_json FROM message WHERE ad_id=:id ORDER BY timestamp_ms DESC LIMIT 1"
			).bindparams(id=str(ad_id))
			row_msg = session.exec(stmt_msg).first()
			if row_msg:
				# Try ad_title column first
				ad_title = getattr(row_msg, "ad_title", None) if hasattr(row_msg, "ad_title") else (row_msg[0] if len(row_msg) > 0 else None)
				# If not found, try extracting from referral_json (ads_context_data)
				if not ad_title:
					try:
						referral_json = getattr(row_msg, "referral_json", None) if hasattr(row_msg, "referral_json") else (row_msg[1] if len(row_msg) > 1 else None)
						if referral_json:
							ref_data = json.loads(referral_json) if isinstance(referral_json, str) else referral_json
							if isinstance(ref_data, dict):
								# Check ads_context_data.ad_title (from Instagram webhook format)
								ads_ctx = ref_data.get("ads_context_data") or {}
								if isinstance(ads_ctx, dict):
									ad_title = ads_ctx.get("ad_title") or ad_title
								# Also check direct fields
								ad_title = ref_data.get("ad_title") or ref_data.get("headline") or ref_data.get("source") or ad_title
					except Exception:
						pass
		except Exception:
			pass
		
		# Use ad_title from messages if available, otherwise use ad_name from ads table
		ad_text = ad_title or ad_name
		
		if not ad_text:
			raise HTTPException(status_code=404, detail="Ad title not found. Please ensure the ad has a title or name.")
		
		# Get product list
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
				products.append({"id": int(pid), "name": str(name)})
			except Exception:
				continue
	
	if not products:
		raise HTTPException(status_code=400, detail="No products found in database")
	
	# Build AI request
	system_prompt = AD_PRODUCT_MATCH_SYSTEM_PROMPT
	
	body = {
		"ad_title": ad_text,
		"known_products": products,
		"schema": {
			"product_id": "int|null",
			"product_name": "str|null",
			"confidence": "float",
			"notes": "str|null",
		},
	}
	
	user_prompt = (
		"Lütfen SADECE geçerli JSON döndür. Markdown/kod bloğu/yorum ekleme. "
		"Tüm alanlar çift tırnaklı olmalı.\nGirdi:\n" + json.dumps(body, ensure_ascii=False)
	)
	
	try:
		result = ai.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"AI request failed: {str(e)}")
	
	# Validate and return result
	if not isinstance(result, dict):
		raise HTTPException(status_code=500, detail="AI returned invalid response")
	
	product_id = result.get("product_id")
	if product_id is not None:
		try:
			product_id = int(product_id)
		except (ValueError, TypeError):
			product_id = None
	
	product_name = result.get("product_name")
	confidence = result.get("confidence", 0.0)
	confidence_float = float(confidence) if confidence is not None else 0.0
	notes = result.get("notes")
	
	# Auto-save if confidence is high enough
	saved = False
	if auto_save and product_id and confidence_float >= min_confidence:
		# Use a new session for saving
		with get_session() as save_session:
			try:
				# Save the mapping (same logic as save_ad_mapping)
				pid = int(product_id)
				try:
					stmt_upsert_sqlite = _text(
						"INSERT OR REPLACE INTO ads_products(ad_id, product_id, sku) VALUES(:id, :pid, :sku)"
					).bindparams(
						id=str(ad_id),
						pid=pid,
						sku=None,
					)
					save_session.exec(stmt_upsert_sqlite)
				except Exception:
					# Fallback for MySQL
					try:
						stmt_upsert_mysql = _text(
							"INSERT INTO ads_products(ad_id, product_id, sku) VALUES(:id, :pid, :sku) "
							"ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), sku=VALUES(sku)"
						).bindparams(
							id=str(ad_id),
							pid=pid,
							sku=None,
						)
						save_session.exec(stmt_upsert_mysql)
					except Exception:
						# Last resort: try separate update/insert
						stmt_sel = _text("SELECT ad_id FROM ads_products WHERE ad_id=:id").bindparams(id=str(ad_id))
						rowm = save_session.exec(stmt_sel).first()
						if rowm:
							stmt_update = _text(
								"UPDATE ads_products SET product_id=:pid, sku=:sku WHERE ad_id=:id"
							).bindparams(
								id=str(ad_id),
								pid=pid,
								sku=None,
							)
							save_session.exec(stmt_update)
						else:
							stmt_insert = _text(
								"INSERT INTO ads_products(ad_id, product_id, sku) VALUES(:id, :pid, :sku)"
							).bindparams(
								id=str(ad_id),
								pid=pid,
								sku=None,
							)
							save_session.exec(stmt_insert)
				save_session.commit()
				saved = True
			except Exception as e:
				# Log error but don't fail the request
				import logging
				_log = logging.getLogger("ads")
				_log.error("Failed to auto-save ad mapping: %s", str(e))
	
	return JSONResponse({
		"product_id": product_id,
		"product_name": product_name,
		"confidence": confidence_float,
		"notes": notes,
		"ad_title": ad_text,
		"saved": saved,
		"auto_saved": saved,
	})


