from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse
from fastapi import Form
from typing import Any, Optional
from sqlalchemy import text as _text

from ..db import get_session


router = APIRouter(prefix="/stories", tags=["stories"])


@router.get("/{story_id}/edit")
def edit_story(request, story_id: str):
	with get_session() as session:
		# Load story cache row and existing mapping (if any)
		story_row: dict[str, Any] = {}
		mapping: dict[str, Any] = {}
		try:
			row = session.exec(_text("SELECT story_id, url, updated_at FROM stories WHERE story_id=:id")).params(id=str(story_id)).first()
		except Exception:
			row = None
		if row:
			story_row = {
				"story_id": getattr(row, "story_id", None) if hasattr(row, "story_id") else (row[0] if len(row) > 0 else None),
				"url": getattr(row, "url", None) if hasattr(row, "url") else (row[1] if len(row) > 1 else None),
				"updated_at": getattr(row, "updated_at", None) if hasattr(row, "updated_at") else (row[2] if len(row) > 2 else None),
			}
		try:
			mp = session.exec(_text("SELECT story_id, product_id, sku FROM stories_products WHERE story_id=:id LIMIT 1")).params(id=str(story_id)).first()
		except Exception:
			mp = None
		if mp:
			mapping = {
				"story_id": getattr(mp, "story_id", None) if hasattr(mp, "story_id") else (mp[0] if len(mp) > 0 else None),
				"product_id": getattr(mp, "product_id", None) if hasattr(mp, "product_id") else (mp[1] if len(mp) > 1 else None),
				"sku": getattr(mp, "sku", None) if hasattr(mp, "sku") else (mp[2] if len(mp) > 2 else None),
			}
	tmpl = request.app.state.templates
	return tmpl.TemplateResponse("story_edit.html", {"request": request, "story": story_row, "mapping": mapping})


@router.post("/{story_id}/save")
def save_story_mapping(story_id: str, sku: Optional[str] = Form(default=None), product_id: Optional[int] = Form(default=None)):
	if not ((sku and sku.strip()) or product_id):
		raise HTTPException(status_code=400, detail="SKU veya Product ID giriniz")
	sku_clean = (sku or "").strip() or None
	pid = product_id
	with get_session() as session:
		# Ensure story exists
		try:
			session.exec(_text("INSERT OR IGNORE INTO stories(story_id, url, updated_at) VALUES (:id, NULL, CURRENT_TIMESTAMP)")).params(id=str(story_id))
		except Exception:
			try:
				session.exec(_text("INSERT IGNORE INTO stories(story_id, url, updated_at) VALUES (:id, NULL, CURRENT_TIMESTAMP)")).params(id=str(story_id))
			except Exception:
				pass
		# Upsert mapping
		try:
			session.exec(_text("INSERT OR REPLACE INTO stories_products(story_id, product_id, sku) VALUES(:id, :pid, :sku)")).params(id=str(story_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
		except Exception:
			# MySQL: emulate replace with insert/update
			try:
				session.exec(_text("INSERT INTO stories_products(story_id, product_id, sku) VALUES(:id, :pid, :sku) ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), sku=VALUES(sku)")).params(id=str(story_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
			except Exception:
				rowm = session.exec(_text("SELECT story_id FROM stories_products WHERE story_id=:id")).params(id=str(story_id)).first()
				if rowm:
					session.exec(_text("UPDATE stories_products SET product_id=:pid, sku=:sku WHERE story_id=:id")).params(id=str(story_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
				else:
					session.exec(_text("INSERT INTO stories_products(story_id, product_id, sku) VALUES(:id, :pid, :sku)")).params(id=str(story_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
	return RedirectResponse(url=f"/stories/{story_id}/edit", status_code=303)


