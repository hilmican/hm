from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse
from fastapi import Form
from typing import Any, Optional
from sqlalchemy import text as _text

from ..db import get_session


def _story_link_key(story_id: str) -> Optional[str]:
	story_id = (story_id or "").strip()
	if not story_id:
		return None
	return story_id if story_id.startswith("story:") else f"story:{story_id}"


router = APIRouter(prefix="/stories", tags=["stories"])


@router.get("/{story_id}/edit")
def edit_story(request, story_id: str):
	with get_session() as session:
		# Load story cache row and existing mapping (if any)
		story_row: dict[str, Any] = {}
		mapping: dict[str, Any] = {}
		try:
			row = session.exec(
				_text(
					"""
					SELECT story_id, url, media_path, media_thumb_path, media_mime, media_checksum, media_fetched_at, updated_at
					FROM stories
					WHERE story_id=:id
					"""
				).params(id=str(story_id))
			).first()
		except Exception:
			row = None
		if row:
			try:
				story_row = {
					"story_id": getattr(row, "story_id", None) if hasattr(row, "story_id") else (row[0] if len(row) > 0 else None),
					"url": getattr(row, "url", None) if hasattr(row, "url") else (row[1] if len(row) > 1 else None),
					"media_path": getattr(row, "media_path", None) if hasattr(row, "media_path") else (row[2] if len(row) > 2 else None),
					"media_thumb_path": getattr(row, "media_thumb_path", None) if hasattr(row, "media_thumb_path") else (row[3] if len(row) > 3 else None),
					"media_mime": getattr(row, "media_mime", None) if hasattr(row, "media_mime") else (row[4] if len(row) > 4 else None),
					"media_checksum": getattr(row, "media_checksum", None) if hasattr(row, "media_checksum") else (row[5] if len(row) > 5 else None),
					"media_fetched_at": getattr(row, "media_fetched_at", None) if hasattr(row, "media_fetched_at") else (row[6] if len(row) > 6 else None),
					"updated_at": getattr(row, "updated_at", None) if hasattr(row, "updated_at") else (row[7] if len(row) > 7 else None),
				}
			except Exception:
				story_row = {}
		try:
			mp = session.exec(
				_text("SELECT story_id, product_id, sku, auto_linked, confidence, ai_result_json FROM stories_products WHERE story_id=:id LIMIT 1")
			).params(id=str(story_id)).first()
		except Exception:
			mp = None
		if mp:
			mapping = {
				"story_id": getattr(mp, "story_id", None) if hasattr(mp, "story_id") else (mp[0] if len(mp) > 0 else None),
				"product_id": getattr(mp, "product_id", None) if hasattr(mp, "product_id") else (mp[1] if len(mp) > 1 else None),
				"sku": getattr(mp, "sku", None) if hasattr(mp, "sku") else (mp[2] if len(mp) > 2 else None),
				"auto_linked": getattr(mp, "auto_linked", None) if hasattr(mp, "auto_linked") else (mp[3] if len(mp) > 3 else None),
				"confidence": getattr(mp, "confidence", None) if hasattr(mp, "confidence") else (mp[4] if len(mp) > 4 else None),
				"ai_result_json": getattr(mp, "ai_result_json", None) if hasattr(mp, "ai_result_json") else (mp[5] if len(mp) > 5 else None),
			}
	link_key = _story_link_key(str(story_row.get("story_id") or story_id)) if story_row.get("story_id") or story_id else None  # type: ignore[arg-type]
	tmpl = request.app.state.templates
	return tmpl.TemplateResponse(
		"story_edit.html",
		{
			"request": request,
			"story": story_row,
			"mapping": mapping,
			"story_link_key": link_key,
		},
	)


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
		try:
			row_story = session.exec(_text("SELECT url FROM stories WHERE story_id=:id LIMIT 1")).params(id=str(story_id)).first()
		except Exception:
			row_story = None
		story_url = None
		if row_story:
			try:
				story_url = getattr(row_story, "url", None) if hasattr(row_story, "url") else (row_story[0] if len(row_story) > 0 else None)
			except Exception:
				story_url = None
		story_key = _story_link_key(str(story_id))
		# Upsert mapping
		try:
			session.exec(
				_text(
					"INSERT OR REPLACE INTO stories_products(story_id, product_id, sku, auto_linked, confidence, ai_result_json) VALUES(:id, :pid, :sku, 0, NULL, NULL)"
				)
			).params(id=str(story_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
		except Exception:
			# MySQL: emulate replace with insert/update
			try:
				session.exec(
					_text(
						"""
						INSERT INTO stories_products(story_id, product_id, sku, auto_linked, confidence, ai_result_json)
						VALUES(:id, :pid, :sku, 0, NULL, NULL)
						ON DUPLICATE KEY UPDATE
							product_id=VALUES(product_id),
							sku=VALUES(sku),
							auto_linked=0,
							confidence=NULL,
							ai_result_json=NULL
						"""
					)
				).params(id=str(story_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
			except Exception:
				rowm = session.exec(_text("SELECT story_id FROM stories_products WHERE story_id=:id")).params(id=str(story_id)).first()
				if rowm:
					session.exec(
						_text(
							"UPDATE stories_products SET product_id=:pid, sku=:sku, auto_linked=0, confidence=NULL, ai_result_json=NULL WHERE story_id=:id"
						)
					).params(id=str(story_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
				else:
					session.exec(
						_text(
							"INSERT INTO stories_products(story_id, product_id, sku, auto_linked, confidence, ai_result_json) VALUES(:id, :pid, :sku, 0, NULL, NULL)"
						)
					).params(id=str(story_id), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
		if story_key:
			try:
				session.exec(
					_text(
						"""
						INSERT INTO ads(ad_id, link_type, name, image_url, link, updated_at)
						VALUES(:id, 'story', :name, :url, :url, CURRENT_TIMESTAMP)
						ON DUPLICATE KEY UPDATE
							link_type='story',
							name=COALESCE(:name, name),
							image_url=COALESCE(:url, image_url),
							link=COALESCE(:url, link),
							updated_at=CURRENT_TIMESTAMP
						"""
					)
				).params(id=str(story_key), name=f"Story {story_id}", url=story_url)
			except Exception:
				try:
					session.exec(
						_text(
							"INSERT OR REPLACE INTO ads(ad_id, link_type, name, image_url, link, updated_at) VALUES(:id, 'story', :name, :url, :url, CURRENT_TIMESTAMP)"
						)
					).params(id=str(story_key), name=f"Story {story_id}", url=story_url)
				except Exception:
					session.exec(
						_text(
							"UPDATE ads SET link_type='story', name=COALESCE(:name,name), image_url=COALESCE(:url,image_url), link=COALESCE(:url,link), updated_at=CURRENT_TIMESTAMP WHERE ad_id=:id"
						)
					).params(id=str(story_key), name=f"Story {story_id}", url=story_url)
			try:
				session.exec(
					_text(
						"""
						INSERT INTO ads_products(ad_id, link_type, product_id, sku, auto_linked)
						VALUES(:id, 'story', :pid, :sku, 0)
						ON DUPLICATE KEY UPDATE
							product_id=VALUES(product_id),
							sku=VALUES(sku),
							link_type='story',
							auto_linked=0
						"""
					)
				).params(id=str(story_key), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
			except Exception:
				try:
					session.exec(
						_text(
							"INSERT OR REPLACE INTO ads_products(ad_id, link_type, product_id, sku, auto_linked) VALUES(:id, 'story', :pid, :sku, 0)"
						)
					).params(id=str(story_key), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
				except Exception:
					session.exec(
						_text(
							"UPDATE ads_products SET product_id=:pid, sku=:sku, auto_linked=0, link_type='story' WHERE ad_id=:id"
						)
					).params(id=str(story_key), pid=(int(pid) if pid is not None else None), sku=(sku_clean or None))
		
		# Update conversations that have messages with this story_id
		# This ensures _detect_focus_product can find the product
		if story_key:
			try:
				# Find all conversations with messages referencing this story
				conv_ids = session.exec(
					_text(
						"""
						SELECT DISTINCT conversation_id
						FROM message
						WHERE story_id = :sid AND conversation_id IS NOT NULL
						"""
					)
				).params(sid=str(story_id)).all()
				
				# Update each conversation's last_link_id and last_link_type
				for conv_row in conv_ids:
					conv_id = conv_row[0] if isinstance(conv_row, (list, tuple)) else (getattr(conv_row, "conversation_id", None) if hasattr(conv_row, "conversation_id") else None)
					if conv_id:
						try:
							session.exec(
								_text(
									"""
									UPDATE conversations
									SET last_link_id = :link_id,
									    last_link_type = 'story'
									WHERE id = :cid
									"""
								)
							).params(link_id=str(story_key), cid=int(conv_id))
							
							# Clear needs_link status if it exists, since product is now linked
							try:
								session.exec(
									_text(
										"""
										UPDATE ai_shadow_state
										SET status = 'pending',
										    updated_at = CURRENT_TIMESTAMP
										WHERE conversation_id = :cid
										  AND status = 'needs_link'
										"""
									)
								).params(cid=int(conv_id))
							except Exception:
								pass
						except Exception:
							pass
			except Exception:
				pass
	
	return RedirectResponse(url=f"/stories/{story_id}/edit", status_code=303)


