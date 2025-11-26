from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query, Request

from ..db import get_session
from ..models import IGCommentActionLog
from ..services import ig_comments

router = APIRouter(prefix="/ig/comments", tags=["instagram-comments"])


@router.get("/moderation")
def moderation_page(request: Request):
	templates = request.app.state.templates
	return templates.TemplateResponse("ig_comments.html", {"request": request})


@router.get("/stream")
def comment_stream(media_id: str = Query(..., description="Instagram media ID"), limit: int = 50, cursor: str | None = None):
	if not media_id:
		raise HTTPException(status_code=400, detail="media_id_required")
	try:
		data = ig_comments.fetch_comments(media_id, limit=limit, after=cursor)
		return data
	except Exception as exc:
		raise HTTPException(status_code=502, detail=f"comment_fetch_failed: {exc}")


@router.post("/{comment_id}/reply")
def reply(comment_id: str, body: dict = Body(...)):
	message = body.get("message")
	if not message:
		raise HTTPException(status_code=400, detail="message_required")
	try:
		return ig_comments.reply_to_comment(comment_id, message)
	except Exception as exc:
		raise HTTPException(status_code=502, detail=f"reply_failed: {exc}")


@router.post("/{comment_id}/hide")
def hide(comment_id: str, hide: bool = Body(True, embed=True)):
	try:
		return ig_comments.hide_comment(comment_id, hide=hide)
	except Exception as exc:
		raise HTTPException(status_code=502, detail=f"hide_failed: {exc}")


@router.delete("/{comment_id}")
def delete(comment_id: str):
	try:
		return ig_comments.delete_comment(comment_id)
	except Exception as exc:
		raise HTTPException(status_code=502, detail=f"delete_failed: {exc}")


@router.post("/{comment_id}/convert")
def convert(comment_id: str):
	try:
		ig_comments.convert_comment_to_dm(comment_id)
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"convert_failed: {exc}")
	return {"status": "ok"}


@router.get("/audit")
def audit_log(limit: int = 20):
	with get_session() as session:
		rows = (
			session.query(IGCommentActionLog)
			.order_by(IGCommentActionLog.created_at.desc())
			.limit(max(1, min(limit, 100)))
			.all()
		)
	return {
		"items": [
			{
				"id": row.id,
				"comment_id": row.comment_id,
				"media_id": row.media_id,
				"action": row.action,
				"payload": row.payload_json,
				"created_at": row.created_at.isoformat() if row.created_at else None,
			}
			for row in rows
		]
	}

