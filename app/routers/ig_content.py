from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Request
from pydantic import BaseModel, Field

from ..services import content_publish

router = APIRouter(prefix="/ig/content", tags=["instagram-content"])


class DraftPayload(BaseModel):
	media_type: str = Field(default="PHOTO", regex="^(PHOTO|VIDEO|REEL)$")
	caption: str | None = None
	image_url: str | None = None
	video_url: str | None = None
	scheduled_at: str | None = Field(
		default=None, description="ISO datetime (UTC). Leave empty to keep as draft."
	)


@router.get("/calendar")
def calendar_page(request: Request):
	templates = request.app.state.templates
	posts = [
		{
			"id": post.id,
			"media_type": post.media_type,
			"caption": post.caption,
			"scheduled_at": post.scheduled_at.isoformat() if post.scheduled_at else None,
			"status": post.status,
			"error_message": post.error_message,
		}
		for post in content_publish.list_scheduled_posts(limit=100)
	]
	return templates.TemplateResponse(
		"ig_calendar.html",
		{
			"request": request,
			"scheduled_posts": posts,
		},
	)


@router.get("/queue")
def queue():
	posts = content_publish.list_scheduled_posts(limit=100)
	return {
		"items": [
			{
				"id": post.id,
				"media_type": post.media_type,
				"caption": post.caption,
				"scheduled_at": post.scheduled_at.isoformat() if post.scheduled_at else None,
				"status": post.status,
				"error_message": post.error_message,
			}
			for post in posts
		]
	}


@router.post("/drafts")
def create_draft(payload: DraftPayload = Body(...)):
	media_payload: dict[str, str] = {}
	if payload.image_url:
		media_payload["image_url"] = payload.image_url
	if payload.video_url:
		media_payload["video_url"] = payload.video_url
	if not media_payload:
		raise HTTPException(status_code=400, detail="media_url_required")
	scheduled_at = None
	if payload.scheduled_at:
		from datetime import datetime

		try:
			scheduled_at = datetime.fromisoformat(payload.scheduled_at)
		except ValueError:
			raise HTTPException(status_code=400, detail="invalid_datetime")
	post = content_publish.create_scheduled_post(
		media_type=payload.media_type,
		caption=payload.caption,
		media_payload=media_payload,
		scheduled_at=scheduled_at,
		created_by_user_id=None,
	)
	return {
		"id": post.id,
		"status": post.status,
		"scheduled_at": post.scheduled_at.isoformat() if post.scheduled_at else None,
	}


@router.post("/{post_id}/publish-now")
def publish_now(post_id: int):
	from ..db import get_session
	from ..models import IGScheduledPost

	with get_session() as session:
		post = session.get(IGScheduledPost, post_id)
		if not post:
			raise HTTPException(status_code=404, detail="post_not_found")
	try:
		content_publish.publish_post(post)
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"publish_failed: {exc}")
	return {"status": "ok"}

