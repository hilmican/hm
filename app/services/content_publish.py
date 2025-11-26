"""
Scheduling/publishing helpers for Instagram posts & reels.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import or_
from sqlmodel import select

from ..db import get_session
from ..models import IGPublishingAudit, IGScheduledPost
from .instagram_api import GRAPH_VERSION, _get_base_token_and_id

_log = logging.getLogger("instagram.publish")

PUBLISH_BATCH_LIMIT = 10


def _now() -> dt.datetime:
	return dt.datetime.utcnow()


def _record_audit(post_id: int, action: str, status: str, payload: Optional[Dict[str, Any]] = None) -> None:
	entry = IGPublishingAudit(
		scheduled_post_id=post_id,
		action=action,
		status=status,
		payload_json=json.dumps(payload) if payload else None,
	)
	with get_session() as session:
		session.add(entry)
		session.commit()


def create_scheduled_post(media_type: str, caption: Optional[str], media_payload: Dict[str, Any], scheduled_at: Optional[dt.datetime], created_by_user_id: Optional[int]) -> IGScheduledPost:
	post = IGScheduledPost(
		media_type=media_type,
		caption=caption,
		media_payload_json=json.dumps(media_payload),
		scheduled_at=scheduled_at,
		status="scheduled" if scheduled_at else "draft",
		created_by_user_id=created_by_user_id,
	)
	with get_session() as session:
		session.add(post)
		session.commit()
		session.refresh(post)
	return post


def list_scheduled_posts(limit: int = 50) -> List[IGScheduledPost]:
	with get_session() as session:
		stmt = (
			select(IGScheduledPost)
			.order_by(IGScheduledPost.scheduled_at.asc().nulls_last(), IGScheduledPost.id.asc())
			.limit(limit)
		)
		return list(session.exec(stmt))


def list_due_posts(limit: int = PUBLISH_BATCH_LIMIT) -> List[IGScheduledPost]:
	now = _now()
	with get_session() as session:
		stmt = (
			select(IGScheduledPost)
			.where(IGScheduledPost.status.in_(("scheduled", "publishing")))
			.where(
				or_(
					IGScheduledPost.scheduled_at.is_(None),
					IGScheduledPost.scheduled_at <= now,
				)
			)
			.order_by(IGScheduledPost.scheduled_at.asc().nulls_last(), IGScheduledPost.id.asc())
			.limit(limit)
		)
		return list(session.exec(stmt))


def update_post_status(post_id: int, *, status: str, error_message: Optional[str] = None, container_id: Optional[str] = None, media_id: Optional[str] = None) -> None:
	with get_session() as session:
		post = session.get(IGScheduledPost, post_id)
		if not post:
			return
		post.status = status
		post.error_message = error_message
		if container_id:
			post.ig_container_id = container_id
		if media_id:
			post.ig_media_id = media_id
		post.updated_at = _now()
		session.add(post)
		session.commit()


def _post_media(token: str, ig_user_id: str, post: IGScheduledPost, media_payload: Dict[str, Any]) -> Dict[str, Any]:
	url = f"https://graph.facebook.com/{GRAPH_VERSION}/{ig_user_id}/media"
	data: Dict[str, Any] = {"access_token": token}
	if post.caption:
		data["caption"] = post.caption
	if post.media_type.upper() in ("VIDEO", "REEL") or media_payload.get("video_url"):
		data["media_type"] = "REELS"
		data["video_url"] = media_payload.get("video_url")
	else:
		data["image_url"] = media_payload.get("image_url")
	with httpx.Client(timeout=60) as client:
		resp = client.post(url, data=data)
		resp.raise_for_status()
		return resp.json()


def _publish_container(token: str, ig_user_id: str, creation_id: str) -> Dict[str, Any]:
	url = f"https://graph.facebook.com/{GRAPH_VERSION}/{ig_user_id}/media_publish"
	payload = {"access_token": token, "creation_id": creation_id}
	with httpx.Client(timeout=30) as client:
		resp = client.post(url, data=payload)
		resp.raise_for_status()
		return resp.json()


def publish_post(post: IGScheduledPost) -> None:
	"""
	Create the media container and publish it immediately (scheduled_at controls worker selection).
	"""
	token, entity_id, _ = _get_base_token_and_id()
	media_payload = json.loads(post.media_payload_json or "{}")
	try:
		container = _post_media(token, entity_id, post, media_payload)
		creation_id = container.get("id")
		_record_audit(post.id, "create_container", "ok", payload=container)
		if not creation_id:
			raise RuntimeError("Missing creation_id from media response")
		update_post_status(post.id, status="publishing", container_id=str(creation_id))
		result = _publish_container(token, entity_id, str(creation_id))
		_record_audit(post.id, "publish", "ok", payload=result)
		update_post_status(post.id, status="published", media_id=str(result.get("id")))
	except Exception as exc:
		_record_audit(post.id, "publish", "error", payload={"error": str(exc)})
		update_post_status(post.id, status="failed", error_message=str(exc))
		raise

