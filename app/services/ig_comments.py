"""
Thin wrappers for Instagram comment moderation endpoints plus audit logging helpers.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

from ..db import get_session
from ..models import IGCommentActionLog
from .instagram_api import GRAPH_VERSION, _get_base_token_and_id

_log = logging.getLogger("instagram.comments")


def _build_url(path: str) -> str:
	return f"https://graph.facebook.com/{GRAPH_VERSION}{path}"


def _log_action(action: str, media_id: Optional[str], comment_id: str, actor_user_id: Optional[int], payload: Optional[Dict[str, Any]] = None) -> None:
	record = IGCommentActionLog(
		media_id=str(media_id) if media_id else None,
		comment_id=str(comment_id),
		action=action,
		actor_user_id=actor_user_id,
		payload_json=json.dumps(payload) if payload else None,
	)
	with get_session() as session:
		session.add(record)
		session.commit()


def fetch_comments(media_id: str, limit: int = 50, after: Optional[str] = None) -> Dict[str, Any]:
	token, _, _ = _get_base_token_and_id()
	params = {
		"access_token": token,
		"fields": "id,username,text,hidden,timestamp,like_count",
		"limit": limit,
	}
	if after:
		params["after"] = after
	url = _build_url(f"/{media_id}/comments")
	with httpx.Client(timeout=20) as client:
		resp = client.get(url, params=params)
		resp.raise_for_status()
		return resp.json()


def list_recent_media(limit: int = 25) -> List[Dict[str, Any]]:
	token, entity_id, is_page = _get_base_token_and_id()
	ig_user_id = os.getenv("IG_USER_ID")
	if not ig_user_id and not is_page:
		ig_user_id = entity_id
	if not ig_user_id:
		raise RuntimeError("IG_USER_ID is required to list media")
	params = {
		"access_token": token,
		"limit": max(1, min(limit, 100)),
		"fields": "id,caption,media_type,media_product_type,permalink,timestamp,comments_count",
	}
	url = _build_url(f"/{ig_user_id}/media")
	with httpx.Client(timeout=20) as client:
		resp = client.get(url, params=params)
		resp.raise_for_status()
		data = resp.json()
		return data.get("data", []) or []


def reply_to_comment(comment_id: str, message: str, actor_user_id: Optional[int] = None) -> Dict[str, Any]:
	token, _, _ = _get_base_token_and_id()
	url = _build_url(f"/{comment_id}/replies")
	payload = {"access_token": token, "message": message}
	with httpx.Client(timeout=20) as client:
		resp = client.post(url, data=payload)
		resp.raise_for_status()
		data = resp.json()
	_log_action("reply", media_id=None, comment_id=comment_id, actor_user_id=actor_user_id, payload={"message": message, "response": data})
	return data


def hide_comment(comment_id: str, hide: bool = True, actor_user_id: Optional[int] = None) -> Dict[str, Any]:
	token, _, _ = _get_base_token_and_id()
	url = _build_url(f"/{comment_id}")
	payload = {"access_token": token, "hide": "true" if hide else "false"}
	with httpx.Client(timeout=20) as client:
		resp = client.post(url, data=payload)
		resp.raise_for_status()
		data = resp.json()
	_log_action("hide" if hide else "unhide", media_id=None, comment_id=comment_id, actor_user_id=actor_user_id, payload={"response": data})
	return data


def delete_comment(comment_id: str, actor_user_id: Optional[int] = None) -> Dict[str, Any]:
	token, _, _ = _get_base_token_and_id()
	url = _build_url(f"/{comment_id}")
	params = {"access_token": token}
	with httpx.Client(timeout=20) as client:
		resp = client.delete(url, params=params)
		resp.raise_for_status()
		data = resp.json()
	_log_action("delete", media_id=None, comment_id=comment_id, actor_user_id=actor_user_id, payload={"response": data})
	return data


def convert_comment_to_dm(comment_id: str, actor_user_id: Optional[int] = None) -> None:
	"""
	Placeholder stub that lets us record audit entries even before the UI is hooked up.
	Actual conversion logic will live in Phase 5 when we connect comments to the inbox.
	"""
	_log_action("convert_dm", media_id=None, comment_id=comment_id, actor_user_id=actor_user_id)

