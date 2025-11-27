from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any, Dict, Optional

from sqlmodel import select

from ..db import get_session
from ..models import AdminMessage, AdminPushoverRecipient
from .pushover import send_pushover_message, is_configured as pushover_configured

log = logging.getLogger("services.admin_notifications")

VALID_TYPES = {"info", "warning", "urgent"}


def create_admin_notification(
	conversation_id: int,
	message: str,
	*,
	message_type: str = "info",
	metadata: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
	"""Persist an admin message and fan it out to configured channels."""
	if not conversation_id or not message:
		return None
	message_type = message_type.lower().strip()
	if message_type not in VALID_TYPES:
		message_type = "info"
	
	# Fetch user info (username, name) from conversation
	user_info: Dict[str, Any] = {}
	try:
		with get_session() as session:
			from sqlalchemy import text as _text
			# Get ig_user_id from conversations table
			conv_row = session.exec(
				_text("SELECT ig_user_id FROM conversations WHERE id=:cid LIMIT 1").params(cid=int(conversation_id))
			).first()
			if conv_row:
				ig_user_id = getattr(conv_row, "ig_user_id", None) if hasattr(conv_row, "ig_user_id") else (conv_row[0] if len(conv_row) > 0 else None)
				if ig_user_id:
					# Get username and name from ig_users table
					user_row = session.exec(
						_text("SELECT username, name FROM ig_users WHERE ig_user_id=:uid LIMIT 1").params(uid=str(ig_user_id))
					).first()
					if user_row:
						username = getattr(user_row, "username", None) if hasattr(user_row, "username") else (user_row[0] if len(user_row) > 0 else None)
						name = getattr(user_row, "name", None) if hasattr(user_row, "name") else (user_row[1] if len(user_row) > 1 else None)
						if username:
							user_info["username"] = str(username)
						if name:
							user_info["name"] = str(name)
						if ig_user_id:
							user_info["ig_user_id"] = str(ig_user_id)
	except Exception as e:
		log.warning("Failed to fetch user info for admin notification conversation_id=%s err=%s", conversation_id, e)
	
	# Merge user_info into metadata
	if metadata is None:
		metadata = {}
	metadata = {**metadata, **user_info}
	metadata_json = json.dumps(metadata, ensure_ascii=False) if metadata else None

	admin_msg_id: Optional[int] = None
	created_at = dt.datetime.utcnow()

	with get_session() as session:
		admin_msg = AdminMessage(
			conversation_id=int(conversation_id),
			message=message,
			message_type=message_type,
			is_read=False,
			metadata_json=metadata_json,
		)
		session.add(admin_msg)
		session.flush()
		session.refresh(admin_msg)
		admin_msg_id = admin_msg.id
		created_at = admin_msg.created_at or created_at

	alert_payload = {
		"id": admin_msg_id,
		"conversation_id": conversation_id,
		"message": message,
		"message_type": message_type,
		"created_at": created_at,
		"metadata": metadata or {},
	}

	try:
		_broadcast_pushover(alert_payload)
	except Exception as exc:
		log.warning("pushover broadcast failed msg_id=%s err=%s", admin_msg_id, exc)

	return admin_msg_id


def _load_active_recipients() -> list[Dict[str, Any]]:
	with get_session() as session:
		rows = session.exec(
			select(AdminPushoverRecipient).where(AdminPushoverRecipient.is_active == True).order_by(AdminPushoverRecipient.created_at.desc())  # noqa: E712
		).all()
	recs: list[Dict[str, Any]] = []
	for row in rows:
		try:
			recs.append(
				{
					"id": row.id,
					"label": row.label,
					"user_key": row.user_key,
				}
			)
		except Exception:
			continue
	return recs


def _build_conversation_url(conversation_id: int) -> Optional[str]:
	base = (os.getenv("APP_URL") or os.getenv("BASE_URL") or "").strip().rstrip("/")
	if not base:
		return None
	return f"{base}/ig/inbox/{conversation_id}"


def _broadcast_pushover(alert_payload: Dict[str, Any]) -> None:
	if not pushover_configured():
		return
	recipients = _load_active_recipients()
	if not recipients:
		return
	message = alert_payload.get("message") or ""
	if not message:
		return
	conversation_id = alert_payload.get("conversation_id")
	message_type = alert_payload.get("message_type", "info")
	title = f"[{message_type.upper()}] Yeni Admin Mesajı"
	url = _build_conversation_url(conversation_id) if conversation_id else None
	url_title = f"Konuşma #{conversation_id}" if conversation_id else None
	priority = 1 if message_type == "urgent" else None

	for rec in recipients:
		user_key = rec.get("user_key")
		if not user_key:
			continue
		ok = send_pushover_message(
			user_key=user_key,
			message=message,
			title=title,
			url=url,
			url_title=url_title,
			priority=priority,
		)
		if not ok:
			log.warning("pushover delivery failed recipient=%s message_id=%s", rec.get("id"), alert_payload.get("id"))

