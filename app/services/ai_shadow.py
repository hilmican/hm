from __future__ import annotations

import datetime as dt
import os
from typing import Any, Optional

from sqlalchemy import text as _text

from ..db import get_session
from ..services.monitoring import increment_counter
from .ai_ig import _detect_focus_product


def touch_shadow_state(
	conversation_id: str | int,
	last_inbound_ms: Optional[int],
	*,
	debounce_seconds: int | None = None,
) -> None:
	"""Upsert or update the shadow state for a conversation when a new inbound message arrives.

	- Uses canonical ``conversation_id`` (conversations.id INT)
	- Sets next_attempt_at to now + debounce_seconds
	- Resets status to 'pending' unless it's currently 'running'
	- Keeps existing postpone_count for ongoing conversations
	"""
	if not conversation_id:
		return

	try:
		cid_int = int(conversation_id)
	except Exception:
		# Avoid corrupting queue with non-integer ids
		return

	# Enable AI shadow for ALL conversations (removed ad/product restriction)
	# The AI can detect product focus from messages even without explicit ad/product links
	# This allows processing conversations like "Ürün hakkında detaylı bilgi alabilir miyim?"
	# where the product is detected from message context rather than ad/post links

	# Use provided debounce_seconds, or get from env var, or default to 5 seconds
	if debounce_seconds is None:
		debounce_seconds = int(os.getenv("AI_REPLY_DEBOUNCE_SECONDS", "5"))
	
	now = dt.datetime.utcnow()
	next_at = now + dt.timedelta(seconds=max(1, int(debounce_seconds)))

	with get_session() as session:
		keep_needs_link = False
		try:
			row_status = session.exec(
				_text("SELECT status FROM ai_shadow_state WHERE conversation_id=:cid LIMIT 1").params(cid=cid_int)
			).first()
			if row_status:
				current_status = getattr(row_status, "status", None) if hasattr(row_status, "status") else (row_status[0] if len(row_status) > 0 else None)
				if (current_status or "").lower() == "needs_link":
					keep_needs_link = True
					try:
						focus_slug, _ = _detect_focus_product(str(conversation_id))
						if focus_slug:
							keep_needs_link = False
					except Exception:
						pass
		except Exception:
			keep_needs_link = False
		try:
			# Try INSERT IGNORE first (creates row if doesn't exist)
			session.exec(
				_text(
					"""
					INSERT IGNORE INTO ai_shadow_state(conversation_id, last_inbound_ms, next_attempt_at, postpone_count, status, ai_images_sent, updated_at)
					VALUES(:cid, :ms, :na, 0, 'pending', 0, CURRENT_TIMESTAMP)
					"""
				).params(cid=cid_int, ms=int(last_inbound_ms or 0), na=next_at.isoformat(" "))
			)
			# Then UPDATE to refresh values (affects both new and existing rows)
			session.exec(
				_text(
					"""
					UPDATE ai_shadow_state
					SET last_inbound_ms=:ms,
					    next_attempt_at=:na,
					    postpone_count=0,
					    status=CASE
					        WHEN status='running' THEN status
					        WHEN :keep = 1 AND status='needs_link' THEN status
					        ELSE 'pending'
					    END,
					    updated_at=CURRENT_TIMESTAMP
					WHERE conversation_id=:cid
					"""
				).params(ms=int(last_inbound_ms or 0), na=next_at.isoformat(" "), cid=cid_int, keep=(1 if keep_needs_link else 0))
			)
		except Exception as e:
			# Log error but don't crash
			try:
				import logging
				logging.getLogger("ai_shadow").warning("touch_shadow_state failed for cid=%s: %s", cid_int, e)
			except Exception:
				pass


def insert_draft(
	conversation_id: int,
	*,
	reply_text: str,
	model: Optional[str],
	confidence: Optional[float],
	reason: Optional[str],
	json_meta: Optional[str],
	actions_json: Optional[str] = None,
	state_json: Optional[str] = None,
	attempt_no: int = 0,
	status: str = "suggested",
) -> int:
	if not conversation_id or not reply_text:
		return 0
	try:
		cid_int = int(conversation_id)
	except Exception:
		return 0
	with get_session() as session:
		row_id = 0
		try:
			session.exec(
				_text(
					"""
					INSERT INTO ai_shadow_reply(conversation_id, reply_text, model, confidence, reason, json_meta, actions_json, state_json, attempt_no, status, created_at)
					VALUES(:cid, :txt, :m, :c, :r, :j, :actions, :state, :a, :s, CURRENT_TIMESTAMP)
					"""
				).params(
					cid=cid_int,
					txt=str(reply_text),
					m=(model or None),
					c=(confidence if confidence is not None else None),
					r=(reason or None),
					j=(json_meta or None),
					actions=(actions_json or None),
					state=(state_json or None),
					a=int(attempt_no or 0),
					s=(status or "suggested"),
				)
			)
			# Try to obtain last insert id backend-agnostic
			try:
				backend = getattr(session.get_bind().engine.url, "get_backend_name", lambda: "")()
			except Exception:
				backend = ""
			try:
				if backend == "mysql":
					row = session.exec(_text("SELECT LAST_INSERT_ID() AS id")).first()
				else:
					row = session.exec(_text("SELECT last_insert_rowid() AS id")).first()
				if row is not None:
					row_id = int(getattr(row, "id", row[0]))
			except Exception:
				row_id = 0
		except Exception:
			row_id = 0
		# metrics
		try:
			if row_id:
				increment_counter("ai_draft", 1)
		except Exception:
			pass
		return row_id


