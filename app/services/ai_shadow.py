from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from sqlalchemy import text as _text

from ..db import get_session
from ..services.monitoring import increment_counter


def touch_shadow_state(
	conversation_id: str | int,
	last_inbound_ms: Optional[int],
	*,
	debounce_seconds: int = 30,
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

	# Only enable AI shadow for conversations that have ad/product or post/product context
	try:
		with get_session() as session:
			# Check if conversation has ad linked to product
			row_ad = session.exec(
				_text(
					"""
					SELECT 1
					FROM conversations c
					JOIN ads_products ap ON ap.ad_id = c.last_ad_id
					WHERE c.id = :cid
					LIMIT 1
					"""
				).params(cid=cid_int)
			).first()
			
			# Check if conversation has posts linked to products
			row_post = session.exec(
				_text(
					"""
					SELECT 1
					FROM message m
					JOIN posts pst ON pst.message_id = m.id
					JOIN posts_products pp ON pp.post_id = pst.post_id
					WHERE m.conversation_id = :cid
					LIMIT 1
					"""
				).params(cid=cid_int)
			).first()
			
			if not row_ad and not row_post:
				# No connected ad/product or post/product → do not enqueue in ai_shadow_state
				return
	except Exception:
		# If this check fails, fail closed (no shadow) rather than crashing
		return

	now = dt.datetime.utcnow()
	next_at = now + dt.timedelta(seconds=max(1, int(debounce_seconds)))

	with get_session() as session:
		try:
			# Try update path first
			session.exec(
				_text(
					"""
					UPDATE ai_shadow_state
					SET last_inbound_ms=:ms,
					    next_attempt_at=:na,
					    status=CASE WHEN status='running' THEN status ELSE 'pending' END,
					    updated_at=CURRENT_TIMESTAMP
					WHERE conversation_id=:cid
					"""
				).params(ms=int(last_inbound_ms or 0), na=next_at.isoformat(" "), cid=cid_int)
			)
			# Insert if not exists
			session.exec(
				_text(
					"""
					INSERT INTO ai_shadow_state(conversation_id, last_inbound_ms, next_attempt_at, postpone_count, status, updated_at)
					SELECT :cid, :ms, :na, 0, 'pending', CURRENT_TIMESTAMP
					WHERE NOT EXISTS (SELECT 1 FROM ai_shadow_state WHERE conversation_id=:cid)
					"""
				).params(cid=cid_int, ms=int(last_inbound_ms or 0), na=next_at.isoformat(" "))
			)
		except Exception:
			# best-effort; do not propagate
			pass


def insert_draft(conversation_id: int, *, reply_text: str, model: Optional[str], confidence: Optional[float], reason: Optional[str], json_meta: Optional[str], attempt_no: int = 0, status: str = "suggested") -> int:
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
					INSERT INTO ai_shadow_reply(conversation_id, reply_text, model, confidence, reason, json_meta, attempt_no, status, created_at)
					VALUES(:cid, :txt, :m, :c, :r, :j, :a, :s, CURRENT_TIMESTAMP)
					"""
				).params(
					cid=cid_int,
					txt=str(reply_text),
					m=(model or None),
					c=(confidence if confidence is not None else None),
					r=(reason or None),
					j=(json_meta or None),
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


