import json
import datetime as dt
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ..db import get_session
from ..models import Message
from .queue import enqueue


def _ensure_ig_account(conn, igba_id: str) -> None:
	conn.exec_driver_sql(
		"""
		INSERT OR IGNORE INTO ig_accounts(igba_id, updated_at)
		VALUES (?, CURRENT_TIMESTAMP)
		""",
		(igba_id,),
	)


def _ensure_ig_user(conn, ig_user_id: str) -> None:
	conn.exec_driver_sql(
		"""
		INSERT OR IGNORE INTO ig_users(ig_user_id)
		VALUES (?)
		""",
		(ig_user_id,),
	)


def _upsert_conversation(conn, igba_id: str, ig_user_id: str, ts_ms: Optional[int]) -> str:
	convo_id = f"{igba_id}:{ig_user_id}"
	conn.exec_driver_sql(
		"""
		INSERT OR IGNORE INTO conversations(convo_id, igba_id, ig_user_id, last_message_at, unread_count)
		VALUES (?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), 0)
		""",
		(convo_id, igba_id, ig_user_id, dt.datetime.utcfromtimestamp((ts_ms or 0) / 1000) if ts_ms else None),
	)
	# update last_message_at if newer
	if ts_ms:
		conn.exec_driver_sql(
			"""
			UPDATE conversations
			SET last_message_at = MAX(last_message_at, ?)
			WHERE convo_id = ?
			""",
			(dt.datetime.utcfromtimestamp(ts_ms / 1000), convo_id),
		)
	return convo_id


def _insert_message(session, event: Dict[str, Any], igba_id: str) -> Optional[int]:
	message_obj = event.get("message") or {}
	if not message_obj:
		return None
	mid = message_obj.get("mid") or message_obj.get("id")
	if not mid:
		return None
	# idempotency by ig_message_id
	exists = session.exec(text("SELECT id FROM message WHERE ig_message_id = :mid").params(mid=str(mid))).first()
	if exists:
		return None
	sender_id = (event.get("sender") or {}).get("id")
	recipient_id = (event.get("recipient") or {}).get("id")
	timestamp_ms = event.get("timestamp")
	# derive direction: compare with owner/page id embedded in webhook entry id (igba_id)
	direction = "in"
	try:
		if sender_id and str(sender_id) == str(igba_id):
			direction = "out"
	except Exception:
		pass
	other_party_id = recipient_id if direction == "out" else sender_id
	conversation_id = f"dm:{other_party_id}" if other_party_id is not None else None
	text_val = message_obj.get("text")
	attachments = message_obj.get("attachments")
	row = Message(
		ig_sender_id=str(sender_id) if sender_id is not None else None,
		ig_recipient_id=str(recipient_id) if recipient_id is not None else None,
		ig_message_id=str(mid),
		text=text_val,
		attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments is not None else None,
		timestamp_ms=int(timestamp_ms) if isinstance(timestamp_ms, (int, float, str)) and str(timestamp_ms).isdigit() else None,
		raw_json=json.dumps(event, ensure_ascii=False),
		conversation_id=conversation_id,
		direction=direction,
	)
	session.add(row)
	# Flush to get DB id for attachments
	session.flush()
	return row.id  # type: ignore


def _create_attachment_stubs(session, message_id: int, mid: str, attachments: Any) -> None:
	# Normalize list of attachments
	items: List[dict] = []
	if isinstance(attachments, list):
		items = attachments
	elif isinstance(attachments, dict) and isinstance(attachments.get("data"), list):
		items = attachments.get("data") or []
	for idx, att in enumerate(items):
		kind = None
		graph_id = None
		try:
			ptype = (att.get("type") or att.get("mime_type") or "").lower()
			if "image" in ptype:
				kind = "image"
			elif "video" in ptype:
				kind = "video"
			elif "audio" in ptype:
				kind = "audio"
			else:
				kind = "file"
		except Exception:
			kind = "file"
		try:
			graph_id = att.get("id") or (att.get("payload") or {}).get("id")
		except Exception:
			graph_id = None
		session.exec(
			text(
				"""
				INSERT INTO attachments(message_id, kind, graph_id, position, fetch_status)
				VALUES (:mid, :kind, :gid, :pos, 'pending')
				"""
			).params(mid=message_id, kind=kind, gid=graph_id, pos=idx)
		)
		# enqueue media fetch by logical key ATT:<message_id>:<pos>
		enqueue("fetch_media", key=f"{message_id}:{idx}", payload={"message_id": message_id, "position": idx})


def upsert_message_from_ig_event(session, event: Dict[str, Any], igba_id: str) -> Optional[int]:
    """Insert message if missing, return internal message_id.

    This mirrors webhook/ingestion logic and enqueues media fetch where needed.
    """
    message_obj = event.get("message") or {}
    if not message_obj:
        return None
    mid = message_obj.get("mid") or message_obj.get("id") or event.get("id")
    if not mid:
        return None
    exists = session.exec(text("SELECT id FROM message WHERE ig_message_id = :mid").params(mid=str(mid))).first()
    if exists:
        row = exists
        return int(row.id if hasattr(row, "id") else row[0])
    sender_id = (event.get("from") or event.get("sender") or {}).get("id")
    recipient_id = None
    try:
        to = (event.get("to") or {}).get("data") or []
        recipient_id = to[0].get("id") if to else None
    except Exception:
        recipient_id = None
    text_val = message_obj.get("message") or message_obj.get("text")
    attachments = message_obj.get("attachments")
    ts_ms = None
    try:
        ts = event.get("created_time") or event.get("timestamp")
        if isinstance(ts, (int, float)):
            ts_ms = int(ts)
    except Exception:
        ts_ms = None
    direction = "in"
    try:
        token, entity_id, is_page = __import__("app.services.instagram_api", fromlist=["_get_base_token_and_id"]).instagram_api._get_base_token_and_id()
        owner = entity_id
        if sender_id and str(sender_id) == str(owner):
            direction = "out"
    except Exception:
        pass
    conversation_id = f"dm:{(recipient_id if direction=='out' else sender_id)}" if ((recipient_id if direction=='out' else sender_id) is not None) else None
    row = Message(
        ig_sender_id=str(sender_id) if sender_id is not None else None,
        ig_recipient_id=str(recipient_id) if recipient_id is not None else None,
        ig_message_id=str(mid),
        text=text_val,
        attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments is not None else None,
        timestamp_ms=int(ts_ms) if ts_ms is not None else None,
        raw_json=json.dumps(event, ensure_ascii=False),
        conversation_id=conversation_id,
        direction=direction,
    )
    session.add(row)
    session.flush()
    if attachments:
        _create_attachment_stubs(session, int(row.id), str(mid), attachments)  # type: ignore[arg-type]
    return int(row.id)


def handle(raw_event_id: int) -> int:
	"""Ingest one raw_event id. Return number of messages inserted."""
	inserted = 0
	with get_session() as session:
		# Load raw payload
		row = session.exec(text("SELECT id, payload FROM raw_events WHERE id = :id").params(id=raw_event_id)).first()
		if not row:
			return 0
		payload_text = row.payload if hasattr(row, "payload") else row[1]
		try:
			payload: Dict[str, Any] = json.loads(payload_text)
		except Exception:
			return 0
		entries: List[Dict[str, Any]] = payload.get("entry", [])
		with session.get_bind().begin() as conn:  # type: ignore
			for entry in entries:
				igba_id = str(entry.get("id")) if entry.get("id") is not None else ""
				if not igba_id:
					continue
				_ensure_ig_account(conn, igba_id)
				# Collect messaging events possibly nested
				messaging_events: List[Dict[str, Any]] = entry.get("messaging") or []
				if not messaging_events and entry.get("changes"):
					for change in entry.get("changes", []):
						val = change.get("value") or {}
						if isinstance(val, dict) and val.get("messaging"):
							messaging_events.extend(val.get("messaging", []))
				for event in messaging_events:
					message_obj = event.get("message") or {}
					if not message_obj or message_obj.get("is_echo") or message_obj.get("is_deleted"):
						continue
					sender_id = (event.get("sender") or {}).get("id")
					if sender_id:
						_ensure_ig_user(conn, str(sender_id))
					mid = message_obj.get("mid") or message_obj.get("id")
					# ensure conversation row exists/updated and possibly enqueue hydration
					try:
						other_party_id = (event.get("recipient") or {}).get("id") if ((event.get("sender") or {}).get("id") == igba_id) else (event.get("sender") or {}).get("id")
						_upsert_conversation(conn, igba_id, str(other_party_id), event.get("timestamp"))
						# one-time hydration enqueue if not hydrated
						cid = f"{igba_id}:{str(other_party_id)}"
						row_h = session.exec(text("SELECT hydrated_at FROM conversations WHERE convo_id=:cid").params(cid=cid)).first()
						need_hydrate = (not row_h) or (not (row_h.hydrated_at if hasattr(row_h, 'hydrated_at') else (row_h[0] if isinstance(row_h,(list,tuple)) else None)))
						if need_hydrate:
							enqueue("hydrate_conversation", key=cid, payload={"igba_id": str(igba_id), "ig_user_id": str(other_party_id), "max_messages": 200})
					except Exception:
						pass
					msg_id = _insert_message(session, event, igba_id)
					if msg_id:
						inserted += 1
						attachments = message_obj.get("attachments")
						if attachments:
							_create_attachment_stubs(session, msg_id, str(mid), attachments)
						# enrichers (idempotent via jobs table uniqueness)
						if sender_id:
							enqueue("enrich_user", key=str(sender_id), payload={"ig_user_id": str(sender_id)})
						enqueue("enrich_page", key=str(igba_id), payload={"igba_id": str(igba_id)})
	return inserted


