from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any, Dict, Optional

from sqlmodel import select

from ..db import get_session
from ..models import AiOrderCandidate, Conversation, Message

log = logging.getLogger("ai.orders")

STATUS_INTERESTED = "interested"
STATUS_VERY_INTERESTED = "very-interested"
STATUS_NOT_INTERESTED = "not-interested"
STATUS_PLACED = "placed"

ALLOWED_STATUSES = {
	STATUS_INTERESTED,
	STATUS_VERY_INTERESTED,
	STATUS_NOT_INTERESTED,
	STATUS_PLACED,
}


def _sanitize_value(value: Any) -> Any:
	if value is None:
		return None
	if isinstance(value, (int, float, bool, str)):
		return value
	if isinstance(value, (dt.date, dt.datetime)):
		return value.isoformat()
	if isinstance(value, dict):
		return {str(k): _sanitize_value(v) for k, v in value.items()}
	if isinstance(value, (list, tuple, set)):
		return [_sanitize_value(v) for v in value]
	return str(value)


def _load_candidate(session, conversation_id: int) -> Optional[AiOrderCandidate]:
	stmt = select(AiOrderCandidate).where(AiOrderCandidate.conversation_id == int(conversation_id)).limit(1)
	return session.exec(stmt).first()


def _append_history(candidate: AiOrderCandidate, status: str, note: Optional[str], extra: Optional[Dict[str, Any]], *, ts: dt.datetime) -> None:
	history: list[dict[str, Any]] = []
	if candidate.status_history_json:
		try:
			raw = json.loads(candidate.status_history_json)
			if isinstance(raw, list):
				history = list(raw)
		except Exception:
			history = []
	entry: Dict[str, Any] = {
		"status": status,
		"note": note or None,
		"ts": ts.isoformat() + "Z",
	}
	if extra:
		entry["extra"] = extra
	history.append(entry)
	# Keep the last 50 events to avoid unbounded growth
	if len(history) > 50:
		history = history[-50:]
	candidate.status_history_json = json.dumps(history, ensure_ascii=False)


def _serialize_candidate(candidate: AiOrderCandidate) -> Dict[str, Any]:
	def _parse_json(raw: Optional[str], default: Any) -> Any:
		if not raw:
			return default
		try:
			val = json.loads(raw)
			return val if isinstance(val, type(default)) or default is None else val
		except Exception:
			return default

	return {
		"id": candidate.id,
		"conversation_id": candidate.conversation_id,
		"status": candidate.status,
		"status_reason": candidate.status_reason,
		"status_history": _parse_json(candidate.status_history_json, []),
		"order_payload": _parse_json(candidate.order_payload_json, None),
		"last_status_at": candidate.last_status_at.isoformat() if candidate.last_status_at else None,
		"placed_at": candidate.placed_at.isoformat() if candidate.placed_at else None,
		"created_at": candidate.created_at.isoformat() if candidate.created_at else None,
		"updated_at": candidate.updated_at.isoformat() if candidate.updated_at else None,
	}


def _update_candidate(conversation_id: int, status: str, *, note: Optional[str], payload: Optional[Dict[str, Any]] = None, metadata: Optional[Dict[str, Any]] = None, mark_placed: bool = False) -> Dict[str, Any]:
	if status not in ALLOWED_STATUSES:
		raise ValueError(f"Unsupported AI order candidate status: {status}")
	now = dt.datetime.utcnow()
	with get_session() as session:
		candidate = _load_candidate(session, conversation_id)
		if candidate is None:
			candidate = AiOrderCandidate(
				conversation_id=int(conversation_id),
				status=status,
				last_status_at=now,
				created_at=now,
				updated_at=now,
			)
		candidate.status = status
		candidate.status_reason = note or None
		candidate.last_status_at = now
		candidate.updated_at = now
		extra = metadata.copy() if metadata else {}
		if payload is not None:
			payload_clean = _sanitize_value(payload)
			candidate.order_payload_json = json.dumps(payload_clean, ensure_ascii=False)
			if extra is not None:
				extra["payload_keys"] = list(payload_clean.keys()) if isinstance(payload_clean, dict) else None
		if mark_placed:
			# Try to use the conversation's last_message_at as the placed_at timestamp
			# This represents when the order was actually placed in the conversation
			conversation = session.exec(
				select(Conversation).where(Conversation.id == conversation_id).limit(1)
			).first()
			if conversation and conversation.last_message_at:
				candidate.placed_at = conversation.last_message_at
			else:
				# Fallback: find the last message timestamp from the conversation
				last_message = session.exec(
					select(Message)
					.where(Message.conversation_id == conversation_id)
					.order_by(Message.timestamp_ms.desc())
					.limit(1)
				).first()
				if last_message and last_message.timestamp_ms:
					# Convert milliseconds timestamp to datetime
					candidate.placed_at = dt.datetime.utcfromtimestamp(last_message.timestamp_ms / 1000.0)
				else:
					# Final fallback: use current time (but this shouldn't happen often)
					log.warning("Could not find conversation or message timestamp for placed order, using current time. conversation_id=%s", conversation_id)
					candidate.placed_at = now
		elif status != STATUS_PLACED:
			candidate.placed_at = None
		_append_history(candidate, status, note, _sanitize_value(extra) if extra else None, ts=now)
		session.add(candidate)
		session.commit()
		session.refresh(candidate)
		log.info(
			"ai_order_candidate status=%s conversation_id=%s note=%s placed=%s",
			status,
			conversation_id,
			note,
			bool(candidate.placed_at),
		)
		return _serialize_candidate(candidate)


def get_candidate_snapshot(conversation_id: int) -> Optional[Dict[str, Any]]:
	with get_session() as session:
		candidate = _load_candidate(session, conversation_id)
		if not candidate:
			return None
		return _serialize_candidate(candidate)


def mark_candidate_interested(conversation_id: int, *, note: Optional[str] = None) -> Dict[str, Any]:
	return _update_candidate(conversation_id, STATUS_INTERESTED, note=note)


def mark_candidate_not_interested(conversation_id: int, *, note: Optional[str] = None) -> Dict[str, Any]:
	return _update_candidate(conversation_id, STATUS_NOT_INTERESTED, note=note)


def mark_candidate_very_interested(conversation_id: int, *, note: Optional[str] = None) -> Dict[str, Any]:
	return _update_candidate(conversation_id, STATUS_VERY_INTERESTED, note=note)


def submit_candidate_order(conversation_id: int, order_payload: Dict[str, Any], *, note: Optional[str] = None) -> Dict[str, Any]:
	metadata = {}
	product = order_payload.get("product") if isinstance(order_payload, dict) else None
	if isinstance(product, dict):
		metadata["product_name"] = product.get("name")
		metadata["sku"] = product.get("sku")
	customer = order_payload.get("customer") if isinstance(order_payload, dict) else None
	if isinstance(customer, dict):
		metadata["customer_name"] = customer.get("name")
	return _update_candidate(
		conversation_id,
		STATUS_PLACED,
		note=note,
		payload=order_payload,
		metadata=metadata,
		mark_placed=True,
	)

