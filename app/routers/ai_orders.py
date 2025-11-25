from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request
from sqlalchemy import or_, text
from sqlmodel import select

from ..db import get_session
from ..models import AiOrderCandidate, Conversation, IGUser
from ..services.ai_orders import ALLOWED_STATUSES

router = APIRouter(prefix="/ai/orders", tags=["ai-orders"])


def _parse_json(raw: Optional[str]) -> Any:
	if not raw:
		return None
	try:
		return json.loads(raw)
	except Exception:
		return None


def _to_iso(value: Optional[dt.datetime]) -> Optional[str]:
	if not value:
		return None
	# Convert UTC naive datetimes to Turkey time (UTC+3) for display consistency
	if value.tzinfo is None:
		value = value + dt.timedelta(hours=3)
	else:
		value = value.astimezone(dt.timezone(dt.timedelta(hours=3)))
	return value.isoformat()


def _format_order_row(candidate: AiOrderCandidate, conversation: Optional[Conversation], ig_user: Optional[IGUser]) -> Dict[str, Any]:
	payload = _parse_json(candidate.order_payload_json) or {}
	history = _parse_json(candidate.status_history_json) or []
	username = getattr(ig_user, "username", None)
	contact_name = getattr(ig_user, "contact_name", None)
	contact_phone = getattr(ig_user, "contact_phone", None)
	return {
		"id": candidate.id,
		"conversation_id": candidate.conversation_id,
		"conversation_link": f"/ig/inbox/{candidate.conversation_id}",
		"status": candidate.status or "interested",
		"status_reason": candidate.status_reason,
		"status_history": history if isinstance(history, list) else [],
		"order_payload": payload if isinstance(payload, dict) else {},
		"username": username,
		"contact_name": contact_name,
		"contact_phone": contact_phone,
		"last_status_at": _to_iso(candidate.last_status_at),
		"placed_at": _to_iso(candidate.placed_at),
		"updated_at": _to_iso(candidate.updated_at),
		"created_at": _to_iso(candidate.created_at),
		"ad_link": getattr(conversation, "last_ad_link", None) if conversation else None,
		"graph_conversation_id": getattr(conversation, "graph_conversation_id", None) if conversation else None,
	}


@router.get("")
def list_ai_order_candidates(request: Request, status: Optional[str] = None, q: Optional[str] = None, limit: int = 200):
	n = max(1, min(int(limit or 200), 500))
	status_filter = (status or "").strip().lower() or None
	if status_filter and status_filter not in ALLOWED_STATUSES:
		status_filter = None
	query_text = (q or "").strip()
	with get_session() as session:
		stmt = (
			select(AiOrderCandidate, Conversation, IGUser)
			.join(Conversation, AiOrderCandidate.conversation_id == Conversation.id)
			.join(IGUser, IGUser.id == Conversation.ig_user_id, isouter=True)
		)
		if status_filter:
			stmt = stmt.where(AiOrderCandidate.status == status_filter)
		if query_text:
			like = f"%{query_text}%"
			stmt = stmt.where(
				or_(
					IGUser.username.ilike(like),
					IGUser.contact_name.ilike(like),
					IGUser.contact_phone.ilike(like),
				)
			)
		stmt = stmt.order_by(AiOrderCandidate.updated_at.desc()).limit(n)
		rows = session.exec(stmt).all()

		count_rows = session.exec(
			text(
				"""
				SELECT COALESCE(status, 'interested') AS st, COUNT(*) AS cnt
				FROM ai_order_candidates
				GROUP BY COALESCE(status, 'interested')
				"""
			)
		).all()
		status_counts = {
			str(getattr(row, "st", row[0])): int(getattr(row, "cnt", row[1]))
			for row in count_rows or []
		}

	orders = []
	for candidate_row, convo_row, user_row in rows:
		orders.append(_format_order_row(candidate_row, convo_row, user_row))
	templates = request.app.state.templates
	return templates.TemplateResponse(
		"ai_orders.html",
		{
			"request": request,
			"orders": orders,
			"filter_status": status_filter,
			"query": query_text,
			"status_counts": status_counts,
			"limit": n,
			"available_statuses": sorted(ALLOWED_STATUSES),
		},
	)

