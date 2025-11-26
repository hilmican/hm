"""
Utility helpers to keep the inbox endpoints thin (assignments, canned responses, DM order drafts).
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, List, Optional

from sqlalchemy import or_
from sqlmodel import select

from ..db import get_session
from ..models import (
	Conversation,
	ConversationAssignment,
	IGCannedResponse,
	IGDMOrderDraft,
	User,
)


def _now() -> dt.datetime:
	return dt.datetime.utcnow()


def list_assignable_users() -> List[Dict[str, Any]]:
	with get_session() as session:
		stmt = select(User).where(User.role != "disabled").order_by(User.username.asc())
		users = session.exec(stmt).all()
		return [{"id": u.id, "username": u.username, "role": u.role} for u in users]


def get_assignment(conversation_id: int) -> Optional[Dict[str, Any]]:
	with get_session() as session:
		row = session.get(ConversationAssignment, conversation_id)
		if not row:
			return None
		user = session.get(User, row.assignee_user_id) if row.assignee_user_id else None
		return {
			"conversation_id": conversation_id,
			"user": {"id": user.id, "username": user.username} if user else None,
			"note": row.note,
			"updated_at": row.updated_at.isoformat() if row.updated_at else None,
		}


def set_assignment(conversation_id: int, assignee_user_id: Optional[int], note: Optional[str], actor_user_id: Optional[int]) -> Dict[str, Any]:
	with get_session() as session:
		row = session.get(ConversationAssignment, conversation_id)
		if row is None:
			row = ConversationAssignment(conversation_id=conversation_id)
		row.assignee_user_id = assignee_user_id
		row.note = (note or "").strip() or None
		row.updated_by_user_id = actor_user_id
		row.updated_at = _now()
		session.add(row)
		session.commit()
		session.refresh(row)
	user = None
	if assignee_user_id:
		with get_session() as session:
			user = session.get(User, assignee_user_id)
	return {
		"conversation_id": conversation_id,
		"user": {"id": user.id, "username": user.username} if user else None,
		"note": row.note,
		"updated_at": row.updated_at.isoformat() if row.updated_at else None,
	}


def _seed_canned_responses(session) -> None:
	existing = session.exec(select(IGCannedResponse).limit(1)).first()
	if existing:
		return
	samples = [
		("Ödeme Hatırlatma", "Merhaba {{customer_name}}, ödemeniz için IBAN bilgilerini tekrar paylaşıyorum: ...", "payment,reminder"),
		("Kargo Bilgisi", "Siparişiniz paketlendi! Kargo takip numaranız: {{tracking_no}}. Ortalama teslimat süresi 1-3 iş günü.", "shipping,info"),
		("Stok Bilgisi", "İstediğiniz ürün şu anda stoklarımızda mevcut. Hangi beden/renk ile devam edelim?", "inventory"),
	]
	now = _now()
	for title, body, tags in samples:
		session.add(
			IGCannedResponse(
				title=title,
				body=body,
				tags=tags,
				language="tr",
				is_active=True,
				created_at=now,
				updated_at=now,
			)
		)
	session.commit()


def list_canned_responses(tag: Optional[str] = None) -> List[Dict[str, Any]]:
	with get_session() as session:
		_seed_canned_responses(session)
		stmt = select(IGCannedResponse).where(IGCannedResponse.is_active == True)  # noqa: E712
		if tag:
			tag_like = f"%{tag.lower()}%"
			stmt = stmt.where(or_(IGCannedResponse.tags.ilike(tag_like), IGCannedResponse.title.ilike(tag_like)))
		stmt = stmt.order_by(IGCannedResponse.title.asc())
		rows = session.exec(stmt).all()
		return [
			{
				"id": row.id,
				"title": row.title,
				"body": row.body,
				"tags": (row.tags or "").split(",") if row.tags else [],
			}
			for row in rows
		]


def save_order_draft(conversation_id: int, payload: Dict[str, Any], actor_user_id: Optional[int]) -> Dict[str, Any]:
	data = IGDMOrderDraft(
		conversation_id=conversation_id,
		payload_json=json.dumps(payload),
		status="draft",
		created_by_user_id=actor_user_id,
	)
	with get_session() as session:
		session.add(data)
		session.commit()
		session.refresh(data)
	return {
		"id": data.id,
		"conversation_id": data.conversation_id,
		"status": data.status,
		"payload": payload,
	}


def build_order_prefill(conversation_id: int) -> Dict[str, Any]:
	with get_session() as session:
		convo = session.get(Conversation, conversation_id)
		if not convo:
			raise ValueError("conversation_not_found")
		contact_row = session.exec(
			select(
				Conversation.last_message_text,
			).where(Conversation.id == conversation_id)
		).first()
		prefill = {
			"conversation_id": conversation_id,
			"ig_user_id": convo.ig_user_id,
			"notes": convo.last_message_text if convo.last_message_text else (contact_row[0] if contact_row else None),
		}
		# Basic CRM fields from ig_users if present
		from sqlalchemy import text as _text
		row_user = session.exec(
			_text(
				"""
				SELECT contact_name, contact_phone, contact_address
				FROM ig_users
				WHERE ig_user_id=:u
				LIMIT 1
				"""
			).params(u=str(convo.ig_user_id))
		).first()
		if row_user:
			prefill["contact_name"] = getattr(row_user, "contact_name", None) or row_user[0]
			prefill["contact_phone"] = getattr(row_user, "contact_phone", None) or row_user[1]
			prefill["contact_address"] = getattr(row_user, "contact_address", None) or row_user[2]
		return prefill

