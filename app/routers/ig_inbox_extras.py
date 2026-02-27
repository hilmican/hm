from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel

from ..services import ig_inbox, ig_profile
from ..db import get_session
from ..models import AdminMessage
from sqlmodel import select

router = APIRouter(prefix="/inbox", tags=["instagram-inbox"])


class AssignmentPayload(BaseModel):
	user_id: int | None = None
	note: str | None = None


class OrderDraftPayload(BaseModel):
	customer_name: str | None = None
	phone: str | None = None
	address: str | None = None
	notes: str | None = None
	items: list[dict[str, str]] | None = None


@router.get("/brand-profile")
def get_brand_profile(force: bool = Query(False, description="Force refresh from Graph")):
	try:
		snapshot = ig_profile.refresh_profile_snapshot() if force else ig_profile.ensure_profile_snapshot()
	except Exception as exc:
		raise HTTPException(status_code=502, detail=f"profile_fetch_failed: {exc}")
	return {
		"username": snapshot.username,
		"name": snapshot.name,
		"profile_picture_url": snapshot.profile_picture_url,
		"followers_count": snapshot.followers_count,
		"follows_count": snapshot.follows_count,
		"media_count": snapshot.media_count,
		"biography": snapshot.biography,
		"website": snapshot.website,
		"refreshed_at": snapshot.refreshed_at.isoformat(),
	}


@router.get("/{conversation_id}/assignment")
def read_assignment(conversation_id: int):
	data = ig_inbox.get_assignment(conversation_id)
	return data or {"conversation_id": conversation_id, "user": None, "note": None}


@router.post("/{conversation_id}/assign")
def assign_conversation(conversation_id: int, payload: AssignmentPayload = Body(...)):
	try:
		return ig_inbox.set_assignment(conversation_id, payload.user_id, payload.note, actor_user_id=None)
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"assignment_failed: {exc}")


@router.get("/canned-responses")
def canned_responses(tag: str | None = Query(default=None)):
	return {"items": ig_inbox.list_canned_responses(tag=tag)}


@router.get("/{conversation_id}/order-prefill")
def order_prefill(conversation_id: int):
	try:
		return ig_inbox.build_order_prefill(conversation_id)
	except ValueError:
		raise HTTPException(status_code=404, detail="conversation_not_found")
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"prefill_error: {exc}")


@router.post("/{conversation_id}/order-drafts")
def save_order_draft(conversation_id: int, payload: OrderDraftPayload = Body(...)):
	data = {
		"customer_name": payload.customer_name,
		"phone": payload.phone,
		"address": payload.address,
		"notes": payload.notes,
		"items": payload.items or [],
	}
	try:
		return ig_inbox.save_order_draft(conversation_id, data, actor_user_id=None)
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"draft_error: {exc}")


@router.post("/admin/messages/mark-all-read")
def mark_all_admin_messages_read():
	"""Tüm okunmamış admin mesajlarını topluca okundu işaretle."""
	try:
		from sqlalchemy import text as _text
		with get_session() as session:
			count_row = session.exec(_text("SELECT COUNT(*) FROM admin_messages WHERE is_read = 0")).first()
			marked_count = int(count_row[0] or 0) if count_row else 0
			session.exec(
				_text(
					"UPDATE admin_messages SET is_read = 1, read_at = CURRENT_TIMESTAMP WHERE is_read = 0"
				)
			)
			return {"status": "ok", "marked_count": marked_count}
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"mark_all_read_error: {exc}")


@router.post("/admin/messages/{message_id}/read")
def mark_admin_message_read(message_id: int):
	"""Admin mesajını okundu olarak işaretle"""
	try:
		with get_session() as session:
			msg = session.get(AdminMessage, message_id)
			if not msg:
				raise HTTPException(status_code=404, detail="message_not_found")
			msg.is_read = True
			# TODO: read_by_user_id ve read_at'i de set et (şimdilik None bırakıyoruz)
			session.add(msg)
			session.commit()
			return {"status": "ok", "message_id": message_id}
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"mark_read_error: {exc}")

