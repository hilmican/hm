"""
Mock conversation tester router for testing AI reply system.
"""
import datetime as dt
import json
from typing import Optional, Union

try:
	from zoneinfo import ZoneInfo
except ImportError:  # Python < 3.9 fallback, though project uses 3.11+
	ZoneInfo = None  # type: ignore

from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy import text as _text
from sqlmodel import select
from starlette.status import HTTP_303_SEE_OTHER

from app.db import get_session
from app.models import Conversation, Message, IGUser, AiShadowReply, AdminMessage
from app.services.mock_tester import (
	create_mock_conversation_from_ad,
	send_mock_message,
)

router = APIRouter(prefix="/mock-tester", tags=["mock-tester"])

_UTC = dt.timezone.utc
if ZoneInfo:
	_UTC3 = ZoneInfo("Europe/Istanbul")
else:
	_UTC3 = dt.timezone(dt.timedelta(hours=3))


def _format_utc3(value: Union[dt.datetime, int, float, str, None]) -> Optional[str]:
	"""Return a human readable UTC+3 string for dt/timestamp inputs."""
	if value is None:
		return None
	dt_value: Optional[dt.datetime] = None

	if isinstance(value, (int, float)):
		# Assume milliseconds when value is large
		try:
			if value > 10_000_000_000:  # looks like ms
				dt_value = dt.datetime.fromtimestamp(value / 1000, tz=_UTC)
			else:
				dt_value = dt.datetime.fromtimestamp(value, tz=_UTC)
		except Exception:
			dt_value = None
	elif isinstance(value, dt.datetime):
		if value.tzinfo is None:
			dt_value = value.replace(tzinfo=_UTC)
		else:
			dt_value = value.astimezone(_UTC)
	else:
		try:
			parsed = dt.datetime.fromisoformat(str(value))
			if parsed.tzinfo is None:
				dt_value = parsed.replace(tzinfo=_UTC)
			else:
				dt_value = parsed.astimezone(_UTC)
		except Exception:
			return str(value)

	if dt_value is None:
		return str(value)

	try:
		local_dt = dt_value.astimezone(_UTC3)
	except Exception:
		local_dt = dt_value

	return local_dt.strftime("%Y-%m-%d %H:%M:%S UTC+3")


@router.get("")
async def mock_tester_index(request: Request):
	"""Main testing interface."""
	templates = request.app.state.templates
	
	# Get list of recent mock conversations for selection
	mock_conversations = []
	try:
		with get_session() as session:
			rows = session.exec(
				_text("""
					SELECT c.id, c.ig_user_id, c.last_message_text, c.last_message_at,
					       u.username, u.name, c.last_link_id, c.last_link_type
					FROM conversations c
					LEFT JOIN ig_users u ON u.ig_user_id = c.ig_user_id
					WHERE c.ig_user_id LIKE 'mock_%'
					ORDER BY c.last_message_at DESC
					LIMIT 20
				""")
			).all()
			
			for row in rows:
				convo_id = row.id if hasattr(row, "id") else row[0]
				ig_user_id = row.ig_user_id if hasattr(row, "ig_user_id") else row[1]
				last_text = row.last_message_text if hasattr(row, "last_message_text") else row[2]
				last_at = row.last_message_at if hasattr(row, "last_message_at") else row[3]
				username = row.username if hasattr(row, "username") else row[4]
				name = row.name if hasattr(row, "name") else row[5]
				link_id = row.last_link_id if hasattr(row, "last_link_id") else row[6]
				link_type = row.last_link_type if hasattr(row, "last_link_type") else row[7]
				last_at_display = _format_utc3(last_at)
				
				mock_conversations.append({
					"conversation_id": convo_id,
					"ig_user_id": ig_user_id,
					"username": username,
					"name": name,
					"last_message_text": last_text,
					"last_message_at": last_at_display,
					"link_id": link_id,
					"link_type": link_type,
				})
	except Exception as e:
		# Log error but don't fail the page
		pass
	
	return templates.TemplateResponse(
		"ig_mock_tester.html",
		{
			"request": request,
			"mock_conversations": mock_conversations,
		},
	)


@router.post("/create")
async def create_conversation(
	request: Request,
	ad_id: Optional[str] = Form(None),
	ad_link: Optional[str] = Form(None),
	ad_title: Optional[str] = Form(None),
	ad_name: Optional[str] = Form(None),
	ad_image_url: Optional[str] = Form(None),
	initial_message_text: Optional[str] = Form(None),
	username: Optional[str] = Form(None),
	name: Optional[str] = Form(None),
):
	"""Create a new mock conversation from an ad click."""
	try:
		result = create_mock_conversation_from_ad(
			ad_id=ad_id,
			ad_link=ad_link,
			ad_title=ad_title,
			ad_name=ad_name,
			ad_image_url=ad_image_url,
			initial_message_text=initial_message_text,
			username=username,
			name=name,
		)
		
		# Redirect to conversation view
		return RedirectResponse(
			url=f"/ig/mock-tester/{result['conversation_id']}",
			status_code=HTTP_303_SEE_OTHER,
		)
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Failed to create conversation: {str(e)}")


@router.post("/{conversation_id}/send")
async def send_message(
	request: Request,
	conversation_id: int,
	message_text: str = Form(...),
):
	"""Send a message as the client in an existing conversation."""
	try:
		result = send_mock_message(
			conversation_id=conversation_id,
			message_text=message_text,
		)
		
		# Return JSON for AJAX or redirect for form submission
		if request.headers.get("accept", "").startswith("application/json"):
			return JSONResponse(content=result)
		else:
			return RedirectResponse(
				url=f"/ig/mock-tester/{conversation_id}",
				status_code=HTTP_303_SEE_OTHER,
			)
	except ValueError as e:
		raise HTTPException(status_code=404, detail=str(e))
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Failed to send message: {str(e)}")


@router.get("/{conversation_id}")
async def view_conversation(request: Request, conversation_id: int, limit: int = 100):
	"""View a mock conversation thread."""
	with get_session() as session:
		# Load conversation
		convo = session.get(Conversation, conversation_id)
		if convo is None:
			raise HTTPException(status_code=404, detail="Conversation not found")
		
		# Verify it's a mock conversation
		if not convo.ig_user_id or not convo.ig_user_id.startswith("mock_"):
			raise HTTPException(status_code=400, detail="Not a mock conversation")
		
		# Load messages
		limit_val = min(max(limit, 1), 500)
		msg_rows = session.exec(
			select(Message)
			.where(Message.conversation_id == conversation_id)
			.order_by(Message.timestamp_ms.desc())
			.limit(limit_val)
		).all()
		msg_rows = list(reversed(msg_rows))  # chronological order
		messages = []
		for row in msg_rows:
			try:
				messages.append({
					"direction": row.direction or "in",
					"text": row.text or "",
					"timestamp_ms": int(row.timestamp_ms or 0),
					"ai_status": row.ai_status,
					"ai_confidence": None,
					"ai_reason": None,
				})
			except Exception:
				continue
		
		# Load user info
		user = session.exec(
			select(IGUser).where(IGUser.ig_user_id == convo.ig_user_id).limit(1)
		).first()
		
		# Load shadow replies
		shadow_replies = []
		shadow_rows = []
		try:
			shadow_rows = session.exec(
				select(AiShadowReply)
				.where(AiShadowReply.conversation_id == conversation_id)
				.order_by(AiShadowReply.created_at.desc())
				.limit(limit_val)
			).all()
			
			for row in shadow_rows:
				shadow_replies.append({
					"id": row.id,
					"reply_text": row.reply_text,
					"model": row.model,
					"confidence": row.confidence,
					"reason": row.reason,
					"status": row.status,
					"created_at": _format_utc3(row.created_at),
					"actions_json": row.actions_json,
				})
		except Exception:
			pass

		# Treat allowed shadow replies as outbound messages for mocks
		if shadow_rows:
			allowed_status = {"sent", "suggested"}
			for row in reversed(shadow_rows):
				text_val = (row.reply_text or "").strip()
				if not text_val:
					continue
				status_val = (row.status or "").lower() if row.status else None
				if status_val and status_val not in allowed_status:
					continue
				ts_ms = 0
				created_at = getattr(row, "created_at", None)
				if isinstance(created_at, dt.datetime):
					try:
						ts_ms = int(created_at.timestamp() * 1000)
					except Exception:
						ts_ms = 0
				messages.append({
					"direction": "out",
					"text": text_val,
					"timestamp_ms": ts_ms,
					"ai_status": row.status or "shadow",
					"ai_confidence": row.confidence,
					"ai_reason": row.reason,
				})
			messages.sort(key=lambda item: item.get("timestamp_ms") or 0)
		
		# Get shadow state
		shadow_state = None
		try:
			row = session.exec(
				_text("""
					SELECT status, last_inbound_ms, next_attempt_at, postpone_count
					FROM ai_shadow_state
					WHERE conversation_id = :cid
					LIMIT 1
				""").params(cid=conversation_id)
			).first()
			if row:
				shadow_state = {
					"status": row.status if hasattr(row, "status") else row[0],
					"last_inbound_ms": row.last_inbound_ms if hasattr(row, "last_inbound_ms") else row[1],
					"next_attempt_at": _format_utc3(row.next_attempt_at if hasattr(row, "next_attempt_at") else row[2]),
					"postpone_count": row.postpone_count if hasattr(row, "postpone_count") else row[3],
				}
		except Exception:
			pass
		
		# Load admin messages
		admin_messages = []
		try:
			admin_rows = session.exec(
				select(AdminMessage)
				.where(AdminMessage.conversation_id == conversation_id)
				.order_by(AdminMessage.created_at.desc())
				.limit(50)
			).all()
			for row in admin_rows:
				admin_messages.append({
					"id": row.id,
					"message": row.message,
					"message_type": row.message_type,
					"is_read": row.is_read,
					"created_at": _format_utc3(row.created_at),
				})
		except Exception:
			admin_messages = []
		
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"ig_mock_tester.html",
			{
				"request": request,
				"conversation_id": conversation_id,
				"conversation": convo,
				"messages": messages,
				"user": user,
				"shadow_replies": shadow_replies,
				"shadow_state": shadow_state,
				"admin_messages": admin_messages,
			},
		)

