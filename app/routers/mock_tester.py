"""
Mock conversation tester router for testing AI reply system.
"""
from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import RedirectResponse, JSONResponse
from starlette.status import HTTP_303_SEE_OTHER
from typing import Optional
import json

from app.db import get_session
from app.models import Conversation, Message, IGUser
from app.services.mock_tester import (
	create_mock_conversation_from_ad,
	send_mock_message,
)
from sqlalchemy import text as _text
from sqlmodel import select

router = APIRouter(prefix="/mock-tester", tags=["mock-tester"])


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
				
				mock_conversations.append({
					"conversation_id": convo_id,
					"ig_user_id": ig_user_id,
					"username": username,
					"name": name,
					"last_message_text": last_text,
					"last_message_at": last_at,
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
		msgs = session.exec(
			select(Message)
			.where(Message.conversation_id == conversation_id)
			.order_by(Message.timestamp_ms.desc())
			.limit(min(max(limit, 1), 500))
		).all()
		msgs = list(reversed(msgs))  # chronological order
		
		# Load user info
		user = session.exec(
			select(IGUser).where(IGUser.ig_user_id == convo.ig_user_id).limit(1)
		).first()
		
		# Load shadow replies
		shadow_replies = []
		try:
			rows = session.exec(
				_text("""
					SELECT id, reply_text, model, confidence, reason, status, created_at, actions_json
					FROM ai_shadow_reply
					WHERE conversation_id = :cid
					ORDER BY created_at DESC
					LIMIT 10
				""").params(cid=conversation_id)
			).all()
			
			for row in rows:
				shadow_replies.append({
					"id": row.id if hasattr(row, "id") else row[0],
					"reply_text": row.reply_text if hasattr(row, "reply_text") else row[1],
					"model": row.model if hasattr(row, "model") else row[2],
					"confidence": row.confidence if hasattr(row, "confidence") else row[3],
					"reason": row.reason if hasattr(row, "reason") else row[4],
					"status": row.status if hasattr(row, "status") else row[5],
					"created_at": row.created_at if hasattr(row, "created_at") else row[6],
					"actions_json": row.actions_json if hasattr(row, "actions_json") else row[7],
				})
		except Exception:
			pass
		
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
					"next_attempt_at": row.next_attempt_at if hasattr(row, "next_attempt_at") else row[2],
					"postpone_count": row.postpone_count if hasattr(row, "postpone_count") else row[3],
				}
		except Exception:
			pass
		
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"ig_mock_tester.html",
			{
				"request": request,
				"conversation_id": conversation_id,
				"conversation": convo,
				"messages": msgs,
				"user": user,
				"shadow_replies": shadow_replies,
				"shadow_state": shadow_state,
			},
		)

