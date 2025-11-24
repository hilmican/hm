"""
Mock conversation tester service for simulating Instagram conversations from ad clicks.
Allows testing AI reply system without real users.
"""
import datetime as dt
import json
import os
import random
import time
from typing import Any, Dict, Optional

from sqlalchemy import text as _text

from app.db import get_session
from app.models import Conversation, IGUser, Message
from app.services.ai_shadow import touch_shadow_state
from app.services.ingest import _get_or_create_conversation_id, _update_conversation_summary_from_message


def _get_igba_id() -> str:
	"""Get the Instagram Business Account ID from environment."""
	igba_id = os.getenv("IGBA_ID") or os.getenv("IG_PAGE_ID") or os.getenv("IG_USER_ID") or ""
	if not igba_id:
		raise ValueError("IGBA_ID, IG_PAGE_ID, or IG_USER_ID must be set in environment")
	return str(igba_id)


def _generate_mock_user_id() -> str:
	"""Generate a unique mock Instagram user ID."""
	timestamp = int(time.time() * 1000)
	random_suffix = random.randint(1000, 9999)
	return f"mock_user_{timestamp}_{random_suffix}"


def _generate_mock_message_id() -> str:
	"""Generate a unique mock Instagram message ID."""
	timestamp = int(time.time() * 1000)
	random_suffix = random.randint(10000, 99999)
	return f"mock_msg_{timestamp}_{random_suffix}"


def create_mock_ig_user(ig_user_id: str, username: Optional[str] = None, name: Optional[str] = None) -> IGUser:
	"""Create or get a mock IGUser."""
	with get_session() as session:
		# Check if user already exists
		existing = session.exec(
			_text("SELECT id FROM ig_users WHERE ig_user_id = :uid LIMIT 1").params(uid=str(ig_user_id))
		).first()
		if existing:
			# Return existing user
			user = session.get(IGUser, existing.id if hasattr(existing, "id") else existing[0])
			if user:
				return user
		
		# Create new user
		username = username or f"mock_user_{random.randint(100, 999)}"
		name = name or f"Mock User {random.randint(1, 100)}"
		
		user = IGUser(
			ig_user_id=str(ig_user_id),
			username=username,
			name=name,
			fetch_status="mock",
		)
		session.add(user)
		session.commit()
		session.refresh(user)
		return user


def create_mock_conversation_from_ad(
	ad_id: Optional[str] = None,
	ad_link: Optional[str] = None,
	ad_title: Optional[str] = None,
	ad_name: Optional[str] = None,
	ad_image_url: Optional[str] = None,
	initial_message_text: Optional[str] = None,
	username: Optional[str] = None,
	name: Optional[str] = None,
) -> Dict[str, Any]:
	"""
	Create a mock conversation from an ad click.
	
	Creates:
	- Mock IGUser
	- Conversation
	- Initial message with referral data (simulating ad click)
	- Triggers shadow reply system
	
	Returns dict with conversation_id, ig_user_id, and message_id.
	"""
	igba_id = _get_igba_id()
	ig_user_id = _generate_mock_user_id()
	ig_message_id = _generate_mock_message_id()
	
	# Create mock user
	user = create_mock_ig_user(ig_user_id, username=username, name=name)
	
	# Get or create conversation
	with get_session() as session:
		conversation_id = _get_or_create_conversation_id(session, igba_id, ig_user_id)
		if not conversation_id:
			raise ValueError("Failed to create conversation")
		
		# Create referral JSON matching Instagram webhook format
		referral_data: Dict[str, Any] = {}
		if ad_id:
			referral_data["ad_id"] = str(ad_id)
			referral_data["ad_id_v2"] = str(ad_id)
		if ad_link:
			referral_data["ad_link"] = str(ad_link)
			referral_data["url"] = str(ad_link)
			referral_data["link"] = str(ad_link)
		if ad_title:
			referral_data["headline"] = str(ad_title)
			referral_data["source"] = str(ad_title)
		if ad_name:
			referral_data["name"] = str(ad_name)
			referral_data["title"] = str(ad_name)
		if ad_image_url:
			referral_data["image_url"] = str(ad_image_url)
			referral_data["thumbnail_url"] = str(ad_image_url)
		
		referral_json = json.dumps(referral_data, ensure_ascii=False) if referral_data else None
		
		# Create initial message (simulating user clicking ad and sending first message)
		timestamp_ms = int(time.time() * 1000)
		message_text = initial_message_text or "Merhaba, bu ürün hakkında bilgi alabilir miyim?"
		
		# Create message using similar pattern to _insert_message
		stmt = _text("""
			INSERT INTO message (
				ig_sender_id, ig_recipient_id, ig_message_id, text, attachments_json,
				timestamp_ms, raw_json, conversation_id, direction, sender_username,
				ad_id, ad_link, ad_title, ad_image_url, ad_name, referral_json, created_at
			) VALUES (
				:sender_id, :recipient_id, :mid, :text, NULL,
				:timestamp_ms, :raw_json, :conversation_id, 'in', :sender_username,
				:ad_id, :ad_link, :ad_title, :ad_image_url, :ad_name, :referral_json, NOW()
			)
		""").bindparams(
			sender_id=str(ig_user_id),
			recipient_id=str(igba_id),
			mid=str(ig_message_id),
			text=message_text,
			timestamp_ms=timestamp_ms,
			raw_json=json.dumps({
				"sender": {"id": str(ig_user_id)},
				"recipient": {"id": str(igba_id)},
				"message": {
					"mid": str(ig_message_id),
					"text": message_text,
					"referral": referral_data,
				},
				"timestamp": timestamp_ms,
			}, ensure_ascii=False),
			conversation_id=int(conversation_id),
			sender_username=user.username,
			ad_id=ad_id,
			ad_link=ad_link,
			ad_title=ad_title,
			ad_image_url=ad_image_url,
			ad_name=ad_name,
			referral_json=referral_json,
		)
		
		session.exec(stmt)
		session.flush()
		
		# Get the inserted message
		msg_row = session.exec(
			_text("SELECT id FROM message WHERE ig_message_id = :mid").params(mid=str(ig_message_id))
		).first()
		if not msg_row:
			raise ValueError("Failed to create message")
		
		message_id = int(msg_row.id if hasattr(msg_row, "id") else msg_row[0])
		message = session.get(Message, message_id)
		if not message:
			raise ValueError("Failed to retrieve created message")
		
		# Update conversation summary
		_update_conversation_summary_from_message(
			session,
			conversation_id,
			timestamp_ms,
			message_row=message,
			text_val=message_text,
			direction="in",
			sender_id=str(ig_user_id),
			recipient_id=str(igba_id),
			ad_id=ad_id,
			ad_link=ad_link,
			ad_title=ad_title,
		)
		
		# Trigger shadow reply system
		touch_shadow_state(
			conversation_id,
			timestamp_ms,
		)
		
		session.commit()
		
		return {
			"conversation_id": conversation_id,
			"ig_user_id": ig_user_id,
			"message_id": message_id,
			"ig_message_id": ig_message_id,
		}


def send_mock_message(
	conversation_id: int,
	message_text: str,
) -> Dict[str, Any]:
	"""
	Send a message as the client in an existing conversation.
	
	Creates an inbound message and triggers shadow reply system.
	"""
	with get_session() as session:
		# Get conversation to find ig_user_id and igba_id
		conv = session.get(Conversation, conversation_id)
		if not conv:
			raise ValueError(f"Conversation {conversation_id} not found")
		
		ig_user_id = conv.ig_user_id
		igba_id = conv.igba_id
		
		if not ig_user_id or not igba_id:
			raise ValueError("Conversation missing ig_user_id or igba_id")
		
		# Generate message ID
		ig_message_id = _generate_mock_message_id()
		timestamp_ms = int(time.time() * 1000)
		
		# Get user for username
		user = session.exec(
			_text("SELECT username FROM ig_users WHERE ig_user_id = :uid LIMIT 1").params(uid=str(ig_user_id))
		).first()
		username = (user.username if hasattr(user, "username") else (user[0] if user else None)) if user else None
		
		# Create message
		stmt = _text("""
			INSERT INTO message (
				ig_sender_id, ig_recipient_id, ig_message_id, text, attachments_json,
				timestamp_ms, raw_json, conversation_id, direction, sender_username,
				created_at
			) VALUES (
				:sender_id, :recipient_id, :mid, :text, NULL,
				:timestamp_ms, :raw_json, :conversation_id, 'in', :sender_username,
				NOW()
			)
		""").bindparams(
			sender_id=str(ig_user_id),
			recipient_id=str(igba_id),
			mid=str(ig_message_id),
			text=message_text,
			timestamp_ms=timestamp_ms,
			raw_json=json.dumps({
				"sender": {"id": str(ig_user_id)},
				"recipient": {"id": str(igba_id)},
				"message": {
					"mid": str(ig_message_id),
					"text": message_text,
				},
				"timestamp": timestamp_ms,
			}, ensure_ascii=False),
			conversation_id=int(conversation_id),
			sender_username=username,
		)
		
		session.exec(stmt)
		session.flush()
		
		# Get the inserted message
		msg_row = session.exec(
			_text("SELECT id FROM message WHERE ig_message_id = :mid").params(mid=str(ig_message_id))
		).first()
		if not msg_row:
			raise ValueError("Failed to create message")
		
		message_id = int(msg_row.id if hasattr(msg_row, "id") else msg_row[0])
		message = session.get(Message, message_id)
		if not message:
			raise ValueError("Failed to retrieve created message")
		
		# Update conversation summary
		_update_conversation_summary_from_message(
			session,
			conversation_id,
			timestamp_ms,
			message_row=message,
			text_val=message_text,
			direction="in",
			sender_id=str(ig_user_id),
			recipient_id=str(igba_id),
			ad_id=None,
			ad_link=None,
			ad_title=None,
		)
		
		# Trigger shadow reply system
		touch_shadow_state(
			conversation_id,
			timestamp_ms,
		)
		
		session.commit()
		
		return {
			"conversation_id": conversation_id,
			"message_id": message_id,
			"ig_message_id": ig_message_id,
		}

