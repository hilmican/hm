#!/usr/bin/env python3
import time
import logging
import datetime as dt
from typing import Any, Optional
import json
import os
import asyncio

from app.db import get_session
from sqlalchemy import text as _text
from sqlmodel import select

from app.services.ai_reply import draft_reply
from app.services.instagram_api import send_message
from app.services.ai_orders import get_candidate_snapshot
from app.models import SystemSetting, Product

log = logging.getLogger("worker.reply")
logging.basicConfig(level=logging.INFO)

# Minimum confidence threshold to auto-send AI replies (0.0-1.0)
AUTO_SEND_CONFIDENCE_THRESHOLD = float(os.getenv("AI_AUTO_SEND_CONFIDENCE", "0.7"))

ORDER_STATUS_TOOL_NAMES = {
	"create_ai_order_candidate",
	"mark_ai_order_not_interested",
	"mark_ai_order_very_interested",
	"place_ai_order_candidate",
}
CRITICAL_ORDER_STEPS = {"awaiting_payment", "awaiting_address", "confirmed_by_customer", "ready_to_ship"}


def _decode_escape_sequences(text: str) -> str:
	"""
	Decode literal escape sequences like \\n, \\t, etc. to actual characters.
	
	When AI returns JSON with double-escaped sequences (e.g., "text\\nhere"),
	json.loads() converts them to literal strings (e.g., "text\nhere" as literal).
	This function converts those literal escape sequences to actual characters.
	
	Also handles cases where escape sequences might be stored with literal backslashes
	(e.g., "text\\nhere" as a 4-character string: backslash, backslash, n).
	
	This handles:
	- Literal backslash-n (2 chars: '\' + 'n') -> actual newline
	- Double-escaped backslash-n (4 chars: '\\' + '\\' + 'n') -> actual newline
	- Any number of backslash layers
	"""
	if not isinstance(text, str):
		return text
	try:
		# Strategy: Use regex to find and replace all literal backslash-n sequences
		# regardless of how many backslashes there are
		import re
		
		# Pattern: one or more backslashes followed by 'n', 't', or 'r'
		# Match literal backslash sequences (not actual newlines)
		patterns = [
			(r'\\+n', '\n'),  # One or more backslashes + n -> newline
			(r'\\+t', '\t'),  # One or more backslashes + t -> tab
			(r'\\+r', '\r'),  # One or more backslashes + r -> return
		]
		
		result = text
		for pattern, replacement in patterns:
			# Replace all matches (greedy match for multiple backslashes)
			result = re.sub(pattern, replacement, result)
		
		return result
	except Exception:
		# If decoding fails, try simple replace as fallback
		try:
			result = text.replace('\\n', '\n').replace('\\t', '\t').replace('\\r', '\r')
			return result
		except Exception:
			return text


# Debounce delay: wait this long after last message before generating reply
# Configurable via AI_REPLY_DEBOUNCE_SECONDS env var (default: 5 seconds)
DEBOUNCE_SECONDS = int(os.getenv("AI_REPLY_DEBOUNCE_SECONDS", "7"))
POSTPONE_WINDOW_SECONDS = 30  # 1 minute
POSTPONE_MAX = 10
# Allow limited automatic retries for paused conversations (defaults to 1 retry)
AUTO_RETRY_MAX = max(0, int(os.getenv("AI_REPLY_AUTO_RETRY_MAX", "1")))


def _is_ai_reply_sending_enabled(conversation_id: int) -> tuple[bool, bool]:
	"""
	Check if AI reply sending is enabled globally and for the product.
	Returns (global_enabled, product_enabled).
	Shadow replies always run regardless of these settings.
	"""
	# Check global setting
	global_enabled = False
	try:
		with get_session() as session:
			setting = session.exec(
				select(SystemSetting).where(SystemSetting.key == "ai_reply_sending_enabled_global")
			).first()
			if setting:
				global_enabled = setting.value.lower() in ("true", "1", "yes")
	except Exception:
		global_enabled = False
	
	# Check product setting
	product_enabled = True  # Default to enabled
	try:
		with get_session() as session:
			# Get product from conversation's focus product
			from app.services.ai_ig import _detect_focus_product
			focus_slug, _ = _detect_focus_product(str(conversation_id))
			if focus_slug:
				prod = session.exec(
					select(Product).where(
						(Product.slug == focus_slug) | (Product.name == focus_slug)
					).limit(1)
				).first()
				if prod:
					product_enabled = getattr(prod, "ai_reply_sending_enabled", True)
	except Exception:
		product_enabled = True
	
	return global_enabled, product_enabled


def _utcnow() -> dt.datetime:
	return dt.datetime.utcnow()


def _now_ms() -> int:
	return int(_utcnow().timestamp() * 1000)


def _postpone(conversation_id: int, *, increment: bool = True) -> None:
	with get_session() as session:
		next_at = _utcnow() + dt.timedelta(seconds=POSTPONE_WINDOW_SECONDS)
		if increment:
			session.exec(
				_text(
					"UPDATE ai_shadow_state SET postpone_count=postpone_count+1, status='paused', next_attempt_at=:na, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
				).params(na=next_at.isoformat(" "), cid=int(conversation_id))
			)
		else:
			session.exec(
				_text(
					"UPDATE ai_shadow_state SET status='paused', next_attempt_at=:na, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
				).params(na=next_at.isoformat(" "), cid=int(conversation_id))
			)


def _set_status(conversation_id: int, status: str, *, state_json: Optional[str] = None) -> None:
	with get_session() as session:
		if state_json is None:
			session.exec(
				_text(
					"UPDATE ai_shadow_state SET status=:s, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
				).params(s=status, cid=int(conversation_id))
			)
		else:
			session.exec(
				_text(
					"UPDATE ai_shadow_state SET status=:s, state_json=:state, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
				).params(s=status, state=state_json, cid=int(conversation_id))
			)


def _coerce_state(raw: Any, fallback: Optional[dict[str, Any]] = None) -> dict[str, Any]:
	if isinstance(raw, dict):
		return raw
	if isinstance(raw, str) and raw.strip():
		try:
			parsed = json.loads(raw)
			if isinstance(parsed, dict):
				return parsed  # type: ignore[return-value]
		except Exception:
			return fallback or {}
	if raw is None:
		return fallback or {}
	return fallback or {}


def main() -> None:
	log.info("worker_reply starting")
	loop_count = 0
	while True:
		loop_count += 1
		# Pull due states
		due: list[dict[str, Any]] = []
		try:
			with get_session() as session:
				rows = session.exec(
					_text(
						"""
						SELECT
							conversation_id,
							last_inbound_ms,
							postpone_count,
							ai_images_sent,
							COALESCE(status,'pending') AS effective_status,
							status AS raw_status,
							state_json
						FROM ai_shadow_state
						WHERE (
								(status = 'pending' OR status IS NULL)
								AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
							)
							OR (
								status = 'paused'
								AND postpone_count > 0
								AND postpone_count <= :auto_retry_max
								AND next_attempt_at IS NOT NULL
								AND next_attempt_at <= CURRENT_TIMESTAMP
							)
						ORDER BY (next_attempt_at IS NULL) DESC, next_attempt_at ASC
						LIMIT 20
						"""
					).params(auto_retry_max=AUTO_RETRY_MAX)
				).all()
				for r in rows:
					item = {
						"conversation_id": r.conversation_id if hasattr(r, "conversation_id") else r[0],
						"last_inbound_ms": int((r.last_inbound_ms if hasattr(r, "last_inbound_ms") else r[1]) or 0),
						"postpone_count": int((r.postpone_count if hasattr(r, "postpone_count") else r[2]) or 0),
						"ai_images_sent": bool((r.ai_images_sent if hasattr(r, "ai_images_sent") else (r[3] if len(r) > 3 else 0)) or 0),
						"status": (
							r.effective_status if hasattr(r, "effective_status") else (r[4] if len(r) > 4 else None)
						) or "pending",
						"raw_status": (
							r.raw_status if hasattr(r, "raw_status") else (r[5] if len(r) > 5 else None)
						),
						"state_json": getattr(r, "state_json", None)
						if hasattr(r, "state_json")
						else (r[6] if len(r) > 6 else None),
					}
					due.append(item)
				
				# Log scan results every 20 loops (10 seconds)
				if loop_count % 20 == 0:
					total_count = 0
					try:
						# COUNT(*) returns a single integer value - access via index [0] from Row
						total_count_row = session.exec(_text("SELECT COUNT(*) FROM ai_shadow_state")).first()
						if total_count_row is not None:
							# Direct index access - SQLAlchemy Row supports this
							try:
								val = total_count_row[0]
								# Ensure we have a numeric value, not a function
								if callable(val):
									total_count = 0
								else:
									total_count = int(val or 0)
							except (TypeError, ValueError, IndexError):
								total_count = 0
					except Exception as count_err:
						# If count query fails, just use 0 - don't let it break the worker
						total_count = 0
					try:
						log.info("worker_reply: scan loop=%d found=%d due items total_in_queue=%d", loop_count, len(due), total_count)
					except Exception:
						pass
		except Exception as e:
			try:
				log.warning("scan error: %s", e)
			except Exception:
				pass
			time.sleep(0.5)
			continue

		if not due:
			time.sleep(0.5)
			continue
		
		# Reactivate paused rows that reached their auto retry window
		if AUTO_RETRY_MAX > 0:
			for st in due:
				if st.get("raw_status") == "paused":
					try:
						with get_session() as session:
							session.exec(
								_text(
									"UPDATE ai_shadow_state SET status='pending', next_attempt_at=NULL, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
								).params(cid=int(st.get("conversation_id")))
							)
						st["status"] = "pending"
					except Exception:
						pass

		log.info("worker_reply: processing %d due conversation(s)", len(due))

		for st in due:
			cid = st.get("conversation_id")
			try:
				cid = int(cid) if cid is not None else None
			except Exception:
				cid = None
			if not cid:
				continue
			# Skip rows that are still marked as error or running
			try:
				with get_session() as session:
					row = session.exec(
						_text("SELECT status FROM ai_shadow_state WHERE conversation_id=:cid").params(cid=int(cid))
					).first()
					status = row.status if row and hasattr(row, "status") else (row[0] if row else None)
			except Exception:
				status = None
			if status == "error":
				log.info("ai_shadow: skipping conversation_id=%s due to error status", cid)
				continue
			if status == "running":
				log.info("ai_shadow: skipping conversation_id=%s due to running status (another worker processing)", cid)
				continue
			# Check if we just sent a reply very recently (within last 10 seconds) to prevent duplicate sends
			try:
				with get_session() as session:
					recent_reply = session.exec(
						_text(
							"SELECT id, created_at FROM ai_shadow_reply WHERE conversation_id=:cid AND status='sent' ORDER BY created_at DESC LIMIT 1"
						).params(cid=int(cid))
					).first()
					if recent_reply:
						created_at = recent_reply.created_at if hasattr(recent_reply, "created_at") else (recent_reply[1] if len(recent_reply) > 1 else None)
						if created_at:
							if isinstance(created_at, str):
								try:
									from dateutil import parser
									created_at = parser.parse(created_at)
								except Exception:
									# If parsing fails, skip the check
									pass
							if isinstance(created_at, dt.datetime):
								# Handle timezone-aware datetimes
								if created_at.tzinfo is not None:
									created_at = created_at.replace(tzinfo=None)
								time_since = (dt.datetime.utcnow() - created_at).total_seconds()
								if time_since < 10:  # Less than 10 seconds ago
									log.info("ai_shadow: skipping conversation_id=%s - just sent reply %.1f seconds ago", cid, time_since)
									continue
			except Exception:
				# If check fails, continue processing (don't block on this)
				pass
			last_ms = int(st.get("last_inbound_ms") or 0)
			postpones = int(st.get("postpone_count") or 0)
			current_state = _coerce_state(st.get("state_json"))
			# If user likely still typing, postpone
			if last_ms > 0 and (_now_ms() - last_ms) < (DEBOUNCE_SECONDS * 1000):
				if postpones >= POSTPONE_MAX:
					_set_status(cid, "exhausted", state_json=st.get("state_json"))
					continue
				_postpone(cid, increment=True)
				continue
			# Transition to running
			try:
				with get_session() as session:
					session.exec(
						_text(
							"UPDATE ai_shadow_state SET status='running', next_attempt_at=NULL, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
						).params(cid=int(cid))
					)
			except Exception:
				continue
			# Generate draft
			try:
				try:
					log.info("ai_shadow: generating draft for conversation_id=%s", cid)
				except Exception:
					pass
				data = draft_reply(int(cid), limit=40, include_meta=True, state=current_state)
				function_callbacks = data.get("function_callbacks") or []
				if function_callbacks:
					try:
						log.info(
							"worker_reply: function_callbacks conversation_id=%s callbacks=%s",
							cid,
							function_callbacks,
						)
					except Exception:
						pass
				try:
					log.info(
						"worker_reply: draft result conversation_id=%s parsed=%s state_updates=%s",
						cid,
						data.get("parsed"),
						data.get("state"),
					)
				except Exception:
					pass
				new_state = _coerce_state(data.get("state"), fallback=current_state)
				state_last_step = ""
				asked_payment = False
				asked_address = False
				if isinstance(new_state, dict):
					state_last_step = str(new_state.get("last_step") or "").strip().lower()
					asked_payment = bool(new_state.get("asked_payment"))
					asked_address = bool(new_state.get("asked_address"))
				candidate_tools_called = [
					cb.get("name")
					for cb in function_callbacks
					if isinstance(cb, dict) and cb.get("name") in ORDER_STATUS_TOOL_NAMES
				]
				if (
					not candidate_tools_called
					and (asked_payment or asked_address or state_last_step in CRITICAL_ORDER_STEPS)
				):
					try:
						candidate_snapshot = get_candidate_snapshot(int(cid))
					except Exception as snapshot_err:
						candidate_snapshot = None
						try:
							log.warning("ai_shadow: candidate snapshot lookup failed cid=%s err=%s", cid, snapshot_err)
						except Exception:
							pass
					if candidate_snapshot is None:
						try:
							log.warning(
								"ai_shadow: missing order status tool call cid=%s last_step=%s asked_payment=%s asked_address=%s",
								cid,
								state_last_step or "unknown",
								asked_payment,
								asked_address,
							)
						except Exception:
							pass
				state_json_dump = json.dumps(new_state, ensure_ascii=False) if new_state else None
				# Block when conversation isn't linked to a product/ad
				if data.get("missing_product_context"):
					warning_text = data.get("notes") or "Konuşma ürüne/posta bağlanmadan AI çalışmaz."
					reason_text = data.get("reason") or "missing_product_context"
					meta = {"error": "missing_product_context", "product_info": data.get("product_info") or {}}
					try:
						with get_session() as session:
							session.exec(
								_text(
									"""
									INSERT INTO ai_shadow_reply(conversation_id, reply_text, model, confidence, reason, json_meta, actions_json, state_json, attempt_no, status, created_at)
									VALUES(:cid, :txt, :model, :conf, :reason, :meta, NULL, :state, :att, 'info', CURRENT_TIMESTAMP)
									"""
								).params(
									cid=int(cid),
									txt=f"⚠️ AI beklemede: {warning_text}",
									model="system",
									conf=0.0,
									reason=reason_text,
									meta=json.dumps(meta, ensure_ascii=False),
									state=state_json_dump,
									att=int(postpones or 0),
								)
							)
							session.exec(
								_text(
									"UPDATE ai_shadow_state SET status='needs_link', state_json=:state, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
								).params(state=state_json_dump, cid=int(cid))
							)
					except Exception:
						pass
					continue
				# Decide whether we should actually propose a reply
				should_reply = bool(data.get("should_reply", True))
				reply_text_raw = (data.get("reply_text") or "").strip()
				# Decode any literal escape sequences (e.g., \\n -> actual newline)
				reply_text = _decode_escape_sequences(reply_text_raw)
				product_images = data.get("product_images") or []
				try:
					# Log raw data (truncated) for debugging model behavior
					try:
						data_str = json.dumps(data, ensure_ascii=False)
					except Exception:
						data_str = str(data)
					log.info(
						"ai_shadow: debug conversation_id=%s raw_data=%s",
						cid,
						(data_str[:2000] + "...") if len(data_str) > 2000 else data_str,
					)
					log.info(
						"ai_shadow: generated draft for conversation_id=%s should_reply=%s reply_len=%s",
						cid,
						should_reply,
						len(reply_text),
					)
				except Exception:
					pass
				if (not should_reply) or (not reply_text):
					# Model indicates no need to reply yet or produced empty text -> pause suggestions
					# BUT: still log the decision for timeline visibility
					try:
						reason_text = data.get("reason") or "no_reply_decision"
						notes_text = data.get("notes") or ""
						decision_text = f"🤖 AI Decision: No reply needed"
						if reason_text and reason_text != "no_reply_decision":
							decision_text += f"\n\nSebep: {reason_text}"
						if notes_text:
							decision_text += f"\n\nNot: {notes_text}"
						actions_json = None
						with get_session() as session:
							session.exec(
								_text(
									"""
									INSERT INTO ai_shadow_reply(conversation_id, reply_text, model, confidence, reason, json_meta, actions_json, state_json, attempt_no, status, created_at)
									VALUES(:cid, :txt, :model, :conf, :reason, :meta, :actions, :state, :att, 'no_reply', CURRENT_TIMESTAMP)
									"""
								).params(
									cid=int(cid),
									txt=decision_text,
									model=str(data.get("model") or ""),
									conf=(float(data.get("confidence") or 0.6)),
									reason=reason_text,
									meta=json.dumps(data.get("debug_meta"), ensure_ascii=False) if data.get("debug_meta") else None,
									actions=actions_json,
									state=state_json_dump,
									att=int(postpones or 0),
								)
							)
							# Mark state as paused
							session.exec(
								_text(
									"UPDATE ai_shadow_state SET status='paused', state_json=:state, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
								).params(state=state_json_dump, cid=int(cid))
							)
					except Exception as pe:
						try:
							log.warning("persist no-reply decision error cid=%s err=%s", cid, pe)
						except Exception:
							pass
					_set_status(cid, "paused", state_json=state_json_dump)
					continue
				# Persist draft
				actions: list[dict[str, Any]] = []
				image_urls = []
				try:
					for img in product_images:
						if isinstance(img, dict) and img.get("url"):
							image_urls.append(str(img.get("url")))
				except Exception:
					image_urls = []
				if image_urls and not bool(st.get("ai_images_sent")):
					action_entry = {
						"type": "send_product_images",
						"image_count": len(image_urls),
						"image_urls": image_urls,
						"trigger": "first_ai_response",
					}
					try:
						debug_meta = data.get("debug_meta")
						if isinstance(debug_meta, dict):
							user_payload = debug_meta.get("user_payload")
							if isinstance(user_payload, dict):
								product_focus = user_payload.get("product_focus")
								if isinstance(product_focus, dict):
									action_entry["product_id"] = product_focus.get("id")
									action_entry["product_name"] = product_focus.get("name")
									action_entry["product_slug"] = product_focus.get("slug") or product_focus.get("slug_or_sku")
					except Exception:
						pass
					actions.append(action_entry)
					try:
						data["actions"] = actions
					except Exception:
						pass
				actions_json = json.dumps(actions, ensure_ascii=False) if actions else None
				confidence = float(data.get("confidence") or 0.6)
				
				# Check if we should auto-send this reply
				should_auto_send = confidence >= AUTO_SEND_CONFIDENCE_THRESHOLD
				
				# Check global and product settings
				global_enabled, product_enabled = _is_ai_reply_sending_enabled(cid)
				if not global_enabled or not product_enabled:
					should_auto_send = False
					if not global_enabled:
						log.info("ai_shadow: auto-send disabled globally for conversation_id=%s", cid)
					if not product_enabled:
						log.info("ai_shadow: auto-send disabled for product in conversation_id=%s", cid)
				
				status_to_set = "sent" if should_auto_send else "suggested"
				
				# Get conversation graph_id or construct dm: format for sending
				conversation_id_for_send: Optional[str] = None
				graph_conversation_id: Optional[str] = None
				ig_user_id: Optional[str] = None
				try:
					with get_session() as session:
						row_conv = session.exec(
							_text(
								"SELECT graph_conversation_id, ig_user_id FROM conversations WHERE id=:cid LIMIT 1"
							).params(cid=int(cid))
						).first()
						if row_conv:
							graph_conversation_id = (row_conv.graph_conversation_id if hasattr(row_conv, "graph_conversation_id") else (row_conv[0] if len(row_conv) > 0 else None)) or None
							ig_user_id = (row_conv.ig_user_id if hasattr(row_conv, "ig_user_id") else (row_conv[1] if len(row_conv) > 1 else None)) or None
						if graph_conversation_id:
							conversation_id_for_send = str(graph_conversation_id)
						elif ig_user_id:
							conversation_id_for_send = f"dm:{ig_user_id}"
				except Exception:
					pass
				
				# Auto-send if confidence threshold met
				sent_message_id: Optional[str] = None
				if should_auto_send and conversation_id_for_send:
					try:
						log.info("ai_shadow: auto-sending reply for conversation_id=%s confidence=%.2f", cid, confidence)
						# Use image URLs that were already extracted from product_images
						# Send images if we have them and haven't sent them before
						image_urls_to_send: list[str] = []
						if image_urls and not bool(st.get("ai_images_sent")):
							image_urls_to_send = image_urls
							log.info("ai_shadow: including %d image(s) in reply", len(image_urls_to_send))
						
						# Send message via async function
						loop = None
						try:
							loop = asyncio.get_event_loop()
						except RuntimeError:
							loop = asyncio.new_event_loop()
							asyncio.set_event_loop(loop)
						
						result = loop.run_until_complete(
							send_message(
								conversation_id=conversation_id_for_send,
								text=reply_text,
								image_urls=image_urls_to_send if image_urls_to_send else None,
							)
						)
						# Get all message IDs (send_message splits by newlines and sends multiple messages)
						all_message_ids = result.get("message_ids") or []
						if result.get("message_id") and result.get("message_id") not in all_message_ids:
							all_message_ids.insert(0, result.get("message_id"))
						sent_message_id = all_message_ids[0] if all_message_ids else None
						log.info("ai_shadow: auto-sent reply message_ids=%s conversation_id=%s images_sent=%d", all_message_ids, cid, len(image_urls_to_send) if image_urls_to_send else 0)
						
						# Mark images as sent if we actually sent any
						if image_urls_to_send:
							try:
								with get_session() as session:
									session.exec(
										_text(
											"UPDATE ai_shadow_state SET ai_images_sent=1 WHERE conversation_id=:cid"
										).params(cid=int(cid))
									)
							except Exception:
								pass
						
						# Persist ALL sent messages to Message table to prevent re-processing via webhooks
						if all_message_ids:
							try:
								with get_session() as session:
									from app.models import Message
									from sqlmodel import select
									import os
									entity_id = os.getenv("IG_PAGE_ID") or os.getenv("IG_USER_ID") or ""
									now_ms = _now_ms()
									# Split reply_text by newlines to match the messages that were sent
									text_lines = [line.strip() for line in reply_text.split('\n') if line.strip()]
									if not text_lines:
										text_lines = [reply_text.strip()]
									
									# Persist each message with its corresponding text line
									for idx, msg_id in enumerate(all_message_ids):
										if not msg_id:
											continue
										# Check if message already exists
										existing = session.exec(select(Message).where(Message.ig_message_id == str(msg_id))).first()
										if not existing:
											# Use the corresponding text line, or the full text if we have fewer lines than messages
											msg_text = text_lines[idx] if idx < len(text_lines) else (text_lines[-1] if text_lines else reply_text)
											msg = Message(
												ig_sender_id=str(entity_id),
												ig_recipient_id=str(ig_user_id) if ig_user_id else None,
												ig_message_id=str(msg_id),
												text=msg_text,
												timestamp_ms=now_ms + idx,  # Slight offset to maintain order
												conversation_id=int(cid),
												direction="out",
												ai_status="sent",
												ai_json=json.dumps({"auto_sent": True, "confidence": confidence, "reason": data.get("reason"), "state": new_state, "message_index": idx, "total_messages": len(all_message_ids)}, ensure_ascii=False),
											)
											session.add(msg)
									session.commit()
							except Exception as persist_err:
								try:
									log.warning("persist sent messages error cid=%s err=%s", cid, persist_err)
								except Exception:
									pass
					except Exception as send_err:
						try:
							log.warning("auto-send error cid=%s err=%s", cid, send_err)
						except Exception:
							pass
						# Fall back to suggested status if send fails
						status_to_set = "suggested"
						should_auto_send = False
				
				try:
					with get_session() as session:
						session.exec(
							_text(
								"""
								INSERT INTO ai_shadow_reply(conversation_id, reply_text, model, confidence, reason, json_meta, actions_json, state_json, attempt_no, status, created_at)
								VALUES(:cid, :txt, :model, :conf, :reason, :meta, :actions, :state, :att, :status, CURRENT_TIMESTAMP)
								"""
							).params(
								cid=int(cid),
								txt=reply_text,
								model=str(data.get("model") or ""),
								conf=confidence,
								reason=(data.get("reason") or "auto"),
								meta=json.dumps(data.get("debug_meta"), ensure_ascii=False) if data.get("debug_meta") else None,
								actions=actions_json,
								state=state_json_dump,
								att=int(postpones or 0),
								status=status_to_set,
							)
						)
						# Mark state as suggested or sent
						session.exec(
							_text(
								"UPDATE ai_shadow_state SET status=:s, state_json=:state, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
							).params(s=status_to_set, state=state_json_dump, cid=int(cid))
						)
						if actions_json:
							session.exec(
								_text(
									"UPDATE ai_shadow_state SET ai_images_sent=1 WHERE conversation_id=:cid"
								).params(cid=int(cid))
							)
				except Exception as pe:
					try:
						log.warning("persist draft error cid=%s err=%s", cid, pe)
					except Exception:
						pass
					_set_status(cid, "error", state_json=state_json_dump)
			except Exception as ge:
				try:
					log.warning("generate error cid=%s err=%s", cid, ge)
				except Exception:
					pass
				_set_status(cid, "error", state_json=state_json_dump if "state_json_dump" in locals() else st.get("state_json"))


if __name__ == "__main__":
	main()


