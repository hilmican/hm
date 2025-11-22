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
from app.models import SystemSetting, Product

log = logging.getLogger("worker.reply")
logging.basicConfig(level=logging.INFO)

# Minimum confidence threshold to auto-send AI replies (0.0-1.0)
AUTO_SEND_CONFIDENCE_THRESHOLD = float(os.getenv("AI_AUTO_SEND_CONFIDENCE", "0.7"))


DEBOUNCE_SECONDS = 30
POSTPONE_WINDOW_SECONDS = 180  # 3 minutes
POSTPONE_MAX = 3


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


def _set_status(conversation_id: int, status: str) -> None:
	with get_session() as session:
		session.exec(
			_text(
				"UPDATE ai_shadow_state SET status=:s, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
			).params(s=status, cid=int(conversation_id))
		)


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
						SELECT conversation_id, last_inbound_ms, postpone_count, ai_images_sent, COALESCE(status,'pending') AS status
						FROM ai_shadow_state
						WHERE (status = 'pending' OR status IS NULL)
						  AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
						ORDER BY (next_attempt_at IS NULL) DESC, next_attempt_at ASC
						LIMIT 20
						"""
					)
				).all()
				for r in rows:
					item = {
						"conversation_id": r.conversation_id if hasattr(r, "conversation_id") else r[0],
						"last_inbound_ms": int((r.last_inbound_ms if hasattr(r, "last_inbound_ms") else r[1]) or 0),
						"postpone_count": int((r.postpone_count if hasattr(r, "postpone_count") else r[2]) or 0),
						"ai_images_sent": bool((r.ai_images_sent if hasattr(r, "ai_images_sent") else (r[3] if len(r) > 3 else 0)) or 0),
						"status": (r.status if hasattr(r, "status") else (r[4] if len(r) > 4 else None)) or "pending",
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
		
		log.info("worker_reply: processing %d due conversation(s)", len(due))

		for st in due:
			cid = st.get("conversation_id")
			try:
				cid = int(cid) if cid is not None else None
			except Exception:
				cid = None
			if not cid:
				continue
			last_ms = int(st.get("last_inbound_ms") or 0)
			postpones = int(st.get("postpone_count") or 0)
			# If user likely still typing, postpone
			if last_ms > 0 and (_now_ms() - last_ms) < (DEBOUNCE_SECONDS * 1000):
				if postpones >= POSTPONE_MAX:
					_set_status(cid, "exhausted")
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
				data = draft_reply(int(cid), limit=40, include_meta=True)
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
									INSERT INTO ai_shadow_reply(conversation_id, reply_text, model, confidence, reason, json_meta, actions_json, attempt_no, status, created_at)
									VALUES(:cid, :txt, :model, :conf, :reason, :meta, NULL, :att, 'info', CURRENT_TIMESTAMP)
									"""
								).params(
									cid=int(cid),
									txt=f"⚠️ AI beklemede: {warning_text}",
									model="system",
									conf=0.0,
									reason=reason_text,
									meta=json.dumps(meta, ensure_ascii=False),
									att=int(postpones or 0),
								)
							)
							session.exec(
								_text(
									"UPDATE ai_shadow_state SET status='needs_link', updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
								).params(cid=int(cid))
							)
					except Exception:
						pass
					continue
				# Decide whether we should actually propose a reply
				should_reply = bool(data.get("should_reply", True))
				reply_text = (data.get("reply_text") or "").strip()
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
									INSERT INTO ai_shadow_reply(conversation_id, reply_text, model, confidence, reason, json_meta, actions_json, attempt_no, status, created_at)
									VALUES(:cid, :txt, :model, :conf, :reason, :meta, :actions, :att, 'no_reply', CURRENT_TIMESTAMP)
									"""
								).params(
									cid=int(cid),
									txt=decision_text,
									model=str(data.get("model") or ""),
									conf=(float(data.get("confidence") or 0.6)),
									reason=reason_text,
									meta=json.dumps(data.get("debug_meta"), ensure_ascii=False) if data.get("debug_meta") else None,
									actions=actions_json,
									att=int(postpones or 0),
								)
							)
							# Mark state as paused
							session.exec(
								_text(
									"UPDATE ai_shadow_state SET status='paused', updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
								).params(cid=int(cid))
							)
					except Exception as pe:
						try:
							log.warning("persist no-reply decision error cid=%s err=%s", cid, pe)
						except Exception:
							pass
					_set_status(cid, "paused")
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
						sent_message_id = result.get("message_id") or (result.get("message_ids")[0] if result.get("message_ids") else None)
						log.info("ai_shadow: auto-sent reply message_id=%s conversation_id=%s images_sent=%d", sent_message_id, cid, len(image_urls_to_send) if image_urls_to_send else 0)
						
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
						
						# Persist the sent message to Message table
						if sent_message_id:
							try:
								with get_session() as session:
									from app.models import Message
									from sqlmodel import select
									# Check if message already exists
									existing = session.exec(select(Message).where(Message.ig_message_id == str(sent_message_id))).first()
									if not existing:
										# Get sender/recipient IDs
										import os
										entity_id = os.getenv("IG_PAGE_ID") or os.getenv("IG_USER_ID") or ""
										now_ms = _now_ms()
										msg = Message(
											ig_sender_id=str(entity_id),
											ig_recipient_id=str(ig_user_id) if ig_user_id else None,
											ig_message_id=str(sent_message_id),
											text=reply_text,
											timestamp_ms=now_ms,
											conversation_id=int(cid),
											direction="out",
											ai_status="sent",
											ai_json=json.dumps({"auto_sent": True, "confidence": confidence, "reason": data.get("reason")}, ensure_ascii=False),
										)
										session.add(msg)
										session.commit()
							except Exception as persist_err:
								try:
									log.warning("persist sent message error cid=%s err=%s", cid, persist_err)
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
								INSERT INTO ai_shadow_reply(conversation_id, reply_text, model, confidence, reason, json_meta, actions_json, attempt_no, status, created_at)
								VALUES(:cid, :txt, :model, :conf, :reason, :meta, :actions, :att, :status, CURRENT_TIMESTAMP)
								"""
							).params(
								cid=int(cid),
								txt=reply_text,
								model=str(data.get("model") or ""),
								conf=confidence,
								reason=(data.get("reason") or "auto"),
								meta=json.dumps(data.get("debug_meta"), ensure_ascii=False) if data.get("debug_meta") else None,
								actions=actions_json,
								att=int(postpones or 0),
								status=status_to_set,
							)
						)
						# Mark state as suggested or sent
						session.exec(
							_text(
								"UPDATE ai_shadow_state SET status=:s, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
							).params(s=status_to_set, cid=int(cid))
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
					_set_status(cid, "error")
			except Exception as ge:
				try:
					log.warning("generate error cid=%s err=%s", cid, ge)
				except Exception:
					pass
				_set_status(cid, "error")


if __name__ == "__main__":
	main()


