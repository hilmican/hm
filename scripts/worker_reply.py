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

from app.services.ai_reply import draft_reply, _sanitize_reply_text, _select_product_images_for_reply
from app.services.instagram_api import send_message
from app.services.ai_orders import get_candidate_snapshot, submit_candidate_order, mark_candidate_very_interested
from app.models import SystemSetting, Product
from app.services.admin_notifications import create_admin_notification

log = logging.getLogger("worker.reply")
logging.basicConfig(level=logging.INFO)

# Minimum confidence threshold to auto-send AI replies (0.0-1.0)
AUTO_SEND_CONFIDENCE_THRESHOLD = float(os.getenv("AI_AUTO_SEND_CONFIDENCE", "0.49"))

ORDER_STATUS_TOOL_NAMES = {
	"create_ai_order_candidate",
	"mark_ai_order_not_interested",
	"mark_ai_order_very_interested",
	"place_ai_order_candidate",
}
CRITICAL_ORDER_STEPS = {"awaiting_payment", "awaiting_address", "confirmed_by_customer", "ready_to_ship"}


def _categorize_outbound_message(
	state: Optional[dict[str, Any]],
	function_callbacks: list[dict[str, Any]],
	reply_text: str,
) -> str:
	"""
	Categorize an outbound AI message based on conversation state and content.
	Returns one of: greeting|information|haggle|sale|address|personal_details|size|color|payment|upsell|follow_up|other
	"""
	if not state:
		state = {}
	
	state_last_step = str(state.get("last_step") or "").strip().lower()
	hail_sent = bool(state.get("hail_sent"))
	asked_payment = bool(state.get("asked_payment"))
	asked_address = bool(state.get("asked_address"))
	upsell_offered = bool(state.get("upsell_offered"))
	
	# Check function callbacks for specific actions
	tool_names = {cb.get("name") for cb in function_callbacks if isinstance(cb, dict)}
	
	# Greeting - first message, introducing product
	if not hail_sent or "greeting" in reply_text.lower()[:50] or "merhaba" in reply_text.lower()[:50]:
		return "greeting"
	
	# Upsell - explicitly offering upsell products
	if upsell_offered or "upsell" in str(tool_names).lower():
		return "upsell"
	
	# Payment - discussing payment methods
	if asked_payment or state_last_step == "awaiting_payment" or "ödeme" in reply_text.lower() or "nakit" in reply_text.lower() or "kart" in reply_text.lower():
		return "payment"
	
	# Address - asking for or confirming address
	if asked_address or state_last_step == "awaiting_address" or "adres" in reply_text.lower():
		return "address"
	
	# Personal details - asking for name, phone, etc.
	if "isim" in reply_text.lower() or "telefon" in reply_text.lower() or "numara" in reply_text.lower():
		return "personal_details"
	
	# Size - discussing sizes
	if state_last_step == "awaiting_size" or "beden" in reply_text.lower() or "boy" in reply_text.lower() or "kilo" in reply_text.lower():
		return "size"
	
	# Color - discussing colors
	if state_last_step == "awaiting_color" or "renk" in reply_text.lower():
		return "color"
	
	# Sale - confirming order, order placed
	if state_last_step in ("confirmed_by_customer", "ready_to_ship", "order_placed") or "sipariş" in reply_text.lower():
		return "sale"
	
	# Haggle - price negotiation, discount
	if "indirim" in reply_text.lower() or "ucuz" in reply_text.lower() or "fiyat" in reply_text.lower() and ("düş" in reply_text.lower() or "azalt" in reply_text.lower()):
		return "haggle"
	
	# Information - providing product info, answering questions
	if "fiyat" in reply_text.lower() or "özellik" in reply_text.lower() or "detay" in reply_text.lower() or len(reply_text) > 50:
		return "information"
	
	# Follow-up - checking in, follow-up messages
	if "nasıl" in reply_text.lower() or "durum" in reply_text.lower() or "ne zaman" in reply_text.lower():
		return "follow_up"
	
	# Default
	return "other"


def _fetch_last_inbound_message(conversation_id: int) -> Optional[dict[str, Any]]:
	with get_session() as session:
		try:
			row = session.exec(
				_text(
					"""
					SELECT id, text, timestamp_ms
					FROM message
					WHERE conversation_id=:cid AND direction='in'
					ORDER BY timestamp_ms DESC
					LIMIT 1
					"""
				).params(cid=int(conversation_id))
			).first()
		except Exception:
			return None
		if not row:
			return None
		try:
			return {
				"id": getattr(row, "id", None) if hasattr(row, "id") else (row[0] if len(row) > 0 else None),
				"text": getattr(row, "text", None) if hasattr(row, "text") else (row[1] if len(row) > 1 else None),
				"timestamp_ms": getattr(row, "timestamp_ms", None)
				if hasattr(row, "timestamp_ms")
				else (row[2] if len(row) > 2 else None),
			}
		except Exception:
			return None


def _persist_admin_notifications(
	conversation_id: int,
	callbacks: list[dict[str, Any]],
	*,
	state_snapshot: Optional[dict[str, Any]] = None,
) -> None:
	if not callbacks:
		return
	trigger_info = _fetch_last_inbound_message(conversation_id)
	for cb in callbacks:
		if not isinstance(cb, dict):
			continue
		args = cb.get("arguments") or {}
		message_text = str(args.get("mesaj") or "").strip()
		if not message_text:
			continue
		message_type = str(args.get("mesaj_tipi") or "info").strip().lower()
		if message_type not in ("info", "warning", "urgent"):
			message_type = "info"
		meta = {
			"conversation_id": int(conversation_id),
			"created_by_ai": True,
			"source": "ai_shadow",
			"auto_blocked": True,
			"trigger_message_id": trigger_info.get("id") if trigger_info else None,
			"trigger_message_text": trigger_info.get("text") if trigger_info else None,
			"trigger_message_ts": trigger_info.get("timestamp_ms") if trigger_info else None,
		}
		if state_snapshot:
			meta["state_snapshot"] = state_snapshot
		try:
			create_admin_notification(
				int(conversation_id),
				message_text,
				message_type=message_type,
				metadata=meta,
			)
		except Exception as exc:
			try:
				log.warning("admin_notification persist error cid=%s err=%s", conversation_id, exc)
			except Exception:
				pass


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
	return text


def _coerce_product_id_set(value: Any) -> set[int]:
	ids: set[int] = set()
	if value is None:
		return ids
	iterable = value if isinstance(value, (list, tuple, set)) else [value]
	for v in iterable:
		try:
			ids.add(int(v))
		except Exception:
			continue
	return ids


def _collect_image_requests(
	callbacks: list[dict[str, Any]],
	*,
	fallback_product_id: Optional[int] = None,
) -> tuple[list[str], set[int]]:
	"""
	Pick up explicit send_product_image_to_customer tool calls and resolve
	images by product/color so we can actually send them.
	"""
	urls: list[str] = []
	product_ids: set[int] = set()
	for cb in callbacks:
		if not isinstance(cb, dict):
			continue
		if cb.get("name") != "send_product_image_to_customer":
			continue
		args = cb.get("result") or cb.get("arguments") or {}
		pid_raw = args.get("product_id") or fallback_product_id
		try:
			pid = int(pid_raw) if pid_raw is not None else None
		except Exception:
			pid = None
		if not pid:
			continue
		color_val = args.get("color")
		variant_key = str(color_val).strip().lower() if color_val else None
		images = _select_product_images_for_reply(pid, variant_key=variant_key)
		if not images and fallback_product_id and fallback_product_id == pid:
			images = _select_product_images_for_reply(pid, variant_key=None)
		for img in images:
			url = img.get("url")
			if url and url not in urls:
				urls.append(url)
		product_ids.add(pid)
	return urls, product_ids


def _unwrap_reply_text(text: str) -> str:
	"""
	If the model wrapped reply_text inside a JSON blob (e.g., {"reply_text":"..."}),
	extract just the human-readable message and drop any embedded state objects.
	"""
	if not text:
		return text
	txt = str(text).strip()
	if txt.startswith("{"):
		try:
			parsed = json.loads(txt)
			if isinstance(parsed, dict):
				candidate = (
					parsed.get("reply_text")
					or parsed.get("text")
					or parsed.get("message")
					or parsed.get("content")
				)
				if isinstance(candidate, str) and candidate.strip():
					txt = candidate.strip()
		except Exception:
			# Fallback: regex extraction when JSON is malformed
			try:
				import re
				m = re.search(r'"reply_text"\s*:\s*"(.+?)"', txt, re.DOTALL)
				if m:
					extracted = m.group(1)
					if extracted:
						txt = extracted.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t")
				elif txt.lstrip().startswith('{"reply_text"'):
					# Last-resort: strip prefix even if closing quote/brace is missing
					try:
						raw = txt.split('{"reply_text"', 1)[1]
						raw = raw.lstrip(" :").lstrip('"')
						raw = raw.rstrip('}"\' \n\r\t')
						if raw:
							txt = raw.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t")
					except Exception:
						pass
			except Exception:
				pass
	try:
		import re
		state_pattern = r'["\'],?\s*"state"\s*:\s*\{'
		match = re.search(state_pattern, txt)
		if match:
			txt = txt[: match.start()].strip()
			txt = re.sub(r'["\']\s*,?\s*$', "", txt)
		if txt.startswith('"') and txt.endswith('"'):
			txt = txt[1:-1]
	except Exception:
		pass
	return txt


# Debounce delay: wait this long after last message before generating reply
# Configurable via AI_REPLY_DEBOUNCE_SECONDS env var (default: 5 seconds)
DEBOUNCE_SECONDS = int(os.getenv("AI_REPLY_DEBOUNCE_SECONDS", "15"))
POSTPONE_WINDOW_SECONDS = 15  # 1 minute
POSTPONE_MAX = 5
# Allow limited automatic retries for paused conversations (defaults to POSTPONE_MAX retries)
AUTO_RETRY_MAX = max(
	0,
	int(os.getenv("AI_REPLY_AUTO_RETRY_MAX", str(POSTPONE_MAX))),
)


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
	result: dict[str, Any] = {}
	if isinstance(raw, dict):
		result = raw
	elif isinstance(raw, str) and raw.strip():
		try:
			parsed = json.loads(raw)
			if isinstance(parsed, dict):
				result = parsed  # type: ignore[assignment]
		except Exception:
			return fallback or {}
	else:
		return fallback or {}
	
	# Ensure cart is always a list
	if "cart" not in result or not isinstance(result.get("cart"), list):
		result["cart"] = []
	
	return result


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
				admin_notification_callbacks = [
					cb
					for cb in function_callbacks
					if isinstance(cb, dict) and cb.get("name") == "yoneticiye_bildirim_gonder"
				]
				admin_escalation_requested = bool(admin_notification_callbacks)
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
				images_sent_products = _coerce_product_id_set(
					current_state.get("images_sent_product_ids") if isinstance(current_state, dict) else None
				)
				if isinstance(new_state, dict):
					images_sent_products.update(_coerce_product_id_set(new_state.get("images_sent_product_ids")))
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

				def _persist_candidate_from_state() -> bool:
					"""Best-effort fallback: if state shows order progress but no tool call happened, persist."""
					if not isinstance(new_state, dict):
						return False
					# Promote to placed if conversation reached order_placed/confirmed/ready_to_ship
					placed_like = state_last_step in ("order_placed", "ready_to_ship", "confirmed_by_customer")
					very_interested_like = asked_payment or asked_address or state_last_step in CRITICAL_ORDER_STEPS
					if not (placed_like or very_interested_like):
						return False
					# Build minimal payload from state
					order_payload: dict[str, Any] = {}
					state_cart = new_state.get("cart") if isinstance(new_state.get("cart"), list) else []
					if state_cart:
						first = state_cart[0] if isinstance(state_cart[0], dict) else {}
						product_payload = {
							"name": first.get("product_name") or first.get("name"),
							"sku": first.get("sku"),
							"color": first.get("color"),
							"size": first.get("size"),
							"quantity": first.get("quantity"),
							"unit_price": first.get("unit_price"),
						}
						product_payload = {k: v for k, v in product_payload.items() if v is not None}
						if product_payload:
							order_payload["product"] = product_payload
						# keep full cart snapshot for debugging/analytics
						order_payload["cart"] = state_cart
					customer_payload = new_state.get("customer")
					if isinstance(customer_payload, dict) and customer_payload:
						order_payload["customer"] = customer_payload
					notes_val = new_state.get("notes") or new_state.get("order_notes")
					if notes_val:
						order_payload["notes"] = notes_val
					try:
						if placed_like:
							submit_candidate_order(int(cid), order_payload or {"state": new_state}, note="auto-marked from ai_shadow state")
						else:
							mark_candidate_very_interested(int(cid), note="auto-marked from ai_shadow state")
						return True
					except Exception as persist_err:
						try:
							log.warning("ai_shadow: auto-persist candidate failed cid=%s err=%s", cid, persist_err)
						except Exception:
							pass
						return False

				if not candidate_tools_called and (asked_payment or asked_address or state_last_step in CRITICAL_ORDER_STEPS):
					auto_persisted = _persist_candidate_from_state()
					if not auto_persisted:
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
				if admin_escalation_requested:
					if not isinstance(new_state, dict):
						new_state = {}
					new_state["needs_admin"] = True
					try:
						_persist_admin_notifications(
							int(cid),
							admin_notification_callbacks,
							state_snapshot=new_state,
						)
					except Exception as notify_err:
						try:
							log.warning("admin_notification persist error cid=%s err=%s", cid, notify_err)
						except Exception:
							pass
				# Ensure cart is always a list before saving state
				if isinstance(new_state, dict):
					if "cart" not in new_state or not isinstance(new_state.get("cart"), list):
						new_state["cart"] = []
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
				if admin_escalation_requested:
					should_reply = False
					if not data.get("reason"):
						try:
							data["reason"] = "needs_admin"
						except Exception:
							pass
					try:
						extra_note = "Yöneticiye eskale edildi; AI cevap vermeyecek."
						notes_val = str(data.get("notes") or "").strip()
						data["notes"] = f"{notes_val}\n{extra_note}".strip() if notes_val else extra_note
					except Exception:
						pass
				reply_text_raw = (data.get("reply_text") or "").strip()
				# Unwrap JSON-wrapped replies and decode any literal escape sequences (e.g., \\n -> actual newline)
				reply_text_raw = _unwrap_reply_text(reply_text_raw)
				reply_text = _decode_escape_sequences(reply_text_raw)
				reply_text = _sanitize_reply_text(reply_text)
				product_images = data.get("product_images") or []
				current_focus_pid = None
				if isinstance(new_state, dict):
					try:
						current_focus_pid = int(new_state.get("current_focus_product_id"))
					except Exception:
						current_focus_pid = None
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
						reason_text = data.get("reason") or ("needs_admin" if admin_escalation_requested else "no_reply_decision")
						notes_text = data.get("notes") or ""
						if admin_escalation_requested:
							decision_text = "⚠️ AI Decision: Konuşma yöneticilere eskale edildi"
						else:
							decision_text = "🤖 AI Decision: No reply needed"
						if reason_text and reason_text not in ("no_reply_decision", "needs_admin"):
							decision_text += f"\n\nSebep: {reason_text}"
						if notes_text:
							decision_text += f"\n\nNot: {notes_text}"
						actions_json = None
						# Build json_meta with debug_meta and function_callbacks
						json_meta_no_reply = {}
						if data.get("debug_meta"):
							json_meta_no_reply.update(data.get("debug_meta"))
						if function_callbacks:
							json_meta_no_reply["function_callbacks"] = function_callbacks
						json_meta_no_reply_str = json.dumps(json_meta_no_reply, ensure_ascii=False) if json_meta_no_reply else None
						
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
									meta=json_meta_no_reply_str,
									actions=actions_json,
									state=state_json_dump,
									att=int(postpones or 0),
								)
							)
							# Mark state as paused or needs_admin based on decision
							next_status = "needs_admin" if admin_escalation_requested else "paused"
							session.exec(
								_text(
									"UPDATE ai_shadow_state SET status=:status, state_json=:state, updated_at=CURRENT_TIMESTAMP WHERE conversation_id=:cid"
								).params(status=next_status, state=state_json_dump, cid=int(cid))
							)
							# Don't change read status for shadow replies - only for actual sent messages
					except Exception as pe:
						try:
							log.warning("persist no-reply decision error cid=%s err=%s", cid, pe)
						except Exception:
							pass
					_set_status(cid, "needs_admin" if admin_escalation_requested else "paused", state_json=state_json_dump)
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
				requested_image_urls, requested_image_pids = _collect_image_requests(
					function_callbacks, fallback_product_id=current_focus_pid
				)
				auto_image_urls: list[str] = []
				auto_image_product_ids: set[int] = set()
				if image_urls and current_focus_pid and current_focus_pid not in images_sent_products:
					auto_image_urls = image_urls
					auto_image_product_ids.add(current_focus_pid)
				image_urls_combined: list[str] = []
				for u in auto_image_urls + requested_image_urls:
					if u and u not in image_urls_combined:
						image_urls_combined.append(u)
				if auto_image_urls:
					action_entry = {
						"type": "send_product_images",
						"image_count": len(auto_image_urls),
						"image_urls": auto_image_urls,
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
				if requested_image_urls:
					actions.append(
						{
							"type": "send_product_images",
							"image_count": len(requested_image_urls),
							"image_urls": requested_image_urls,
							"trigger": "tool_send_product_image",
							"product_id": current_focus_pid or None,
						}
					)
				try:
					if actions:
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
				
				# Determine whether this thread has ever had an outbound message
				is_first_outbound = False
				try:
					with get_session() as session:
						row_first = session.exec(
							_text(
								"SELECT 1 FROM message WHERE conversation_id=:cid AND direction='out' LIMIT 1"
							).params(cid=int(cid))
						).first()
						is_first_outbound = row_first is None
				except Exception:
					is_first_outbound = False
				
				first_message_override = False
				if is_first_outbound and global_enabled and product_enabled and not should_auto_send:
					first_message_override = True
					should_auto_send = True
					try:
						log.info(
							"ai_shadow: first-message auto-send override conversation_id=%s confidence=%.2f threshold=%.2f",
							cid,
							confidence,
							AUTO_SEND_CONFIDENCE_THRESHOLD,
						)
					except Exception:
						pass
				
				low_confidence_block = (
					(not should_auto_send)
					and (not first_message_override)
					and global_enabled
					and product_enabled
					and (confidence < AUTO_SEND_CONFIDENCE_THRESHOLD)
				)
				
				status_to_set = "sent" if should_auto_send else "suggested"
				
				# Get conversation graph_id or construct dm: format for sending
				conversation_id_for_send: Optional[str] = None
				graph_conversation_id: Optional[str] = None
				ig_user_id: Optional[str] = None
				conversation_username: Optional[str] = None
				conversation_name: Optional[str] = None
				conversation_last_message: Optional[str] = None
				conversation_ig_user_id: Optional[str] = None
				try:
					with get_session() as session:
						row_conv = session.exec(
							_text(
								"""
								SELECT
									c.graph_conversation_id,
									c.ig_user_id,
									c.last_sender_username,
									c.last_message_text,
									u.username AS ig_username,
									u.name AS ig_name
								FROM conversations c
								LEFT JOIN ig_users u ON u.ig_user_id = c.ig_user_id
								WHERE c.id=:cid
								LIMIT 1
								"""
							).params(cid=int(cid))
						).first()
						if row_conv:
							graph_conversation_id = (row_conv.graph_conversation_id if hasattr(row_conv, "graph_conversation_id") else (row_conv[0] if len(row_conv) > 0 else None)) or None
							ig_user_id = (row_conv.ig_user_id if hasattr(row_conv, "ig_user_id") else (row_conv[1] if len(row_conv) > 1 else None)) or None
							last_sender_username = (row_conv.last_sender_username if hasattr(row_conv, "last_sender_username") else (row_conv[2] if len(row_conv) > 2 else None)) or None
							last_message_text = (row_conv.last_message_text if hasattr(row_conv, "last_message_text") else (row_conv[3] if len(row_conv) > 3 else None)) or None
							ig_username = None
							ig_name = None
							try:
								ig_username = (
									row_conv.ig_username
									if hasattr(row_conv, "ig_username")
									else (row_conv[4] if len(row_conv) > 4 else None)
								)
							except Exception:
								ig_username = None
							try:
								ig_name = (
									row_conv.ig_name
									if hasattr(row_conv, "ig_name")
									else (row_conv[5] if len(row_conv) > 5 else None)
								)
							except Exception:
								ig_name = None
							conversation_username = str(ig_username or last_sender_username or "").strip() or None
							conversation_name = str(ig_name or "").strip() or None
							conversation_last_message = (
								str(last_message_text).strip() if isinstance(last_message_text, str) else None
							)
							conversation_ig_user_id = str(ig_user_id) if ig_user_id else None
						if graph_conversation_id:
							conversation_id_for_send = str(graph_conversation_id)
						elif ig_user_id:
							conversation_id_for_send = f"dm:{ig_user_id}"
				except Exception:
					pass
				
				# Auto-send if confidence threshold met
				sent_message_id: Optional[str] = None
				images_were_sent = False
				if should_auto_send and conversation_id_for_send:
					try:
						log.info("ai_shadow: auto-sending reply for conversation_id=%s confidence=%.2f", cid, confidence)
						# Use image URLs that were already extracted from product_images
						image_urls_to_send: list[str] = []
						if image_urls_combined:
							image_urls_to_send = image_urls_combined
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
							images_were_sent = True
							images_sent_products.update(auto_image_product_ids)
							images_sent_products.update(requested_image_pids)
							if isinstance(new_state, dict):
								try:
									new_state["images_sent_product_ids"] = sorted(images_sent_products)
								except Exception:
									new_state["images_sent_product_ids"] = list(images_sent_products)
								# Ensure cart is always a list before saving
								if "cart" not in new_state or not isinstance(new_state.get("cart"), list):
									new_state["cart"] = []
							state_json_dump = json.dumps(new_state, ensure_ascii=False) if new_state else None
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
									# Image messages (if any) come first; map text lines only to the last N ids
									text_start_idx = max(0, len(all_message_ids) - len(text_lines))
									
									# Persist each message with its corresponding text line
									for idx, msg_id in enumerate(all_message_ids):
										if not msg_id:
											continue
										is_text_msg = idx >= text_start_idx
										msg_text = (
											text_lines[idx - text_start_idx]
											if is_text_msg and (idx - text_start_idx) < len(text_lines)
											else None
										)

										# Check if message already exists
										existing = session.exec(select(Message).where(Message.ig_message_id == str(msg_id))).first()

										if existing:
											# Backfill missing text/ai_status/ai_json on webhook echoes that arrive without text
											needs_update = False
											if msg_text and not (getattr(existing, "text", None) or "").strip():
												existing.text = msg_text
												needs_update = True
											if getattr(existing, "ai_status", None) != "sent":
												existing.ai_status = "sent"
												needs_update = True
											if not getattr(existing, "ai_json", None):
												existing.ai_json = json.dumps(
													{
														"auto_sent": True,
														"confidence": confidence,
														"reason": data.get("reason"),
														"state": new_state,
														"message_index": idx,
														"total_messages": len(all_message_ids),
														"is_text": is_text_msg,
													},
													ensure_ascii=False,
												)
												needs_update = True
											if needs_update:
												session.add(existing)
											continue

										# Categorize message based on state and content
										message_category = _categorize_outbound_message(new_state, function_callbacks, msg_text)
										
										# Insert new row
										msg = Message(
											ig_sender_id=str(entity_id),
											ig_recipient_id=str(ig_user_id) if ig_user_id else None,
											ig_message_id=str(msg_id),
											text=msg_text,
											timestamp_ms=now_ms + idx,  # Slight offset to maintain order
											conversation_id=int(cid),
											direction="out",
											ai_status="sent",
											sender_type="ai",  # Explicitly mark as AI-generated
											product_id=current_focus_pid,  # Store product focus for this message
											message_category=message_category,  # Categorize message for bulk processing
											ai_json=json.dumps(
												{
													"auto_sent": True,
													"confidence": confidence,
													"reason": data.get("reason"),
													"state": new_state,
													"message_index": idx,
													"total_messages": len(all_message_ids),
													"is_text": is_text_msg,
												},
												ensure_ascii=False,
											),
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
				
				# Mark images as suggested (even if not sent) to prevent re-suggesting them
				# Only update if images weren't already marked as sent (to avoid duplicate updates)
				if (auto_image_urls or requested_image_urls) and not images_were_sent:
					images_sent_products.update(auto_image_product_ids)
					images_sent_products.update(requested_image_pids)
					if isinstance(new_state, dict):
						try:
							new_state["images_sent_product_ids"] = sorted(images_sent_products)
						except Exception:
							new_state["images_sent_product_ids"] = list(images_sent_products)
						# Ensure cart is always a list before saving
						if "cart" not in new_state or not isinstance(new_state.get("cart"), list):
							new_state["cart"] = []
					state_json_dump = json.dumps(new_state, ensure_ascii=False) if new_state else None
				
				try:
					# Build json_meta with debug_meta and function_callbacks
					json_meta_dict = {}
					if data.get("debug_meta"):
						json_meta_dict.update(data.get("debug_meta"))
					if function_callbacks:
						json_meta_dict["function_callbacks"] = function_callbacks
					json_meta_str = json.dumps(json_meta_dict, ensure_ascii=False) if json_meta_dict else None
					
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
								meta=json_meta_str,
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
						if actions_json and images_were_sent:
							session.exec(
								_text(
									"UPDATE ai_shadow_state SET ai_images_sent=1 WHERE conversation_id=:cid"
								).params(cid=int(cid))
							)
						if low_confidence_block:
							try:
								user_label_parts = []
								if conversation_username:
									user_label_parts.append(conversation_username)
								elif conversation_name:
									user_label_parts.append(conversation_name)
								elif conversation_ig_user_id:
									user_label_parts.append(f"ig:{conversation_ig_user_id}")
								thread_label = f"Konuşma #{cid}"
								if user_label_parts:
									thread_label = f"{' '.join(user_label_parts)} · {thread_label}"
								alert_text = (
									f"{thread_label}: AI otomatik gönderemedi "
									f"(güven {confidence:.2f} < {AUTO_SEND_CONFIDENCE_THRESHOLD:.2f}). "
									"Lütfen konuşmayı kontrol et."
								)
								create_admin_notification(
									int(cid),
									alert_text,
									message_type="warning",
									metadata={
										"conversation_id": int(cid),
										"kind": "low_confidence_shadow",
										"confidence": confidence,
										"threshold": AUTO_SEND_CONFIDENCE_THRESHOLD,
										"state": new_state,
										"username": conversation_username,
										"user_name": conversation_name,
										"ig_user_id": conversation_ig_user_id,
										"last_message_text": conversation_last_message,
									},
								)
							except Exception:
								try:
									log.warning("admin_notification low confidence creation error cid=%s", cid)
								except Exception:
									pass
						
						# Process admin notifications ONLY for actual sent messages (not shadow replies)
						# Check if yoneticiye_bildirim_gonder was called and message was actually sent
						if status_to_set == "sent" and should_auto_send:
							notification_callbacks = [cb for cb in function_callbacks if cb.get("name") == "yoneticiye_bildirim_gonder"]
							for notif_cb in notification_callbacks:
								try:
									notif_args = notif_cb.get("arguments", {})
									mesaj = str(notif_args.get("mesaj") or "").strip()
									mesaj_tipi = str(notif_args.get("mesaj_tipi") or "info").strip()
									if mesaj and mesaj_tipi in ["info", "warning", "urgent"]:
										create_admin_notification(
											int(cid),
											mesaj,
											message_type=mesaj_tipi,
											metadata={
												"conversation_id": cid,
												"created_by_ai": True,
												"sent_with_message": True,
											},
										)
										try:
											log.info("ai_shadow: created admin notification for conversation_id=%s (message was sent)", cid)
										except Exception:
											pass
								except Exception as notif_err:
									try:
										log.warning("admin_notification creation error cid=%s err=%s", cid, notif_err)
									except Exception:
										pass
						
						# Update unread_count ONLY for actual sent messages (not shadow replies)
						# Shadow replies (suggested status) should not change read status
						# Only when message is actually sent (status='sent' and should_auto_send=True)
						if status_to_set == "sent" and should_auto_send:
							if confidence >= AUTO_SEND_CONFIDENCE_THRESHOLD:
								# Confident reply that was actually sent: mark as read
								session.exec(
									_text(
										"UPDATE conversations SET unread_count=0 WHERE id=:cid"
									).params(cid=int(cid))
								)
								try:
									log.info("ai_shadow: marked conversation_id=%s as read (sent message, confidence=%.2f >= threshold=%.2f)", cid, confidence, AUTO_SEND_CONFIDENCE_THRESHOLD)
								except Exception:
									pass
							else:
								# Low confidence but still sent: mark as unread (needs human review)
								session.exec(
									_text(
										"UPDATE conversations SET unread_count=GREATEST(1, unread_count) WHERE id=:cid"
									).params(cid=int(cid))
								)
								try:
									log.info("ai_shadow: marked conversation_id=%s as unread (sent message, confidence=%.2f < threshold=%.2f)", cid, confidence, AUTO_SEND_CONFIDENCE_THRESHOLD)
								except Exception:
									pass
						# For shadow replies (suggested status), don't change read status
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


