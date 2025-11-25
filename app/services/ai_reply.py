from __future__ import annotations

import datetime as dt
import logging
import json
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlmodel import select

from ..db import get_session
from ..models import (
	Message,
	Product,
	Item,
	Conversation,
	IGUser,
	AIPretext,
	ProductImage,
	AiShadowReply,
)
from .ai import (
	AIClient,
	get_ai_shadow_model_from_settings,
	get_shadow_temperature_setting,
	is_shadow_temperature_opt_out,
)
from .ai_context import VariantExclusions, parse_variant_exclusions, variant_is_excluded
from .ai_ig import _detect_focus_product
from .ai_utils import parse_height_weight, calculate_size_suggestion, detect_color_count
from .prompts import get_global_system_prompt


MAX_AI_IMAGES_PER_REPLY = int(os.getenv("AI_MAX_PRODUCT_IMAGES", "3"))
log = logging.getLogger("ai.reply")


def _log_function_callback(
	conversation_id: int,
	name: str,
	arguments: Dict[str, Any],
	result: Dict[str, Any],
) -> None:
	try:
		log.info(
			"ai_function_callback conversation_id=%s name=%s arguments=%s result=%s",
			conversation_id,
			name,
			arguments,
			result,
		)
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
		# Convert string to bytes representation to see actual bytes
		# This helps us understand what's really stored
		text_bytes = text.encode('utf-8', errors='ignore')
		
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


def _format_transcript(messages: List[Dict[str, Any]], max_chars: int = 16000) -> str:
	parts: List[str] = []
	for m in messages:
		role = (m.get("direction") or "in").lower()
		ts = int(m.get("timestamp_ms") or 0)
		txt = (m.get("text") or "").strip()
		parts.append(f"[{role}] {ts}: {txt}")
	out = "\n".join(parts)
	return out[-max_chars:] if len(out) > max_chars else out


def _shadow_system_prompt(base_extra: Optional[str] = None) -> str:
	"""
	Return the system prompt for shadow replies.

	For this flow we intentionally **do not** hard-code any system text in code:
	the caller is expected to provide a per-product system message
	(`Product.ai_system_msg`). If it is missing, we fall back to an empty
	system prompt and rely entirely on the prompt/user message.

	That per-product system message should describe things like:
	- role/voice of the assistant
	- JSON output schema (e.g. should_reply, reply_text, confidence, reason, notes)
	- high-level behavior rules
	"""
	return base_extra or ""


def _load_focus_product_and_stock(conversation_id: int) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
	"""
	Resolve focus product (if any) and build a stock snapshot for that product.

	Returns (product_info, stock_list) where:
	- product_info: {"id": int|None, "name": str|None, "slug_or_sku": str|None, "confidence": float}
	- stock_list: [{"sku":..., "name":..., "color":..., "size":..., "price":...}, ...]
	"""
	focus_slug, focus_conf = _detect_focus_product(str(conversation_id))
	product_info: Optional[Dict[str, Any]] = None
	stock: List[Dict[str, Any]] = []
	product_default_price: Optional[float] = None
	variant_exclusions: VariantExclusions = VariantExclusions()
	if not focus_slug:
		return None, stock
	with get_session() as session:
		# Try to resolve by SKU first
		try:
			rowi = session.exec(
				select(Item).where(Item.sku == str(focus_slug)).limit(1)
			).first()
		except Exception:
			rowi = None
		pid: Optional[int] = None
		if rowi:
			try:
				pid = int(rowi.product_id) if rowi.product_id is not None else None
			except Exception:
				pid = None
			# seed stock with this item
			try:
				stock.append(
					{
						"sku": rowi.sku,
						"name": rowi.name,
						"color": rowi.color,
						"size": rowi.size,
						"price": rowi.price,
					}
				)
			except Exception:
				pass
		# If no product id yet, resolve Product by slug or name
		if pid is None:
			try:
				rowp = session.exec(
					select(Product).where((Product.slug == str(focus_slug)) | (Product.name == str(focus_slug))).limit(1)
				).first()
			except Exception:
				rowp = None
			if rowp:
				try:
					pid = int(rowp.id) if rowp.id is not None else None
				except Exception:
					pid = None
		# Load siblings / variants for that product id
		if pid is not None:
			try:
				rows_it = session.exec(
					select(Item).where(Item.product_id == pid).limit(200)
				).all()
			except Exception:
				rows_it = []
			for r in rows_it:
				try:
					sku2 = r.sku
					if not isinstance(sku2, str):
						continue
					if any(it.get("sku") == sku2 for it in stock):
						continue
					stock.append(
						{
							"sku": sku2,
							"name": r.name,
							"color": r.color,
							"size": r.size,
							"price": r.price,
						}
					)
				except Exception:
					continue
		# Build product_info including optional name/id from Product
		p_name: Optional[str] = None
		p_id_val: Optional[int] = pid
		p_slug: Optional[str] = None
		try:
			if pid is not None:
				rowp2 = session.exec(select(Product).where(Product.id == pid).limit(1)).first()
			else:
				rowp2 = session.exec(
					select(Product).where((Product.slug == str(focus_slug)) | (Product.name == str(focus_slug))).limit(1)
				).first()
		except Exception:
			rowp2 = None
		if rowp2:
			try:
				p_id_val = int(rowp2.id) if rowp2.id is not None else p_id_val
			except Exception:
				pass
			try:
				p_name = rowp2.name
			except Exception:
				pass
			try:
				p_slug = rowp2.slug
			except Exception:
				pass
			try:
				product_default_price = float(rowp2.default_price) if rowp2.default_price is not None else None
			except Exception:
				product_default_price = None
			try:
				variant_exclusions = parse_variant_exclusions(getattr(rowp2, "ai_variant_exclusions", None))
			except Exception:
				variant_exclusions = VariantExclusions()
		product_info = {
			"id": p_id_val,
			"name": p_name,
			"slug_or_sku": focus_slug,
			"slug": p_slug,
			"confidence": float(focus_conf or 0.0),
		}
		if product_default_price is not None:
			for entry in stock:
				try:
					entry["price"] = product_default_price
				except Exception:
					continue
		if not variant_exclusions.is_empty():
			filtered: List[Dict[str, Any]] = []
			for entry in stock:
				if variant_is_excluded(variant_exclusions, entry.get("color"), entry.get("size")):
					continue
				filtered.append(entry)
			stock = filtered
		if not stock:
			stock.append(
				{
					"sku": (product_info.get("slug_or_sku") if product_info else focus_slug) or f"product:{p_id_val or ''}",
					"name": product_info.get("name") if product_info else None,
					"color": None,
					"size": None,
					"price": product_default_price,
				}
			)
	return product_info, stock


def _guess_variant_key_from_message(
    stock: List[Dict[str, Any]], last_customer_message: str
) -> Optional[str]:
	"""
	Heuristic: if last customer message contains exactly one of the known
	color values in stock, treat that as the preferred variant_key.
	"""
	text = (last_customer_message or "").strip().lower()
	if not text or not stock:
		return None
	colors: set[str] = set()
	for entry in stock:
		try:
			c = (entry.get("color") or "").strip().lower()
		except Exception:
			c = ""
		if c:
			colors.add(c)
	if not colors:
		return None
	matches = [c for c in colors if c in text]
	if len(matches) == 1:
		return matches[0]
	return None


def _select_product_images_for_reply(
	product_id: Optional[int],
	*,
	variant_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
	"""
	Fetch product images marked for AI, optionally preferring a given variant_key.

	- Always filters by product_id and ai_send = true.
	- If variant_key is provided, prefers images with that key but still allows
	  generic images (variant_key is NULL).
	- Applies ai_send_order first, then position, then id.
	"""
	if not product_id:
		return []

	from sqlmodel import select as _select
	from sqlalchemy import case, or_  # type: ignore[import]

	out: List[Dict[str, Any]] = []
	with get_session() as session:
		stmt = _select(ProductImage).where(
			ProductImage.product_id == int(product_id),
			ProductImage.ai_send == True,  # noqa: E712
		)
		if variant_key:
			vk = variant_key.strip().lower()
			stmt = stmt.where(
				or_(
					ProductImage.variant_key.is_(None),
					ProductImage.variant_key == vk,
				)
			)
		order_nulls = case((ProductImage.ai_send_order.is_(None), 1), else_=0)
		stmt = stmt.order_by(
			order_nulls.asc(),
			ProductImage.ai_send_order.asc(),
			ProductImage.position.asc(),
			ProductImage.id.asc(),
		)
		rows = session.exec(stmt).all()
		
		# Materialize results while session is still active
		for img in rows:
			if len(out) >= MAX_AI_IMAGES_PER_REPLY:
				break
			if not img.url:
				continue
			out.append(
				{
					"id": img.id,
					"url": img.url,
					"variant_key": img.variant_key,
				}
			)
	return out


def _load_customer_info(conversation_id: int) -> Dict[str, Any]:
	"""Load customer information (username, name, contact_name) from IGUser."""
	customer_info = {
		"username": None,
		"name": None,
		"contact_name": None,
	}
	try:
		with get_session() as session:
			# Get conversation to find ig_user_id
			conv = session.exec(
				select(Conversation).where(Conversation.id == int(conversation_id)).limit(1)
			).first()
			if conv and conv.ig_user_id:
				# Get IGUser info
				ig_user = session.exec(
					select(IGUser).where(IGUser.ig_user_id == str(conv.ig_user_id)).limit(1)
				).first()
				if ig_user:
					customer_info["username"] = ig_user.username
					customer_info["name"] = ig_user.name
					customer_info["contact_name"] = ig_user.contact_name
	except Exception:
		pass
	return customer_info


def _load_history(conversation_id: int, *, limit: int = 40) -> Tuple[List[Dict[str, Any]], str]:
	"""
	Load recent messages for this conversation and return (history_list, last_customer_message).
	
	history_list: [{"dir": "in|out", "text": "str", "timestamp_ms": int}, ...]
	"""
	history_from_messages: List[Dict[str, Any]] = []
	last_customer_message = ""
	last_customer_idx: Optional[int] = None
	is_mock_conversation = False
	shadow_rows: List[AiShadowReply] = []
	mock_outbound: List[Dict[str, Any]] = []
	limit_val = max(1, min(limit, 100))

	def _dt_to_ms(value: Any) -> int:
		if isinstance(value, (int, float)):
			return int(value)
		if isinstance(value, dt.datetime):
			try:
				return int(value.timestamp() * 1000)
			except Exception:
				return 0
		return 0

	with get_session() as session:
		try:
			conv_row = session.exec(
				select(Conversation.ig_user_id)
				.where(Conversation.id == int(conversation_id))
				.limit(1)
			).first()
			if conv_row:
				if isinstance(conv_row, str):
					ig_user_id = conv_row
				else:
					ig_user_id = (
						conv_row.ig_user_id
						if hasattr(conv_row, "ig_user_id")
						else (conv_row[0] if len(conv_row) > 0 else None)
					)
				if ig_user_id and str(ig_user_id).startswith("mock_"):
					is_mock_conversation = True
		except Exception:
			is_mock_conversation = False

		msgs = (
			session.exec(
				select(Message)
				.where(Message.conversation_id == int(conversation_id))
				.order_by(Message.timestamp_ms.asc())
				.limit(limit_val)
			).all()
		)

		if is_mock_conversation:
			try:
				shadow_query = (
					select(AiShadowReply)
					.where(AiShadowReply.conversation_id == int(conversation_id))
					.where(
						(AiShadowReply.status.is_(None))
						| (AiShadowReply.status.in_(("sent", "suggested")))
					)
					.order_by(AiShadowReply.created_at.asc())
					.limit(limit_val)
				)
				shadow_rows = session.exec(shadow_query).all() or []
			except Exception:
				shadow_rows = []

	for m in msgs:
		try:
			entry = {
				"dir": (m.direction or "in"),
				"text": (m.text or ""),
				"timestamp_ms": int(m.timestamp_ms or 0),
			}
			history_from_messages.append(entry)
		except Exception:
			continue

	if shadow_rows:
		for reply in shadow_rows:
			try:
				text_val = (reply.reply_text or "").strip()
				if not text_val:
					continue
				entry = {
					"dir": "out",
					"text": text_val,
					"timestamp_ms": _dt_to_ms(getattr(reply, "created_at", None)),
				}
				mock_outbound.append(entry)
			except Exception:
				continue

	history_trim_source = list(history_from_messages)
	if history_trim_source:
		history_trim_source.sort(key=lambda item: item.get("timestamp_ms") or 0)
		last_customer_idx = None
		last_customer_message = ""
		for idx, entry in enumerate(history_trim_source):
			if (entry.get("dir") or "in").lower() == "in" and (entry.get("text") or "").strip():
				last_customer_idx = idx
				last_customer_message = entry.get("text") or ""
	if last_customer_idx is not None:
		history_trimmed = history_trim_source[: last_customer_idx + 1]
	else:
		history_trimmed = history_trim_source
		if not last_customer_message:
			last_customer_message = ""

	if mock_outbound:
		history_trimmed = (history_trimmed or []) + mock_outbound
		history_trimmed.sort(key=lambda item: item.get("timestamp_ms") or 0)

	return history_trimmed, last_customer_message


def _detect_conversation_flags(history: List[Dict[str, Any]], product_info: Optional[Dict[str, Any]]) -> Dict[str, bool]:
	flags: Dict[str, bool] = {}
	product_name = ""
	if isinstance(product_info, dict):
		product_name = str(
			product_info.get("name")
			or product_info.get("slug")
			or product_info.get("slug_or_sku")
			or ""
		).lower()
	for entry in history:
		if (entry.get("dir") or "in").lower() != "out":
			continue
		text = (entry.get("text") or "").strip().lower()
		if not text:
			continue
		if "₺" in text or "adet" in text:
			flags["intro_shared"] = True
		if product_name and product_name in text:
			flags["product_name_shared"] = True
		if flags.get("intro_shared") and (not product_name or flags.get("product_name_shared")):
			break
	return flags


def _normalize_state(value: Any, *, fallback: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
	if isinstance(value, dict):
		return value  # type: ignore[return-value]
	if isinstance(value, str) and value.strip():
		try:
			parsed = json.loads(value)
			if isinstance(parsed, dict):
				return parsed  # type: ignore[return-value]
		except Exception:
			return fallback or {}
	return fallback or {}


def draft_reply(
	conversation_id: int,
	*,
	limit: int = 40,
	include_meta: bool = False,
	state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
	"""
	Create a suggested reply (shadow) for a conversation.

	The AI is expected to return JSON with at least:
	  - should_reply: bool  (False => we will NOT show/send any suggestion)
	  - reply_text: str     (next suggested message when should_reply is true)
	  - confidence: float   (0..1)
	  - reason: str         (short explanation for debugging)
	  - notes: str|null
	"""
	client = AIClient(model=get_ai_shadow_model_from_settings())
	if not client.enabled:
		raise RuntimeError("AI client is not configured. Set OPENAI_API_KEY.")

	# Context from DB: product focus + stock + history
	product_info, stock = _load_focus_product_and_stock(int(conversation_id))
	product_info = product_info or {}
	if not product_info.get("id"):
		# Do not proceed when we can't identify a concrete product.
		return {
			"should_reply": False,
			"reply_text": "",
			"confidence": 0.0,
			"reason": "missing_product_context",
			"notes": "Konuşma herhangi bir reklam/post ürünü ile eşleşmediği için AI devre dışı.",
			"missing_product_context": True,
			"product_info": product_info,
		}
	history, last_customer_message = _load_history(int(conversation_id), limit=limit)
	conversation_flags = _detect_conversation_flags(history, product_info)
	transcript = _format_transcript(
		[
			{"direction": h.get("dir"), "timestamp_ms": h.get("timestamp_ms"), "text": h.get("text")}
			for h in history
		]
	)

	# Choose product images for this reply (based on product + last customer message)
	product_images: List[Dict[str, Any]] = []
	try:
		pid = product_info.get("id") if isinstance(product_info, dict) else None
		variant_key = _guess_variant_key_from_message(stock, last_customer_message)
		product_images = _select_product_images_for_reply(pid, variant_key=variant_key)
	except Exception:
		product_images = []

	store_conf: Dict[str, Any] = {
		"brand": "HiMan",
		"shipping": {"carrier": "SÜRAT", "eta_business_days": "2-3", "transparent_bag": True},
		"exchange": {"customer_to_shop": 100, "shop_to_customer": 200, "note": "Toplam ~300 TL değişim kargo"},
		"payment_options": ["cod_cash", "cod_card"],
	}

	# Parse height and weight from last customer message
	parsed_hw = parse_height_weight(last_customer_message)
	height_cm = parsed_hw.get("height_cm")
	weight_kg = parsed_hw.get("weight_kg")
	if height_cm or weight_kg:
		try:
			log.info(
				"draft_reply parsed_hw conversation_id=%s height_cm=%s weight_kg=%s",
				conversation_id,
				height_cm,
				weight_kg,
			)
		except Exception:
			pass
	
	# Calculate size suggestion if we have height/weight and product_id
	size_suggestion: Optional[str] = None
	product_id_val = product_info.get("id") if product_info else None
	if height_cm and weight_kg and product_id_val:
		try:
			size_suggestion = calculate_size_suggestion(height_cm, weight_kg, product_id_val)
			log.info(
				"draft_reply size_lookup conversation_id=%s product_id=%s height_cm=%s weight_kg=%s suggestion=%s",
				conversation_id,
				product_id_val,
				height_cm,
				weight_kg,
				size_suggestion,
			)
		except Exception:
			size_suggestion = None
	
	# Detect if product has multiple colors
	has_multiple_colors = detect_color_count(stock)
	
	# Check for double price (2'li fiyat) - look for items with quantity=2 or special pricing
	# For now, we'll check if there's a pattern in stock items that suggests 2-item pricing
	# This is a simple heuristic - can be enhanced with actual product configuration
	double_price: Optional[float] = None
	# TODO: Add proper double_price detection from product configuration or stock patterns
	
	parsed_data: Dict[str, Any] = {}
	if height_cm:
		parsed_data["height_cm"] = height_cm
	if weight_kg:
		parsed_data["weight_kg"] = weight_kg
	if size_suggestion:
		parsed_data["size_suggestion"] = size_suggestion
	
	# Build product_focus with additional metadata
	product_focus_data: Dict[str, Any] = product_info or {"id": None, "name": None, "slug_or_sku": None, "slug": None, "confidence": 0.0}
	product_focus_data["has_multiple_colors"] = has_multiple_colors
	if double_price is not None:
		product_focus_data["double_price"] = double_price

	user_payload: Dict[str, Any] = {
		"store": store_conf,
		"product_focus": product_focus_data,
		"stock": stock,
		"history": history,
		"last_customer_message": last_customer_message,
		"transcript": transcript,
		"product_images": product_images,
	}
	state_payload = dict(state or {})
	hail_already_sent = bool(state_payload.get("hail_sent"))
	user_payload["state"] = state_payload
	if conversation_flags:
		user_payload["conversation_flags"] = conversation_flags
	if parsed_data:
		user_payload["parsed"] = parsed_data
		try:
			log.info("draft_reply parsed_payload conversation_id=%s data=%s", conversation_id, parsed_data)
		except Exception:
			pass
	function_callbacks: List[Dict[str, Any]] = []
	tools: List[Dict[str, Any]] = []
	tool_handlers: Dict[str, Callable[[Dict[str, Any]], str]] = {}
	measurements_from_tool = False
	
	# Wrap context JSON in a clear instruction so the model returns our desired schema
	context_json = json.dumps(user_payload, ensure_ascii=False)
	tools.append(
		{
			"type": "function",
			"function": {
				"name": "set_customer_measurements",
				"description": "Kullanıcının verdiği boy ve kilo bilgilerini backend'e ilet.",
				"parameters": {
					"type": "object",
					"properties": {
						"height_cm": {
							"type": "integer",
							"description": "Kullanıcının boyu (santimetre). Örnek: 178",
						},
						"weight_kg": {
							"type": "integer",
							"description": "Kullanıcının kilosu (kg). Örnek: 78",
						},
					},
					"required": ["height_cm", "weight_kg"],
				},
			},
		}
	)

	def _handle_measurement_tool(args: Dict[str, Any]) -> str:
		nonlocal height_cm, weight_kg, size_suggestion, measurements_from_tool, parsed_data
		height_val = args.get("height_cm")
		weight_val = args.get("weight_kg")
		try:
			if height_val is not None:
				height_cm = int(height_val)
		except (ValueError, TypeError):
			pass
		try:
			if weight_val is not None:
				weight_kg = int(weight_val)
		except (ValueError, TypeError):
			pass
		if height_cm and weight_kg and product_id_val:
			try:
				size_suggestion_local = calculate_size_suggestion(height_cm, weight_kg, product_id_val)
				if size_suggestion_local:
					size_suggestion = size_suggestion_local
			except Exception:
				pass
		callback_result: Dict[str, Any] = {}
		if size_suggestion:
			callback_result["size_suggestion"] = size_suggestion
		callback_entry = {
			"name": "set_customer_measurements",
			"arguments": {
				"height_cm": height_cm,
				"weight_kg": weight_kg,
			},
			"result": callback_result,
		}
		function_callbacks.append(callback_entry)
		_log_function_callback(conversation_id, callback_entry["name"], callback_entry["arguments"], callback_result)
		measurements_from_tool = True
		payload = {
			"height_cm": height_cm,
			"weight_kg": weight_kg,
		}
		if size_suggestion:
			payload["size_suggestion"] = size_suggestion
		for key, value in payload.items():
			if value:
				parsed_data[key] = value
		return json.dumps(payload, ensure_ascii=False)

	tool_handlers["set_customer_measurements"] = _handle_measurement_tool

	user_prompt = (
		"=== KRİTİK TALİMATLAR ===\n"
		"1. SADECE ve SADECE aşağıdaki JSON şemasına UYGUN bir JSON obje döndür.\n"
		"2. Markdown, kod bloğu, yorum, açıklama veya başka hiçbir metin EKLEME.\n"
		"3. JSON dışında hiçbir şey yazma.\n"
		"4. Tüm alanlar zorunludur (notes hariç, o null olabilir).\n\n"
		"=== ZORUNLU JSON ŞEMASI ===\n"
		"{\n"
		'  "should_reply": boolean,        // ZORUNLU: true veya false\n'
		'  "reply_text": string,           // ZORUNLU: Cevap metni (boş string OLAMAZ)\n'
		'  "confidence": number,           // ZORUNLU: 0.0 ile 1.0 arası sayı\n'
		'  "reason": string,               // ZORUNLU: Kısa açıklama\n'
		'  "notes": string | null,         // OPSİYONEL: null veya string\n'
		'  "state": object | null          // OPSİYONEL: Güncel durum sözlüğü (örn. {"asked_color": true})\n'
		"}\n\n"
		"=== ÖNEMLİ UYARILAR ===\n"
		"- reply_text ASLA boş string olamaz. Cevap vermeyeceksen bile makul bir açıklama yaz.\n"
		"- should_reply boolean olmalı (true/false), string değil.\n"
		"- confidence sayı olmalı (0.0-1.0), string değil.\n"
		"- JSON dışında hiçbir metin, açıklama veya yorum ekleme.\n\n"
		"=== BAĞLAM VERİSİ (SADECE BİLGİ İÇİN) ===\n"
		"CONTEXT_JSON_START\n"
		f"{context_json}\n"
		"CONTEXT_JSON_END\n"
		"\n=== FONKSİYON TALİMATI ===\n"
		"- Kullanıcı boy+kilo verdiğinde `set_customer_measurements` fonksiyonunu MUTLAKA çağır.\n"
		"- Fonksiyon çıktısındaki `size_suggestion` varsa aynen kullan; yoksa beden tablosunu takip et.\n"
		"- Fonksiyon çağrısı yapmadan yeni ölçü isteme.\n"
		"- Fonksiyon çağrısı yaptığını veya ölçüleri backend'e ilettiğini kullanıcıya ASLA söyleme; sadece sonuçla devam et.\n"
	)
	if function_callbacks:
		user_prompt += "\n=== FONKSİYON ÇAĞRILARI ===\n"
		user_prompt += json.dumps(function_callbacks, ensure_ascii=False)
		user_prompt += "\nBu kayıtlar backend fonksiyon çağrılarının sonucudur; ölçümleri tekrar isteme.\n"

	# Load customer info for gender detection
	customer_info = _load_customer_info(int(conversation_id))

	# Load pretext and product system message
	pretext_content: Optional[str] = None
	product_extra_sys: Optional[str] = None
	if product_info and product_info.get("id") is not None:
		try:
			with get_session() as session:
				p = session.exec(
					select(Product).where(Product.id == int(product_info["id"])).limit(1)
				).first()
				if p:
					# Get product's ai_system_msg (existing)
					if getattr(p, "ai_system_msg", None):
						product_extra_sys = p.ai_system_msg  # type: ignore[assignment]
					
					# Get pretext
					pretext_id = getattr(p, "pretext_id", None)
					if pretext_id:
						# Use product's selected pretext
						pretext = session.exec(
							select(AIPretext).where(AIPretext.id == int(pretext_id)).limit(1)
						).first()
						if pretext:
							pretext_content = pretext.content
					else:
						# Use default pretext (first one marked as default, or first one)
						pretext = session.exec(
							select(AIPretext).where(AIPretext.is_default == True).limit(1)
						).first()
						if not pretext:
							# Fallback to first pretext if no default
							pretext = session.exec(
								select(AIPretext).order_by(AIPretext.id.asc()).limit(1)
							).first()
						if pretext:
							pretext_content = pretext.content
						else:
							# Fallback to file-based global system prompt if no pretext in DB
							pretext_content = get_global_system_prompt()
		except Exception:
			# Fallback to file-based global system prompt on error
			pretext_content = get_global_system_prompt()
	else:
		# No product focus - use default pretext
		try:
			with get_session() as session:
				pretext = session.exec(
					select(AIPretext).where(AIPretext.is_default == True).limit(1)
				).first()
				if not pretext:
					pretext = session.exec(
						select(AIPretext).order_by(AIPretext.id.asc()).limit(1)
					).first()
				if pretext:
					pretext_content = pretext.content
				else:
					# Fallback to file-based global system prompt if no pretext in DB
					pretext_content = get_global_system_prompt()
		except Exception:
			# Fallback to file-based global system prompt on error
			pretext_content = get_global_system_prompt()

	# Build gender detection instructions
	hail_instructions = f"""
## Hitap Takibi Durumu

Şu anki state.hail_sent değeri: {"true" if hail_already_sent else "false"}

Kurallar:
1. Eğer state.hail_sent false ise bu cevabın başında sadece bir kez uygun hitapla (abim/ablam/efendim) selamla ve cevabı üretirken state objesine `{{"hail_sent": true}}` ekle.
2. Eğer state.hail_sent true ise artık selamlama yapma; doğrudan içeriğe geç ve state.hail_sent değerini true olarak koru.
3. Selamlama sadece ilk yanıtta yapılır; asla ikinci kez tekrarlama.
"""

	repeat_guard_instructions = f"""
## Tekrar Kontrolü

conversation_flags.intro_shared = {"true" if conversation_flags.get("intro_shared") else "false"}

- Eğer intro_shared true ise fiyatı ve ürün özetini tekrarlama; sadece yeni bilgiler (ör. beden sonucu, kargo/ödeme adımı) paylaş.
- intro_shared false ise ilk mesajda fiyatı net yazıp kısa teknik özeti tek seferde ver.
"""

	gender_instructions = f"""
## Müşteri Hitap Kuralları

Müşteri bilgileri:
- Kullanıcı adı: {customer_info.get("username") or "bilinmiyor"}
- İsim: {customer_info.get("name") or customer_info.get("contact_name") or "bilinmiyor"}
- İletişim adı: {customer_info.get("contact_name") or "bilinmiyor"}

HITAP KURALLARI:
1. Müşterinin cinsiyetini belirlemek için yukarıdaki bilgileri (kullanıcı adı, isim, iletişim adı) kullan.
2. Eğer müşteri ERKEK ise: "abim" kullan (örnek: "Merhabalar abim")
3. Eğer müşteri KADIN ise: "ablam" kullan (örnek: "Merhabalar ablam")
4. Eğer cinsiyeti belirleyemiyorsan: "efendim" kullan (örnek: "Merhabalar efendim")
5. Cinsiyet belirleme kriterleri:
   - İsimdeki son ekler: "-a", "-e" gibi ekler genelde kadın isimlerinde görülür
   - Türkçe kadın isimleri: Ayşe, Fatma, Zeynep, Elif, Emine, Hatice, Merve, Seda, vb.
   - Türkçe erkek isimleri: Mehmet, Ali, Ahmet, Mustafa, Hasan, Hüseyin, İbrahim, vb.
   - Belirsizse veya emin değilsen "efendim" kullan

ÖNEMLİ: Asla yanlış cinsiyete hitap etme. Emin değilsen "efendim" kullan.
"""

	# Combine: pretext + gender instructions + product system message
	# Add explicit instruction priority header to ensure strict adherence
	sys_prompt_parts: List[str] = []
	
	# Add strict instruction header with JSON mode enforcement
	sys_prompt_parts.append(
		"=== KRİTİK SİSTEM TALİMATLARI ===\n"
		"Bu talimatlar kesinlikle uyulması gereken kurallardır. Hiçbir durumda bu talimatları görmezden gelme veya değiştirme.\n"
		"Tüm cevaplar JSON formatında olmalı ve aşağıdaki kurallara kesinlikle uymalıdır.\n\n"
		"JSON MODE ZORUNLU: Sen bir JSON generator'sın. Sadece geçerli JSON objesi döndür.\n"
		"- Markdown kod bloğu kullanma\n"
		"- Açıklama veya yorum ekleme\n"
		"- JSON dışında hiçbir metin yazma\n"
		"- Tüm alanlar zorunludur (notes hariç)\n"
	)
	
	if pretext_content:
		sys_prompt_parts.append(f"=== ÜRÜN ÖZEL TALİMATLARI ===\n{pretext_content}")
	
	sys_prompt_parts.append(hail_instructions)
	sys_prompt_parts.append(repeat_guard_instructions)
	sys_prompt_parts.append(gender_instructions)
	
	if product_extra_sys:
		sys_prompt_parts.append(f"=== EK ÜRÜN TALİMATLARI ===\n{product_extra_sys}")

	sys_prompt = "\n\n".join(sys_prompt_parts) if sys_prompt_parts else gender_instructions

	# Use lower temperature for stricter instruction following, unless disabled
	temperature = get_shadow_temperature_setting()
	temp_opt_out = is_shadow_temperature_opt_out()
	
	def _build_gen_kwargs(include_raw: bool = False) -> Dict[str, Any]:
		kwargs: Dict[str, Any] = {
			"system_prompt": sys_prompt,
			"user_prompt": user_prompt,
			"temperature": None if temp_opt_out else temperature,
		}
		if tools:
			kwargs["tools"] = tools
			kwargs["tool_choice"] = "auto"
			kwargs["tool_handlers"] = tool_handlers
		if include_raw:
			kwargs["include_raw"] = True
		return kwargs
	
	raw_response: Any = None
	if include_meta:
		data, raw_response = client.generate_json(**_build_gen_kwargs(include_raw=True))
	else:
		data = client.generate_json(**_build_gen_kwargs())
	if not isinstance(data, dict):
		raise RuntimeError("AI returned non-dict JSON for shadow reply")
	if function_callbacks:
		data["function_callbacks"] = function_callbacks

	# Normalize output
	def _coerce_bool(val: Any, default: bool = True) -> bool:
		if isinstance(val, bool):
			return val
		if val is None:
			return default
		try:
			if isinstance(val, (int, float)):
				return bool(val)
			s = str(val).strip().lower()
			if s in ("true", "1", "yes", "y", "evet"):
				return True
			if s in ("false", "0", "no", "n", "hayir", "hayır"):
				return False
		except Exception:
			pass
		return default

	should_reply = _coerce_bool(data.get("should_reply"), default=True)
	reply_text_raw = (data.get("reply_text") or "").strip()
	# Decode any literal escape sequences (e.g., \\n -> actual newline)
	reply_text = _decode_escape_sequences(reply_text_raw)
	try:
		conf_raw = float(data.get("confidence") if data.get("confidence") is not None else 0.6)
	except Exception:
		conf_raw = 0.6
	confidence = max(0.0, min(1.0, conf_raw))
	reason = (data.get("reason") or "auto")
	notes = (data.get("notes") or None)

	reply: Dict[str, Any] = {
		"should_reply": should_reply,
		"reply_text": reply_text,
		"confidence": confidence,
		"reason": reason,
		"notes": notes,
		"model": client.model,
	}
	if parsed_data:
		reply["parsed"] = parsed_data
	if function_callbacks:
		reply["function_callbacks"] = function_callbacks
	reply["state"] = _normalize_state(data.get("state"), fallback=state_payload)
	if product_images:
		reply["product_images"] = product_images
	if include_meta:
		# Attach debug metadata so callers (e.g., worker) can persist it for inspection.
		# user_payload is a dict; system prompt and raw_response may be large, so consumers
		# can choose to truncate when displaying.
		try:
			reply["debug_meta"] = {
				"system_prompt": sys_prompt,
				"user_payload": user_payload,
				"raw_response": raw_response,
			}
		except Exception:
			# best-effort; never break reply normalization
			pass
	return reply


