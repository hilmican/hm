from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from sqlmodel import select

from ..db import get_session
from ..models import Message, Product, Item
from .ai import AIClient
from .ai_context import VariantExclusions, parse_variant_exclusions, variant_is_excluded
from .ai_ig import _detect_focus_product


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


def _load_history(conversation_id: int, *, limit: int = 40) -> Tuple[List[Dict[str, Any]], str]:
	"""
	Load recent messages for this conversation and return (history_list, last_customer_message).
	
	history_list: [{"dir": "in|out", "text": "str", "timestamp_ms": int}, ...]
	"""
	history_all: List[Dict[str, Any]] = []
	last_customer_message = ""
	last_customer_idx: Optional[int] = None
	with get_session() as session:
		msgs = (
			session.exec(
				select(Message)
				.where(Message.conversation_id == int(conversation_id))
				.order_by(Message.timestamp_ms.asc())
				.limit(max(1, min(limit, 100)))
			).all()
		)
	for m in msgs:
		try:
			entry = {
				"dir": (m.direction or "in"),
				"text": (m.text or ""),
				"timestamp_ms": int(m.timestamp_ms or 0),
			}
			history_all.append(entry)
			if (entry["dir"] or "in").lower() == "in" and (entry["text"] or "").strip():
				last_customer_message = entry["text"] or ""
				last_customer_idx = len(history_all) - 1
		except Exception:
			continue
	if last_customer_idx is not None:
		history_trimmed = history_all[: last_customer_idx + 1]
	else:
		history_trimmed = history_all
		if not last_customer_message:
			last_customer_message = ""
	return history_trimmed, last_customer_message


def draft_reply(conversation_id: int, *, limit: int = 40, include_meta: bool = False) -> Dict[str, Any]:
	"""
	Create a suggested reply (shadow) for a conversation.

	The AI is expected to return JSON with at least:
	  - should_reply: bool  (False => we will NOT show/send any suggestion)
	  - reply_text: str     (next suggested message when should_reply is true)
	  - confidence: float   (0..1)
	  - reason: str         (short explanation for debugging)
	  - notes: str|null
	"""
	client = AIClient(model=os.getenv("AI_SHADOW_MODEL", "gpt-4o-mini"))
	if not client.enabled:
		raise RuntimeError("AI client is not configured. Set OPENAI_API_KEY.")

	# Context from DB: product focus + stock + history
	product_info, stock = _load_focus_product_and_stock(int(conversation_id))
	history, last_customer_message = _load_history(int(conversation_id), limit=limit)
	transcript = _format_transcript(
		[
			{"direction": h.get("dir"), "timestamp_ms": h.get("timestamp_ms"), "text": h.get("text")}
			for h in history
		]
	)

	store_conf: Dict[str, Any] = {
		"brand": "HiMan",
		"shipping": {"carrier": "SÜRAT", "eta_business_days": "2-3", "transparent_bag": True},
		"exchange": {"customer_to_shop": 100, "shop_to_customer": 200, "note": "Toplam ~300 TL değişim kargo"},
		"payment_options": ["cod_cash", "cod_card"],
	}

	user_payload: Dict[str, Any] = {
		"store": store_conf,
		"product_focus": product_info or {"id": None, "name": None, "slug_or_sku": None, "slug": None, "confidence": 0.0},
		"stock": stock,
		"history": history,
		"last_customer_message": last_customer_message,
		"transcript": transcript,
	}
	# Wrap context JSON in a clear instruction so the model returns our desired schema
	context_json = json.dumps(user_payload, ensure_ascii=False)
	user_prompt = (
		"Sen HiMan için Instagram DM satış asistanısın.\n"
		"Aşağıda mağaza, ürün ve konuşma geçmişiyle ilgili bir JSON göreceksin.\n"
		"Sadece aşağıdaki şemaya UYGUN, tek bir JSON obje döndür:\n"
		"{\n"
		'  \"should_reply\": bool,           // müşteriye şu anda cevap önerilmeli mi?\n'
		'  \"reply_text\": string,          // Önerdiğin mesaj; boş BIRAKMA, cevap vermeyeceksen makul bir açıklama yaz\n'
		'  \"confidence\": number,          // 0.0 – 1.0 arası güven skoru\n'
		'  \"reason\": string,              // kısa açıklama, örn: \"müşteri beden soruyor\"\n'
		'  \"notes\": string | null        // operatör için ek notlar (isteğe bağlı)\n'
		"}\n\n"
		"Bir metin sohbeti YAZMA, sadece bu JSON objesini üret.\n"
		"İçerik bağlamı (değiştirmeden kullan):\n"
		"CONTEXT_JSON_START\n"
		f"{context_json}\n"
		"CONTEXT_JSON_END\n"
	)

	# Optional per-product system tweaks from Product.ai_system_msg
	product_extra_sys: Optional[str] = None
	if product_info and product_info.get("id") is not None:
		try:
			with get_session() as session:
				p = session.exec(
					select(Product).where(Product.id == int(product_info["id"])).limit(1)
				).first()
			if p and getattr(p, "ai_system_msg", None):
				product_extra_sys = p.ai_system_msg  # type: ignore[assignment]
		except Exception:
			product_extra_sys = None

	sys_prompt = _shadow_system_prompt(base_extra=product_extra_sys)

	raw_response: Any = None
	if include_meta:
		data, raw_response = client.generate_json(
			system_prompt=sys_prompt,
			user_prompt=user_prompt,
			include_raw=True,
			temperature=0.3,
		)
	else:
		data = client.generate_json(
			system_prompt=sys_prompt,
			user_prompt=user_prompt,
			temperature=0.3,
		)
	if not isinstance(data, dict):
		raise RuntimeError("AI returned non-dict JSON for shadow reply")

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
	reply_text = (data.get("reply_text") or "").strip()
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


