from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from sqlmodel import select

from ..db import get_session
from ..models import Message, Product, Item, Conversation, IGUser, AIPretext, ProductImage
from .ai import AIClient
from .ai_context import VariantExclusions, parse_variant_exclusions, variant_is_excluded
from .ai_ig import _detect_focus_product


MAX_AI_IMAGES_PER_REPLY = int(os.getenv("AI_MAX_PRODUCT_IMAGES", "3"))


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

	user_payload: Dict[str, Any] = {
		"store": store_conf,
		"product_focus": product_info or {"id": None, "name": None, "slug_or_sku": None, "slug": None, "confidence": 0.0},
		"stock": stock,
		"history": history,
		"last_customer_message": last_customer_message,
		"transcript": transcript,
		"product_images": product_images,
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
		except Exception:
			pass
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
		except Exception:
			pass

	# Build gender detection instructions
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
	sys_prompt_parts: List[str] = []
	if pretext_content:
		sys_prompt_parts.append(pretext_content)
	sys_prompt_parts.append(gender_instructions)
	if product_extra_sys:
		sys_prompt_parts.append(product_extra_sys)

	sys_prompt = "\n\n".join(sys_prompt_parts) if sys_prompt_parts else gender_instructions

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


