from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text as _text
from sqlmodel import select

from ..db import get_session
from ..models import Message, Product
from .ai import AIClient
from .ai_context import VariantExclusions, parse_variant_exclusions, variant_is_excluded


class AIAdapter:
	"""Adapter to allow swapping to OpenAI Agents/Assistants later without changing call sites."""
	def __init__(self, mode: str = "direct"):
		self._mode = mode
		from .ai import get_ai_model_from_settings
		model = get_ai_model_from_settings()
		self._client = AIClient(model=model)

	def generate_json(self, system_prompt: str, user_prompt: str, *, temperature: float = 0.2) -> Dict[str, Any]:
		# For now, direct JSON-mode; later, branch by self._mode and call Assistants API.
		data = self._client.generate_json(system_prompt=sys_prompt(system_prompt), user_prompt=user_prompt, temperature=temperature)
		if isinstance(data, tuple):
			data = data[0]
		return data if isinstance(data, dict) else {}


def sys_prompt(text: str) -> str:
	"""Hook to prepend/transform system prompt if needed."""
	return text or ""

def lookup_price(sku_or_slug: str) -> Optional[float]:
	"""Return default/unit price if available (prioritizing product defaults)."""
	from ..models import Item
	with get_session() as session:
		try:
			# Prefer product lookup by slug/name first
			prow = session.exec(
				select(Product).where((Product.slug == sku_or_slug) | (Product.name == sku_or_slug)).limit(1)
			).first()
			if prow and prow.default_price is not None:
				return float(prow.default_price)
		except Exception:
			prow = None
		try:
			row = session.exec(select(Item).where(Item.sku == sku_or_slug).limit(1)).first()
		except Exception:
			row = None
		if row:
			# Attempt to use the parent product default price if available
			try:
				if getattr(row, "product_id", None) is not None:
					prow2 = session.exec(select(Product).where(Product.id == int(row.product_id)).limit(1)).first()
				else:
					prow2 = None
			except Exception:
				prow2 = None
			if prow2 and prow2.default_price is not None:
				return float(prow2.default_price)
			try:
				if row.price is not None:
					return float(row.price)
			except Exception:
				pass
	return None


def build_product_link(sku_or_slug: str) -> Optional[str]:
	"""Best-effort product URL builder (placeholder)."""
	try:
		base = "https://himanshop.example.com/p/"
		return base + sku_or_slug
	except Exception:
		return None


def get_stock_snapshot(sku: str) -> Optional[dict]:
	"""Placeholder stock snapshot; extend once stock tables are in use."""
	try:
		return {"sku": sku, "available": True}
	except Exception:
		return None


def _detect_focus_product(conversation_id: str) -> Tuple[Optional[str], float]:
	"""Best-effort focus product detection.
	Returns (sku_or_slug, confidence). Currently a lightweight heuristic:
	- check conversation's last_link_id/last_link_type (supports posts via ads_products)
	- use ad_id→sku mapping if present (ads_products table optional)
	- keyword match against Product.ai_tags on last N inbound messages
	"""
	with get_session() as session:
		# Check conversation's last_link_id/last_link_type first (includes posts)
		try:
			stmt_conv = _text(
				"""
				SELECT c.last_link_id, c.last_link_type, c.last_ad_id
				FROM conversations c
				WHERE c.id = :cid
				LIMIT 1
				"""
			).bindparams(cid=str(conversation_id))
			row_conv = session.exec(stmt_conv).first()
			if row_conv:
				last_link_id = getattr(row_conv, "last_link_id", None) if hasattr(row_conv, "last_link_id") else (row_conv[0] if len(row_conv) > 0 else None)
				last_link_type = getattr(row_conv, "last_link_type", None) if hasattr(row_conv, "last_link_type") else (row_conv[1] if len(row_conv) > 1 else None)
				last_ad_id = getattr(row_conv, "last_ad_id", None) if hasattr(row_conv, "last_ad_id") else (row_conv[2] if len(row_conv) > 2 else None)
				# Use last_link_id if present (handles both ads and posts)
				link_id_to_check = last_link_id or last_ad_id
				# Determine link_type: prefer last_link_type, fallback to 'ad' if last_ad_id exists
				link_type_to_check = last_link_type if last_link_type else ("ad" if last_ad_id else None)
				if link_id_to_check and link_type_to_check:
					# Resolve product via ads_products with explicit link_type filter (works for both 'ad' and 'post')
					rp = session.exec(
						_text(
							"""
							SELECT ap.sku, ap.product_id, p.slug, p.name
							FROM ads_products ap
							LEFT JOIN product p ON ap.product_id = p.id
							WHERE ap.ad_id = :id AND ap.link_type = :link_type
							LIMIT 1
							"""
						).params(id=str(link_id_to_check), link_type=str(link_type_to_check))
					).first()
				elif link_id_to_check:
					# Fallback: try without link_type filter if link_type is unknown (backward compatibility)
					rp = session.exec(
						_text(
							"""
							SELECT ap.sku, ap.product_id, p.slug, p.name
							FROM ads_products ap
							LEFT JOIN product p ON ap.product_id = p.id
							WHERE ap.ad_id = :id
							LIMIT 1
							"""
						).params(id=str(link_id_to_check))
					).first()
				else:
					rp = None
				if rp:
					sku = getattr(rp, "sku", None) if hasattr(rp, "sku") else (rp[0] if len(rp) > 0 else None)
					pid = getattr(rp, "product_id", None) if hasattr(rp, "product_id") else (rp[1] if len(rp) > 1 else None)
					slug = getattr(rp, "slug", None) if hasattr(rp, "slug") else (rp[2] if len(rp) > 2 else None)
					pname = getattr(rp, "name", None) if hasattr(rp, "name") else (rp[3] if len(rp) > 3 else None)
					if sku:
						return str(sku), 0.9
					if slug:
						return str(slug), 0.9
					if pname:
						return str(pname), 0.85
		except Exception:
			pass
		# Fallback: ad_id mapping from messages (backward compatibility)
		try:
			stmt_last_ad = _text(
				"""
				SELECT m.ad_id
				FROM message m
				WHERE m.conversation_id=:cid AND m.ad_id IS NOT NULL
				ORDER BY m.timestamp_ms DESC, m.id DESC LIMIT 1
				"""
			).bindparams(cid=str(conversation_id))
			row = session.exec(stmt_last_ad).first()
			ad_id = (row.ad_id if hasattr(row, "ad_id") else (row[0] if row else None)) if row else None
			if ad_id:
				# Prefer SKU when present (backwards compatible), else resolve via linked product
				rp = session.exec(
					_text(
						"""
						SELECT ap.sku, ap.product_id, p.slug, p.name
						FROM ads_products ap
						LEFT JOIN product p ON ap.product_id = p.id
						WHERE ap.ad_id = :id
						LIMIT 1
						"""
					).params(id=str(ad_id))
				).first()
				if rp:
					sku = getattr(rp, "sku", None) if hasattr(rp, "sku") else (rp[0] if len(rp) > 0 else None)
					pid = getattr(rp, "product_id", None) if hasattr(rp, "product_id") else (rp[1] if len(rp) > 1 else None)
					slug = getattr(rp, "slug", None) if hasattr(rp, "slug") else (rp[2] if len(rp) > 2 else None)
					pname = getattr(rp, "name", None) if hasattr(rp, "name") else (rp[3] if len(rp) > 3 else None)
					if sku:
						return str(sku), 0.9
					if slug:
						return str(slug), 0.9
					if pname:
						return str(pname), 0.85
		except Exception:
			pass
		# keyword match
		try:
			msgs = session.exec(
				select(Message).where(Message.conversation_id == conversation_id).order_by(Message.timestamp_ms.desc()).limit(30)
			).all()
			texts = " ".join([(m.text or "") for m in msgs if (m.direction or "in") == "in"])
			if texts.strip():
				products: List[Product] = session.exec(select(Product)).all()  # type: ignore
				best: Tuple[Optional[str], float] = (None, 0.0)
				for p in products:
					try:
						tags = []
						if p.ai_tags:
							tags = json.loads(p.ai_tags) if isinstance(p.ai_tags, str) else p.ai_tags
							if not isinstance(tags, list):
								tags = []
						score = 0.0
						for t in tags:
							if isinstance(t, str) and t and t.lower() in texts.lower():
								score += 0.25
						if score > best[1]:
							best = (p.slug or p.name, min(0.95, score))
					except Exception:
						continue
				return best
		except Exception:
			pass
	return (None, 0.0)


def build_prompt(conversation_id: str, customer_text: str) -> Tuple[str, str]:
	"""Return (system_prompt, user_prompt) assembled from DB and recent context."""
	focus, conf = _detect_focus_product(conversation_id)
	sys_msg = ""
	prompt_msg = ""
	store_conf: Dict[str, Any] = {
		"brand": "HiMan",
		"shipping": {"carrier": "SÜRAT", "eta_business_days": "2-3", "transparent_bag": True},
		"exchange": {"customer_to_shop": 100, "shop_to_customer": 200, "note": "Toplam ~300 TL değişim kargo"},
		"payment_options": ["cod_cash", "cod_card"],
	}
	stock: List[Dict[str, Any]] = []
	product_default_price: Optional[float] = None
	variant_exclusions: VariantExclusions = VariantExclusions()
	history: List[Dict[str, Any]] = []

	def _hydrate_product_context(prod: Optional[Product]) -> None:
		nonlocal sys_msg, prompt_msg, product_default_price, variant_exclusions
		if not prod:
			return
		try:
			val = getattr(prod, "ai_system_msg", None)
			if val is not None:
				sys_msg = val
		except Exception:
			pass
		try:
			val = getattr(prod, "ai_prompt_msg", None)
			if val is not None:
				prompt_msg = val
		except Exception:
			pass
		try:
			if prod.default_price is not None:
				product_default_price = float(prod.default_price)
		except Exception:
			pass
		try:
			variant_exclusions = parse_variant_exclusions(getattr(prod, "ai_variant_exclusions", None))
		except Exception:
			variant_exclusions = VariantExclusions()

	with get_session() as session:
		product_row: Optional[Product] = None
		try:
			if focus:
				product_row = session.exec(
					select(Product).where((Product.slug == str(focus)) | (Product.name == str(focus))).limit(1)
				).first()
		except Exception:
			product_row = None
		_hydrate_product_context(product_row)

		pid: Optional[int] = None
		try:
			if focus:
				rowi = session.exec(
					_text(
						"SELECT sku, name, color, size, price, product_id FROM item WHERE sku=:s LIMIT 1"
					).params(s=str(focus))
				).first()
			else:
				rowi = None
		except Exception:
			rowi = None
		if rowi:
			sku = getattr(rowi, "sku", None) if hasattr(rowi, "sku") else (rowi[0] if len(rowi) > 0 else None)
			name = getattr(rowi, "name", None) if hasattr(rowi, "name") else (rowi[1] if len(rowi) > 1 else None)
			color = getattr(rowi, "color", None) if hasattr(rowi, "color") else (rowi[2] if len(rowi) > 2 else None)
			size = getattr(rowi, "size", None) if hasattr(rowi, "size") else (rowi[3] if len(rowi) > 3 else None)
			price = getattr(rowi, "price", None) if hasattr(rowi, "price") else (rowi[4] if len(rowi) > 4 else None)
			pid_raw = getattr(rowi, "product_id", None) if hasattr(rowi, "product_id") else (rowi[5] if len(rowi) > 5 else None)
			try:
				pid = int(pid_raw) if pid_raw is not None else None
			except Exception:
				pid = None
			if sku:
				stock.append({"sku": sku, "name": name, "color": color, "size": size, "price": price})
		if pid is None and focus:
			try:
				rowp = session.exec(
					select(Product).where((Product.slug == str(focus)) | (Product.name == str(focus))).limit(1)
				).first()
			except Exception:
				rowp = None
			if rowp:
				try:
					pid = int(rowp.id) if rowp.id is not None else None
				except Exception:
					pid = None
				_hydrate_product_context(rowp)
		if pid is not None:
			try:
				rows_sib = session.exec(
					_text("SELECT sku, name, color, size, price FROM item WHERE product_id=:pid LIMIT 200").params(
						pid=int(pid)
					)
				).all()
			except Exception:
				rows_sib = []
			for r in rows_sib:
				try:
					sku2 = r.sku if hasattr(r, "sku") else (r[0] if len(r) > 0 else None)
					if not isinstance(sku2, str) or any(it.get("sku") == sku2 for it in stock):
						continue
					name2 = r.name if hasattr(r, "name") else (r[1] if len(r) > 1 else None)
					color2 = r.color if hasattr(r, "color") else (r[2] if len(r) > 2 else None)
					size2 = r.size if hasattr(r, "size") else (r[3] if len(r) > 3 else None)
					price2 = r.price if hasattr(r, "price") else (r[4] if len(r) > 4 else None)
					stock.append({"sku": sku2, "name": name2, "color": color2, "size": size2, "price": price2})
				except Exception:
					continue
			if product_row is None:
				try:
					product_row = session.exec(select(Product).where(Product.id == int(pid)).limit(1)).first()
				except Exception:
					product_row = None
				_hydrate_product_context(product_row)
		try:
			msgs = session.exec(
				select(Message)
				.where(Message.conversation_id == conversation_id)
				.order_by(Message.timestamp_ms.asc())
				.limit(25)
			).all()
			history = [{"dir": (m.direction or "in"), "text": (m.text or "")} for m in msgs]
		except Exception:
			history = []

	if product_default_price is not None:
		for entry in stock:
			try:
				entry["price"] = product_default_price
			except Exception:
				continue
	if not variant_exclusions.is_empty():
		filtered_stock: List[Dict[str, Any]] = []
		for entry in stock:
			if variant_is_excluded(variant_exclusions, entry.get("color"), entry.get("size")):
				continue
			filtered_stock.append(entry)
		stock = filtered_stock
	if not stock and focus:
		fallback_name = None
		try:
			if product_row:
				fallback_name = getattr(product_row, "name", None)
		except Exception:
			fallback_name = None
		stock.append(
			{
				"sku": str(focus),
				"name": fallback_name or str(focus),
				"color": None,
				"size": None,
				"price": product_default_price,
			}
		)

	# fallback system if missing
	if not sys_msg:
		sys_msg = "Sen HiMan için Instagram DM satış asistanısın. Kısa ve net yanıtla; satış akışını ilerlet."
	user_prompt = json.dumps(
		{
			"store": store_conf,
			"product_focus": {"id": focus, "confidence": conf},
			"stock": stock,
			"history": history,
			"customer_message": customer_text,
			"contract": {
				"reply": "...",
				"intent": "...",
				"next_action": "...",
				"order_draft": {"items": [], "totals": {"subtotal": 0, "shipping": 0, "discount": 0, "grand_total": 0}},
				"flags": {},
			},
		},
		ensure_ascii=False,
	)
	return sys_msg, user_prompt


def propose_reply(conversation_id: str, customer_text: str) -> Dict[str, Any]:
	"""Call the AI client in JSON mode and return the parsed dict (or empty on error)."""
	sys, usr = build_prompt(conversation_id, customer_text)
	try:
		from .ai import get_ai_model_from_settings
		model = get_ai_model_from_settings()
		client = AIClient(model=model)
		data = client.generate_json(system_prompt=sys, user_prompt=usr, temperature=0.2)
		if isinstance(data, tuple):
			data = data[0]
		if isinstance(data, dict):
			return data
	except Exception:
		return {}
	return {}

import datetime as dt
import json
from typing import Any, Dict, Optional, Tuple, List
import os

from sqlmodel import select

from ..db import get_session
from ..models import Message
from .ai import AIClient
from .prompts import get_ig_purchase_prompt
from .monitoring import ai_run_log
from ..utils.normalize import normalize_phone
import logging


log = logging.getLogger("ig_ai.process")


def _format_transcript(messages: List[Any], max_chars: int = 15000) -> str:
    parts: List[str] = []
    for m in messages:
        # Support SQLModel instances or plain dicts
        try:
            role = ((m.direction if hasattr(m, "direction") else (m.get("direction") if isinstance(m, dict) else "in")) or "in").lower()
        except Exception:
            role = "in"
        try:
            ts = (m.timestamp_ms if hasattr(m, "timestamp_ms") else (m.get("timestamp_ms") if isinstance(m, dict) else 0)) or 0
        except Exception:
            ts = 0
        try:
            txt = ((m.text if hasattr(m, "text") else (m.get("text") if isinstance(m, dict) else "")) or "").strip()
        except Exception:
            txt = ""
        parts.append(f"[{role}] {ts}: {txt}")
    txt = "\n".join(parts)
    # trim to model budget if needed
    if len(txt) > max_chars:
        return txt[-max_chars:]
    return txt


def analyze_conversation(conversation_id: str, *, limit: int = 200, run_id: Optional[int] = None, include_meta: bool = False) -> Dict[str, Any]:
    """Run AI over a single conversation to detect purchase and extract contacts.

    Returns a dict with keys: purchase_detected, buyer_name, phone, address, notes,
    product_mentions (list), possible_order_ids (list)
    """
    from .ai import get_ai_model_from_settings
    model = get_ai_model_from_settings()
    client = AIClient(model=model)
    if not client.enabled:
        raise RuntimeError("AI client is not configured. Set OPENAI_API_KEY.")
    with get_session() as session:
        msgs = session.exec(
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.timestamp_ms.asc())
            .limit(min(max(limit, 1), 500))
        ).all()
        # Serialize messages into simple dicts to avoid detached/lazy load issues
        simple_msgs: List[Dict[str, Any]] = []
        for m in msgs:
            try:
                simple_msgs.append({
                    "direction": (m.direction or "in"),
                    "timestamp_ms": (m.timestamp_ms or 0),
                    "text": (m.text or ""),
                })
            except Exception:
                continue
    transcript = _format_transcript(simple_msgs)
    # Log context preparation
    try:
        ai_run_log(int(run_id), "debug", "prepare_context", {
            "conversation_id": conversation_id,
            "messages": len(msgs),
            "transcript_len": len(transcript),
        }) if run_id is not None else None
    except Exception:
        pass
    schema_hint = (
        '{"purchase_detected": true|false, "buyer_name": "str|null", "phone": "str|null", '
        '"address": "str|null", "notes": "str|null", "product_mentions": ["str"], '
        '"possible_order_ids": ["str"], "price": 0|null}'
    )
    user_prompt = (
        "Aşağıda bir DM konuşması transkripti var. \n"
        "Lütfen kesin satın alma olup olmadığını belirle ve bilgileri çıkar.\n"
        "Bu çıktı dışa aktarım için kullanılacaktır; satın alma varsa alıcı ad-soyad, telefon ve adresi konuşmanın TÜMÜNDE dikkatle ara ve mümkünse doldur.\n"
        "Uydurma yapma; metinde yoksa null bırak.\n\n"
        f"Şema: {schema_hint}\n\n"
        f"Transkript:\n{transcript}"
    )
    # Optional prompt logging (truncated) when enabled
    try:
        if os.getenv("AI_LOG_PROMPT", "0") not in ("0", "false", "False", "") and run_id is not None:
            system_prompt_now = get_ig_purchase_prompt()
            ai_run_log(int(run_id), "debug", "ai_prompt", {
                "system_prompt": system_prompt_now[:800],
                "user_prompt": user_prompt[:1200],
                "conversation_id": conversation_id,
            })
    except Exception:
        pass
    if include_meta:
        data, raw_response = client.generate_json(
            system_prompt=get_ig_purchase_prompt(),
            user_prompt=user_prompt,
            include_raw=True,
        )
    else:
        data = client.generate_json(system_prompt=get_ig_purchase_prompt(), user_prompt=user_prompt)
        raw_response = None
    try:
        if run_id is not None:
            ai_run_log(int(run_id), "info", "ai_response", {
                "conversation_id": conversation_id,
                "purchase_detected": bool(data.get("purchase_detected")),
                "has_phone": bool(data.get("phone")),
                "has_address": bool(data.get("address")),
                "mentions": len(data.get("product_mentions") or []),
            })
    except Exception:
        pass
    if not isinstance(data, dict):
        raise RuntimeError("AI returned non-dict JSON")
    # Normalize keys presence
    def _parse_price(val: Any) -> Optional[float]:
        if val is None:
            return None
        try:
            if isinstance(val, (int, float)):
                return float(val)
            s = str(val)
            # keep digits and separators, then normalize comma to dot
            import re
            cleaned = re.sub(r"[^0-9,\.]", "", s)
            cleaned = cleaned.replace(",", ".")
            if cleaned.count(".") > 1:
                # collapse extra dots: keep first, drop others
                first = cleaned.find(".")
                cleaned = cleaned[: first + 1] + cleaned[first + 1 :].replace(".", "")
            return float(cleaned) if cleaned else None
        except Exception:
            return None

    out: Dict[str, Any] = {
        "purchase_detected": bool(data.get("purchase_detected", False)),
        "buyer_name": data.get("buyer_name"),
        "phone": data.get("phone"),
        "address": data.get("address"),
        "notes": data.get("notes"),
        "product_mentions": data.get("product_mentions") or [],
        "possible_order_ids": data.get("possible_order_ids") or [],
        "price": _parse_price(data.get("price")),
    }
    if include_meta:
        out["meta"] = {
            "ai_model": client.model,
            "system_prompt": get_ig_purchase_prompt(),
            "user_prompt": user_prompt,
            "raw_response": raw_response,
        }
    return out


def process_run(
    *,
    run_id: int,
    date_from: Optional[dt.date],
    date_to: Optional[dt.date],
    min_age_minutes: int = 60,
    limit: int = 200,
    reprocess: bool = False,
    conversation_id: Optional[str] = None,
    debug_run_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Process eligible conversations for a run id and persist results.

    Returns counters summary.
    """
    from sqlalchemy import text as _text
    from .matching import link_order_for_extraction  # lazy import

    now = dt.datetime.utcnow()
    min_age_delta = dt.timedelta(minutes=max(0, int(min_age_minutes)))
    cutoff_dt = now - min_age_delta

    considered = 0
    processed = 0
    linked = 0
    purchases = 0
    purchases_unlinked = 0
    errors: List[str] = []

    with get_session() as session:
        # Optional reprocess: clear processed markers for in-scope conversations
        if reprocess:
            try:
                params_clear: Dict[str, Any] = {"cutoff": cutoff_dt.isoformat(" ")}
                where_clear = ["last_message_at <= :cutoff"]
                if date_from and date_to and date_from <= date_to:
                    dt_end = date_to + dt.timedelta(days=1)
                    params_clear["df"] = f"{date_from.isoformat()} 00:00:00"
                    params_clear["dte"] = f"{dt_end.isoformat()} 00:00:00"
                    where_clear.append("last_message_at >= :df AND last_message_at < :dte")
                elif date_from:
                    params_clear["df"] = f"{date_from.isoformat()} 00:00:00"
                    where_clear.append("last_message_at >= :df")
                elif date_to:
                    dt_end = date_to + dt.timedelta(days=1)
                    params_clear["dte"] = f"{dt_end.isoformat()} 00:00:00"
                    where_clear.append("last_message_at < :dte")
                # IMPORTANT: do NOT clear linked_order_id on reprocess; preserve manual links
                set_cols_full = "ai_processed_at=NULL, ai_status=NULL, ai_run_id=NULL"
                set_cols_nostatus = "ai_processed_at=NULL, ai_run_id=NULL"
                sql_clear_full = ("UPDATE conversations SET " + set_cols_full + " WHERE " + " AND ".join(where_clear))
                sql_clear_nostatus = ("UPDATE conversations SET " + set_cols_nostatus + " WHERE " + " AND ".join(where_clear))
                rc = None
                try:
                    rc = session.exec(_text(sql_clear_full).params(**params_clear)).rowcount
                except Exception as e:
                    # If ai_status column is missing, retry without it
                    if "Unknown column 'ai_status'" in str(e):
                        try:
                            rc = session.exec(_text(sql_clear_nostatus).params(**params_clear)).rowcount
                        except Exception:
                            rc = 0
                    else:
                        raise
                try:
                    ai_run_log(run_id, "info", "reprocess_clear", {"cleared": int(rc or 0)})
                except Exception:
                    pass
            except Exception as e:
                try:
                    ai_run_log(run_id, "error", "reprocess_clear_error", {"error": str(e)})
                except Exception:
                    pass
        # Select eligible conversations by last_message_at and previously processed watermark
        params: Dict[str, Any] = {"cutoff": cutoff_dt.isoformat(" ")}
        if conversation_id:
            sql = "SELECT convo_id FROM conversations WHERE convo_id = :single LIMIT 1"
            row = session.exec(_text(sql).params(single=conversation_id)).first()
            if row:
                cid_val = row.convo_id if hasattr(row, "convo_id") else row[0]
                convo_ids = [str(cid_val)]
            else:
                convo_ids = [conversation_id]
            considered = len(convo_ids)
        else:
            # Build candidate conversations from messages table (DB-agnostic)
            cutoff_ms = int(cutoff_dt.timestamp() * 1000)
            msg_where = ["m.conversation_id IS NOT NULL", "(m.timestamp_ms IS NULL OR m.timestamp_ms <= :cutoff_ms)"]
            msg_params: Dict[str, Any] = {"cutoff_ms": int(cutoff_ms)}
            if date_from and date_to and date_from <= date_to:
                ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
                ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
                msg_where.append("(m.timestamp_ms IS NULL OR (m.timestamp_ms >= :ms_from AND m.timestamp_ms < :ms_to))")
                msg_params["ms_from"] = int(ms_from)
                msg_params["ms_to"] = int(ms_to)
            elif date_from:
                ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
                msg_where.append("(m.timestamp_ms IS NULL OR m.timestamp_ms >= :ms_from)")
                msg_params["ms_from"] = int(ms_from)
            elif date_to:
                ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
                msg_where.append("(m.timestamp_ms IS NULL OR m.timestamp_ms < :ms_to)")
                msg_params["ms_to"] = int(ms_to)
            # Determine backend for UNIX timestamp conversion
            try:
                backend = getattr(session.get_bind().engine.url, "get_backend_name", lambda: "")()
            except Exception:
                backend = ""
            ts_expr = "COALESCE(UNIX_TIMESTAMP(ac.ai_process_time),0)*1000" if backend == "mysql" else "COALESCE(strftime('%s', ac.ai_process_time),0)*1000"
            # conversations are now the single source; use conversations.ai_process_time/ai_processed_at
            sql_msg = (
                "SELECT t.conversation_id FROM ("
                " SELECT m.conversation_id, MAX(COALESCE(m.timestamp_ms,0)) AS last_ts"
                " FROM message m WHERE " + " AND ".join(msg_where) +
                " GROUP BY m.conversation_id"
                ") t LEFT JOIN conversations c ON c.id = t.conversation_id "
                + ("WHERE (c.ai_process_time IS NULL OR t.last_ts > " + ts_expr + ") " if not reprocess else " ")
                + f"ORDER BY t.last_ts DESC LIMIT {int(limit)}"
            )
            rows = session.exec(_text(sql_msg).params(**msg_params)).all()
            convo_ids = [int(r.conversation_id if hasattr(r, "conversation_id") else r[0]) for r in rows]
            considered = len(convo_ids)
        # Fallback: if conversations table yields 0, select distinct conversation_id
        # from messages by timestamp window so we can still process
        if not conversation_id and considered == 0:
            try:
                cutoff_ms = int(cutoff_dt.timestamp() * 1000)
                msg_where = ["(m.timestamp_ms IS NULL OR m.timestamp_ms <= :cutoff_ms)", "m.conversation_id IS NOT NULL"]
                msg_params: Dict[str, Any] = {"cutoff_ms": int(cutoff_ms)}
                if date_from and date_to and date_from <= date_to:
                    ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
                    ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
                    msg_where.append("(m.timestamp_ms IS NULL OR (m.timestamp_ms >= :ms_from AND m.timestamp_ms < :ms_to))")
                    msg_params["ms_from"] = int(ms_from)
                    msg_params["ms_to"] = int(ms_to)
                elif date_from:
                    ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
                    msg_where.append("(m.timestamp_ms IS NULL OR m.timestamp_ms >= :ms_from)")
                    msg_params["ms_from"] = int(ms_from)
                elif date_to:
                    ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
                    msg_where.append("(m.timestamp_ms IS NULL OR m.timestamp_ms < :ms_to)")
                    msg_params["ms_to"] = int(ms_to)
                sql_msg = (
                    "SELECT DISTINCT m.conversation_id FROM message m WHERE " + " AND ".join(msg_where) + f" ORDER BY 1 DESC LIMIT {int(limit)}"
                )
                rows_m = session.exec(_text(sql_msg).params(**msg_params)).all()
                convo_ids = [r.conversation_id if hasattr(r, "conversation_id") else r[0] for r in rows_m]
                considered = len(convo_ids)
                ai_run_log(run_id, "info", "fallback_candidates", {"count": int(considered)})
            except Exception as e:
                ai_run_log(run_id, "error", "fallback_error", {"error": str(e)})
    try:
        log.info(
            "ig_ai run start rid=%s df=%s dt=%s min_age=%s limit=%s considered=%s",
            run_id,
            (date_from.isoformat() if date_from else None),
            (date_to.isoformat() if date_to else None),
            int(min_age_minutes),
            int(limit),
            considered,
        )
    except Exception:
        pass
    ai_run_log(run_id, "info", "run_start", {
        "date_from": (date_from.isoformat() if date_from else None),
        "date_to": (date_to.isoformat() if date_to else None),
        "min_age_minutes": int(min_age_minutes),
        "limit": int(limit),
        "considered": int(considered),
    })
    # Initialize run row with considered count for early visibility
    try:
        from sqlalchemy import text as _text  # local alias if not imported above
        with get_session() as session:
            session.exec(
                _text(
                    """
                    UPDATE ig_ai_run SET
                      conversations_considered = :cns,
                      conversations_processed = COALESCE(conversations_processed, 0),
                      orders_linked = COALESCE(orders_linked, 0),
                      purchases_detected = COALESCE(purchases_detected, 0),
                      purchases_unlinked = COALESCE(purchases_unlinked, 0)
                    WHERE id = :rid
                    """
                ).params(cns=int(considered), rid=int(run_id))
            )
    except Exception:
        pass

    # Helper to check cancellation flag quickly
    def _is_cancelled() -> bool:
        try:
            with get_session() as s2:
                row = s2.exec(_text("SELECT cancelled_at FROM ig_ai_run WHERE id=:id").params(id=int(run_id))).first()
                if not row:
                    return False
                val = row.cancelled_at if hasattr(row, 'cancelled_at') else (row[0] if isinstance(row, (list, tuple)) else None)
                return bool(val)
        except Exception:
            return False

    if _is_cancelled():
        errors.append("cancelled")
        with get_session() as session:
            try:
                session.exec(_text("UPDATE ig_ai_run SET completed_at=CURRENT_TIMESTAMP WHERE id=:id").params(id=int(run_id)))
            except Exception:
                pass
        return {
            "considered": 0,
            "processed": 0,
            "linked": 0,
            "purchases": 0,
            "purchases_unlinked": 0,
            "errors": errors,
        }

    debug_entries: list[dict[str, Any]] = []
    include_meta = bool(debug_run_id)

    for cid in convo_ids:
        ai_run_log(run_id, "debug", "analyze_start", {"conversation_id": cid})
        try:
            data = analyze_conversation(cid, limit=limit, run_id=run_id, include_meta=include_meta)
        except Exception as e:
            errors.append(f"{cid}: {e}")
            ai_run_log(run_id, "error", "analyze_error", {"conversation_id": cid, "error": str(e)})
            # persist error status so we don't spin forever; keep ai_json for debugging
            with get_session() as session:
                try:
                    session.exec(
                        _text(
                            "UPDATE conversations SET ai_status=:s, ai_json=:j, ai_processed_at=CURRENT_TIMESTAMP, ai_process_time=CURRENT_TIMESTAMP, ai_run_id=:rid WHERE convo_id=:cid"
                        ).params(s="error", j=json.dumps({"error": str(e)}), rid=run_id, cid=cid)
                    )
                except Exception:
                    pass
            if include_meta and conversation_id and cid == conversation_id:
                debug_entries.append({
                    "conversation_id": cid,
                    "status": "error",
                    "error": str(e),
                })
            continue

        def _clean_field(val: Any) -> Optional[str]:
            if val is None:
                return None
            try:
                if isinstance(val, str):
                    stripped = val.strip()
                    return stripped or None
                stripped = str(val).strip()
                return stripped or None
            except Exception:
                return None

        meta_info = None
        if include_meta and isinstance(data, dict):
            meta_info = data.get("meta")
            if isinstance(data, dict):
                data = dict(data)
                data.pop("meta", None)

        status = "no_purchase"
        linked_order_id: Optional[int] = None
        buyer_name_clean = _clean_field(data.get("buyer_name"))
        phone_clean = _clean_field(data.get("phone"))
        address_clean = _clean_field(data.get("address"))
        # Normalize honorific/non-name placeholders
        banned_names = {"abi","abim","kardeşim","kardesim","hocam","usta","kanka","canım","canim"}
        try:
            if buyer_name_clean:
                if buyer_name_clean.strip().lower() in banned_names:
                    buyer_name_clean = None
        except Exception:
            pass
        # Normalize phone to digits and reduce to last 10
        phone_digits = normalize_phone(phone_clean)
        phone_last10 = phone_digits[-10:] if len(phone_digits) >= 10 else phone_digits
        data["buyer_name"] = buyer_name_clean
        data["phone"] = phone_last10 or None
        data["address"] = address_clean
        try:
            ai_run_log(run_id, "debug", "normalize_phone", {
                "phone_input": phone_clean,
                "digits": phone_digits,
                "last10": phone_last10,
            }) if run_id is not None else None
        except Exception:
            pass
        # Enforce minimum contact info for purchase: at least one of (name, phone, address)
        has_min_contact = bool((buyer_name_clean and buyer_name_clean.strip()) or (phone_last10 and len(phone_last10) >= 7) or (address_clean and address_clean.strip()))
        effective_purchase = bool(data.get("purchase_detected")) and has_min_contact
        # Reflect enforcement into debug/result payload so single-debug shows the final decision
        if not effective_purchase:
            try:
                data["purchase_detected"] = False
            except Exception:
                pass
        if effective_purchase:
            purchases += 1
            try:
                with get_session() as session:
                    linked_order_id = link_order_for_extraction(session, data, date_from=date_from, date_to=date_to, run_id=run_id)
            except Exception as le:
                errors.append(f"{cid}: match_err {le}")
                linked_order_id = None
            if linked_order_id:
                linked += 1
                status = "ok"
                try:
                    log.info("ig_ai convo=%s purchase=1 linked_order_id=%s", cid, int(linked_order_id))
                except Exception:
                    pass
                ai_run_log(run_id, "info", "purchase_linked", {"conversation_id": cid, "order_id": int(linked_order_id)})
            else:
                purchases_unlinked += 1
                status = "ambiguous"
                try:
                    log.info("ig_ai convo=%s purchase=1 linked_order_id=null", cid)
                except Exception:
                    pass
                ai_run_log(run_id, "info", "purchase_unlinked", {"conversation_id": cid})
        else:
            try:
                log.debug("ig_ai convo=%s purchase=0", cid)
            except Exception:
                pass
            ai_run_log(run_id, "debug", "no_purchase", {"conversation_id": cid})

        with get_session() as session:
            try:
                ai_run_log(run_id, "debug", "persist_start", {"conversation_id": cid, "status": status, "linked_order_id": linked_order_id})
                # Persist conversations fields
                session.exec(
                    _text(
                        """
                        UPDATE conversations SET
                          contact_name = COALESCE(NULLIF(TRIM(contact_name), ''), :name),
                          contact_phone = COALESCE(NULLIF(TRIM(contact_phone), ''), :phone),
                          contact_address = COALESCE(NULLIF(TRIM(contact_address), ''), :addr),
                          ai_status = :st,
                          ai_json = :js,
                          ai_processed_at = CURRENT_TIMESTAMP,
                          ai_process_time = CURRENT_TIMESTAMP,
                          linked_order_id = COALESCE(linked_order_id, :oid),
                          ai_run_id = :rid
                        WHERE convo_id = :cid
                        """
                    ).params(
                        name=buyer_name_clean,
                        phone=phone_clean,
                        addr=address_clean,
                        st=status,
                        js=json.dumps(data, ensure_ascii=False),
                        oid=linked_order_id,
                        rid=run_id,
                        cid=cid,
                    )
                )
                # Also back-fill order.ig_conversation_id if linked
                if linked_order_id and cid:
                    session.exec(
                        _text('UPDATE `order` SET ig_conversation_id = COALESCE(ig_conversation_id, :cid) WHERE id=:oid').params(cid=cid, oid=int(linked_order_id))
                    )
                # Update ai_conversations watermark/status (vendor-neutral via upsert-like two-step)
                try:
                    session.exec(
                        _text(
                            """
                            INSERT INTO ai_conversations(convo_id, ai_process_time, ai_status, ai_json, linked_order_id)
                            VALUES(:cid, CURRENT_TIMESTAMP, :st, :js, :oid)
                            """
                        ).params(cid=cid, st=status, js=json.dumps(data, ensure_ascii=False), oid=linked_order_id)
                    )
                except Exception:
                    # Fallback to UPDATE when row exists
                    try:
                        session.exec(
                            _text(
                                """
                                UPDATE ai_conversations
                                SET ai_process_time=CURRENT_TIMESTAMP,
                                    ai_status=:st,
                                    ai_json=:js,
                                    linked_order_id=COALESCE(linked_order_id, :oid)
                                WHERE convo_id=:cid
                                """
                            ).params(cid=cid, st=status, js=json.dumps(data, ensure_ascii=False), oid=linked_order_id)
                        )
                    except Exception:
                        pass
                # Optional: write history row
                try:
                    session.exec(
                        _text(
                            """
                            INSERT INTO ig_ai_result(convo_id, run_id, status, ai_json, linked_order_id, created_at)
                            VALUES(:cid, :rid, :st, :js, :oid, CURRENT_TIMESTAMP)
                            """
                        ).params(cid=cid, rid=run_id, st=status, js=json.dumps(data, ensure_ascii=False), oid=linked_order_id)
                    )
                except Exception:
                    pass
                processed += 1
                ai_run_log(run_id, "info", "persist_done", {"conversation_id": cid, "status": status, "linked_order_id": linked_order_id})
            except Exception as pe:
                errors.append(f"{cid}: persist_err {pe}")
                ai_run_log(run_id, "error", "persist_error", {"conversation_id": cid, "error": str(pe)})
                ai_run_log(run_id, "error", "persist_error", {"conversation_id": cid, "error": str(pe)})

        # Persist live counters for UI progress (best-effort)
        try:
            with get_session() as session:
                session.exec(
                    _text(
                        """
                        UPDATE ig_ai_run SET
                          conversations_processed = :prs,
                          orders_linked = :lnk,
                          purchases_detected = :pur,
                          purchases_unlinked = :pun
                        WHERE id = :rid
                        """
                    ).params(
                        prs=int(processed),
                        lnk=int(linked),
                        pur=int(purchases),
                        pun=int(purchases_unlinked),
                        rid=int(run_id),
                    )
                )
        except Exception:
            pass

        # cancellation check between items
        if _is_cancelled():
            errors.append("cancelled")
            break

        if include_meta and conversation_id and cid == conversation_id:
            debug_entries.append({
                "conversation_id": cid,
                "status": status,
                "linked_order_id": linked_order_id,
                "result": data,
                "meta": meta_info,
                "errors": list(errors),
            })

    # Update run row
    with get_session() as session:
        try:
            session.exec(
                _text(
                    """
                    UPDATE ig_ai_run SET
                      completed_at = CURRENT_TIMESTAMP,
                      conversations_considered = :cns,
                      conversations_processed = :prs,
                      orders_linked = :lnk,
                      purchases_detected = :pur,
                      purchases_unlinked = :pun,
                      errors_json = :err
                    WHERE id = :rid
                    """
                ).params(
                    cns=considered,
                    prs=processed,
                    lnk=linked,
                    pur=purchases,
                    pun=purchases_unlinked,
                    err=json.dumps(errors, ensure_ascii=False) if errors else None,
                    rid=run_id,
                )
            )
        except Exception:
            # Fallback: at least mark completion timestamp
            try:
                session.exec(_text("UPDATE ig_ai_run SET completed_at=CURRENT_TIMESTAMP WHERE id=:rid").params(rid=run_id))
            except Exception:
                pass
    try:
        log.info(
            "ig_ai run done rid=%s considered=%s processed=%s linked=%s purchases=%s unlinked=%s errors=%s",
            run_id,
            considered,
            processed,
            linked,
            purchases,
            purchases_unlinked,
            len(errors),
        )
    except Exception:
        pass
    ai_run_log(run_id, "info", "run_done", {
        "considered": int(considered),
        "processed": int(processed),
        "linked": int(linked),
        "purchases": int(purchases),
        "unlinked": int(purchases_unlinked),
        "errors": len(errors),
    })

    result_summary: Dict[str, Any] = {
        "considered": considered,
        "processed": processed,
        "linked": linked,
        "purchases": purchases,
        "purchases_unlinked": purchases_unlinked,
        "errors": errors,
    }
    if debug_entries:
        result_summary["debug_entries"] = debug_entries

    return result_summary


