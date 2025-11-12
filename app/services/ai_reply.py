from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from sqlmodel import select

from ..db import get_session
from ..models import Message
from .ai import AIClient


def _format_transcript(messages: List[Dict[str, Any]], max_chars: int = 16000) -> str:
	parts: List[str] = []
	for m in messages:
		role = (m.get("direction") or "in").lower()
		ts = int(m.get("timestamp_ms") or 0)
		txt = (m.get("text") or "").strip()
		parts.append(f"[{role}] {ts}: {txt}")
	out = "\n".join(parts)
	return out[-max_chars:] if len(out) > max_chars else out


def _system_prompt() -> str:
	# Lightweight style/rules distilled from the HiMan assistant spec
	return (
		"Sen HiMan için Instagram DM satış asistanısın. Amacın, müşteriyi nazik ve samimi "
		"bir dille hızlıca doğru bedene yönlendirip siparişe dönüştürmek ve sipariş sonrası kargo/değişim "
		"gibi destekleri net şekilde vermektir.\n"
		"- Samimi konuş; 'abim/ablacım' hitabını kullan. Emojiyi aşırıya kaçmadan kullan.\n"
		"- Kısa, parça-parça mesajlar gönder; selam, fiyat, ürün özeti, beden sorusu, ödeme seçeneği gibi adımları ayır.\n"
		"- Gereksiz resmi dil kullanma; doğrudan ve net ol. Uydurma bilgi verme.\n"
		"- Varsayılan akış: selam → fiyat/ürün özeti → beden soru/öneri → ödeme seçeneği → adres toplama → kargo bilgisi → memnuniyet mesajı.\n"
		"- Mesajın amacı: müşterinin söylediklerine göre en uygun sonraki adımı yazmak.\n"
		"ÇIKTI JSON olsun: {\"reply_text\":\"str\",\"confidence\":0..1,\"reason\":\"str\",\"notes\":\"str?\"}\n"
	)


def draft_reply(conversation_id: str, *, limit: int = 40, include_meta: bool = False) -> Dict[str, Any]:
	"""Create a suggested reply (shadow) for a conversation using the last N messages."""
	client = AIClient(model=os.getenv("AI_SHADOW_MODEL", "gpt-4o-mini"))
	if not client.enabled:
		raise RuntimeError("AI client is not configured. Set OPENAI_API_KEY.")
	with get_session() as session:
		msgs = session.exec(
			select(Message)
			.where(Message.conversation_id == conversation_id)
			.order_by(Message.timestamp_ms.asc())
			.limit(max(1, min(limit, 100)))
		).all()
		simple: List[Dict[str, Any]] = []
		for m in msgs:
			try:
				simple.append({
					"direction": (m.direction or "in"),
					"timestamp_ms": (m.timestamp_ms or 0),
					"text": (m.text or ""),
				})
			except Exception:
				continue
	transcript = _format_transcript(simple)
	user_prompt = (
		"Aşağıda bir DM konuşması transkripti var. Son kullanıcının yazmayı bitirmesi için 30sn beklediğimizi varsay. "
		"Şimdi, bir sonraki EN MANTIKLI tek mesajı hazırla; çok uzun olmasın, gerekiyorsa peş peşe 1-2 kısa cümle kullan. "
		"Telefon/adres gibi hassas bilgileri yalnızca kullanıcı net şekilde talep ederse iste.\n\n"
		f"Transkript:\n{transcript}"
	)
	if include_meta:
		data, _raw = client.generate_json(
			system_prompt=_system_prompt(),
			user_prompt=user_prompt,
			include_raw=True,
			temperature=0.3,
		)
	else:
		data = client.generate_json(
			system_prompt=_system_prompt(),
			user_prompt=user_prompt,
			temperature=0.3,
		)
	if not isinstance(data, dict):
		raise RuntimeError("AI returned non-dict JSON for shadow reply")
	# Normalize output
	reply = {
		"reply_text": (data.get("reply_text") or "").strip(),
		"confidence": float(data.get("confidence") or 0.6),
		"reason": (data.get("reason") or "auto"),
		"notes": (data.get("notes") or None),
		"model": client.model,
	}
	return reply


