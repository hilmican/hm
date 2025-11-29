from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text as _text
from sqlmodel import select

from ..db import get_session
from ..models import Message
from .ai import AIClient, get_ai_model_from_settings
from .ai_orders import _update_candidate, ALLOWED_STATUSES
from .prompts import IG_ORDER_CANDIDATE_PROMPT

log = logging.getLogger("ai.orders.detection")


def _format_transcript(messages: List[Any], max_chars: int = 15000) -> str:
	"""Format messages into a transcript with in/out direction markers."""
	parts: List[str] = []
	for m in messages:
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


def analyze_conversation_for_order_candidate(conversation_id: int, run_id: Optional[int] = None) -> Dict[str, Any]:
	"""Run AI over a single conversation to extract order candidate information and determine status.
	
	Returns a dict with keys: status, customer, product, measurements, price, notes
	"""
	model = get_ai_model_from_settings()
	client = AIClient(model=model)
	if not client.enabled:
		raise RuntimeError("AI client is not configured. Set OPENAI_API_KEY.")
	
	with get_session() as session:
		msgs = session.exec(
			select(Message)
			.where(Message.conversation_id == conversation_id)
			.order_by(Message.timestamp_ms.asc())
			.limit(500)
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
	
	schema_hint = (
		'{"status": "interested|very-interested|placed|not-interested", '
		'"customer": {"name": "str|null", "phone": "str|null", "address": "str|null", "city": "str|null"}, '
		'"product": {"name": "str|null", "size": "str|null", "color": "str|null", "quantity": "int|null"}, '
		'"measurements": {"height_cm": "int|null", "weight_kg": "int|null"}, '
		'"price": "float|null", "notes": "str|null", '
		'"purchase_barriers": ["str"]|null, '
		'"conversion_factors": ["str"]|null, '
		'"conversation_quality": {"response_speed": "fast|normal|slow|null", "clarity": "clear|moderate|unclear|null", "helpfulness": "very-helpful|helpful|not-helpful|null", "proactivity": "proactive|reactive|passive|null"}, '
		'"customer_sentiment": {"engagement": "high|medium|low|null", "satisfaction": "satisfied|neutral|dissatisfied|null", "urgency": "urgent|normal|casual|null"}, '
		'"improvement_areas": ["str"], '
		'"what_worked_well": ["str"]}'
	)
	
	user_prompt = (
		"Aşağıda bir DM konuşması transkripti var. \n"
		"Lütfen konuşmayı kapsamlı bir şekilde analiz et, sipariş bilgilerini çıkar, durumu belirle ve konuşmanın kalitesi ile iyileştirme alanlarını değerlendir.\n"
		"Durum: interested (ilgi gösterdi), very-interested (detaylar verdi), placed (sipariş tamamlandı), not-interested (vazgeçti/sipariş yok).\n"
		"Uydurma yapma; metinde yoksa null bırak. Ancak analiz ve değerlendirme alanlarını (purchase_barriers, conversion_factors, conversation_quality, vb.) doldur.\n\n"
		f"Şema: {schema_hint}\n\n"
		f"Transkript:\n{transcript}"
	)
	
	try:
		data = client.generate_json(system_prompt=IG_ORDER_CANDIDATE_PROMPT, user_prompt=user_prompt)
	except Exception as e:
		log.error("AI analysis failed for conversation_id=%s: %s", conversation_id, str(e))
		raise RuntimeError(f"AI analysis failed: {e}")
	
	if not isinstance(data, dict):
		raise RuntimeError("AI returned non-dict JSON")
	
	# Validate and normalize status
	status = data.get("status", "").strip().lower()
	if status not in ALLOWED_STATUSES:
		log.warning("Invalid status '%s' from AI for conversation_id=%s, defaulting to 'interested'", status, conversation_id)
		status = "interested"
	
	# Normalize price
	def _parse_price(val: Any) -> Optional[float]:
		if val is None:
			return None
		try:
			if isinstance(val, (int, float)):
				return float(val)
			s = str(val)
			import re
			cleaned = re.sub(r"[^0-9,\.]", "", s)
			cleaned = cleaned.replace(",", ".")
			if cleaned.count(".") > 1:
				first = cleaned.find(".")
				cleaned = cleaned[: first + 1] + cleaned[first + 1 :].replace(".", "")
			return float(cleaned) if cleaned else None
		except Exception:
			return None
	
	# Build normalized output
	out: Dict[str, Any] = {
		"status": status,
		"customer": data.get("customer") or {},
		"product": data.get("product") or {},
		"measurements": data.get("measurements") or {},
		"price": _parse_price(data.get("price")),
		"notes": data.get("notes"),
		"purchase_barriers": data.get("purchase_barriers"),
		"conversion_factors": data.get("conversion_factors"),
		"conversation_quality": data.get("conversation_quality") or {},
		"customer_sentiment": data.get("customer_sentiment") or {},
		"improvement_areas": data.get("improvement_areas") or [],
		"what_worked_well": data.get("what_worked_well") or [],
	}
	
	# Ensure customer, product, measurements are dicts
	if not isinstance(out["customer"], dict):
		out["customer"] = {}
	if not isinstance(out["product"], dict):
		out["product"] = {}
	if not isinstance(out["measurements"], dict):
		out["measurements"] = {}
	
	# Ensure conversation_quality and customer_sentiment are dicts
	if not isinstance(out["conversation_quality"], dict):
		out["conversation_quality"] = {}
	if not isinstance(out["customer_sentiment"], dict):
		out["customer_sentiment"] = {}
	
	# Ensure arrays are lists
	if not isinstance(out["improvement_areas"], list):
		out["improvement_areas"] = []
	if not isinstance(out["what_worked_well"], list):
		out["what_worked_well"] = []
	if out["purchase_barriers"] is not None and not isinstance(out["purchase_barriers"], list):
		out["purchase_barriers"] = []
	if out["conversion_factors"] is not None and not isinstance(out["conversion_factors"], list):
		out["conversion_factors"] = []
	
	return out


def process_conversations_by_date_range(
	start_date: dt.date,
	end_date: dt.date,
	limit: int = 100,
	run_id: Optional[int] = None,
	skip_processed: bool = True,
) -> Dict[str, Any]:
	"""Process conversations by date range and create/update AI order candidates.
	
	Args:
		start_date: Start date for conversation filtering
		end_date: End date for conversation filtering
		limit: Maximum number of conversations to process
		run_id: Optional run ID for logging
		skip_processed: If True, skip conversations that already have AI order candidates
	
	Returns summary statistics: processed, created, updated, errors, skipped
	"""
	processed = 0
	created = 0
	updated = 0
	skipped = 0
	errors: List[str] = []
	
	# Convert dates to milliseconds
	start_dt = dt.datetime.combine(start_date, dt.time.min)
	end_dt = dt.datetime.combine(end_date + dt.timedelta(days=1), dt.time.min)
	start_ms = int(start_dt.timestamp() * 1000)
	end_ms = int(end_dt.timestamp() * 1000)
	
	# Get conversation IDs from messages in date range
	with get_session() as session:
		if skip_processed:
			# Exclude conversations that already have AI order candidates
			# Also exclude legacy dm: conversation_ids (they should be migrated to integer IDs)
			sql = (
				"SELECT DISTINCT m.conversation_id FROM message m "
				"LEFT JOIN ai_order_candidates aoc ON aoc.conversation_id = m.conversation_id "
				"WHERE m.timestamp_ms >= :start_ms AND m.timestamp_ms < :end_ms "
				"AND m.conversation_id IS NOT NULL "
				"AND CAST(m.conversation_id AS TEXT) NOT LIKE 'dm:%' "
				"AND aoc.id IS NULL "
				"ORDER BY m.conversation_id DESC "
				"LIMIT :lim"
			)
		else:
			# Include all conversations, even if they have candidates (for reprocessing)
			# Exclude legacy dm: conversation_ids
			sql = (
				"SELECT DISTINCT conversation_id FROM message "
				"WHERE timestamp_ms >= :start_ms AND timestamp_ms < :end_ms "
				"AND conversation_id IS NOT NULL "
				"AND CAST(conversation_id AS TEXT) NOT LIKE 'dm:%' "
				"ORDER BY conversation_id DESC "
				"LIMIT :lim"
			)
		rows = session.exec(_text(sql).params(start_ms=start_ms, end_ms=end_ms, lim=int(limit))).all()
		# Handle different return types: int (direct value), object with attribute, or tuple/list
		def _extract_conversation_id(r):
			if isinstance(r, int):
				return r
			if hasattr(r, "conversation_id"):
				return r.conversation_id
			if isinstance(r, (tuple, list)) and len(r) > 0:
				return r[0]
			# Try to access as Row object with index
			try:
				return r[0]
			except (TypeError, IndexError):
				return r
		
		def _normalize_conversation_id(cid):
			"""Convert conversation_id to integer, skipping dm: format strings."""
			if isinstance(cid, int):
				return cid
			if isinstance(cid, str):
				# Skip dm: format strings (legacy data that should be migrated)
				if cid.startswith("dm:"):
					return None
				# Try to convert to int
				try:
					return int(cid)
				except (ValueError, TypeError):
					return None
			# Try to convert other types
			try:
				return int(cid)
			except (ValueError, TypeError):
				return None
		
		# Extract and normalize conversation IDs, filtering out None values
		conversation_ids = [cid for cid in [_normalize_conversation_id(_extract_conversation_id(r)) for r in rows] if cid is not None]
		
		# If skip_processed is False, we still want to count how many would have been skipped
		if not skip_processed and conversation_ids:
			from ..models import AiOrderCandidate
			existing_candidates = session.exec(
				select(AiOrderCandidate.conversation_id).where(
					AiOrderCandidate.conversation_id.in_(conversation_ids)
				)
			).all()
			# SQLModel returns integers directly for single-column selects
			existing_ids = {_extract_conversation_id(c) for c in existing_candidates}
			skipped = len([c for c in conversation_ids if c in existing_ids])
	
	log.info(
		"Processing %d conversations for date range %s to %s (skip_processed=%s)",
		len(conversation_ids),
		start_date.isoformat(),
		end_date.isoformat(),
		skip_processed,
	)
	
	# Process each conversation
	for conv_id in conversation_ids:
		try:
			# Analyze conversation
			result = analyze_conversation_for_order_candidate(conv_id, run_id=run_id)
			
			# Build order payload with all insights
			order_payload: Dict[str, Any] = {
				"customer": result.get("customer", {}),
				"product": result.get("product", {}),
				"measurements": result.get("measurements", {}),
				"price": result.get("price"),
				"notes": result.get("notes"),
				# Include all insight fields
				"purchase_barriers": result.get("purchase_barriers"),
				"conversion_factors": result.get("conversion_factors"),
				"conversation_quality": result.get("conversation_quality", {}),
				"customer_sentiment": result.get("customer_sentiment", {}),
				"improvement_areas": result.get("improvement_areas", []),
				"what_worked_well": result.get("what_worked_well", []),
			}
			
			# Build status reason from notes or default
			status_reason = result.get("notes") or f"AI analizi: {result.get('status', 'unknown')}"
			
			# Check if candidate already exists
			from ..models import AiOrderCandidate
			with get_session() as session:
				existing = session.exec(
					select(AiOrderCandidate).where(AiOrderCandidate.conversation_id == conv_id).limit(1)
				).first()
				is_new = existing is None
			
			# Update or create candidate
			_update_candidate(
				conversation_id=conv_id,
				status=result.get("status", "interested"),
				note=status_reason,
				payload=order_payload,
				mark_placed=(result.get("status") == "placed"),
			)
			
			if is_new:
				created += 1
			else:
				updated += 1
			
			processed += 1
			
		except Exception as e:
			error_msg = f"conversation_id={conv_id}: {str(e)}"
			errors.append(error_msg)
			log.error("Error processing conversation %s: %s", conv_id, str(e))
			continue
	
	return {
		"processed": processed,
		"created": created,
		"updated": updated,
		"skipped": skipped,
		"errors": errors,
		"total_conversations": len(conversation_ids),
		"skip_processed": skip_processed,
	}

