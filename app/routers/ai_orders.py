from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import or_, text, func
from sqlmodel import select

from ..db import get_session
from ..models import AiOrderCandidate, Conversation, IGUser
from ..services.ai_orders import ALLOWED_STATUSES
from ..services.ai_orders_detection import process_conversations_by_date_range

router = APIRouter(prefix="/ai/orders", tags=["ai-orders"])


def _parse_json(raw: Optional[str]) -> Any:
	if not raw:
		return None
	try:
		return json.loads(raw)
	except Exception:
		return None


def _to_iso(value: Optional[dt.datetime]) -> Optional[str]:
	if not value:
		return None
	# Convert UTC naive datetimes to Turkey time (UTC+3) for display consistency
	if value.tzinfo is None:
		value = value + dt.timedelta(hours=3)
	else:
		value = value.astimezone(dt.timezone(dt.timedelta(hours=3)))
	return value.isoformat()


def _format_order_row(candidate: AiOrderCandidate, conversation: Optional[Conversation], ig_user: Optional[IGUser]) -> Dict[str, Any]:
	payload = _parse_json(candidate.order_payload_json) or {}
	history = _parse_json(candidate.status_history_json) or []
	username = getattr(ig_user, "username", None)
	contact_name = getattr(ig_user, "contact_name", None)
	contact_phone = getattr(ig_user, "contact_phone", None)
	
	# Extract insight fields from payload
	insights = {
		"purchase_barriers": payload.get("purchase_barriers"),
		"conversion_factors": payload.get("conversion_factors"),
		"conversation_quality": payload.get("conversation_quality", {}),
		"customer_sentiment": payload.get("customer_sentiment", {}),
		"improvement_areas": payload.get("improvement_areas", []),
		"what_worked_well": payload.get("what_worked_well", []),
	}
	
	return {
		"id": candidate.id,
		"conversation_id": candidate.conversation_id,
		"conversation_link": f"/ig/inbox/{candidate.conversation_id}",
		"status": candidate.status or "interested",
		"status_reason": candidate.status_reason,
		"status_history": history if isinstance(history, list) else [],
		"order_payload": payload if isinstance(payload, dict) else {},
		"insights": insights,
		"username": username,
		"contact_name": contact_name,
		"contact_phone": contact_phone,
		"last_status_at": _to_iso(candidate.last_status_at),
		"placed_at": _to_iso(candidate.placed_at),
		"updated_at": _to_iso(candidate.updated_at),
		"created_at": _to_iso(candidate.created_at),
		"ad_link": getattr(conversation, "last_ad_link", None) if conversation else None,
		"graph_conversation_id": getattr(conversation, "graph_conversation_id", None) if conversation else None,
	}


@router.get("")
def list_ai_order_candidates(request: Request, status: Optional[str] = None, q: Optional[str] = None, limit: int = 200, start: Optional[str] = None, end: Optional[str] = None):
	def _parse_date(value: Optional[str]) -> Optional[dt.date]:
		if not value:
			return None
		try:
			return dt.date.fromisoformat(value)
		except Exception:
			return None
	
	n = max(1, min(int(limit or 200), 500))
	status_filter = (status or "").strip().lower() or None
	if status_filter and status_filter not in ALLOWED_STATUSES:
		status_filter = None
	query_text = (q or "").strip()
	start_date = _parse_date(start)
	end_date = _parse_date(end)
	
	with get_session() as session:
		stmt = (
			select(AiOrderCandidate, Conversation, IGUser)
			.join(Conversation, AiOrderCandidate.conversation_id == Conversation.id)
			.join(IGUser, IGUser.id == Conversation.ig_user_id, isouter=True)
		)
		if status_filter:
			stmt = stmt.where(AiOrderCandidate.status == status_filter)
		if query_text:
			like = f"%{query_text}%"
			stmt = stmt.where(
				or_(
					IGUser.username.ilike(like),
					IGUser.contact_name.ilike(like),
					IGUser.contact_phone.ilike(like),
				)
			)
		# Date filtering: Use placed_at if available (for placed orders), otherwise use conversation's last_message_at
		# This filters by when the order was actually placed or when the conversation happened
		if start_date or end_date:
			# Use COALESCE to prefer placed_at, fall back to last_message_at
			# If both are None, the row won't match the date filter (which is correct)
			date_field = func.coalesce(
				AiOrderCandidate.placed_at,
				Conversation.last_message_at
			)
			if start_date:
				start_dt = dt.datetime.combine(start_date, dt.time.min)
				stmt = stmt.where(date_field >= start_dt)
			if end_date:
				end_dt = dt.datetime.combine(end_date + dt.timedelta(days=1), dt.time.min)
				stmt = stmt.where(date_field < end_dt)
		
		# Get all matching rows first (without limit for accurate counts)
		# Order by the same date field used for filtering
		order_date_field = func.coalesce(
			AiOrderCandidate.placed_at,
			Conversation.last_message_at,
			AiOrderCandidate.updated_at  # Final fallback for ordering
		)
		stmt_unlimited = stmt.order_by(order_date_field.desc())
		all_matching_rows = session.exec(stmt_unlimited).all()
		
		# Calculate status counts from all matching rows
		status_counts = {status: 0 for status in ALLOWED_STATUSES}
		for candidate_row, convo_row, user_row in all_matching_rows:
			status_val = (candidate_row.status or "interested").strip().lower()
			if status_val in ALLOWED_STATUSES:
				status_counts[status_val] = status_counts.get(status_val, 0) + 1
		
		# Now get limited rows for display
		# Order by the same date field used for filtering
		order_date_field = func.coalesce(
			AiOrderCandidate.placed_at,
			Conversation.last_message_at,
			AiOrderCandidate.updated_at  # Final fallback for ordering
		)
		stmt = stmt.order_by(order_date_field.desc()).limit(n)
		rows = session.exec(stmt).all()

	orders = []
	for candidate_row, convo_row, user_row in rows:
		orders.append(_format_order_row(candidate_row, convo_row, user_row))
	templates = request.app.state.templates
	return templates.TemplateResponse(
		"ai_orders.html",
		{
			"request": request,
			"orders": orders,
			"filter_status": status_filter,
			"query": query_text,
			"start_date": start_date.isoformat() if start_date else "",
			"end_date": end_date.isoformat() if end_date else "",
			"status_counts": status_counts,
			"limit": n,
			"available_statuses": sorted(ALLOWED_STATUSES),
		},
	)


@router.get("/detect")
def detect_ai_orders_page(request: Request, start: Optional[str] = None, end: Optional[str] = None, limit: int = 100, process: Optional[str] = None, skip_processed: Optional[str] = None):
	"""UI page for batch processing conversations by date range."""
	def _parse_date(value: Optional[str]) -> Optional[dt.date]:
		if not value:
			return None
		try:
			return dt.date.fromisoformat(value)
		except Exception:
			return None
	
	start_date = _parse_date(start)
	end_date = _parse_date(end)
	
	# Default to last 7 days if no dates provided (for form display only)
	if not start_date or not end_date:
		today = dt.date.today()
		end_date = today
		start_date = today - dt.timedelta(days=7)
	
	# Parse skip_processed option
	# When form is submitted (process=1), if skip_processed is not provided, checkbox was unchecked = reprocess (False)
	# If skip_processed is provided and not explicitly "0"/"false", it's True (skip)
	if process:
		# Form submission - checkbox unchecked means parameter not sent
		if skip_processed is None:
			skip_processed_flag = False  # Reprocess all
		else:
			skip_processed_flag = skip_processed not in ("0", "false", "False")
	else:
		# Initial page load - default to True (skip processed)
		skip_processed_flag = True if skip_processed is None else (skip_processed not in ("0", "false", "False"))
	
	result = None
	# Only process if dates are provided AND process parameter is set (form submission)
	if start_date and end_date and process:
		try:
			result = process_conversations_by_date_range(start_date, end_date, limit=limit, skip_processed=skip_processed_flag)
		except Exception as e:
			result = {
				"processed": 0,
				"created": 0,
				"updated": 0,
				"skipped": 0,
				"errors": [str(e)],
				"total_conversations": 0,
				"skip_processed": skip_processed_flag,
			}
	
	templates = request.app.state.templates
	return templates.TemplateResponse(
		"ai_orders_detect.html",
		{
			"request": request,
			"start_date": start_date.isoformat() if start_date else "",
			"end_date": end_date.isoformat() if end_date else "",
			"limit": limit,
			"skip_processed": skip_processed_flag,
			"result": result,
		},
	)


@router.post("/detect")
def detect_ai_orders_api(start: str, end: str, limit: int = 100, skip_processed: bool = True):
	"""API endpoint for batch processing conversations by date range."""
	def _parse_date(value: str) -> dt.date:
		try:
			return dt.date.fromisoformat(value)
		except Exception:
			raise HTTPException(status_code=400, detail=f"Invalid date format: {value}")
	
	start_date = _parse_date(start)
	end_date = _parse_date(end)
	
	if end_date < start_date:
		raise HTTPException(status_code=400, detail="end_date must be >= start_date")
	
	try:
		result = process_conversations_by_date_range(start_date, end_date, limit=limit, skip_processed=skip_processed)
		return result
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

