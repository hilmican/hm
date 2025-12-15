"""
Extract Q&A pairs from conversations for product Q&A import.

Groups consecutive IN messages as questions and consecutive OUT messages as answers,
detects product focus for each Q&A segment, and filters by date range.
"""
from typing import List, Dict, Optional, Tuple
import datetime as dt
from dataclasses import dataclass

from sqlmodel import Session, select, text
from sqlalchemy import or_, and_

from ..models import Conversation, Message, Product


@dataclass
class ExtractedQA:
	"""Represents a Q&A pair extracted from a conversation."""
	conversation_id: int
	question: str
	answer: str
	product_id: Optional[int]
	product_name: Optional[str]
	question_start_timestamp: Optional[int]  # Timestamp of first question message
	answer_end_timestamp: Optional[int]  # Timestamp of last answer message


def extract_qa_from_conversation(
	session: Session,
	conversation_id: int,
	min_question_length: int = 5,
	min_answer_length: int = 5,
	start_timestamp_ms: Optional[int] = None,
	end_timestamp_ms: Optional[int] = None,
	category_filter: Optional[str] = None,
	product_hint: Optional[Tuple[Optional[int], Optional[str]]] = None,
) -> List[ExtractedQA]:
	"""
	Extract Q&A pairs from a conversation.
	
	Groups consecutive IN messages as questions and consecutive OUT messages as answers.
	Skips messages with only attachments (no text).
	
	Args:
		session: Database session
		conversation_id: Conversation ID to extract from
		min_question_length: Minimum length for question text to be included
		min_answer_length: Minimum length for answer text to be included
		start_timestamp_ms: Optional start timestamp filter (only include messages after this)
		end_timestamp_ms: Optional end timestamp filter (only include messages before this)
		category_filter: Optional message category filter (only include Q&As where question messages have this category)
		
	Returns:
		List of ExtractedQA pairs found in the conversation
	"""
	# Build query for messages
	query = (
		select(Message)
		.where(Message.conversation_id == conversation_id)
		.where(Message.direction.is_not(None))
		.where(Message.direction.in_(["in", "out"]))
		.where(Message.text.is_not(None))
		.where(Message.text != "")
	)
	
	# Add timestamp filters if provided
	if start_timestamp_ms is not None:
		query = query.where(Message.timestamp_ms >= start_timestamp_ms)
	if end_timestamp_ms is not None:
		query = query.where(Message.timestamp_ms <= end_timestamp_ms)
	
	# Get all messages for this conversation, ordered by timestamp
	messages = session.exec(query.order_by(Message.timestamp_ms.asc())).all()
	
	if not messages:
		return []
	
	qa_pairs: List[ExtractedQA] = []
	current_question_parts: List[str] = []
	current_answer_parts: List[str] = []
	current_question_messages: List[Message] = []  # Track question messages for category filtering
	current_direction: Optional[str] = None
	question_start_ts: Optional[int] = None
	current_answer_end_ts: Optional[int] = None
	
	def flush_qa():
		"""Flush current question-answer pair if valid."""
		nonlocal current_question_parts, current_answer_parts, question_start_ts, current_answer_end_ts, current_question_messages
		
		if current_question_parts and current_answer_parts:
			# Apply category filter if specified
			if category_filter is not None:
				# Check if any question message has the specified category
				has_matching_category = any(
					msg.message_category == category_filter
					for msg in current_question_messages
					if msg.message_category
				)
				if not has_matching_category:
					# Reset and skip this Q&A pair
					current_question_parts = []
					current_answer_parts = []
					current_question_messages = []
					question_start_ts = None
					current_answer_end_ts = None
					return
			
			question_text = " ".join(current_question_parts).strip()
			answer_text = " ".join(current_answer_parts).strip()
			
			# Only include if both meet minimum length requirements
			if len(question_text) >= min_question_length and len(answer_text) >= min_answer_length:
				# Detect product focus at the time of the question
				if product_hint is not None:
					product_id, product_name = product_hint
				else:
					product_id, product_name = _detect_product_focus_at_timestamp(
						session, conversation_id, question_start_ts
					)
				
				qa_pairs.append(ExtractedQA(
					conversation_id=conversation_id,
					question=question_text,
					answer=answer_text,
					product_id=product_id,
					product_name=product_name,
					question_start_timestamp=question_start_ts,
					answer_end_timestamp=current_answer_end_ts,
				))
		
		# Reset for next pair
		current_question_parts = []
		current_answer_parts = []
		current_question_messages = []
		question_start_ts = None
		current_answer_end_ts = None
	
	# Process messages in chronological order
	for msg in messages:
		if not msg.text or not msg.text.strip():
			# Skip messages with no text (only attachments)
			continue
		
		msg_text = msg.text.strip()
		msg_direction = msg.direction.lower() if msg.direction else None
		
		if msg_direction == "in":
			if current_direction == "out" and current_answer_parts:
				# Switching from answer to question - flush current Q&A pair
				flush_qa()
			
			# Add to current question (start new question or continue existing)
			if not current_question_parts:
				question_start_ts = msg.timestamp_ms
			current_question_parts.append(msg_text)
			current_question_messages.append(msg)  # Track message for category filtering
			current_direction = "in"
		
		elif msg_direction == "out":
			# Switching from question to answer - start answering
			# (the question is already collected, now collect the answer)
			
			# Add to current answer
			if not current_answer_parts and current_question_parts:
				# This is the start of an answer for the current question
				pass
			current_answer_parts.append(msg_text)
			current_answer_end_ts = msg.timestamp_ms
			current_direction = "out"
	
	# Flush any remaining Q&A pair
	flush_qa()
	
	return qa_pairs


def _detect_product_focus_at_timestamp(
	session: Session,
	conversation_id: int,
	timestamp_ms: Optional[int],
) -> Tuple[Optional[int], Optional[str]]:
	"""
	Detect product focus for a conversation at a specific timestamp.
	
	For now, we use the conversation's current product focus.
	In the future, this could be enhanced to track product changes over time.
	"""
	from .ai_ig import _detect_focus_product
	
	try:
		focus_slug, confidence = _detect_focus_product(str(conversation_id))
		
		if focus_slug and confidence > 0.5:
			# Try to find product by slug or name
			product = session.exec(
				select(Product).where(
					or_(
						Product.slug == focus_slug,
						Product.name == focus_slug,
					)
				).limit(1)
			).first()
			
			if product and product.id:
				return product.id, product.name
	except Exception:
		pass
	
	return None, None


def extract_qa_from_conversations(
	session: Session,
	start_date: dt.date,
	end_date: dt.date,
	product_id_filter: Optional[int] = None,
	min_question_length: int = 5,
	min_answer_length: int = 5,
	category_filter: Optional[str] = None,
) -> List[ExtractedQA]:
	"""
	Extract Q&A pairs from conversations within a date range.
	
	Args:
		session: Database session
		start_date: Start date for conversation filtering
		end_date: End date for conversation filtering
		product_id_filter: If provided, only include Q&A pairs for this product
		min_question_length: Minimum length for question text
		min_answer_length: Minimum length for answer text
		category_filter: Optional message category filter (only include Q&As where question messages have this category)
		
	Returns:
		List of ExtractedQA pairs from all matching conversations
	"""
	# Convert dates to datetime for comparison with last_message_at
	start_datetime = dt.datetime.combine(start_date, dt.time.min)
	end_datetime = dt.datetime.combine(end_date, dt.time.max)
	
	# Convert to timestamps (milliseconds since epoch) for message filtering
	start_timestamp_ms = int(start_datetime.timestamp() * 1000)
	end_timestamp_ms = int(end_datetime.timestamp() * 1000)
	
	# First collect candidate conversation ids via messages to avoid scanning all conversations
	msg_filters = [
		Message.timestamp_ms >= start_timestamp_ms,
		Message.timestamp_ms <= end_timestamp_ms,
		Message.direction.is_not(None),
		Message.direction.in_(["in", "out"]),
		Message.text.is_not(None),
		Message.text != "",
	]
	if category_filter:
		msg_filters.append(Message.message_category == category_filter)
	base_convo_query = select(Message.conversation_id).where(and_(*msg_filters)).distinct()
	rows = session.exec(base_convo_query).all()
	# Extract conversation_id values - handle both tuple/Row objects and direct values
	convo_ids = []
	for row in rows:
		if isinstance(row, (tuple, list)) and len(row) > 0:
			convo_ids.append(row[0])
		elif hasattr(row, '__getitem__'):
			try:
				convo_ids.append(row[0])
			except (IndexError, TypeError):
				convo_ids.append(row)
		else:
			convo_ids.append(row)
	# Optionally narrow down by product mapping using the conversation's last linked ad/post
	if product_id_filter is not None and convo_ids:
		rows = session.exec(
			select(Conversation.id)
			.where(Conversation.id.in_(convo_ids))
			.where(
				text(
					"""
					EXISTS (
						SELECT 1 FROM ads_products ap
						WHERE ap.ad_id = COALESCE(Conversation.last_link_id, Conversation.last_ad_id)
						  AND ap.link_type = COALESCE(Conversation.last_link_type, 'ad')
						  AND ap.product_id = :pid
					)
					"""
				)
			)
		).params(pid=product_id_filter).all()
		# Extract conversation IDs - handle both tuple/Row objects and direct values
		filtered_convo_ids = []
		for row in rows:
			if isinstance(row, (tuple, list)) and len(row) > 0:
				filtered_convo_ids.append(row[0])
			elif hasattr(row, '__getitem__'):
				try:
					filtered_convo_ids.append(row[0])
				except (IndexError, TypeError):
					filtered_convo_ids.append(row)
			else:
				filtered_convo_ids.append(row)
		convo_ids = filtered_convo_ids
	
	all_qa_pairs: List[ExtractedQA] = []
	
	for cid in convo_ids:
		try:
			qa_pairs = extract_qa_from_conversation(
				session,
				cid,
				min_question_length=min_question_length,
				min_answer_length=min_answer_length,
				start_timestamp_ms=start_timestamp_ms,
				end_timestamp_ms=end_timestamp_ms,
				category_filter=category_filter,
				product_hint=(product_id_filter, None) if product_id_filter is not None else None,
			)
			
			# Apply product filter if specified
			if product_id_filter is not None:
				qa_pairs = [
					qa for qa in qa_pairs
					if qa.product_id == product_id_filter
				]
			
			all_qa_pairs.extend(qa_pairs)
		except Exception:
			# Skip conversations that fail extraction
			continue
	
	return all_qa_pairs


def filter_qa_by_product(
	qa_pairs: List[ExtractedQA],
	product_id: int,
) -> Tuple[List[ExtractedQA], List[ExtractedQA]]:
	"""
	Filter Q&A pairs by product ID.
	
	Returns:
		Tuple of (matching_pairs, filtered_out_pairs)
	"""
	matching = [qa for qa in qa_pairs if qa.product_id == product_id]
	filtered_out = [qa for qa in qa_pairs if qa.product_id != product_id]
	return matching, filtered_out

