from __future__ import annotations

import datetime as dt
import logging
import json
import os
import unicodedata
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlmodel import select

from ..db import get_session
from ..models import (
	Message,
	Product,
	ProductUpsell,
	Item,
	Conversation,
	IGUser,
	AIPretext,
	ProductImage,
	AiShadowReply,
	AdminMessage,
)
from .ai import (
	AIClient,
	get_ai_shadow_model_from_settings,
	get_shadow_temperature_setting,
	is_shadow_temperature_opt_out,
)
from .ai_context import VariantExclusions, parse_variant_exclusions, variant_is_excluded
from .ai_ig import _detect_focus_product
from .ai_orders import (
	get_candidate_snapshot,
	mark_candidate_interested,
	mark_candidate_not_interested,
	mark_candidate_very_interested,
	submit_candidate_order,
)
from .ai_utils import parse_height_weight, calculate_size_suggestion, detect_color_count
from .prompts import get_global_system_prompt, get_serializer_prompt
from ..utils.normalize import TURKISH_MAP


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


def _sanitize_reply_text(text: str) -> str:
	"""
	Remove control characters and normalize Unicode so that replies stay readable
	even if the model emitted invalid escape sequences.

	Strategy:
	- Drop ASCII control chars (except tabs/newlines) that break rendering
	- Normalize to NFKC to collapse oddities
	- Transliterate well-known Turkish letters via TURKISH_MAP
	- Strip combining marks to fall back to ASCII when needed
	"""
	if not isinstance(text, str):
		return ""
	# Remove problematic control chars but preserve newlines/tabs
	filtered_chars: list[str] = []
	for ch in text:
		code = ord(ch)
		if ch in ("\n", "\t"):
			filtered_chars.append(ch)
		elif code >= 32:
			filtered_chars.append(ch)
	cleaned = "".join(filtered_chars)
	if not cleaned:
		return ""
	# Normalize and transliterate Turkish-specific letters
	cleaned = unicodedata.normalize("NFKC", cleaned)
	cleaned = cleaned.translate(TURKISH_MAP)
	cleaned = unicodedata.normalize("NFKD", cleaned)
	# Drop combining marks (accents) to fall back to ASCII
	cleaned = "".join(ch for ch in cleaned if not unicodedata.combining(ch))
	# Collapse Windows-style newlines and excess whitespace
	cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
	lines = [" ".join(part for part in line.split() if part) for line in cleaned.split("\n")]
	return "\n".join(line for line in lines if line).strip()


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


def _load_manual_upsells(product_id: int) -> Dict[int, Dict[str, Any]]:
	"""
	Load manually configured upsells for a product (if any).
	"""
	if not product_id:
		return {}
	upsell_map: Dict[int, Dict[str, Any]] = {}
	try:
		with get_session() as session:
			rows = (
				session.exec(
					select(ProductUpsell, Product)
					.join(Product, ProductUpsell.upsell_product_id == Product.id)
					.where(
						ProductUpsell.product_id == product_id,
						ProductUpsell.is_active.is_(True),
					)
					.order_by(ProductUpsell.position.asc(), ProductUpsell.id.asc())
				)
				.all()
				or []
			)
		for pu, upsell_prod in rows:
			if not upsell_prod or not upsell_prod.id:
				continue
			copy_text = (getattr(pu, "copy_text", None) or getattr(pu, "copy", None) or "").strip() or (upsell_prod.name or f"Ürün {upsell_prod.id}")
			upsell_map[upsell_prod.id] = {
				"product_id": upsell_prod.id,
				"product_name": upsell_prod.name,
				"copy": copy_text,
				"cooccurrence_count": None,
				"source": "manual",
				"position": pu.position,
			}
		return upsell_map
	except Exception as exc:
		try:
			log.warning("_load_manual_upsells failed product_id=%s error=%s", product_id, str(exc)[:200])
		except Exception:
			pass
		return {}


def _calculate_upsell_recommendations(product_id: int, limit_orders: int = 100, min_cooccurrence: int = 2) -> Dict[int, Dict[str, Any]]:
	"""
	Calculate upsell recommendations based on order history.
	
	For a given product, finds the latest X orders that contain it, then identifies
	other products that were frequently ordered together with it.
	
	Args:
		product_id: The product ID to find upsells for
		limit_orders: Maximum number of recent orders to analyze (default: 100)
		min_cooccurrence: Minimum number of times a product must appear together (default: 2)
	
	Returns:
		Dict mapping product_id to upsell config: {
			product_id: {
				"product_id": int,
				"product_name": str,
				"copy": str,  # Suggested upsell text
				"cooccurrence_count": int,  # How many times it appeared together
			}
		}
	"""
	if not product_id:
		return {}
	
	from collections import defaultdict
	from sqlalchemy import and_, or_
	from ..models import OrderItem, Order
	
	upsell_map: Dict[int, Dict[str, Any]] = {}
	
	try:
		with get_session() as session:
			# Step 1: Find recent orders that contain this product
			# We need to join OrderItem -> Item -> Product to find orders with this product
			# Fix SQL: When using DISTINCT, ORDER BY columns must be in SELECT
			# Use a subquery approach to avoid DISTINCT + ORDER BY conflict
			from sqlalchemy import func
			# First, get distinct order_ids with their max Order.id for sorting
			subquery = (
				select(
					OrderItem.order_id,
					func.max(Order.id).label("max_order_id")
				)
				.join(Item, OrderItem.item_id == Item.id)
				.where(Item.product_id == product_id)
				.join(Order, OrderItem.order_id == Order.id)
				.where(
					# Only consider completed orders (not cancelled/refunded)
					or_(
						Order.status.is_(None),
						Order.status.not_in(("iptal", "iade", "refunded", "cancelled")),
					)
				)
				.group_by(OrderItem.order_id)
			).subquery()
			
			# Then order by max_order_id and get the order_ids
			recent_orders_query = (
				select(subquery.c.order_id)
				.order_by(subquery.c.max_order_id.desc())
				.limit(limit_orders)
			)
			
			order_ids_result = session.exec(recent_orders_query).all()
			order_ids = [oid for oid in order_ids_result if oid]
			
			if not order_ids:
				return {}
			
			# Step 2: For each of these orders, find all other products
			# Count how many times each other product appears with our target product
			cooccurrence_count: Dict[int, int] = defaultdict(int)
			product_names: Dict[int, str] = {}
			product_prices: Dict[int, Optional[float]] = {}
			
			# Get all OrderItems for these orders, excluding items from our target product
			# We'll query OrderItem and join to get Item and Product info
			other_order_items = session.exec(
				select(OrderItem)
				.join(Item, OrderItem.item_id == Item.id)
				.join(Product, Item.product_id == Product.id)
				.where(
					and_(
						OrderItem.order_id.in_(order_ids),
						Item.product_id != product_id,
						Item.product_id.is_not(None),
					)
				)
			).all()
			
			# Process each order item to count cooccurrences
			for oi in other_order_items:
				item = session.exec(select(Item).where(Item.id == oi.item_id)).first()
				if not item or not item.product_id or item.product_id == product_id:
					continue
				
				other_pid = item.product_id
				cooccurrence_count[other_pid] += 1
				
				# Get product name if not already cached
				if other_pid not in product_names:
					prod = session.exec(select(Product).where(Product.id == other_pid)).first()
					if prod:
						product_names[other_pid] = prod.name or f"Product {other_pid}"
					else:
						product_names[other_pid] = f"Product {other_pid}"
				
				# Get price if not already cached
				if other_pid not in product_prices and item.price:
					try:
						product_prices[other_pid] = float(item.price)
					except (ValueError, TypeError):
						pass
			
			# Step 3: Build upsell config for products that meet minimum cooccurrence
			for other_pid, count in cooccurrence_count.items():
				if count >= min_cooccurrence:
					product_name = product_names.get(other_pid, f"Product {other_pid}")
					price = product_prices.get(other_pid)
					
					# Generate suggested copy text
					copy_parts = []
					if price:
						copy_parts.append(f"{product_name} - {price:.0f}₺")
					else:
						copy_parts.append(product_name)
					
					copy_text = f"Bununla birlikte {copy_parts[0]} de almak ister misin?"
					
					upsell_map[other_pid] = {
						"product_id": other_pid,
						"product_name": product_name,
						"copy": copy_text,
						"cooccurrence_count": count,
					}
			
			# Sort by cooccurrence count (descending) and limit to top 3
			sorted_upsells = sorted(
				upsell_map.items(),
				key=lambda x: x[1]["cooccurrence_count"],
				reverse=True
			)[:3]
			
			return {pid: config for pid, config in sorted_upsells}
			
	except Exception as e:
		try:
			log.warning("_calculate_upsell_recommendations failed product_id=%s error=%s", product_id, str(e)[:200])
		except Exception:
			pass
		return {}


def _compact_stock_list(stock: List[Dict[str, Any]]) -> Dict[str, Any]:
	"""
	Transform a full stock list (all SKU combinations) into a compact format.
	
	Returns a compact structure with:
	- colors: [list of unique colors]
	- sizes: [list of unique sizes]
	- price_range: {min, max} (if prices available)
	- price: single price if all items have the same price
	"""
	if not stock:
		return {
			"colors": [],
			"sizes": [],
			"price_range": None,
		}
	
	colors: set[str] = set()
	sizes: set[str] = set()
	prices: list[float] = []
	
	for entry in stock:
		color = entry.get("color")
		if color and isinstance(color, str) and color.strip():
			colors.add(color.strip())
		
		size = entry.get("size")
		if size and isinstance(size, str) and size.strip():
			sizes.add(size.strip())
		
		price = entry.get("price")
		if price is not None:
			try:
				price_val = float(price)
				if price_val > 0:
					prices.append(price_val)
			except (ValueError, TypeError):
				pass
	
	# Sort colors alphabetically
	colors_sorted = sorted(list(colors))
	
	# Sort sizes: try numeric first, fallback to alphabetical
	def _sort_size_key(size_str: str) -> tuple[int, float | str]:
		"""Sort key for sizes: numeric first, then alphabetical."""
		try:
			# Try to extract numeric part
			num_val = float(size_str.strip())
			return (0, num_val)  # 0 = numeric, use numeric value for sorting
		except (ValueError, TypeError):
			return (1, size_str.lower())  # 1 = non-numeric, use alphabetical
	
	sizes_sorted = sorted(list(sizes), key=_sort_size_key)
	
	result: Dict[str, Any] = {
		"colors": colors_sorted,
		"sizes": sizes_sorted,
	}
	
	if prices:
		result["price_range"] = {
			"min": min(prices),
			"max": max(prices),
		}
		# If all prices are the same, include a single price
		if len(set(prices)) == 1:
			result["price"] = prices[0]
	else:
		result["price_range"] = None
	
	return result


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
	
	For REAL conversations: Only include messages actually sent to client (ai_status='sent' or NULL for manual).
	For MOCK conversations: Include shadow replies with status='sent' or 'suggested'.
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

		# For REAL conversations: Only include messages actually sent to client
		# For MOCK conversations: Include all messages (they're simulated)
		if is_mock_conversation:
			# Mock: include all messages
			msgs = (
				session.exec(
					select(Message)
					.where(Message.conversation_id == int(conversation_id))
					.order_by(Message.timestamp_ms.asc())
					.limit(limit_val)
				).all()
			)
		else:
			# Real: Only include inbound messages OR outbound messages that were actually sent
			from sqlalchemy import or_
			msgs = (
				session.exec(
					select(Message)
					.where(Message.conversation_id == int(conversation_id))
					.where(
						# Include all inbound messages (from customer)
						(Message.direction == "in")
						|
						# Include outbound messages that were actually sent
						(
							(Message.direction == "out")
							& (
								(Message.ai_status == "sent")
								| (Message.ai_status.is_(None))  # Manual messages (no ai_status)
							)
						)
					)
					.order_by(Message.timestamp_ms.asc())
					.limit(limit_val)
				).all()
			)

		# For mock conversations, also load shadow replies
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

	# Only add shadow replies for mock conversations
	if is_mock_conversation and shadow_rows:
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

	# Only merge shadow replies for mock conversations
	if is_mock_conversation and mock_outbound:
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

	# Transform stock list into compact format (colors, sizes, price_range instead of all SKU combinations)
	stock_compact = _compact_stock_list(stock)

	# Search for matching Q&As if we have a product and customer message
	matching_qas: List[Dict[str, Any]] = []
	qa_search_metadata: Dict[str, Any] = {
		"attempted": False,
		"reason": None,
		"query": None,
		"product_id": None,
		"error": None,
		"limit": 3,
		"min_similarity": 0.7,
	}
	if product_id_val and last_customer_message:
		qa_search_metadata["attempted"] = True
		qa_search_metadata["query"] = last_customer_message
		qa_search_metadata["product_id"] = product_id_val
		try:
			from .embeddings import search_product_qas
			qa_results = search_product_qas(
				product_id_val,
				last_customer_message,
				limit=3,
				min_similarity=0.7,
			)
			matching_qas = [
				{
					"question": qa.question,
					"answer": qa.answer,
					"similarity": round(similarity, 3),
				}
				for qa, similarity in qa_results
			]
			qa_search_metadata["result_count"] = len(matching_qas)
			if matching_qas:
				try:
					log.info(
						"draft_reply found_matching_qas conversation_id=%s product_id=%s count=%s",
						conversation_id,
						product_id_val,
						len(matching_qas),
					)
				except Exception:
					pass
		except Exception as exc:
			# Don't fail if Q&A search fails, just log and continue
			qa_search_metadata["error"] = str(exc)
			try:
				log.warning("draft_reply qa_search_failed conversation_id=%s error=%s", conversation_id, exc)
			except Exception:
				pass
	else:
		# Search was not attempted - record why
		if not product_id_val:
			qa_search_metadata["reason"] = "product_id eksik (ürün bulunamadı)"
		elif not last_customer_message:
			qa_search_metadata["reason"] = "last_customer_message eksik (müşteri mesajı yok)"

	# Calculate upsell recommendations: prefer manual config, fall back to auto
	upsell_config: Dict[str, Any] = {}
	if product_id_val:
		manual_upsells = _load_manual_upsells(product_id_val)
		if manual_upsells:
			upsell_config["by_product_id"] = manual_upsells
			try:
				log.info(
					"draft_reply upsell_config_manual conversation_id=%s product_id=%s count=%s",
					conversation_id,
					product_id_val,
					len(manual_upsells),
				)
			except Exception:
				pass
		else:
			try:
				upsell_recommendations = _calculate_upsell_recommendations(product_id_val, limit_orders=100, min_cooccurrence=2)
				if upsell_recommendations:
					# Build upsell_config structure: by_product_id[product_id] = {product_id, product_name, copy, ...}
					upsell_config["by_product_id"] = upsell_recommendations
					try:
						log.info(
							"draft_reply upsell_config_auto conversation_id=%s product_id=%s count=%s",
							conversation_id,
							product_id_val,
							len(upsell_recommendations),
						)
					except Exception:
						pass
			except Exception as exc:
				try:
					log.warning("draft_reply upsell_calc_failed conversation_id=%s product_id=%s error=%s", conversation_id, product_id_val, str(exc)[:200])
				except Exception:
					pass
	
	user_payload: Dict[str, Any] = {
		"store": store_conf,
		"product_focus": product_focus_data,
		"stock": stock_compact,
		"history": history,
		"last_customer_message": last_customer_message,
		"transcript": transcript,
		"product_images": product_images,
	}
	if matching_qas:
		user_payload["matching_qas"] = matching_qas
	if upsell_config:
		user_payload["upsell_config"] = upsell_config
	state_payload = dict(state or {})
	# Seed essential state flags so prompt conditions (e.g., upsell) work deterministically
	if product_id_val:
		state_payload.setdefault("current_focus_product_id", product_id_val)
	state_payload.setdefault("upsell_offered", False)
	state_payload.setdefault("upsell_accepted", False)
	hail_already_sent = bool(state_payload.get("hail_sent"))
	# Detect if this is a first message (no previous AI replies in history)
	is_first_message = not hail_already_sent and len([h for h in history if h.get("dir") == "out"]) == 0
	user_payload["state"] = state_payload
	try:
		order_candidate_snapshot = get_candidate_snapshot(int(conversation_id))
	except Exception:
		order_candidate_snapshot = None
	if order_candidate_snapshot:
		user_payload["ai_order_candidate"] = order_candidate_snapshot
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

	def _clean_tool_str(value: Any) -> Optional[str]:
		if value is None:
			return None
		text = str(value).strip()
		return text or None

	def _handle_candidate_status_tool(
		tool_name: str,
		func: Callable[[int, Optional[str]], Dict[str, Any]],
		args: Dict[str, Any],
	) -> str:
		note_val = _clean_tool_str(args.get("note") or args.get("reason") or args.get("summary"))
		result = func(int(conversation_id), note=note_val)
		callback_entry = {
			"name": tool_name,
			"arguments": {"note": note_val},
			"result": {
				"candidate_id": result.get("id"),
				"conversation_id": result.get("conversation_id"),
				"status": result.get("status"),
			},
		}
		function_callbacks.append(callback_entry)
		_log_function_callback(conversation_id, callback_entry["name"], callback_entry["arguments"], callback_entry["result"])
		return json.dumps(result, ensure_ascii=False)

	tools.append(
		{
			"type": "function",
			"function": {
				"name": "create_ai_order_candidate",
				"description": "Kullanıcı ilk mesajımıza cevap verip ürüne ilgisini gösterdiğinde bu fonksiyonu çağır ve durumunu 'interested' olarak kaydet.",
				"parameters": {
					"type": "object",
					"properties": {
						"note": {
							"type": "string",
							"description": "Kısaca kullanıcı neden ilgilendi; örn. 'Fiyat sordu'.",
						}
					},
				},
			},
		}
	)

	def _handle_create_candidate_tool(args: Dict[str, Any]) -> str:
		return _handle_candidate_status_tool("create_ai_order_candidate", mark_candidate_interested, args)

	tool_handlers["create_ai_order_candidate"] = _handle_create_candidate_tool

	tools.append(
		{
			"type": "function",
			"function": {
				"name": "mark_ai_order_not_interested",
				"description": "Müşteri artık almak istemediğini veya ürüne ilgisinin kalmadığını söylediğinde çağır ve durumu 'not-interested' yap.",
				"parameters": {
					"type": "object",
					"properties": {
						"note": {
							"type": "string",
							"description": "Gerekirse vazgeçme sebebini yaz; örn. 'Beden yok dedi'.",
						}
					},
				},
			},
		}
	)

	def _handle_not_interested_tool(args: Dict[str, Any]) -> str:
		return _handle_candidate_status_tool("mark_ai_order_not_interested", mark_candidate_not_interested, args)

	tool_handlers["mark_ai_order_not_interested"] = _handle_not_interested_tool

	tools.append(
		{
			"type": "function",
			"function": {
				"name": "mark_ai_order_very_interested",
				"description": "Müşteri siparişi tamamlama yolundaysa (adres/ödeme gibi detayları topluyorsan) durumu 'very-interested' yap.",
				"parameters": {
					"type": "object",
					"properties": {
						"note": {
							"type": "string",
							"description": "İlerleme notu; örn. 'Adres yazdırdı, ödeme soruyor'.",
						}
					},
				},
			},
		}
	)

	def _handle_very_interested_tool(args: Dict[str, Any]) -> str:
		return _handle_candidate_status_tool("mark_ai_order_very_interested", mark_candidate_very_interested, args)

	tool_handlers["mark_ai_order_very_interested"] = _handle_very_interested_tool

	# Focus product change
	tools.append(
		{
			"type": "function",
			"function": {
				"name": "change_focus_product",
				"description": "Konuşmanın odaklandığı ürünü değiştir.",
				"parameters": {
					"type": "object",
					"properties": {
						"product_id": {"type": "integer"},
						"reason": {"type": "string"},
					},
					"required": ["product_id"],
				},
			},
		}
	)

	def _handle_change_focus_tool(args: Dict[str, Any]) -> str:
		pid = _clean_positive_int(args.get("product_id"))
		reason = _clean_tool_str(args.get("reason"))
		if pid:
			state_payload["current_focus_product_id"] = pid
		if reason:
			state_payload["focus_change_reason"] = reason
		result = {"product_id": pid, "reason": reason}
		callback_entry = {"name": "change_focus_product", "arguments": args, "result": result}
		function_callbacks.append(callback_entry)
		_log_function_callback(conversation_id, "change_focus_product", args, result)
		return json.dumps(result, ensure_ascii=False)

	tool_handlers["change_focus_product"] = _handle_change_focus_tool

	# Add cart item
	tools.append(
		{
			"type": "function",
			"function": {
				"name": "add_cart_item",
				"description": "Sepete yeni ürün satırı ekle.",
				"parameters": {
					"type": "object",
					"properties": {
						"product_id": {"type": "integer"},
						"product_name": {"type": "string"},
						"sku": {"type": "string"},
						"color": {"type": "string"},
						"size": {"type": "string"},
						"quantity": {"type": "integer", "minimum": 1},
						"unit_price": {"type": "number"},
						"is_upsell": {"type": "boolean"},
					},
					"required": ["product_id", "product_name", "sku", "color", "size", "quantity", "unit_price", "is_upsell"],
				},
			},
		}
	)

	def _handle_add_cart_item(args: Dict[str, Any]) -> str:
		item = {
			"product_id": _clean_positive_int(args.get("product_id")),
			"product_name": _clean_tool_str(args.get("product_name")),
			"sku": _clean_tool_str(args.get("sku")),
			"color": _clean_tool_str(args.get("color")),
			"size": _clean_tool_str(args.get("size")),
			"quantity": _clean_int(args.get("quantity"), default=1),
			"unit_price": _clean_float(args.get("unit_price")),
			"is_upsell": bool(args.get("is_upsell")),
		}
		state_cart = state_payload.get("cart")
		if not isinstance(state_cart, list):
			state_cart = []
		state_cart.append(item)
		state_payload["cart"] = state_cart
		if item.get("is_upsell"):
			state_payload["upsell_offered"] = True
			state_payload["upsell_accepted"] = True
			state_payload["upsell_product_id"] = item.get("product_id")
		if not state_payload.get("last_step"):
			state_payload["last_step"] = "awaiting_payment"
		result = {"added": item}
		callback_entry = {"name": "add_cart_item", "arguments": args, "result": result}
		function_callbacks.append(callback_entry)
		_log_function_callback(conversation_id, "add_cart_item", args, result)
		return json.dumps(result, ensure_ascii=False)

	tool_handlers["add_cart_item"] = _handle_add_cart_item

	# Customer image analysis
	tools.append(
		{
			"type": "function",
			"function": {
				"name": "analyze_customer_image",
				"description": "Müşterinin gönderdiği görseli analiz et (beden, stil, renk, kombin önerisi vs.)",
				"parameters": {
					"type": "object",
					"properties": {
						"image_id": {"type": "string", "description": "Backend'in verdiği görsel ID"},
						"purpose": {"type": "string", "description": "Analiz amacı (stil, beden, kombin gibi)"},
						"notes": {"type": "string"},
					},
					"required": ["image_id", "purpose"],
				},
			},
		}
	)

	def _handle_analyze_image(args: Dict[str, Any]) -> str:
		payload = {
			"image_id": _clean_tool_str(args.get("image_id")),
			"purpose": _clean_tool_str(args.get("purpose")),
			"notes": _clean_tool_str(args.get("notes")),
		}
		callback_entry = {"name": "analyze_customer_image", "arguments": args, "result": payload}
		function_callbacks.append(callback_entry)
		_log_function_callback(conversation_id, "analyze_customer_image", args, payload)
		return json.dumps(payload, ensure_ascii=False)

	tool_handlers["analyze_customer_image"] = _handle_analyze_image

	# Send product image to customer
	tools.append(
		{
			"type": "function",
			"function": {
				"name": "send_product_image_to_customer",
				"description": "Müşterinin talep ettiği ürün/renk için uygun görseli müşteriye gönder.",
				"parameters": {
					"type": "object",
					"properties": {
						"product_id": {"type": "integer"},
						"color": {"type": "string", "description": "İstenen renk (örn. 'MAVİ', 'BEYAZ')"},
						"view_type": {
							"type": "string",
							"enum": ["front", "back", "detail", "full_body"],
							"description": "Varsa özel görünüm isteği",
						},
						"reason": {"type": "string"},
					},
					"required": ["product_id", "color"],
				},
			},
		}
	)

	def _handle_send_product_image(args: Dict[str, Any]) -> str:
		payload = {
			"product_id": _clean_positive_int(args.get("product_id")),
			"color": _clean_tool_str(args.get("color")),
			"view_type": _clean_tool_str(args.get("view_type")) or "front",
			"reason": _clean_tool_str(args.get("reason")),
		}
		callback_entry = {"name": "send_product_image_to_customer", "arguments": args, "result": payload}
		function_callbacks.append(callback_entry)
		_log_function_callback(conversation_id, "send_product_image_to_customer", args, payload)
		return json.dumps(payload, ensure_ascii=False)

	tool_handlers["send_product_image_to_customer"] = _handle_send_product_image

	def _drop_none(data: Dict[str, Any]) -> Dict[str, Any]:
		return {k: v for k, v in data.items() if v is not None}

	def _clean_float(val: Any) -> Optional[float]:
		try:
			if val is None or val == "":
				return None
			return float(val)
		except Exception:
			return None

	def _clean_int(val: Any, default: int = 1) -> int:
		try:
			num = int(val)
			return num if num > 0 else default
		except Exception:
			return default

	def _clean_positive_int(val: Any) -> Optional[int]:
		try:
			if val is None or val == "":
				return None
			num = int(val)
			return num if num > 0 else None
		except Exception:
			return None

	tools.append(
		{
			"type": "function",
			"function": {
				"name": "place_ai_order_candidate",
				"description": "Müşteri siparişi tamamlamak için gerekli tüm bilgileri verdiğinde bu fonksiyonu çağır. Kayıt, insan ekip tarafından incelenip gerçek siparişe dönüştürülecek.",
				"parameters": {
					"type": "object",
					"properties": {
						"product": {
							"type": "object",
							"description": "Ürün/variant bilgileri",
							"properties": {
								"name": {"type": "string", "description": "Ürün adı veya slug"},
								"sku": {"type": "string"},
								"color": {"type": "string"},
								"size": {"type": "string"},
								"variant": {"type": "string", "description": "Opsiyonel varyant etiketi"},
								"quantity": {"type": "integer", "minimum": 1, "default": 1},
								"unit_price": {"type": "number"},
								"total_price": {"type": "number"},
							},
							"required": ["name"],
						},
						"customer": {
							"type": "object",
							"description": "Müşteri iletişim ve adres bilgileri",
							"properties": {
								"name": {"type": "string"},
								"phone": {"type": "string"},
								"address": {"type": "string"},
								"city": {"type": "string"},
								"notes": {"type": "string"},
							},
							"required": ["name", "phone", "address"],
						},
						"shipping": {
							"type": "object",
							"properties": {
								"method": {"type": "string", "description": "örn. kapıda ödeme"},
								"cost": {"type": "number"},
								"notes": {"type": "string"},
							},
						},
						"payment": {
							"type": "object",
							"properties": {
								"method": {"type": "string", "description": "örn. kapıda nakit"},
								"status": {"type": "string", "description": "örn. 'beklemede'"},
								"amount": {"type": "number"},
							},
						},
						"measurements": {
							"type": "object",
							"properties": {
								"height_cm": {"type": "integer"},
								"weight_kg": {"type": "integer"},
							},
						},
						"notes": {
							"type": "string",
							"description": "Müşteri tarafından verilen ekstra talimatlar veya önemli bilgiler.",
						},
					},
					"required": ["product", "customer"],
				},
			},
		}
	)

	def _handle_place_order_tool(args: Dict[str, Any]) -> str:
		product_args = args.get("product") or {}
		customer_args = args.get("customer") or {}
		product_name = _clean_tool_str(product_args.get("name"))
		if not product_name:
			raise ValueError("product.name is required")
		customer_name = _clean_tool_str(customer_args.get("name"))
		customer_phone = _clean_tool_str(customer_args.get("phone"))
		customer_address = _clean_tool_str(customer_args.get("address"))
		if not (customer_name and customer_phone and customer_address):
			raise ValueError("customer.name, customer.phone ve customer.address zorunludur")
		product_payload = _drop_none(
			{
				"name": product_name,
				"sku": _clean_tool_str(product_args.get("sku")),
				"color": _clean_tool_str(product_args.get("color")),
				"size": _clean_tool_str(product_args.get("size")),
				"variant": _clean_tool_str(product_args.get("variant")),
				"quantity": _clean_int(product_args.get("quantity"), default=1),
				"unit_price": _clean_float(product_args.get("unit_price")),
				"total_price": _clean_float(product_args.get("total_price")),
			}
		)
		customer_payload = _drop_none(
			{
				"name": customer_name,
				"phone": customer_phone,
				"address": customer_address,
				"city": _clean_tool_str(customer_args.get("city")),
				"notes": _clean_tool_str(customer_args.get("notes")),
			}
		)
		shipping_args = args.get("shipping") or {}
		shipping_payload = _drop_none(
			{
				"method": _clean_tool_str(shipping_args.get("method")),
				"cost": _clean_float(shipping_args.get("cost")),
				"notes": _clean_tool_str(shipping_args.get("notes")),
			}
		)
		payment_args = args.get("payment") or {}
		payment_payload = _drop_none(
			{
				"method": _clean_tool_str(payment_args.get("method")),
				"status": _clean_tool_str(payment_args.get("status")),
				"amount": _clean_float(payment_args.get("amount")),
			}
		)
		measurements_args = args.get("measurements") or {}
		measurements_payload = _drop_none(
			{
				"height_cm": _clean_positive_int(measurements_args.get("height_cm")),
				"weight_kg": _clean_positive_int(measurements_args.get("weight_kg")),
			}
		)
		order_payload: Dict[str, Any] = {
			"product": product_payload,
			"customer": customer_payload,
		}
		if shipping_payload:
			order_payload["shipping"] = shipping_payload
		if payment_payload:
			order_payload["payment"] = payment_payload
		if measurements_payload:
			order_payload["measurements"] = measurements_payload
		order_notes = _clean_tool_str(args.get("notes"))
		if order_notes:
			order_payload["notes"] = order_notes
		result = submit_candidate_order(int(conversation_id), order_payload, note=order_notes)
		callback_entry = {
			"name": "place_ai_order_candidate",
			"arguments": {
				"product_name": product_name,
				"customer_phone": customer_phone,
				"has_shipping": bool(shipping_payload),
			},
			"result": {
				"candidate_id": result.get("id"),
				"status": result.get("status"),
				"order_payload": result.get("order_payload"),
			},
		}
		function_callbacks.append(callback_entry)
		_log_function_callback(conversation_id, callback_entry["name"], callback_entry["arguments"], callback_entry["result"])
		return json.dumps(result, ensure_ascii=False)

	tool_handlers["place_ai_order_candidate"] = _handle_place_order_tool

	# Yöneticiye bildirim gönderme fonksiyonu
	tools.append(
		{
			"type": "function",
			"function": {
				"name": "yoneticiye_bildirim_gonder",
				"description": "Yönetici müdahalesi gereken durumlarda BU FONKSİYONU ÇAĞIR. Sadece mesajda 'eskale ediyorum' demek YETERLİ DEĞİL - mutlaka bu fonksiyonu çağır. Aşağıdaki durumlardan herhangi biri gerçekleştiğinde bu fonksiyonu çağır: 1) Satış akışı dışında bir konu sorulduğunda, 2) Değişim/iade doğrudan talep edildiğinde, 3) Müşteri yanlış cevap verildiğini ima ettiğinde, 4) Aynı cevabı birden çok kez vermek zorunda kalındığında, 5) Birden fazla ürün alternatifi (renk/beden dışında) istendiğinde, 6) Müşteri ofise gelmek istediğinde.",
				"parameters": {
					"type": "object",
					"properties": {
						"mesaj": {
							"type": "string",
							"description": "Yöneticiye gönderilecek bildirim mesajı. Konuşma bağlamını ve müşterinin isteğini açıkça belirt. Örn: 'Müşteri değişim talep etti: [detaylar]' veya 'Satış akışı dışında soru: [soru]'",
						},
						"mesaj_tipi": {
							"type": "string",
							"enum": ["info", "warning", "urgent"],
							"description": "Bildirim tipi: 'info' (bilgi), 'warning' (uyarı), 'urgent' (acil). Değişim/iade veya müşteri şikayeti için 'urgent' kullan.",
							"default": "info",
						}
					},
					"required": ["mesaj"],
				},
			},
		}
	)

	def _handle_admin_notification_tool(args: Dict[str, Any]) -> str:
		"""Yöneticiye bildirim gönderme handler'ı - sadece bilgi toplar, gerçek gönderim worker'da yapılır"""
		mesaj = str(args.get("mesaj") or "").strip()
		mesaj_tipi = str(args.get("mesaj_tipi") or "info").strip()
		
		if not mesaj:
			return json.dumps({"error": "Mesaj boş olamaz"}, ensure_ascii=False)
		
		if mesaj_tipi not in ["info", "warning", "urgent"]:
			mesaj_tipi = "info"
		
		# Don't create admin message here - just store the intent
		# The actual notification will be created in worker_reply.py when message is actually sent
		result = {
			"conversation_id": conversation_id,
			"message": mesaj,
			"message_type": mesaj_tipi,
			"status": "pending",  # Will be created when message is actually sent
		}
		
		callback_entry = {
			"name": "yoneticiye_bildirim_gonder",
			"arguments": args,
			"result": result,
		}
		function_callbacks.append(callback_entry)
		_log_function_callback(conversation_id, callback_entry["name"], callback_entry["arguments"], callback_entry["result"])
		
		return json.dumps(result, ensure_ascii=False)

	tool_handlers["yoneticiye_bildirim_gonder"] = _handle_admin_notification_tool

	# Build confidence instruction for first messages
	confidence_instruction = ""
	if is_first_message:
		confidence_instruction = (
			"=== CONFIDENCE KURALI (İLK MESAJ) ===\n"
			"Bu konuşmanın İLK AI mesajı olduğu için:\n"
			"- Standart selamlama + fiyat + ürün özeti + beden sorusu gibi rutin ilk mesajlar için confidence değerini 0.7 veya üzeri kullan.\n"
			"- İlk mesajlar genellikle yüksek güvenilirlik gerektirir çünkü standart bir akıştır.\n"
			"- Sadece belirsiz veya karmaşık durumlarda confidence'i 0.7'in altına düşür.\n"
			"- Örnek: Standart \"Merhabalar abim, Adet 1599₺, ürün özeti, beden sorusu\" mesajı için confidence: 0.75-0.85 arası uygundur.\n\n"
		)
	
	# Add Q&A instructions if we have matching Q&As
	qa_instruction = ""
	if matching_qas:
		qa_instruction = (
			"\n=== EŞLEŞEN Q&A'LAR ===\n"
			"Müşterinin sorusuna benzer sorular ve cevapları aşağıda bulunmaktadır. "
			"Bu cevapları kullanarak müşteriye uygun bir yanıt ver. "
			"Eğer eşleşen Q&A varsa, onların cevaplarını temel al ama müşterinin sorusuna özelleştir.\n"
			+ "\n".join([
				f"Q: {qa['question']}\nA: {qa['answer']}\n(Benzerlik: {qa['similarity']:.1%})\n"
				for qa in matching_qas
			]) + "\n"
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

	# Combine: pretext + gender instructions + product system message for Agent stage
	sys_prompt_parts: List[str] = []
	sys_prompt_parts.append(
		"=== KRİTİK SİSTEM TALİMATLARI (AGENT) ===\n"
		"Sen satış akışını yöneten DM asistanısın. Tool çağırabilirsin; çıkışın müşteriye gidecek DM metni olmalı. JSON yazma, markdown kullanma."
	)
	if pretext_content:
		sys_prompt_parts.append(f"=== ÜRÜN ÖZEL TALİMATLARI ===\n{pretext_content}")
	sys_prompt_parts.append(hail_instructions)
	sys_prompt_parts.append(repeat_guard_instructions)
	sys_prompt_parts.append(gender_instructions)

	message_order_instruction = """
=== MESAJ SIRASI VE YANIT KURALI (KRİTİK) ===
Mesaj sırası ÇOK ÖNEMLİDİR. Her zaman kullanıcının cevap verilmemiş mesajlarına yanıt ver.
1. History mesajları timestamp_ms sıralıdır.
2. Son AI OUT mesajından sonraki TÜM IN mesajlarına tek yanıtta cevap ver.
3. Birden fazla soruya yanıt verirken her birini ayrı satır/paragrafta cevaplayabilirsin.
4. Geçmişte cevaplanmış mesajları tekrar etme.
"""
	sys_prompt_parts.append(message_order_instruction)
	if product_extra_sys:
		sys_prompt_parts.append(f"=== EK ÜRÜN TALİMATLARI ===\n{product_extra_sys}")

	sys_prompt = "\n\n".join(sys_prompt_parts) if sys_prompt_parts else gender_instructions

	# Agent user prompt (tool usage + context)
	agent_user_prompt = (
		"=== AMAÇ ===\n"
		"Satış akışını yönet, gerektiğinde tool çağır, tek bir DM cevabı üret.\n\n"
		"=== BAĞLAM VERİSİ ===\n"
		"CONTEXT_JSON_START\n"
		f"{context_json}\n"
		"CONTEXT_JSON_END\n\n"
		"=== TOOL TALİMATI ===\n"
		"- Boy+kilo geldiğinde `set_customer_measurements` çağır ve gelen beden önerisini kullan.\n"
		"- Sipariş akışında: `create_ai_order_candidate` (ilgi), `mark_ai_order_very_interested` (ilerliyor), `mark_ai_order_not_interested` (vazgeçti), tüm bilgiler tamam ise `place_ai_order_candidate`.\n"
		"- Odak ürün değişince `change_focus_product` çağır (reason alanını kısaca doldur).\n"
		"- Sepete satır eklemek için `add_cart_item` kullan; her satır için product_id, sku, color, size, quantity, unit_price, is_upsell zorunlu.\n"
		"- Müşteri fotoğraf gönderdi ve analiz istiyorsa `analyze_customer_image`.\n"
		"- Müşteri belirli renk/görsel isterse stokta varsa `send_product_image_to_customer` (view_type=front varsayılan) çağır; stokta yoksa dürüstçe yok de.\n"
		"- Admin gerektiren durumlarda `yoneticiye_bildirim_gonder` çağırmayı unutma.\n"
		"  - Satış akışı dışı konu, doğrudan değişim/iade talebi, memnuniyetsizlik/🤦 tonu,\n"
		"    aynı cevabı defalarca vermen gerekirse, farklı ürün alternatifi isteği, ofis ziyareti isteği.\n\n"
		"=== UPSELL VE ÇOKLU ÜRÜN KURALLARI ===\n"
		"- Upsell ürünleri ve metni `upsell_config` içinden gelir; uydurma yapma.\n"
		"- Ana ürün beden+renk tamamlandıysa ve upsell_offered=false ise ödeme öncesi tek sefer upsell teklif et.\n"
		"- 'ikisinide istiyorum' gibi ifadeler çoklu satın alma demektir; her renk/ürün için ayrı `add_cart_item` satırı ekle.\n"
	)
	if function_callbacks:
		agent_user_prompt += "\n=== FONKSİYON ÇAĞRILARI ===\n"
		agent_user_prompt += json.dumps(function_callbacks, ensure_ascii=False)
		agent_user_prompt += "\nBu kayıtlar backend fonksiyon çağrılarının sonucudur; tekrar isteme.\n"

	# Use lower temperature for stricter instruction following, unless disabled
	temperature = get_shadow_temperature_setting()
	temp_opt_out = is_shadow_temperature_opt_out()

	def _build_agent_kwargs(include_raw: bool = False, include_request_payload: bool = False) -> Dict[str, Any]:
		kwargs: Dict[str, Any] = {
			"system_prompt": sys_prompt,
			"user_prompt": agent_user_prompt,
			"temperature": None if temp_opt_out else temperature,
		}
		if tools:
			kwargs["tools"] = tools
			kwargs["tool_choice"] = "auto"
			kwargs["tool_handlers"] = tool_handlers
		if include_raw:
			kwargs["include_raw"] = True
		if include_request_payload:
			kwargs["include_request_payload"] = True
		return kwargs

	agent_raw: Any = None
	agent_request_payload: Any = None
	agent_reply_text: str = ""
	if include_meta:
		agent_result = client.generate_chat(**_build_agent_kwargs(include_raw=True, include_request_payload=True))
		if isinstance(agent_result, tuple):
			if len(agent_result) == 3:
				agent_reply_text, agent_raw, agent_request_payload = agent_result
			elif len(agent_result) == 2:
				if isinstance(agent_result[1], str):
					agent_reply_text, agent_raw = agent_result
				else:
					agent_reply_text, agent_request_payload = agent_result
			else:
				agent_reply_text = agent_result[0]
		else:
			agent_reply_text = agent_result
	else:
		agent_reply_text = client.generate_chat(**_build_agent_kwargs())

	agent_reply_text = _sanitize_reply_text(_decode_escape_sequences(agent_reply_text or ""))

	# Serializer stage
	serializer_prompt = get_serializer_prompt()
	serializer_payload = {
		"context": user_payload,
		"state": state_payload,
		"agent_reply_text": agent_reply_text,
		"function_callbacks": function_callbacks,
	}
	serializer_user_prompt = (
		"Agent cevabını ve güncel state'i aşağıdaki JSON şemana dök.\n"
		"reply_text alanına agent_reply_text'i aynen yaz.\n"
		"should_reply: DM gönderdiysek true; yöneticiye eskalasyon yapıldıysa false.\n"
		"CONTEXT_JSON_START\n"
		f"{json.dumps(serializer_payload, ensure_ascii=False)}\n"
		"CONTEXT_JSON_END\n"
	)

	def _build_serializer_kwargs(include_raw: bool = False, include_request_payload: bool = False) -> Dict[str, Any]:
		kwargs: Dict[str, Any] = {
			"system_prompt": serializer_prompt,
			"user_prompt": serializer_user_prompt,
			"temperature": None if temp_opt_out else temperature,
		}
		if include_raw:
			kwargs["include_raw"] = True
		if include_request_payload:
			kwargs["include_request_payload"] = True
		return kwargs

	raw_response: Any = None
	api_request_payload: Any = None
	if include_meta:
		serializer_result = client.generate_json(**_build_serializer_kwargs(include_raw=True, include_request_payload=True))
		if isinstance(serializer_result, tuple):
			if len(serializer_result) == 3:
				data, raw_response, api_request_payload = serializer_result
			elif len(serializer_result) == 2:
				if isinstance(serializer_result[1], str):
					data, raw_response = serializer_result
				else:
					data, api_request_payload = serializer_result
			else:
				data = serializer_result[0]
		else:
			data = serializer_result
	else:
		data = client.generate_json(**_build_serializer_kwargs())

	# Handle non-dict responses gracefully
	if data is None or not isinstance(data, dict):
		# Log the actual type and content for debugging
		data_type = type(data).__name__ if data is not None else "NoneType"
		data_preview = str(data)[:500] if data is not None else "None"
		try:
			log.error(
				"serializer_non_dict_json conversation_id=%s type=%s preview=%s",
				conversation_id,
				data_type,
				data_preview,
			)
		except Exception:
			pass
		
		# Try to extract dict from list or other structures
		if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
			# If it's a list with a dict, use the first dict
			data = data[0]
			try:
				log.warning("serializer_extracted_from_list conversation_id=%s", conversation_id)
			except Exception:
				pass
		elif isinstance(data, str):
			# Try to parse as JSON string
			try:
				parsed = json.loads(data)
				if isinstance(parsed, dict):
					data = parsed
					try:
						log.warning("serializer_parsed_from_string conversation_id=%s", conversation_id)
					except Exception:
						pass
			except Exception:
				pass
		
		# If still not a dict, create a fallback response
		if data is None or not isinstance(data, dict):
			# Clean agent_reply_text before using it as fallback
			clean_agent_text = agent_reply_text or ""
			parsed_agent_reply = None
			if isinstance(clean_agent_text, str) and clean_agent_text.lstrip().startswith("{"):
				try:
					parsed_agent_reply = json.loads(clean_agent_text)
				except Exception:
					parsed_agent_reply = None
			if isinstance(parsed_agent_reply, dict):
				candidate_text = (
					parsed_agent_reply.get("reply_text")
					or parsed_agent_reply.get("text")
					or parsed_agent_reply.get("message")
					or parsed_agent_reply.get("content")
				)
				if isinstance(candidate_text, str) and candidate_text.strip():
					clean_agent_text = candidate_text.strip()
				parsed_state = parsed_agent_reply.get("state")
				if isinstance(parsed_state, dict):
					try:
						# Only merge when we don't already have state payload
						if not state_payload:
							state_payload = parsed_state
					except Exception:
						pass
			if clean_agent_text and isinstance(clean_agent_text, str):
				# Remove any embedded state objects from agent text (legacy malformed JSON)
				try:
					import re
					state_pattern = r'["\'],?\s*"state"\s*:\s*\{'
					match = re.search(state_pattern, clean_agent_text)
					if match:
						clean_agent_text = clean_agent_text[:match.start()].strip()
						clean_agent_text = re.sub(r'["\']\s*,?\s*$', '', clean_agent_text)
				except Exception:
					pass
			
			data = {
				"should_reply": True,
				"reply_text": clean_agent_text,
				"confidence": 0.5,
				"reason": f"serializer_returned_{data_type}_fallback_to_agent",
				"notes": f"Serializer stage returned {data_type} instead of dict. Using agent reply as fallback.",
				"state": state_payload,  # Use the state we already have
			}
			try:
				log.warning(
					"serializer_fallback conversation_id=%s agent_reply_len=%s",
					conversation_id,
					len(clean_agent_text),
				)
			except Exception:
				pass

	if function_callbacks:
		data["function_callbacks"] = function_callbacks

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
	reply_text_raw = (data.get("reply_text") or agent_reply_text or "").strip()
	
	# Fix: If reply_text contains a serialized JSON object (e.g., {"reply_text": "..."}),
	# extract the actual text and optionally propagate confidence/reason fields.
	if reply_text_raw and reply_text_raw.lstrip().startswith("{"):
		try:
			parsed_reply = json.loads(reply_text_raw)
			if isinstance(parsed_reply, dict):
				candidate_text = (
					parsed_reply.get("reply_text")
					or parsed_reply.get("text")
					or parsed_reply.get("message")
					or parsed_reply.get("content")
				)
				if isinstance(candidate_text, str) and candidate_text.strip():
					reply_text_raw = candidate_text.strip()
				# Use confidence/reason from parsed payload when serializer left them empty
				if "confidence" in parsed_reply and data.get("confidence") is None:
					data["confidence"] = parsed_reply.get("confidence")
				if "reason" in parsed_reply and not data.get("reason"):
					data["reason"] = parsed_reply.get("reason")
		except Exception:
			# Last-resort: strip prefix even if closing quote/brace is missing
			try:
				if reply_text_raw.lstrip().startswith('{"reply_text"'):
					raw = reply_text_raw.split('{"reply_text"', 1)[1]
					raw = raw.lstrip(" :").lstrip('"')
					raw = raw.rstrip('}"\' \n\r\t')
					if raw:
						reply_text_raw = raw
			except Exception:
				pass
	
	# Fix: If reply_text contains JSON-like structures (e.g., state object), extract just the text part
	# This can happen if the serializer malformed the JSON and embedded state in reply_text
	if reply_text_raw:
		# Check for common patterns where state object got embedded in reply_text
		# Pattern: "text","state":{...} or "text", "state":{...}
		import re
		# Match pattern where reply_text ends with JSON state object
		# Look for: ","state": or ',"state": followed by JSON object
		state_pattern = r'["\'],?\s*"state"\s*:\s*\{'
		match = re.search(state_pattern, reply_text_raw)
		if match:
			# Extract text before the state object
			reply_text_raw = reply_text_raw[:match.start()].strip()
			# Remove trailing quote and comma if present
			reply_text_raw = re.sub(r'["\']\s*,?\s*$', '', reply_text_raw)
		
		# Also check for any remaining JSON-like structures and clean them
		if reply_text_raw.startswith('"') and reply_text_raw.endswith('"'):
			# Remove outer quotes if present
			reply_text_raw = reply_text_raw[1:-1]
	
	reply_text = _sanitize_reply_text(_decode_escape_sequences(reply_text_raw))
	try:
		conf_raw = float(data.get("confidence") if data.get("confidence") is not None else 0.59)
	except Exception:
		conf_raw = 0.59
	confidence = max(0.0, min(1.0, conf_raw))
	reason = (data.get("reason") or "auto")
	notes = (data.get("notes") or None)
	admin_escalation_requested = any(
		cb.get("name") == "yoneticiye_bildirim_gonder" for cb in (function_callbacks or []) if isinstance(cb, dict)
	)
	# Don't escalate to admin just because serializer failed - that's a technical issue, not a business escalation
	if admin_escalation_requested and not reason.startswith("serializer_returned_"):
		should_reply = False
		if not reason or reason == "auto":
			reason = "needs_admin"

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
		try:
			debug_meta = {
				"system_prompt": sys_prompt,
				"user_payload": user_payload,
				"raw_response": raw_response,
				"qa_search_metadata": qa_search_metadata,
				"agent_raw": agent_raw,
				"agent_request_payload": agent_request_payload,
				"serializer_request_payload": api_request_payload,
			}
			reply["debug_meta"] = debug_meta
		except Exception:
			pass
	return reply


