import json
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ..db import get_session, engine
from ..models import Message, Conversation, IGUser
from .instagram_api import fetch_message_details  # type: ignore
import logging as _lg
_log = _lg.getLogger("ingest")
_log_up = _lg.getLogger("ingest.upsert")
from .queue import enqueue
from sqlalchemy import text as _sql_text


@dataclass
class _InsertResult:
	message_id: int
	message_text: Optional[str]
	attachments: Any
	conversation_id: Optional[int]
	timestamp_ms: Optional[int]
	direction: Optional[str]
	ad_id: Optional[str]
	ad_link: Optional[str]
	ad_title: Optional[str]
	ad_name: Optional[str]
	referral_json: Optional[str]

# MySQL-only backend


def _iter_attachment_items(attachments: Any) -> List[dict]:
	if isinstance(attachments, list):
		return [att for att in attachments if isinstance(att, dict)]
	if isinstance(attachments, dict):
		data = attachments.get("data")
		if isinstance(data, list):
			return [att for att in data if isinstance(att, dict)]
	return []


def _attachment_item_has_media(att: dict) -> bool:
	if att.get("file_url") or att.get("image_url") or att.get("video_url"):
		return True
	image_data = att.get("image_data")
	if isinstance(image_data, dict) and (image_data.get("url") or image_data.get("preview_url")):
		return True
	payload = att.get("payload")
	if isinstance(payload, dict):
		if payload.get("url") or payload.get("attachment_id"):
			return True
		image_data = payload.get("image_data")
		if isinstance(image_data, dict) and (image_data.get("url") or image_data.get("preview_url")):
			return True
	return False


def _extract_template_elements(attachments: Any) -> List[dict]:
	elements: List[dict] = []
	for att in _iter_attachment_items(attachments):
		payload = att.get("payload") or {}
		candidates: List[dict] = []
		generic = payload.get("generic")
		if isinstance(generic, dict) and isinstance(generic.get("elements"), list):
			candidates = [el for el in generic.get("elements") if isinstance(el, dict)]
		elif isinstance(payload.get("elements"), list):
			candidates = [el for el in payload.get("elements") if isinstance(el, dict)]
		elif isinstance(payload.get("cards"), list):
			candidates = [el for el in payload.get("cards") if isinstance(el, dict)]
		if candidates:
			elements.extend(candidates)
	return elements


def _attachments_have_visible_content(attachments: Any) -> bool:
	if not attachments:
		return False
	items = _iter_attachment_items(attachments)
	for att in items:
		if _attachment_item_has_media(att):
			return True
	template_elements = _extract_template_elements(items)
	if template_elements:
		return True
	return False


def _derive_template_preview(attachments: Any) -> Optional[str]:
	elements = _extract_template_elements(attachments)
	if not elements:
		return None
	first = elements[0]
	title = (first.get("title") or first.get("header") or "").strip()
	subtitle = (first.get("subtitle") or first.get("description") or "").strip()
	if title and subtitle:
		return f"{title} — {subtitle}"
	return title or subtitle or None


def _fetch_message_details_sync(mid: str) -> Optional[Dict[str, Any]]:
	if not mid:
		return None
	try:
		import asyncio

		loop = asyncio.new_event_loop()
		try:
			asyncio.set_event_loop(loop)
			return loop.run_until_complete(fetch_message_details(str(mid)))
		finally:
			asyncio.set_event_loop(None)
			loop.close()
	except Exception as e:
		try:
			_log.debug("ingest: detail fetch failed mid=%s err=%s", str(mid)[:60], str(e)[:200])
		except Exception:
			pass
		return None


def _maybe_expand_attachments(mid: Optional[str], attachments: Any) -> Any:
	if not mid:
		return attachments
	if _attachments_have_visible_content(attachments):
		return attachments
	detail = _fetch_message_details_sync(str(mid))
	if not detail:
		return attachments
	alt = detail.get("attachments")
	if not alt:
		message_field = detail.get("message")
		if isinstance(message_field, dict):
			alt = message_field.get("attachments")
	if _attachments_have_visible_content(alt):
		try:
			_log.debug("ingest: expanded attachments via Graph detail mid=%s", str(mid)[:60])
		except Exception:
			pass
		return alt
	return attachments


def _ensure_ig_account(conn, igba_id: str) -> None:
	"""Ensure ig_accounts row exists for given igba_id (MySQL dialect)."""
	conn.exec_driver_sql(
		"""
		INSERT IGNORE INTO ig_accounts(igba_id, updated_at)
		VALUES (%s, CURRENT_TIMESTAMP)
		""",
		(igba_id,),
	)


def _ensure_ig_user(conn, ig_user_id: str) -> None:
	"""Ensure ig_users row exists for given ig_user_id (MySQL dialect)."""
	conn.exec_driver_sql(
		"""
		INSERT IGNORE INTO ig_users(ig_user_id)
		VALUES (%s)
		""",
		(ig_user_id,),
	)


def _ensure_ig_user_with_data(ig_user_id: str, igba_id: str | None = None) -> None:
	"""Ensure IG user exists with data (username, name, etc.) without holding long DB transactions."""
	if not ig_user_id:
		return

	def _needs_enrichment() -> bool:
		with get_session() as session:
			row = session.exec(
				text("SELECT ig_user_id, username, fetch_status FROM ig_users WHERE ig_user_id=:id LIMIT 1").params(
					id=str(ig_user_id)
				)
			).first()
			if not row:
				return True
			username = getattr(row, "username", None) or (row[1] if len(row) > 1 else None)
			fetch_status = getattr(row, "fetch_status", None) or (row[2] if len(row) > 2 else None)
			return not (username and str(fetch_status or "").lower() == "ok")

	if not _needs_enrichment():
		return

	try:
		import asyncio

		# Create new event loop if one doesn't exist (same pattern as _fetch_message_details_sync)
		try:
			loop = asyncio.get_event_loop()
		except RuntimeError:
			loop = asyncio.new_event_loop()
			asyncio.set_event_loop(loop)
		
		from .enrichers import enrich_user

		result = loop.run_until_complete(enrich_user(ig_user_id))
		if result:
			_log.info("ingest: user %s enriched synchronously", ig_user_id)
		else:
			_log.debug("ingest: user %s already had data or fetch failed", ig_user_id)
		return
	except Exception as e:
		_log.warning("ingest: failed to enrich user %s synchronously: %s", ig_user_id, e)

	# Fallback: ensure at least the row exists
	try:
		with get_session() as session:
			session.exec(text("INSERT IGNORE INTO ig_users(ig_user_id) VALUES (:id)").params(id=str(ig_user_id)))
	except Exception:
		pass


def _extract_graph_conversation_id_from_message_id(mid: str, page_id: Optional[str] = None) -> Optional[str]:
	"""
	Extract Graph API conversation ID from a message ID.
	
	Graph API message IDs are base64-encoded and contain the conversation thread ID.
	Format: base64("ig_message_item:1:IGMessageThread:<page_id>:<thread_id>:<message_id>")
	We extract the thread part to construct: base64("ig_message_thread:1:IGMessageThread:<page_id>:<thread_id>")
	"""
	if not mid:
		return None
	try:
		import base64
		import re
		mid_str = str(mid)
		# Try decoding with different padding
		decoded_str = None
		for pad in ['', '=', '==', '===']:
			try:
				decoded = base64.b64decode(mid_str + pad)
				decoded_str = decoded.decode('utf-8', errors='ignore')
				if 'IGMessage' in decoded_str or 'Thread' in decoded_str:
					break
			except Exception:
				continue
		
		if not decoded_str:
			return None
		
		# Look for the thread pattern: IGMessageThread:<page_id>:<thread_id>
		match = re.search(r'IGMessageThread[^:]*:(\d+):(\d+)', decoded_str)
		if match:
			page_part = match.group(1)
			thread_part = match.group(2)
			# Reconstruct the Graph conversation ID
			conv_str = f"ig_message_thread:1:IGMessageThread:{page_part}:{thread_part}"
			return base64.b64encode(conv_str.encode('utf-8')).decode('utf-8').rstrip('=')
		
		# Fallback: try to find thread ID by looking for page_id in decoded string
		if page_id and page_id in decoded_str:
			page_pos = decoded_str.find(page_id)
			if page_pos >= 0:
				remaining = decoded_str[page_pos + len(page_id):]
				thread_match = re.search(r':(\d+)', remaining)
				if thread_match:
					thread_part = thread_match.group(1)
					conv_str = f"ig_message_thread:1:IGMessageThread:{page_id}:{thread_part}"
					return base64.b64encode(conv_str.encode('utf-8')).decode('utf-8').rstrip('=')
	except Exception:
		pass
	return None


def _get_or_create_conversation_id(
	session, igba_id: str, ig_user_id: Optional[str]
) -> Optional[int]:
	"""
	Resolve or create the canonical Conversation row for (igba_id, ig_user_id).

	Returns the internal conversations.id primary key, or None when other_party
	cannot be determined.
	"""
	if not igba_id or not ig_user_id:
		return None
	from sqlalchemy import text as _t
	# Try fast path: existing row for this (page, user) pair.
	# If historical duplicates exist, prefer the one that already has a Graph conversation id.
	row = session.exec(
		_t(
			"""
			SELECT id
			FROM conversations
			WHERE igba_id=:g AND ig_user_id=:u
			ORDER BY CASE WHEN graph_conversation_id IS NULL THEN 1 ELSE 0 END, id ASC
			LIMIT 1
			"""
		).params(g=str(igba_id), u=str(ig_user_id))
	).first()
	if row:
		try:
			return int(getattr(row, "id", row[0]))
		except Exception:
			return None

	# Insert minimal row; timestamps and summary fields will be updated by caller.
	now_dt = dt.datetime.utcnow()
	try:
		session.exec(
			_t(
				"""
				INSERT INTO conversations(igba_id, ig_user_id, last_message_at, unread_count)
				VALUES (:g, :u, :ts, 0)
				"""
			).params(g=str(igba_id), u=str(ig_user_id), ts=now_dt)
		)
	except Exception:
		# Best-effort; fall through to lookup.
		pass
	row2 = session.exec(
		_t(
			"""
			SELECT id
			FROM conversations
			WHERE igba_id=:g AND ig_user_id=:u
			ORDER BY CASE WHEN graph_conversation_id IS NULL THEN 1 ELSE 0 END, id ASC
			LIMIT 1
			"""
		).params(g=str(igba_id), u=str(ig_user_id))
	).first()
	if not row2:
		return None
	try:
		return int(getattr(row2, "id", row2[0]))
	except Exception:
		return None


def _update_conversation_summary_from_message(
	session,
	conversation_pk: Optional[int],
	ts_raw: Any,
	*,
	message_row: Message,
	text_val: Optional[str],
	direction: Optional[str],
	sender_id: Optional[str],
	recipient_id: Optional[str],
	ad_id: Optional[str],
	ad_link: Optional[str],
	ad_title: Optional[str],
	post_id: Optional[str] = None,
) -> None:
	"""
	Update conversations last-* summary fields from a newly inserted Message.

	Behaves like the old ai_conversations upsert logic: only advances summary when
	the new message timestamp is newer than the stored one.
	"""
	if not conversation_pk:
		return
	try:
		from sqlalchemy import text as _text

		# Normalize timestamp
		ts_val: Optional[int] = None
		try:
			if isinstance(ts_raw, (int, float)):
				ts_val = int(ts_raw)
			elif isinstance(ts_raw, str):
				digits = "".join(ch for ch in ts_raw if ch.isdigit())
				if digits:
					ts_val = int(digits)
		except Exception:
			ts_val = None
		if ts_val is None:
			try:
				ts_val = int(message_row.timestamp_ms) if message_row.timestamp_ms is not None else None
			except Exception:
				ts_val = None
		if ts_val is None:
			try:
				import time as _t

				ts_val = int(_t.time() * 1000)
			except Exception:
				ts_val = None
		if ts_val is None:
			return
		ts_dt = None
		try:
			ts_dt = dt.datetime.utcfromtimestamp(ts_val / 1000.0)
		except Exception:
			ts_dt = None
		
		# Determine link_type and link_id (prioritize post over ad)
		link_type = None
		link_id = None
		if post_id:
			link_type = 'post'
			link_id = str(post_id)
		elif ad_id:
			link_type = 'ad'
			link_id = str(ad_id)
		
		session.exec(
			_text(
				"""
				UPDATE conversations
				SET
				  last_message_id = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) THEN :mid
				    ELSE last_message_id
				  END,
				  last_message_timestamp_ms = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) THEN :ts
				    ELSE last_message_timestamp_ms
				  END,
				  last_message_text = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) THEN :txt
				    ELSE last_message_text
				  END,
				  last_message_direction = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) THEN :dir
				    ELSE last_message_direction
				  END,
				  ig_sender_id = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) THEN :sid
				    ELSE ig_sender_id
				  END,
				  ig_recipient_id = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) THEN :rid
				    ELSE ig_recipient_id
				  END,
				  -- Only advance ad metadata when the new message actually carries ad info (deprecated)
				  last_ad_id = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) AND :adid IS NOT NULL THEN :adid
				    ELSE last_ad_id
				  END,
				  last_ad_link = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) AND :alink IS NOT NULL THEN :alink
				    ELSE last_ad_link
				  END,
				  last_ad_title = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) AND :atitle IS NOT NULL THEN :atitle
				    ELSE last_ad_title
				  END,
				  -- New unified link tracking
				  last_link_type = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) AND :link_type IS NOT NULL THEN :link_type
				    ELSE last_link_type
				  END,
				  last_link_id = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) AND :link_id IS NOT NULL THEN :link_id
				    ELSE last_link_id
				  END,
				  last_message_at = CASE
				    WHEN :ts >= COALESCE(last_message_timestamp_ms, 0) AND :ts_dt IS NOT NULL
				      THEN COALESCE(:ts_dt, last_message_at)
				    ELSE last_message_at
				  END
				WHERE id = :cid
				"""
			).params(
				cid=int(conversation_pk),
				mid=int(message_row.id),
				ts=int(ts_val),
				ts_dt=ts_dt,
				txt=(text_val or ""),
				dir=(direction or "in"),
				sid=(str(sender_id) if sender_id is not None else None),
				rid=(str(recipient_id) if recipient_id is not None else None),
				adid=(str(ad_id) if ad_id is not None else None),
				alink=ad_link,
				atitle=ad_title,
				link_type=link_type,
				link_id=link_id,
			)
		)
	except Exception as e:
		import logging as _lg

		_lg.getLogger("ingest.upsert").warning(
			"ingest upsert conversations failed convo_pk=%s mid=%s err=%s",
			str(conversation_pk),
			str(getattr(message_row, "id", None)),
			str(e)[:200],
		)


def _insert_message(session, event: Dict[str, Any], igba_id: str) -> Optional[_InsertResult]:
	message_obj = event.get("message") or {}
	if not message_obj:
		return None
	mid = message_obj.get("mid") or message_obj.get("id")
	if not mid:
		return None
	# idempotency by ig_message_id - use INSERT IGNORE to avoid race conditions
	# First check if exists (fast path for already-processed messages)
	exists = session.exec(text("SELECT id FROM message WHERE ig_message_id = :mid LIMIT 1").params(mid=str(mid))).first()
	if exists:
		return None
	sender_id = (event.get("sender") or {}).get("id")
	recipient_id = (event.get("recipient") or {}).get("id")
	timestamp_ms = event.get("timestamp")
	# derive direction: compare sender with owner/page id (check both igba_id from webhook and configured owner)
	direction = "in"
	try:
		# Check against webhook entry id (igba_id)
		if sender_id and str(sender_id) == str(igba_id):
			direction = "out"
		else:
			# Also check against configured owner ID (IG_PAGE_ID or IG_USER_ID)
			token, owner_id, is_page = __import__(
				"app.services.instagram_api", fromlist=["_get_base_token_and_id"]
			).instagram_api._get_base_token_and_id()
			if sender_id and str(sender_id) == str(owner_id):
				direction = "out"
			# Also check IG_USER_ID if it's different from owner_id (for business accounts)
			try:
				import os
				ig_user_id = os.getenv("IG_USER_ID")
				if ig_user_id and sender_id and str(sender_id) == str(ig_user_id):
					direction = "out"
			except Exception:
				pass
	except Exception:
		pass
	# Canonical conversation key is always (page_id, user_id), independent of direction.
	page_id = str(igba_id) if igba_id is not None else ""
	user_id: Optional[str] = None
	try:
		if sender_id and page_id and str(sender_id) != page_id:
			user_id = str(sender_id)
		elif recipient_id and page_id and str(recipient_id) != page_id:
			user_id = str(recipient_id)
	except Exception:
		user_id = None
	# Resolve or create canonical Conversation row (internal integer id)
	conversation_pk = _get_or_create_conversation_id(session, page_id, user_id)
	
	# Extract Graph conversation ID from message ID for hydration
	graph_conversation_id = None
	if conversation_pk is not None and mid:
		graph_conversation_id = _extract_graph_conversation_id_from_message_id(str(mid), page_id)
		# Store Graph conversation ID on the conversation row if we extracted it
		if graph_conversation_id:
			try:
				from sqlalchemy import text as _t
				session.exec(
					_t(
						"UPDATE conversations SET graph_conversation_id=:gc WHERE id=:cid AND (graph_conversation_id IS NULL OR graph_conversation_id=:gc)"
					).params(gc=str(graph_conversation_id), cid=int(conversation_pk))
				)
			except Exception:
				pass
	
	text_val = message_obj.get("text")
	attachments = _maybe_expand_attachments(str(mid), message_obj.get("attachments"))
	# Story reply (best-effort)
	story_id = None
	story_url = None
	try:
		story_obj = ((message_obj.get("reply_to") or {}).get("story") or {})
		story_id = str(story_obj.get("id") or "") or None
		story_url = story_obj.get("url") or None
	except Exception:
		story_id = None
		story_url = None
	# Post extraction from attachments (best-effort)
	post_id = None
	post_title = None
	post_url = None
	try:
		if attachments and isinstance(attachments, list):
			for att in attachments:
				if att.get("type") in ("ig_post", "share"):
					payload = att.get("payload", {})
					media_id = payload.get("ig_post_media_id")
					if media_id:
						post_id = str(media_id)
						post_title = payload.get("title")
						post_url = payload.get("url")
						break
	except Exception:
		post_id = None
		post_title = None
		post_url = None
	if not text_val:
		text_val = _derive_template_preview(attachments)
	# Ad/referral extraction (best-effort)
	ad_id = None
	ad_link = None
	ad_title = None
	ad_img = None
	ad_name = None
	referral_json_val = None
	try:
		ref = (event.get("referral") or message_obj.get("referral") or {})
		# Only process referral if it's a dict with actual data (not empty)
		if isinstance(ref, dict) and ref:
			try:
				_log_up.info(
					"insert.webhook: referral raw mid=%s type=%s keys=%s",
					str(mid),
					type(ref).__name__,
					list(ref.keys()),
				)
			except Exception:
				pass
			ad_id = str(ref.get("ad_id") or ref.get("ad_id_v2") or "") or None
			ad_link = ref.get("ad_link") or ref.get("url") or ref.get("link") or None
			ad_title = ref.get("headline") or ref.get("source") or ref.get("type") or None
			ad_img = ref.get("image_url") or ref.get("thumbnail_url") or ref.get("picture") or ref.get("media_url") or None
			ad_name = ref.get("name") or ref.get("title") or None
			referral_json_val = json.dumps(ref, ensure_ascii=False)
			try:
				_log_up.info(
					"insert.webhook: referral parsed mid=%s ad_id=%s ad_link=%s ad_title=%s",
					str(mid),
					str(ad_id),
					str(ad_link),
					str(ad_title),
				)
			except Exception:
				pass
		# Also parse Ads Library id from ad_link query param when present
		if not ad_id and ad_link and "facebook.com/ads/library" in str(ad_link):
			try:
				from urllib.parse import urlparse, parse_qs
				q = parse_qs(urlparse(str(ad_link)).query)
				aid = (q.get("id") or [None])[0]
				ad_id = str(aid) if aid else None
				try:
					_log_up.info(
						"insert.webhook: referral ad_id from ads_library mid=%s ad_id=%s",
						str(mid),
						str(ad_id),
					)
				except Exception:
					pass
			except Exception as e:
				try:
					_log_up.warning(
						"insert.webhook: referral ads_library parse error mid=%s err=%s",
						str(mid),
						str(e),
					)
				except Exception:
					pass
	except Exception as e:
		try:
			_log_up.warning(
				"insert.webhook: referral parse error mid=%s err=%s",
				str(mid),
				str(e),
			)
		except Exception:
			pass
		ad_id = ad_link = ad_title = ad_img = ad_name = None
		referral_json_val = None
	# Debug logging: capture mapping decisions for this message to aid troubleshooting
	try:
		_log_up.info(
			"insert.webhook: mid=%s from=%s to=%s igba_id=%s user_id=%s conv_pk=%s graph_cid=%s direction=%s ad_id=%s",
			str(mid),
			str(sender_id),
			str(recipient_id),
			str(igba_id),
			str(user_id),
			str(conversation_pk),
			str(graph_conversation_id),
			str(direction),
			str(ad_id),
		)
	except Exception:
		# Never break ingestion because of debug logging
		pass

	# Use INSERT IGNORE to avoid race conditions and lock contention
	# This prevents duplicate key errors when multiple workers process the same message
	try:
		from sqlalchemy import text as _t
		# Try INSERT IGNORE first - this is atomic and avoids lock contention
		stmt = _t("""
			INSERT IGNORE INTO message (
				ig_sender_id, ig_recipient_id, ig_message_id, text, attachments_json,
				timestamp_ms, raw_json, conversation_id, direction, sender_username,
				story_id, story_url, ad_id, ad_link, ad_title, ad_image_url, ad_name, referral_json, created_at
			) VALUES (
				:sender_id, :recipient_id, :mid, :text, :attachments_json,
				:timestamp_ms, :raw_json, :conversation_id, :direction, :sender_username,
				:story_id, :story_url, :ad_id, :ad_link, :ad_title, :ad_image_url, :ad_name, :referral_json, NOW()
			)
		""").bindparams(
			sender_id=str(sender_id) if sender_id is not None else None,
			recipient_id=str(recipient_id) if recipient_id is not None else None,
			mid=str(mid),
			text=text_val,
			attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments is not None else None,
			timestamp_ms=int(timestamp_ms) if isinstance(timestamp_ms, (int, float, str)) and str(timestamp_ms).isdigit() else None,
			raw_json=json.dumps(event, ensure_ascii=False),
			conversation_id=int(conversation_pk) if conversation_pk is not None else None,
			direction=direction,
			sender_username=None,  # Will be set by enricher if needed
			story_id=story_id,
			story_url=story_url,
			ad_id=ad_id,
			ad_link=ad_link,
			ad_title=ad_title,
			ad_image_url=ad_img,
			ad_name=ad_name,
			referral_json=referral_json_val,
		)
		result = session.exec(stmt)
		session.flush()
		
		# Check if insert actually happened (INSERT IGNORE returns 0 rows if duplicate)
		# Fetch the message ID
		msg_row = session.exec(text("SELECT id FROM message WHERE ig_message_id = :mid").params(mid=str(mid))).first()
		if not msg_row:
			# Another process inserted it between our check and insert, or insert failed silently
			return None
		message_id = int(msg_row.id if hasattr(msg_row, "id") else msg_row[0])
		
		# Always update raw_json with the latest callback data (even if message already existed)
		# This ensures we capture all callback data including reply_to for investigation
		raw_json_data = json.dumps(event, ensure_ascii=False)
		try:
			session.exec(
				text("UPDATE message SET raw_json = :raw_json WHERE ig_message_id = :mid").params(
					raw_json=raw_json_data,
					mid=str(mid)
				)
			)
		except Exception:
			# Best-effort; don't fail if update fails
			pass
		
		# Create Message object for return value
		row = Message(
			id=message_id,
			ig_sender_id=str(sender_id) if sender_id is not None else None,
			ig_recipient_id=str(recipient_id) if recipient_id is not None else None,
			ig_message_id=str(mid),
			text=text_val,
			attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments is not None else None,
			timestamp_ms=int(timestamp_ms) if isinstance(timestamp_ms, (int, float, str)) and str(timestamp_ms).isdigit() else None,
			raw_json=raw_json_data,
			conversation_id=int(conversation_pk) if conversation_pk is not None else None,
			direction=direction,
			story_id=story_id,
			story_url=story_url,
			ad_id=ad_id,
			ad_link=ad_link,
			ad_title=ad_title,
			ad_image_url=ad_img,
			ad_name=ad_name,
			referral_json=referral_json_val,
		)
	except Exception as e:
		# If INSERT IGNORE fails for any reason, check if message exists (race condition handled)
		try:
			_log_up.warning("insert.webhook: INSERT IGNORE failed mid=%s err=%s, checking if exists", str(mid), str(e)[:200])
		except Exception:
			pass
		msg_row = session.exec(text("SELECT id FROM message WHERE ig_message_id = :mid").params(mid=str(mid))).first()
		if not msg_row:
			# Insert failed and message doesn't exist - return None
			return None
		message_id = int(msg_row.id if hasattr(msg_row, "id") else msg_row[0])
		# Always update raw_json with the latest callback data (even if message already existed)
		# This ensures we capture all callback data including reply_to for investigation
		raw_json_data = json.dumps(event, ensure_ascii=False)
		try:
			session.exec(
				text("UPDATE message SET raw_json = :raw_json WHERE ig_message_id = :mid").params(
					raw_json=raw_json_data,
					mid=str(mid)
				)
			)
		except Exception:
			# Best-effort; don't fail if update fails
			pass
		# Fetch full row for return value
		row = session.get(Message, message_id)
		if not row:
			return None
	# Upsert conversations last-* fields (summary) keyed by internal id
	_update_conversation_summary_from_message(
		session,
		conversation_pk,
		timestamp_ms,
		message_row=row,
		text_val=text_val,
		direction=direction,
		sender_id=str(sender_id) if sender_id is not None else None,
		recipient_id=str(recipient_id) if recipient_id is not None else None,
		ad_id=str(ad_id) if ad_id is not None else None,
		ad_link=ad_link,
		ad_title=ad_title,
		post_id=post_id,
	)
	# upsert ads cache
	try:
		if ad_id:
			try:
				_log_up.info(
					"insert.webhook: ads upsert start mid=%s ad_id=%s name=%s link=%s",
					str(mid),
					str(ad_id),
					str(ad_name),
					str(ad_link),
				)
			except Exception:
				pass
			# MySQL-only: use INSERT IGNORE (idempotent on PK ad_id)
			try:
				stmt_ins_ignore = _sql_text(
					"INSERT IGNORE INTO ads(ad_id, link_type, name, image_url, link, updated_at) "
					"VALUES (:id, 'ad', :n, :img, :lnk, CURRENT_TIMESTAMP)"
				).bindparams(id=ad_id, n=ad_name, img=ad_img, lnk=ad_link)
				session.exec(stmt_ins_ignore)
				try:
					_log_up.info(
						"insert.webhook: ads INSERT IGNORE ok mid=%s ad_id=%s",
						str(mid),
						str(ad_id),
					)
				except Exception:
					pass
			except Exception as e_mysql:
				try:
					_log_up.error(
						"insert.webhook: ads INSERT IGNORE failed mid=%s ad_id=%s err=%s",
						str(mid),
						str(ad_id),
						str(e_mysql),
					)
				except Exception:
					pass
			try:
				stmt_upd = _sql_text(
					"UPDATE ads SET "
					"link_type='ad', "
					"name=COALESCE(:n,name), "
					"image_url=COALESCE(:img,image_url), "
					"link=COALESCE(:lnk,link), "
					"updated_at=CURRENT_TIMESTAMP "
					"WHERE ad_id=:id"
				).bindparams(id=ad_id, n=ad_name, img=ad_img, lnk=ad_link)
				session.exec(stmt_upd)
				try:
					_log_up.info(
						"insert.webhook: ads UPDATE ok mid=%s ad_id=%s",
						str(mid),
						str(ad_id),
					)
				except Exception:
					pass
			except Exception as e3:
				try:
					_log_up.error(
						"insert.webhook: ads UPDATE failed mid=%s ad_id=%s err=%s",
						str(mid),
						str(ad_id),
						str(e3),
					)
				except Exception:
					pass
	except Exception as outer:
		try:
			_log_up.error(
				"insert.webhook: ads upsert outer error mid=%s ad_id=%s err=%s",
				str(mid),
				str(ad_id),
				str(outer),
			)
		except Exception:
			pass
	# upsert posts cache (stored in ads table with link_type='post')
	try:
		if post_id:
			try:
				_log_up.info(
					"insert.webhook: posts upsert start mid=%s post_id=%s title=%s url=%s",
					str(mid),
					str(post_id),
					str(post_title),
					str(post_url),
				)
			except Exception:
				pass
			# MySQL-only: use INSERT IGNORE (idempotent on PK ad_id)
			try:
				stmt_ins_ignore = _sql_text(
					"INSERT IGNORE INTO ads(ad_id, link_type, name, link, updated_at) "
					"VALUES (:id, 'post', :n, :lnk, CURRENT_TIMESTAMP)"
				).bindparams(id=post_id, n=post_title, lnk=post_url)
				session.exec(stmt_ins_ignore)
				try:
					_log_up.info(
						"insert.webhook: posts INSERT IGNORE ok mid=%s post_id=%s",
						str(mid),
						str(post_id),
					)
				except Exception:
					pass
			except Exception as e_mysql:
				try:
					_log_up.error(
						"insert.webhook: posts INSERT IGNORE failed mid=%s post_id=%s err=%s",
						str(mid),
						str(post_id),
						str(e_mysql),
					)
				except Exception:
					pass
			try:
				stmt_upd = _sql_text(
					"UPDATE ads SET "
					"link_type='post', "
					"name=COALESCE(:n,name), "
					"link=COALESCE(:lnk,link), "
					"updated_at=CURRENT_TIMESTAMP "
					"WHERE ad_id=:id"
				).bindparams(id=post_id, n=post_title, lnk=post_url)
				session.exec(stmt_upd)
				try:
					_log_up.info(
						"insert.webhook: posts UPDATE ok mid=%s post_id=%s",
						str(mid),
						str(post_id),
					)
				except Exception:
					pass
			except Exception as e3:
				try:
					_log_up.error(
						"insert.webhook: posts UPDATE failed mid=%s post_id=%s err=%s",
						str(mid),
						str(post_id),
						str(e3),
					)
				except Exception:
					pass
	except Exception as outer:
		try:
			_log_up.error(
				"insert.webhook: posts upsert outer error mid=%s post_id=%s err=%s",
				str(mid),
				str(post_id),
				str(outer),
			)
		except Exception:
			pass
	# upsert stories cache
	try:
		if story_id:
			session.exec(_sql_text("INSERT IGNORE INTO stories(story_id, url, updated_at) VALUES (:id, :url, CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
			session.exec(_sql_text("UPDATE stories SET url=COALESCE(:url,url), updated_at=CURRENT_TIMESTAMP WHERE story_id=:id")).params(id=str(story_id), url=(str(story_url) if story_url else None))
	except Exception:
		pass
	shadow_ts = None
	try:
		if isinstance(timestamp_ms, (int, float)):
			shadow_ts = int(timestamp_ms)
		elif isinstance(timestamp_ms, str) and timestamp_ms.isdigit():
			shadow_ts = int(timestamp_ms)
	except Exception:
		shadow_ts = None
	return _InsertResult(
		message_id=int(row.id),  # type: ignore[arg-type]
		message_text=text_val,
		attachments=attachments,
		conversation_id=int(conversation_pk) if conversation_pk is not None else None,
		timestamp_ms=shadow_ts,
		direction=direction,
		ad_id=str(ad_id) if ad_id is not None else None,
		ad_link=ad_link,
		ad_title=ad_title,
		ad_name=ad_name,
		referral_json=referral_json_val if isinstance(referral_json_val, str) else json.dumps(referral_json_val)
		if referral_json_val is not None
		else None,
	)


def _derive_ad_title_for_linking(
	ad_title: Optional[str],
	ad_name: Optional[str],
	referral_json_val: Optional[str],
) -> Optional[str]:
	ad_title_final = ad_title
	if referral_json_val:
		try:
			ref_data = json.loads(referral_json_val) if isinstance(referral_json_val, str) else referral_json_val
			if isinstance(ref_data, dict):
				ads_ctx = ref_data.get("ads_context_data") or {}
				if isinstance(ads_ctx, dict):
					ctx_title = ads_ctx.get("ad_title")
					if ctx_title and ctx_title.strip() and ctx_title.strip().upper() not in ("ADS", "AD", "ADVERTISEMENT"):
						ad_title_final = ctx_title
				if not ad_title_final or ad_title_final.strip().upper() in ("ADS", "AD", "ADVERTISEMENT"):
					ad_title_final = ref_data.get("ad_title") or ref_data.get("headline") or ref_data.get("source") or ad_title_final
		except Exception:
			pass
	return ad_title_final or ad_name


def _auto_link_instagram_post(message_id: int, message_text: Optional[str], attachments: Any) -> None:
	"""
	Automatically detect Instagram post attachments and link them to products using AI,
	running any remote/API work outside of active DB transactions.
	"""
	try:
		if not attachments:
			return

		post_info = None
		if isinstance(attachments, list):
			for att in attachments:
				if att.get("type") in ("ig_post", "share"):
					payload = att.get("payload", {})
					media_id = payload.get("ig_post_media_id")
					if media_id:
						post_info = {
							"ig_post_media_id": str(media_id),
							"title": payload.get("title"),
							"url": payload.get("url"),
						}
						break

		if not post_info or not post_info.get("ig_post_media_id"):
			return

		post_id = str(post_info["ig_post_media_id"])

		from ..models import Product
		from sqlmodel import select

		with get_session() as session:
			existing = session.exec(
				_sql_text("SELECT ad_id FROM ads_products WHERE ad_id=:pid AND link_type='post'").bindparams(pid=post_id)
			).first()
			if existing:
				_log.debug("ingest: post %s already linked, skipping", post_id)
				return
			products = session.exec(select(Product).limit(500)).all()
			product_list = [{"id": p.id, "name": p.name, "slug": p.slug} for p in products]

		try:
			from .ai import AIClient
			from ..services.prompts import AD_PRODUCT_MATCH_SYSTEM_PROMPT

			ai = AIClient()
			if not ai or not getattr(ai, "enabled", False):
				_log.debug("ingest: AI not available for post linking")
				return
		except Exception as e:
			_log.debug("ingest: failed to initialize AI for post linking: %s", e)
			return

		post_text = f"{post_info.get('title', '')} {message_text or ''}".strip()

		body = {
			"ad_title": post_text,
			"known_products": [{"id": p["id"], "name": p["name"]} for p in product_list],
			"schema": {
				"product_id": "int|null",
				"product_name": "str|null",
				"confidence": "float",
				"notes": "str|null",
			},
		}

		user_prompt = (
			"Lütfen SADECE geçerli JSON döndür. Markdown/kod bloğu/yorum ekleme. "
			"Tüm alanlar çift tırnaklı olmalı.\nGirdi:\n" + json.dumps(body, ensure_ascii=False)
		)

		suggestion = ai.generate_json(system_prompt=AD_PRODUCT_MATCH_SYSTEM_PROMPT, user_prompt=user_prompt)

		product_id = suggestion.get("product_id") or suggestion.get("suggested_product_id")
		product_name = suggestion.get("product_name") or suggestion.get("suggested_product_name")
		confidence = suggestion.get("confidence", 0.0)
		confidence_float = float(confidence) if confidence is not None else 0.0

		if product_id is not None:
			try:
				product_id = int(product_id)
			except (ValueError, TypeError):
				product_id = None

		if not product_id and product_name:
			from ..utils.slugify import slugify

			slug = slugify(product_name)
			with get_session() as session:
				existing = session.exec(select(Product).where(Product.slug == slug)).first()
				if existing:
					product_id = existing.id
				else:
					new_product = Product(
						name=product_name,
						slug=slug,
						default_unit="adet",
						default_price=None,
					)
					session.add(new_product)
					session.flush()
					if new_product.id:
						product_id = new_product.id

		min_confidence = 0.7
		if not product_id or confidence_float < min_confidence:
			_log.debug(
				"ingest: post %s not auto-linked (product_id=%s, confidence=%.2f < %.2f)",
				post_id,
				product_id,
				confidence_float,
				min_confidence,
			)
			return

		with get_session() as session:
			# Store post in ads table with link_type='post'
			try:
				stmt_upsert = _sql_text("""
					INSERT INTO ads(ad_id, link_type, name, link, updated_at)
					VALUES (:pid, 'post', :title, :url, CURRENT_TIMESTAMP)
					ON DUPLICATE KEY UPDATE
						link_type='post',
						name=VALUES(name),
						link=VALUES(link),
						updated_at=CURRENT_TIMESTAMP
				""").bindparams(
					pid=str(post_id),
					title=post_info.get("title"),
					url=post_info.get("url"),
				)
				session.exec(stmt_upsert)
			except Exception:
				stmt_sel = _sql_text("SELECT ad_id FROM ads WHERE ad_id=:pid").bindparams(pid=str(post_id))
				existing_post = session.exec(stmt_sel).first()
				if existing_post:
					stmt_update = _sql_text("""
						UPDATE ads SET link_type='post', name=:title, link=:url, updated_at=CURRENT_TIMESTAMP
						WHERE ad_id=:pid
					""").bindparams(
						pid=str(post_id),
						title=post_info.get("title"),
						url=post_info.get("url"),
					)
					session.exec(stmt_update)
				else:
					stmt_insert = _sql_text("""
						INSERT INTO ads(ad_id, link_type, name, link)
						VALUES (:pid, 'post', :title, :url)
					""").bindparams(
						pid=str(post_id),
						title=post_info.get("title"),
						url=post_info.get("url"),
					)
					session.exec(stmt_insert)

			# Link post to product in ads_products with link_type='post'
			try:
				stmt_link = _sql_text("""
					INSERT INTO ads_products(ad_id, link_type, product_id, sku, auto_linked)
					VALUES (:pid, 'post', :prod_id, NULL, 1)
					ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), auto_linked=1, link_type='post'
				""").bindparams(
					pid=str(post_id),
					prod_id=int(product_id),
				)
				session.exec(stmt_link)
			except Exception:
				stmt_sel = _sql_text("SELECT ad_id FROM ads_products WHERE ad_id=:pid").bindparams(pid=str(post_id))
				existing_link = session.exec(stmt_sel).first()
				if existing_link:
					stmt_update = _sql_text("""
						UPDATE ads_products SET link_type='post', product_id=:prod_id, auto_linked=1 WHERE ad_id=:pid
					""").bindparams(pid=str(post_id), prod_id=int(product_id))
					session.exec(stmt_update)
				else:
					stmt_insert = _sql_text("""
						INSERT INTO ads_products(ad_id, link_type, product_id, sku, auto_linked)
						VALUES (:pid, 'post', :prod_id, NULL, 1)
					""").bindparams(pid=str(post_id), prod_id=int(product_id))
					session.exec(stmt_insert)

		_log.info(
			"ingest: auto-linked post %s to product %s (confidence: %.2f)",
			post_id,
			product_id,
			confidence_float,
		)
	except Exception as e:
		_log.warning("ingest: _auto_link_instagram_post outer error: %s", e)


def _auto_link_ad(ad_id: str, ad_title: Optional[str], ad_name: Optional[str]) -> None:
	"""
	Automatically link an ad to a product using AI without holding open DB transactions
	while waiting on remote APIs. Only saves if AI confidence is >= 0.7.
	"""
	try:
		if not ad_id:
			return

		ad_text = ad_title or ad_name
		if not ad_text:
			_log.debug("ingest: ad %s has no title/name, skipping auto-link", ad_id)
			return

		from ..models import Product
		from sqlmodel import select

		with get_session() as session:
			existing = session.exec(
				_sql_text("SELECT ad_id FROM ads_products WHERE ad_id=:id AND link_type='ad'").bindparams(id=str(ad_id))
			).first()
			if existing:
				_log.debug("ingest: ad %s already linked, skipping", ad_id)
				return
			products = session.exec(select(Product).limit(500)).all()
			product_list = [{"id": p.id, "name": p.name} for p in products]

		try:
			from .ai import AIClient
			from ..services.prompts import AD_PRODUCT_MATCH_SYSTEM_PROMPT

			ai = AIClient()
			if not ai or not getattr(ai, "enabled", False):
				_log.debug("ingest: AI not available for ad linking")
				return
		except Exception as e:
			_log.debug("ingest: failed to initialize AI for ad linking: %s", e)
			return

		body = {
			"ad_title": ad_text,
			"known_products": product_list,
			"schema": {
				"product_id": "int|null",
				"product_name": "str|null",
				"confidence": "float",
				"notes": "str|null",
			},
		}

		user_prompt = (
			"Lütfen SADECE geçerli JSON döndür. Markdown/kod bloğu/yorum ekleme. "
			"Tüm alanlar çift tırnaklı olmalı.\nGirdi:\n" + json.dumps(body, ensure_ascii=False)
		)

		result = ai.generate_json(system_prompt=AD_PRODUCT_MATCH_SYSTEM_PROMPT, user_prompt=user_prompt)

		product_id = result.get("product_id")
		product_name = result.get("product_name")
		confidence = result.get("confidence", 0.0)
		confidence_float = float(confidence) if confidence is not None else 0.0

		if product_id is not None:
			try:
				product_id = int(product_id)
			except (ValueError, TypeError):
				product_id = None

		if not product_id and product_name:
			from ..utils.slugify import slugify

			slug = slugify(product_name)
			with get_session() as session:
				existing = session.exec(select(Product).where(Product.slug == slug)).first()
				if existing:
					product_id = existing.id
				else:
					new_product = Product(
						name=product_name,
						slug=slug,
						default_unit="adet",
						default_price=None,
					)
					session.add(new_product)
					session.flush()
					if new_product.id:
						product_id = new_product.id

		min_confidence = 0.7
		if not product_id or confidence_float < min_confidence:
			_log.debug(
				"ingest: ad %s not auto-linked (product_id=%s, confidence=%.2f < %.2f)",
				ad_id,
				product_id,
				confidence_float,
				min_confidence,
			)
			return

		with get_session() as session:
			try:
				stmt_upsert_mysql = _sql_text(
					"INSERT INTO ads_products(ad_id, link_type, product_id, sku, auto_linked) VALUES(:id, 'ad', :pid, :sku, 1) "
					"ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), sku=VALUES(sku), auto_linked=1, link_type='ad'"
				).bindparams(
					id=str(ad_id),
					pid=int(product_id),
					sku=None,
				)
				session.exec(stmt_upsert_mysql)
			except Exception:
				try:
					stmt_upsert_sqlite = _sql_text(
						"INSERT OR REPLACE INTO ads_products(ad_id, link_type, product_id, sku, auto_linked) VALUES(:id, 'ad', :pid, :sku, 1)"
					).bindparams(
						id=str(ad_id),
						pid=int(product_id),
						sku=None,
					)
					session.exec(stmt_upsert_sqlite)
				except Exception:
					stmt_sel = _sql_text("SELECT ad_id FROM ads_products WHERE ad_id=:id").bindparams(id=str(ad_id))
					rowm = session.exec(stmt_sel).first()
					if rowm:
						stmt_update = _sql_text(
							"UPDATE ads_products SET link_type='ad', product_id=:pid, sku=:sku, auto_linked=1 WHERE ad_id=:id"
						).bindparams(
							id=str(ad_id),
							pid=int(product_id),
							sku=None,
						)
						session.exec(stmt_update)
					else:
						stmt_insert = _sql_text(
							"INSERT INTO ads_products(ad_id, link_type, product_id, sku, auto_linked) VALUES(:id, 'ad', :pid, :sku, 1)"
						).bindparams(
							id=str(ad_id),
							pid=int(product_id),
							sku=None,
						)
						session.exec(stmt_insert)

		_log.info(
			"ingest: auto-linked ad %s to product %s (confidence: %.2f)",
			ad_id,
			product_id,
			confidence_float,
		)
	except Exception as e:
		_log.warning("ingest: _auto_link_ad outer error: %s", e)


def _create_attachment_stubs(session, message_id: int, mid: str, attachments: Any) -> None:
	# Normalize list of attachments
	items: List[dict] = []
	if isinstance(attachments, list):
		items = attachments
	elif isinstance(attachments, dict) and isinstance(attachments.get("data"), list):
		items = attachments.get("data") or []
	for idx, att in enumerate(items):
		kind = None
		graph_id = None
		try:
			ptype = (att.get("type") or att.get("mime_type") or "").lower()
			if "image" in ptype:
				kind = "image"
			elif "video" in ptype:
				kind = "video"
			elif "audio" in ptype:
				kind = "audio"
			else:
				kind = "file"
		except Exception:
			kind = "file"
		try:
			graph_id = att.get("id") or (att.get("payload") or {}).get("id")
		except Exception:
			graph_id = None
		session.exec(
			text(
				"""
				INSERT INTO attachments(message_id, kind, graph_id, position, fetch_status)
				VALUES (:mid, :kind, :gid, :pos, 'pending')
				"""
			).params(mid=message_id, kind=kind, gid=graph_id, pos=idx)
		)
		# enqueue media fetch by logical key ATT:<message_id>:<pos>
		enqueue("fetch_media", key=f"{message_id}:{idx}", payload={"message_id": message_id, "position": idx})


def upsert_message_from_ig_event(session, event: Dict[str, Any] | str, igba_id: str) -> Optional[int]:
	"""Insert message if missing, return internal message_id.

	This mirrors webhook/ingestion logic and enqueues media fetch where needed.
	"""
	# Normalize: if Graph returned only an id string, fetch full message details
	if isinstance(event, str):
		try:
			import asyncio as _aio
			loop = _aio.get_event_loop()
			try:
				_log_up.info("upsert: received str id=%s; fetching details", str(event)[:120])
			except Exception:
				pass
			detail = loop.run_until_complete(fetch_message_details(str(event)))
			if isinstance(detail, dict):
				try:
					_log_up.info("upsert: fetched details keys=%s", list(detail.keys())[:8])
				except Exception:
					pass
				event = detail
			else:
				try:
					_log_up.warning("upsert: detail fetch returned non-dict for id=%s type=%s", str(event)[:120], type(detail).__name__)
				except Exception:
					pass
				return None
		except Exception:
			try:
				_log_up.warning("upsert: detail fetch failed for id=%s", str(event)[:120])
			except Exception:
				pass
			return None
	# Defensive: if event is still not a dict, bail
	if not isinstance(event, dict):
		try:
			_log_up.warning("upsert: event not dict after normalization type=%s", type(event).__name__)
		except Exception:
			pass
		return None
	# Normalize message field differences between webhook (dict) and Graph fetch (string)
	raw_message_field = event.get("message")
	message_obj: Dict[str, Any] = {}
	text_val: Optional[str] = None
	if isinstance(raw_message_field, dict):
		message_obj = raw_message_field
		text_val = raw_message_field.get("message") or raw_message_field.get("text")
	elif isinstance(raw_message_field, str):
		# Graph messages API returns the text as a plain string under "message"
		text_val = raw_message_field
		message_obj = {}
	else:
		message_obj = {}
		text_val = None
	# Determine message id from multiple possible locations
	mid = (message_obj.get("mid") if isinstance(message_obj, dict) else None) or (message_obj.get("id") if isinstance(message_obj, dict) else None) or event.get("id")
	if not mid:
		return None
	exists = session.exec(text("SELECT id FROM message WHERE ig_message_id = :mid").params(mid=str(mid))).first()
	if exists:
		row = exists
		message_id = int(row.id if hasattr(row, "id") else row[0])
		# Always update raw_json with the latest callback data (even if message already existed)
		# This ensures we capture all callback data including reply_to for investigation
		raw_json_data = json.dumps(event, ensure_ascii=False)
		try:
			session.exec(
				text("UPDATE message SET raw_json = :raw_json WHERE ig_message_id = :mid").params(
					raw_json=raw_json_data,
					mid=str(mid)
				)
			)
		except Exception:
			# Best-effort; don't fail if update fails
			pass
		return message_id
	sender_id = (event.get("from") or event.get("sender") or {}).get("id")
	# Best-effort capture of username when hydrating via Graph messages API
	sender_username = None
	try:
		un = (event.get("from") or {}).get("username")
		if isinstance(un, str) and un.strip():
			sender_username = un.strip()
	except Exception:
		sender_username = None
	recipient_id = None
	try:
		to = (event.get("to") or {}).get("data") or []
		recipient_id = to[0].get("id") if to else None
	except Exception:
		recipient_id = None
	# Attachments may be top-level (Graph fetch) or nested under message (webhook)
	attachments = event.get("attachments") or (message_obj.get("attachments") if isinstance(message_obj, dict) else None)
	attachments = _maybe_expand_attachments(str(mid), attachments)
	# Story reply (rare in Graph fetch; best-effort if present)
	story_id = None
	story_url = None
	try:
		st = ((message_obj.get("reply_to") or {}).get("story") or {})
		story_id = str(st.get("id") or "") or None
		story_url = st.get("url") or None
	except Exception:
		story_id = None
		story_url = None
	if not text_val:
		text_val = _derive_template_preview(attachments)
	ts_ms = None
	try:
		ts = event.get("created_time") or event.get("timestamp")
		if isinstance(ts, (int, float)):
			ts_ms = int(ts)
		elif isinstance(ts, str):
			# Normalize common Graph formats like "2025-11-13T14:25:43+0000" or ISO8601
			try:
				from datetime import datetime as _dt
				val = ts.replace("+0000", "+00:00")
				ts_ms = int(_dt.fromisoformat(val).timestamp() * 1000)
			except Exception:
				# Fallback: keep only digits to accommodate "1763034242207" style strings
				digits = "".join(ch for ch in ts if ch.isdigit())
				if digits:
					try:
						ts_ms = int(digits)
					except Exception:
						ts_ms = None
	except Exception:
		ts_ms = None
	direction = "in"
	owner = None
	try:
		token, entity_id, is_page = __import__(
			"app.services.instagram_api", fromlist=["_get_base_token_and_id"]
		).instagram_api._get_base_token_and_id()
		owner = entity_id
		# Check against configured owner ID
		if sender_id and str(sender_id) == str(owner):
			direction = "out"
		# Also check IG_USER_ID if it's different from owner_id (for business accounts)
		if direction == "in":
			try:
				import os
				ig_user_id = os.getenv("IG_USER_ID")
				if ig_user_id and sender_id and str(sender_id) == str(ig_user_id):
					direction = "out"
			except Exception:
				pass
		# Also check against igba_id if provided (webhook entry ID)
		if direction == "in" and igba_id:
			if sender_id and str(sender_id) == str(igba_id):
				direction = "out"
	except Exception:
		pass
	# Canonical conversation key is always (page_id, user_id), independent of direction.
	page_id = str(igba_id or owner or "")
	user_id: Optional[str] = None
	try:
		if sender_id and page_id and str(sender_id) != page_id:
			user_id = str(sender_id)
		elif recipient_id and page_id and str(recipient_id) != page_id:
			user_id = str(recipient_id)
	except Exception:
		user_id = None
	# Prefer Graph conversation id when hydrate provides it (for mapping), but internal
	# conversation_id is always our own integer PK.
	graph_cid = None
	try:
		graph_cid = event.get("__graph_conversation_id")
	except Exception:
		graph_cid = None
	
	# If not provided by hydrate, try to extract from message ID
	if not graph_cid and mid:
		graph_cid = _extract_graph_conversation_id_from_message_id(str(mid), page_id)
	
	# Resolve or create Conversation row using (page_id, user_id)
	conversation_pk = _get_or_create_conversation_id(session, page_id, user_id)
	# If Graph conversation id is known, persist mapping on the Conversation row
	if conversation_pk is not None and graph_cid:
		try:
			from sqlalchemy import text as _t

			session.exec(
				_t(
					"UPDATE conversations SET graph_conversation_id=:gc WHERE id=:cid AND (graph_conversation_id IS NULL OR graph_conversation_id=:gc)"
				).params(gc=str(graph_cid), cid=int(conversation_pk))
			)
		except Exception:
			pass
	# Ad/referral extraction (best-effort)
	ad_id = None
	ad_link = None
	ad_title = None
	ad_img = None
	ad_name = None
	referral_json_val = None
	try:
		ref = (event.get("referral") or message_obj.get("referral") or {})
		# Only process referral if it's a dict with actual data (not empty)
		if isinstance(ref, dict) and ref:
			try:
				_log_up.info(
					"upsert.graph: referral raw mid=%s type=%s keys=%s",
					str(mid),
					type(ref).__name__,
					list(ref.keys()),
				)
			except Exception:
				pass
			ad_id = str(ref.get("ad_id") or ref.get("ad_id_v2") or "") or None
			ad_link = ref.get("ad_link") or ref.get("url") or ref.get("link") or None
			ad_title = ref.get("headline") or ref.get("source") or ref.get("type") or None
			ad_img = ref.get("image_url") or ref.get("thumbnail_url") or ref.get("picture") or ref.get("media_url") or None
			ad_name = ref.get("name") or ref.get("title") or None
			referral_json_val = json.dumps(ref, ensure_ascii=False)
			try:
				_log_up.info(
					"upsert.graph: referral parsed mid=%s ad_id=%s ad_link=%s ad_title=%s",
					str(mid),
					str(ad_id),
					str(ad_link),
					str(ad_title),
				)
			except Exception:
				pass
	except Exception as e:
		try:
			_log_up.warning(
				"upsert.graph: referral parse error mid=%s err=%s",
				str(mid),
				str(e),
			)
		except Exception:
			pass
		ad_id = ad_link = ad_title = ad_img = ad_name = None
		referral_json_val = None
	# Debug logging: capture mapping decisions for hydrated/Graph-fetched messages
	try:
		_log_up.info(
			"upsert.graph: mid=%s from=%s to=%s igba_id=%s owner=%s user_id=%s conv_pk=%s graph_cid=%s direction=%s ad_id=%s",
			str(mid),
			str(sender_id),
			str(recipient_id),
			str(igba_id),
			str(owner),
			str(user_id),
			str(conversation_pk),
			str(graph_cid),
			str(direction),
			str(ad_id),
		)
	except Exception:
		# Never break ingestion because of debug logging
		pass

	row = Message(
		ig_sender_id=str(sender_id) if sender_id is not None else None,
		ig_recipient_id=str(recipient_id) if recipient_id is not None else None,
		ig_message_id=str(mid),
		text=text_val,
		attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments is not None else None,
		timestamp_ms=int(ts_ms) if ts_ms is not None else None,
		raw_json=json.dumps(event, ensure_ascii=False),
		conversation_id=int(conversation_pk) if conversation_pk is not None else None,
		direction=direction,
		sender_username=sender_username,
		story_id=story_id,
		story_url=story_url,
		ad_id=ad_id,
		ad_link=ad_link,
		ad_title=ad_title,
		ad_image_url=ad_img,
		ad_name=ad_name,
		referral_json=referral_json_val,
	)
	session.add(row)
	session.flush()
	if attachments:
		_create_attachment_stubs(session, int(row.id), str(mid), attachments)  # type: ignore[arg-type]
	# Upsert conversations last-* fields (summary) keyed by internal id
	_update_conversation_summary_from_message(
		session,
		conversation_pk,
		ts_ms,
		message_row=row,
		text_val=text_val,
		direction=direction,
		sender_id=str(sender_id) if sender_id is not None else None,
		recipient_id=str(recipient_id) if recipient_id is not None else None,
		ad_id=str(ad_id) if ad_id is not None else None,
		ad_link=ad_link,
		ad_title=ad_title,
	)
	# story cache
	try:
		if story_id:
			from sqlalchemy import text as _t
			session.exec(_t("INSERT IGNORE INTO stories(story_id, url, updated_at) VALUES (:id,:url,CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
			session.exec(_t("UPDATE stories SET url=COALESCE(:url,url), updated_at=CURRENT_TIMESTAMP WHERE story_id=:id")).params(id=str(story_id), url=(str(story_url) if story_url else None))
	except Exception:
		pass
	return int(row.id)


def handle(raw_event_id: int) -> int:
	"""Ingest one raw_event id. Return number of messages inserted.

	This implementation keeps database transactions as small as possible:
	- One short read to fetch the raw_events payload.
	- Then, for each entry/message, a separate short-lived session/transaction
	  is used for DB writes. Graph API calls are done outside those
	  transactions via _ensure_ig_user_with_data, which manages its own
	  persistence if needed.

	This reduces lock hold times in MySQL and makes lock wait timeouts less likely.
	"""
	inserted = 0

	# Step 1: load raw_events payload in its own short transaction
	with get_session() as session:
		row = session.exec(
			text("SELECT id, payload FROM raw_events WHERE id = :id").params(id=raw_event_id)
		).first()
		if not row:
			return 0
		payload_text = row.payload if hasattr(row, "payload") else row[1]

	try:
		payload: Dict[str, Any] = json.loads(payload_text)
	except Exception:
		return 0

	entries: List[Dict[str, Any]] = payload.get("entry", [])

	# Step 2: process each entry with small, per-entry/per-message transactions
	for entry in entries:
		igba_id = str(entry.get("id")) if entry.get("id") is not None else ""
		if not igba_id:
			continue

		# Collect messaging events possibly nested
		messaging_events: List[Dict[str, Any]] = entry.get("messaging") or []
		if not messaging_events and entry.get("changes"):
			for change in entry.get("changes", []):
				val = change.get("value") or {}
				if isinstance(val, dict) and val.get("messaging"):
					messaging_events.extend(val.get("messaging", []))

		for event in messaging_events:
			message_obj = event.get("message") or {}
			# For Instagram, we WANT to store echo messages (our own replies) so they appear
			# in the conversation. We only skip when there is no message object at all
			# or when the message is explicitly deleted.
			if not message_obj or message_obj.get("is_deleted"):
				continue

			sender_id = (event.get("sender") or {}).get("id")
			recipient_id = (event.get("recipient") or {}).get("id")

			# Determine which user to enrich (the one that's NOT the page) - max 1 Graph API request.
			# This function manages its own DB usage; we avoid holding our own long transaction here.
			user_to_enrich = None
			if sender_id and str(sender_id) != str(igba_id):
				user_to_enrich = str(sender_id)
			elif recipient_id and str(recipient_id) != str(igba_id):
				user_to_enrich = str(recipient_id)

			if user_to_enrich:
				# Enrich IG user info without keeping the current transaction open
				_ensure_ig_user_with_data(user_to_enrich, str(igba_id))

			mid = message_obj.get("mid") or message_obj.get("id")

			insert_result: Optional[_InsertResult] = None
			# Insert message + attachments in a short-lived transaction
			with get_session() as session_msg:
				# Ensure page/account row exists (MySQL dialect)
				try:
					_ensure_ig_account(session_msg.get_bind(), igba_id)  # type: ignore[arg-type]
				except Exception:
					# Best-effort; do not fail ingestion because of ig_accounts
					pass

				insert_result = _insert_message(session_msg, event, igba_id)
				if insert_result:
					inserted += 1
					attachments = insert_result.attachments
					if attachments:
						_create_attachment_stubs(session_msg, insert_result.message_id, str(mid), attachments)

					# Hydration is now manual-only (via UI/admin actions) to minimize Graph API requests.
					# We only enrich user info (1 request) and let hydration happen on-demand.

					# Enqueue enrich_page (idempotent via jobs table uniqueness)
					try:
						enqueue("enrich_page", key=str(igba_id), payload={"igba_id": str(igba_id)})
					except Exception:
						pass

			# Run slow/remote follow-ups outside the DB transaction
			if insert_result:
				try:
					if (insert_result.direction or "in") == "in" and insert_result.conversation_id:
						from .ai_shadow import touch_shadow_state

						touch_shadow_state(
							int(insert_result.conversation_id),
							int(insert_result.timestamp_ms) if insert_result.timestamp_ms is not None else None,
						)
				except Exception:
					pass
				try:
					if insert_result.attachments:
						_auto_link_instagram_post(insert_result.message_id, insert_result.message_text, insert_result.attachments)
				except Exception as e:
					try:
						_log.warning("ingest: auto-link post deferred error mid=%s msg=%s", str(mid), str(e))
					except Exception:
						pass

				try:
					if insert_result.ad_id:
						ad_title_final = _derive_ad_title_for_linking(
							insert_result.ad_title,
							insert_result.ad_name,
							insert_result.referral_json,
						)
						if ad_title_final or insert_result.ad_name:
							_auto_link_ad(insert_result.ad_id, ad_title_final, insert_result.ad_name)
				except Exception as e:
					try:
						_log.warning("ingest: auto-link ad deferred error mid=%s ad_id=%s err=%s", str(mid), str(insert_result.ad_id), str(e))
					except Exception:
						pass

		# Additionally handle referral-only webhook events (messaging_referrals)
		try:
			ref_events = entry.get("messaging_referrals") or []
			for rev in ref_events:
				try:
					sender_id = (rev.get("sender") or {}).get("id")
					recipient_id = (rev.get("recipient") or {}).get("id")
					ref = rev.get("referral") or {}
					ad_id = str(ref.get("ad_id") or ref.get("ad_id_v2") or "") or None
					ad_link = ref.get("ad_link") or ref.get("url") or ref.get("referer_uri") or None
					ad_title = ref.get("headline") or ref.get("source") or ref.get("type") or None
					referral_json_val = None
					try:
						referral_json_val = json.dumps(ref, ensure_ascii=False)
					except Exception:
						referral_json_val = None
					# Determine other party id (user id) and resolve conversation
					other_party_id = (
						sender_id if sender_id and str(sender_id) != str(igba_id) else recipient_id
					)
					if other_party_id:
						with get_session() as session_ref:
							conv_pk = _get_or_create_conversation_id(
								session_ref,
								str(igba_id) if igba_id is not None else "",
								str(other_party_id),
							)
							if conv_pk:
								# Update the latest message in this conversation with ad metadata if missing
								rowm = session_ref.exec(
									text(
										"SELECT id FROM message WHERE conversation_id=:cid ORDER BY timestamp_ms DESC, id DESC LIMIT 1"
									)
								).params(cid=int(conv_pk)).first()
								if rowm:
									mid_last = (
										rowm.id
										if hasattr(rowm, "id")
										else (rowm[0] if isinstance(rowm, (list, tuple)) else None)
									)
									if mid_last:
										session_ref.exec(
											text(
												"UPDATE message SET ad_id=COALESCE(ad_id, :adid), ad_link=COALESCE(ad_link, :link), ad_title=COALESCE(ad_title, :title), referral_json=COALESCE(referral_json, :ref) WHERE id=:id"
											)
										).params(
											id=int(mid_last),
											adid=ad_id,
											link=ad_link,
											title=ad_title,
											ref=referral_json_val,
										)
				except Exception:
					pass
		except Exception:
			pass

	return inserted


