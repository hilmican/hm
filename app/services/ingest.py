import json
import datetime as dt
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

# MySQL-only backend


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


def _ensure_ig_user_with_data(session, ig_user_id: str, igba_id: str | None = None) -> None:
	"""Ensure IG user exists with data (username, name, etc.).

	This only enriches user info via Graph API. Thread hydration is handled separately
	when we have graph_conversation_id to avoid unnecessary API calls.
	"""
	# Check if user exists and has data
	row = session.exec(
		text("SELECT ig_user_id, username, fetch_status FROM ig_users WHERE ig_user_id=:id LIMIT 1").params(
			id=str(ig_user_id)
		)
	).first()
	if row:
		username = getattr(row, "username", None) or (row[1] if len(row) > 1 else None)
		fetch_status = getattr(row, "fetch_status", None) or (row[2] if len(row) > 2 else None)
		# If we have username and successful fetch, consider it complete
		if username and str(fetch_status or "").lower() == "ok":
			return

	# User doesn't exist or is incomplete - fetch synchronously
	try:
		import asyncio

		loop = asyncio.get_event_loop()
		# Use the async enrich_user function (idempotent: checks fetch_status='ok' internally)
		from .enrichers import enrich_user

		result = loop.run_until_complete(enrich_user(ig_user_id))
		if result:
			_log.info("ingest: user %s enriched synchronously", ig_user_id)
		else:
			_log.debug("ingest: user %s already had data or fetch failed", ig_user_id)
	except Exception as e:
		_log.warning("ingest: failed to enrich user %s synchronously: %s", ig_user_id, e)
		# Fallback: ensure at least the row exists
		try:
			# MySQL-safe fallback: INSERT IGNORE to avoid duplicate errors
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
				  -- Only advance ad metadata when the new message actually carries ad info
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


def _insert_message(session, event: Dict[str, Any], igba_id: str) -> Optional[int]:
	message_obj = event.get("message") or {}
	if not message_obj:
		return None
	mid = message_obj.get("mid") or message_obj.get("id")
	if not mid:
		return None
	# idempotency by ig_message_id
	exists = session.exec(text("SELECT id FROM message WHERE ig_message_id = :mid").params(mid=str(mid))).first()
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
	attachments = message_obj.get("attachments")
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
	# Ad/referral extraction (best-effort)
	ad_id = None
	ad_link = None
	ad_title = None
	ad_img = None
	ad_name = None
	referral_json_val = None
	try:
		ref = (event.get("referral") or message_obj.get("referral") or {})
		try:
			_log_up.info(
				"insert.webhook: referral raw mid=%s type=%s keys=%s",
				str(mid),
				type(ref).__name__,
				list(ref.keys()) if isinstance(ref, dict) else None,
			)
		except Exception:
			pass
		if isinstance(ref, dict):
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

	row = Message(
		ig_sender_id=str(sender_id) if sender_id is not None else None,
		ig_recipient_id=str(recipient_id) if recipient_id is not None else None,
		ig_message_id=str(mid),
		text=text_val,
		attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments is not None else None,
		timestamp_ms=int(timestamp_ms) if isinstance(timestamp_ms, (int, float, str)) and str(timestamp_ms).isdigit() else None,
		raw_json=json.dumps(event, ensure_ascii=False),
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
	session.add(row)
	# Flush to get DB id for attachments
	session.flush()
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
					"INSERT IGNORE INTO ads(ad_id, name, image_url, link, updated_at) "
					"VALUES (:id, :n, :img, :lnk, CURRENT_TIMESTAMP)"
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
	# upsert stories cache
	try:
		if story_id:
			session.exec(_sql_text("INSERT IGNORE INTO stories(story_id, url, updated_at) VALUES (:id, :url, CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
			session.exec(_sql_text("UPDATE stories SET url=COALESCE(:url,url), updated_at=CURRENT_TIMESTAMP WHERE story_id=:id")).params(id=str(story_id), url=(str(story_url) if story_url else None))
	except Exception:
		pass
	# Touch AI shadow state on inbound messages to start debounce timer
	try:
		if (direction or "in") == "in" and conversation_pk:
			from .ai_shadow import touch_shadow_state

			ts_val = int(timestamp_ms) if isinstance(timestamp_ms, (int, float)) else (
				int(str(timestamp_ms)) if isinstance(timestamp_ms, str) and str(timestamp_ms).isdigit() else None
			)
			# Use canonical internal conversation id (int)
			touch_shadow_state(int(conversation_pk), ts_val)
	except Exception:
		pass
	
	# Auto-link Instagram posts to products
	try:
		if attachments:
			_auto_link_instagram_post(session, row, attachments)
	except Exception as e:
		# Never break message ingestion if post linking fails
		try:
			_log.warning("ingest: auto-link post failed for mid=%s: %s", str(mid), str(e))
		except Exception:
			pass
	
	# Auto-link ads to products
	try:
		if ad_id:
			# Try to get ad_title from referral_json, preferring ads_context_data.ad_title
			# This is more reliable than the top-level ad_title which may be generic like "ADS"
			ad_title_final = ad_title
			if referral_json_val:
				try:
					ref_data = json.loads(referral_json_val) if isinstance(referral_json_val, str) else referral_json_val
					if isinstance(ref_data, dict):
						# Check ads_context_data.ad_title first (from Instagram webhook format)
						# This contains the actual post/product title, not just "ADS"
						ads_ctx = ref_data.get("ads_context_data") or {}
						if isinstance(ads_ctx, dict):
							ctx_title = ads_ctx.get("ad_title")
							# Prefer ads_context_data.ad_title if it exists and is more meaningful
							if ctx_title and ctx_title.strip() and ctx_title.strip().upper() not in ("ADS", "AD", "ADVERTISEMENT"):
								ad_title_final = ctx_title
						# Also check direct fields if we don't have a good title yet
						if not ad_title_final or ad_title_final.strip().upper() in ("ADS", "AD", "ADVERTISEMENT"):
							ad_title_final = ref_data.get("ad_title") or ref_data.get("headline") or ref_data.get("source") or ad_title_final
				except Exception:
					pass
			# Use ad_title_final or fallback to ad_name
			if ad_title_final or ad_name:
				_auto_link_ad(session, ad_id, ad_title_final, ad_name)
	except Exception as e:
		# Never break message ingestion if ad linking fails
		try:
			_log.warning("ingest: auto-link ad failed for mid=%s ad_id=%s: %s", str(mid), str(ad_id), str(e))
		except Exception:
			pass
	
	return row.id  # type: ignore


def _auto_link_instagram_post(session, msg: Message, attachments: Any) -> None:
	"""
	Automatically detect Instagram post attachments and link them to products using AI.
	This runs asynchronously and doesn't block message ingestion if it fails.
	"""
	try:
		# Extract post info from attachments
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
		
		post_id = post_info["ig_post_media_id"]
		
		# Check if already linked
		try:
			existing = session.exec(
				_sql_text("SELECT post_id FROM posts_products WHERE post_id=:pid").bindparams(pid=post_id)
			).first()
			if existing:
				_log.debug("ingest: post %s already linked, skipping", post_id)
				return
		except Exception:
			pass
		
		# Get AI client (non-blocking if not available)
		try:
			from .ai import AIClient
			from ..models import Product
			from sqlmodel import select
			from ..utils.slugify import slugify
			
			ai = AIClient()
			if not ai or not getattr(ai, "enabled", False):
				_log.debug("ingest: AI not available for post linking")
				return
		except Exception as e:
			_log.debug("ingest: failed to initialize AI for post linking: %s", e)
			return
		
		# Get products for context
		try:
			products = session.exec(select(Product).limit(500)).all()
			product_list = [{"id": p.id, "name": p.name, "slug": p.slug} for p in products]
		except Exception as e:
			_log.warning("ingest: failed to load products for post linking: %s", e)
			return
		
		# AI suggestion using unified prompt system
		try:
			from ..services.prompts import AD_PRODUCT_MATCH_SYSTEM_PROMPT
			
			system_prompt = AD_PRODUCT_MATCH_SYSTEM_PROMPT
			
			# Build prompt with post title and message text
			post_text = f"{post_info.get('title', '')} {msg.text or ''}".strip()
			
			body = {
				"ad_title": post_text,  # Reuse the same structure
				"known_products": [{"id": p.id, "name": p.name} for p in products],
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
			
			suggestion = ai.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
			
			# Map response to expected format (using same schema as ads)
			product_id = suggestion.get("product_id") or suggestion.get("suggested_product_id")
			product_name = suggestion.get("product_name") or suggestion.get("suggested_product_name")
			confidence = suggestion.get("confidence", 0.0)
			confidence_float = float(confidence) if confidence is not None else 0.0
			
			# Create product if needed
			if not product_id and product_name:
				slug = slugify(product_name)
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
			
			# Only auto-link if confidence is high enough (>= 0.7)
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
			
			# Create post record
			try:
				stmt_upsert = _sql_text("""
					INSERT INTO posts(post_id, ig_post_media_id, title, url, message_id, updated_at)
					VALUES (:pid, :media_id, :title, :url, :msg_id, CURRENT_TIMESTAMP)
					ON DUPLICATE KEY UPDATE
						title=VALUES(title),
						url=VALUES(url),
						message_id=VALUES(message_id),
						updated_at=CURRENT_TIMESTAMP
				""").bindparams(
					pid=str(post_id),
					media_id=str(post_info["ig_post_media_id"]),
					title=post_info.get("title"),
					url=post_info.get("url"),
					msg_id=int(msg.id or 0),
				)
				session.exec(stmt_upsert)
			except Exception:
				# SQLite fallback
				stmt_sel = _sql_text("SELECT post_id FROM posts WHERE post_id=:pid").bindparams(pid=str(post_id))
				existing_post = session.exec(stmt_sel).first()
				if existing_post:
					stmt_update = _sql_text("""
						UPDATE posts SET title=:title, url=:url, message_id=:msg_id, updated_at=CURRENT_TIMESTAMP
						WHERE post_id=:pid
					""").bindparams(
						pid=str(post_id),
						title=post_info.get("title"),
						url=post_info.get("url"),
						msg_id=int(msg.id or 0),
					)
					session.exec(stmt_update)
				else:
					stmt_insert = _sql_text("""
						INSERT INTO posts(post_id, ig_post_media_id, title, url, message_id)
						VALUES (:pid, :media_id, :title, :url, :msg_id)
					""").bindparams(
						pid=str(post_id),
						media_id=str(post_info["ig_post_media_id"]),
						title=post_info.get("title"),
						url=post_info.get("url"),
						msg_id=int(msg.id or 0),
					)
					session.exec(stmt_insert)
			
			# Link to product (mark as auto_linked)
			try:
				stmt_link = _sql_text("""
					INSERT INTO posts_products(post_id, product_id, sku, auto_linked)
					VALUES (:pid, :prod_id, NULL, 1)
					ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), auto_linked=1
				""").bindparams(
					pid=str(post_id),
					prod_id=int(product_id),
				)
				session.exec(stmt_link)
			except Exception:
				# SQLite fallback
				stmt_sel = _sql_text("SELECT post_id FROM posts_products WHERE post_id=:pid").bindparams(pid=str(post_id))
				existing_link = session.exec(stmt_sel).first()
				if existing_link:
					stmt_update = _sql_text("""
						UPDATE posts_products SET product_id=:prod_id, auto_linked=1 WHERE post_id=:pid
					""").bindparams(pid=str(post_id), prod_id=int(product_id))
					session.exec(stmt_update)
				else:
					stmt_insert = _sql_text("""
						INSERT INTO posts_products(post_id, product_id, sku, auto_linked)
						VALUES (:pid, :prod_id, NULL, 1)
					""").bindparams(pid=str(post_id), prod_id=int(product_id))
					session.exec(stmt_insert)
			
			_log.info(
				"ingest: auto-linked post %s to product %s (confidence: %.2f)",
				post_id,
				product_id,
				confidence_float,
			)
			
		except Exception as e:
			_log.warning("ingest: failed to auto-link post %s: %s", post_id, e)
			# Don't raise - we don't want to break message ingestion
			pass
	except Exception as e:
		# Outer exception handler for the entire function
		_log.warning("ingest: _auto_link_instagram_post outer error: %s", e)
		pass


def _auto_link_ad(session, ad_id: str, ad_title: Optional[str], ad_name: Optional[str]) -> None:
	"""
	Automatically link an ad to a product using AI when a message with ad data is ingested.
	This runs synchronously during message ingestion but doesn't block if it fails.
	Only saves if AI confidence is >= 0.7.
	"""
	try:
		if not ad_id:
			return
		
		# Use ad_title from message, fallback to ad_name
		ad_text = ad_title or ad_name
		if not ad_text:
			_log.debug("ingest: ad %s has no title/name, skipping auto-link", ad_id)
			return
		
		# Check if already linked
		try:
			existing = session.exec(
				_sql_text("SELECT ad_id FROM ads_products WHERE ad_id=:id").bindparams(id=str(ad_id))
			).first()
			if existing:
				_log.debug("ingest: ad %s already linked, skipping", ad_id)
				return
		except Exception:
			pass
		
		# Get AI client (non-blocking if not available)
		try:
			from .ai import AIClient
			from ..models import Product
			from sqlmodel import select
			from ..services.prompts import AD_PRODUCT_MATCH_SYSTEM_PROMPT
			
			ai = AIClient()
			if not ai or not getattr(ai, "enabled", False):
				_log.debug("ingest: AI not available for ad linking")
				return
		except Exception as e:
			_log.debug("ingest: failed to initialize AI for ad linking: %s", e)
			return
		
		# Get products for context
		try:
			products = session.exec(select(Product).limit(500)).all()
			product_list = [{"id": p.id, "name": p.name} for p in products]
		except Exception as e:
			_log.warning("ingest: failed to load products for ad linking: %s", e)
			return
		
		# AI suggestion using unified prompt system
		try:
			system_prompt = AD_PRODUCT_MATCH_SYSTEM_PROMPT
			
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
			
			result = ai.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
			
			# Extract product_id and confidence
			product_id = result.get("product_id")
			if product_id is not None:
				try:
					product_id = int(product_id)
				except (ValueError, TypeError):
					product_id = None
			
			product_name = result.get("product_name")
			confidence = result.get("confidence", 0.0)
			confidence_float = float(confidence) if confidence is not None else 0.0
			
			# Only auto-link if confidence is high enough (>= 0.7)
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
			
			# Save the mapping (mark as auto_linked)
			try:
				stmt_upsert_sqlite = _sql_text(
					"INSERT OR REPLACE INTO ads_products(ad_id, product_id, sku, auto_linked) VALUES(:id, :pid, :sku, 1)"
				).bindparams(
					id=str(ad_id),
					pid=product_id,
					sku=None,
				)
				session.exec(stmt_upsert_sqlite)
			except Exception:
				# Fallback for MySQL
				try:
					stmt_upsert_mysql = _sql_text(
						"INSERT INTO ads_products(ad_id, product_id, sku, auto_linked) VALUES(:id, :pid, :sku, 1) "
						"ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), sku=VALUES(sku), auto_linked=1"
					).bindparams(
						id=str(ad_id),
						pid=product_id,
						sku=None,
					)
					session.exec(stmt_upsert_mysql)
				except Exception:
					# Last resort: try separate update/insert
					stmt_sel = _sql_text("SELECT ad_id FROM ads_products WHERE ad_id=:id").bindparams(id=str(ad_id))
					rowm = session.exec(stmt_sel).first()
					if rowm:
						stmt_update = _sql_text(
							"UPDATE ads_products SET product_id=:pid, sku=:sku, auto_linked=1 WHERE ad_id=:id"
						).bindparams(
							id=str(ad_id),
							pid=product_id,
							sku=None,
						)
						session.exec(stmt_update)
					else:
						stmt_insert = _sql_text(
							"INSERT INTO ads_products(ad_id, product_id, sku, auto_linked) VALUES(:id, :pid, :sku, 1)"
						).bindparams(
							id=str(ad_id),
							pid=product_id,
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
			_log.warning("ingest: failed to auto-link ad %s: %s", ad_id, e)
			# Don't raise - we don't want to break message ingestion
			pass
	except Exception as e:
		# Outer exception handler for the entire function
		_log.warning("ingest: _auto_link_ad outer error: %s", e)
		pass


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
		return int(row.id if hasattr(row, "id") else row[0])
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
		try:
			_log_up.info(
				"upsert.graph: referral raw mid=%s type=%s keys=%s",
				str(mid),
				type(ref).__name__,
				list(ref.keys()) if isinstance(ref, dict) else None,
			)
		except Exception:
			pass
		if isinstance(ref, dict):
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
				# Enrich IG user info in a short-lived session to avoid long-held transactions
				from .ingest import _ensure_ig_user_with_data as _ensure_user  # type: ignore
				with get_session() as session_user:
					_ensure_user(session_user, user_to_enrich, str(igba_id))

			mid = message_obj.get("mid") or message_obj.get("id")

			# Insert message + attachments in a short-lived transaction
			with get_session() as session_msg:
				# Ensure page/account row exists (MySQL dialect)
				try:
					_ensure_ig_account(session_msg.get_bind(), igba_id)  # type: ignore[arg-type]
				except Exception:
					# Best-effort; do not fail ingestion because of ig_accounts
					pass

				msg_id = _insert_message(session_msg, event, igba_id)
				if msg_id:
					inserted += 1
					attachments = message_obj.get("attachments")
					if attachments:
						_create_attachment_stubs(session_msg, msg_id, str(mid), attachments)

					# Hydration is now manual-only (via UI/admin actions) to minimize Graph API requests.
					# We only enrich user info (1 request) and let hydration happen on-demand.

					# Enqueue enrich_page (idempotent via jobs table uniqueness)
					try:
						enqueue("enrich_page", key=str(igba_id), payload={"igba_id": str(igba_id)})
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


