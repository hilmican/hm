import json
import datetime as dt
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ..db import get_session
from ..models import Message
from .instagram_api import fetch_message_details  # type: ignore
from .queue import enqueue
from sqlalchemy import text as _sql_text


def _ensure_ig_account(conn, igba_id: str) -> None:
	conn.exec_driver_sql(
		"""
		INSERT OR IGNORE INTO ig_accounts(igba_id, updated_at)
		VALUES (?, CURRENT_TIMESTAMP)
		""",
		(igba_id,),
	)


def _ensure_ig_user(conn, ig_user_id: str) -> None:
	conn.exec_driver_sql(
		"""
		INSERT OR IGNORE INTO ig_users(ig_user_id)
		VALUES (?)
		""",
		(ig_user_id,),
	)


def _upsert_conversation(conn, igba_id: str, ig_user_id: str, ts_ms: Optional[int]) -> str:
	convo_id = f"{igba_id}:{ig_user_id}"
	conn.exec_driver_sql(
		"""
		INSERT OR IGNORE INTO conversations(convo_id, igba_id, ig_user_id, last_message_at, unread_count)
		VALUES (?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), 0)
		""",
		(convo_id, igba_id, ig_user_id, dt.datetime.utcfromtimestamp((ts_ms or 0) / 1000) if ts_ms else None),
	)
	# update last_message_at if newer
	if ts_ms:
		conn.exec_driver_sql(
			"""
			UPDATE conversations
			SET last_message_at = MAX(last_message_at, ?)
			WHERE convo_id = ?
			""",
			(dt.datetime.utcfromtimestamp(ts_ms / 1000), convo_id),
		)
	return convo_id


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
	# derive direction: compare with owner/page id embedded in webhook entry id (igba_id)
	direction = "in"
	try:
		if sender_id and str(sender_id) == str(igba_id):
			direction = "out"
	except Exception:
		pass
	other_party_id = recipient_id if direction == "out" else sender_id
	conversation_id = f"dm:{other_party_id}" if other_party_id is not None else None
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
		if isinstance(ref, dict):
			ad_id = str(ref.get("ad_id") or ref.get("ad_id_v2") or "") or None
			ad_link = ref.get("ad_link") or ref.get("url") or ref.get("link") or None
			ad_title = ref.get("headline") or ref.get("source") or ref.get("type") or None
			ad_img = ref.get("image_url") or ref.get("thumbnail_url") or ref.get("picture") or ref.get("media_url") or None
			ad_name = ref.get("name") or ref.get("title") or None
			referral_json_val = json.dumps(ref, ensure_ascii=False)
		# Also parse Ads Library id from ad_link query param when present
		if not ad_id and ad_link and "facebook.com/ads/library" in str(ad_link):
			try:
				from urllib.parse import urlparse, parse_qs
				q = parse_qs(urlparse(str(ad_link)).query)
				aid = (q.get("id") or [None])[0]
				ad_id = str(aid) if aid else None
			except Exception:
				pass
	except Exception:
		ad_id = ad_link = ad_title = ad_img = ad_name = None
		referral_json_val = None
	row = Message(
		ig_sender_id=str(sender_id) if sender_id is not None else None,
		ig_recipient_id=str(recipient_id) if recipient_id is not None else None,
		ig_message_id=str(mid),
		text=text_val,
		attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments is not None else None,
		timestamp_ms=int(timestamp_ms) if isinstance(timestamp_ms, (int, float, str)) and str(timestamp_ms).isdigit() else None,
		raw_json=json.dumps(event, ensure_ascii=False),
		conversation_id=conversation_id,
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
	# Upsert ai_conversations last-* fields
	try:
		from sqlalchemy import text as _text
		# Robust timestamp normalization (mirror webhook path)
		ts_val = None
		try:
			if isinstance(timestamp_ms, (int, float)):
				ts_val = int(timestamp_ms)
			elif isinstance(timestamp_ms, str):
				digits = "".join(ch for ch in timestamp_ms if ch.isdigit())
				if digits:
					ts_val = int(digits)
		except Exception:
			ts_val = None
		if ts_val is None:
			try:
				ts_val = int(row.timestamp_ms) if row.timestamp_ms is not None else None
			except Exception:
				ts_val = None
		if ts_val is None:
			try:
				import time as _t
				ts_val = int(_t.time() * 1000)
			except Exception:
				ts_val = None
		if conversation_id and ts_val is not None:
			# ensure placeholder exists
			try:
				session.exec(_sql_text("INSERT OR IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=str(conversation_id))
			except Exception:
				try:
					session.exec(_sql_text("INSERT IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=str(conversation_id))
				except Exception:
					pass
			# SQLite upsert
			try:
				session.exec(
					_sql_text(
						"""
						INSERT INTO ai_conversations(convo_id, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title)
						VALUES (:cid, :mid, :ts, :txt, :dir, NULL, :sid, :rid, :adid, :alink, :atitle)
						ON CONFLICT(convo_id) DO UPDATE SET
						  last_message_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_id ELSE ai_conversations.last_message_id END,
						  last_message_timestamp_ms=GREATEST(COALESCE(ai_conversations.last_message_timestamp_ms,0), excluded.last_message_timestamp_ms),
						  last_message_text=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_text ELSE ai_conversations.last_message_text END,
						  last_message_direction=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_direction ELSE ai_conversations.last_message_direction END,
						  ig_sender_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.ig_sender_id ELSE ai_conversations.ig_sender_id END,
						  ig_recipient_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.ig_recipient_id ELSE ai_conversations.ig_recipient_id END,
						  last_ad_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_id ELSE ai_conversations.last_ad_id END,
						  last_ad_link=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_link ELSE ai_conversations.last_ad_link END,
						  last_ad_title=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_title ELSE ai_conversations.last_ad_title END
						"""
					).params(
						cid=str(conversation_id),
						mid=int(row.id),
						ts=int(ts_val),
						txt=(text_val or ""),
						dir=(direction or "in"),
						sid=(str(sender_id) if sender_id is not None else None),
						rid=(str(recipient_id) if recipient_id is not None else None),
						adid=(str(ad_id) if ad_id is not None else None),
						alink=ad_link,
						atitle=ad_title,
					)
				)
			except Exception:
				# MySQL upsert
				try:
					session.exec(
						_sql_text(
							"""
							INSERT INTO ai_conversations(convo_id, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title)
							VALUES (:cid, :mid, :ts, :txt, :dir, NULL, :sid, :rid, :adid, :alink, :atitle)
							ON DUPLICATE KEY UPDATE
							  last_message_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_id), ai_conversations.last_message_id),
							  last_message_timestamp_ms=GREATEST(COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_timestamp_ms)),
							  last_message_text=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_text), ai_conversations.last_message_text),
							  last_message_direction=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_direction), ai_conversations.last_message_direction),
							  ig_sender_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(ig_sender_id), ai_conversations.ig_sender_id),
							  ig_recipient_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(ig_recipient_id), ai_conversations.ig_recipient_id),
							  last_ad_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_id), ai_conversations.last_ad_id),
							  last_ad_link=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_link), ai_conversations.last_ad_link),
							  last_ad_title=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_title), ai_conversations.last_ad_title)
							"""
						).params(
							cid=str(conversation_id),
							mid=int(row.id),
							ts=int(ts_val),
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
					_lg.getLogger("ingest.upsert").warning("ingest upsert ai_conversations failed cid=%s mid=%s ts=%s err=%s", str(conversation_id), str(row.id), str(ts_val), str(e)[:200])
	except Exception as e:
		import logging as _lg
		_lg.getLogger("ingest.upsert").warning("ingest upsert outer failed cid=%s mid=%s err=%s", str(conversation_id), str(getattr(row, 'id', None)), str(e)[:200])
	# upsert ads cache
	try:
		if ad_id:
			try:
				session.exec(_sql_text("INSERT OR IGNORE INTO ads(ad_id, name, image_url, link, updated_at) VALUES (:id, :n, :img, :lnk, CURRENT_TIMESTAMP)")).params(id=ad_id, n=ad_name, img=ad_img, lnk=ad_link)
			except Exception:
				session.exec(_sql_text("INSERT IGNORE INTO ads(ad_id, name, image_url, link, updated_at) VALUES (:id, :n, :img, :lnk, CURRENT_TIMESTAMP)")).params(id=ad_id, n=ad_name, img=ad_img, lnk=ad_link)
			session.exec(_sql_text("UPDATE ads SET name=COALESCE(:n,name), image_url=COALESCE(:img,image_url), link=COALESCE(:lnk,link), updated_at=CURRENT_TIMESTAMP WHERE ad_id=:id")).params(id=ad_id, n=ad_name, img=ad_img, lnk=ad_link)
	except Exception:
		pass
	# upsert stories cache
	try:
		if story_id:
			try:
				session.exec(_sql_text("INSERT OR IGNORE INTO stories(story_id, url, updated_at) VALUES (:id, :url, CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
			except Exception:
				try:
					session.exec(_sql_text("INSERT IGNORE INTO stories(story_id, url, updated_at) VALUES (:id, :url, CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
				except Exception:
					row_s = session.exec(_sql_text("SELECT story_id FROM stories WHERE story_id=:id")).params(id=str(story_id)).first()
					if row_s:
						session.exec(_sql_text("UPDATE stories SET url=COALESCE(:url,url), updated_at=CURRENT_TIMESTAMP WHERE story_id=:id")).params(id=str(story_id), url=(str(story_url) if story_url else None))
					else:
						session.exec(_sql_text("INSERT INTO stories(story_id, url, updated_at) VALUES (:id, :url, CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
	except Exception:
		pass
	# Touch AI shadow state on inbound messages to start debounce timer
	try:
		if (direction or "in") == "in" and conversation_id:
			from .ai_shadow import touch_shadow_state
			ts_val = int(timestamp_ms) if isinstance(timestamp_ms, (int, float)) else (int(str(timestamp_ms)) if isinstance(timestamp_ms, str) and str(timestamp_ms).isdigit() else None)
			touch_shadow_state(str(conversation_id), ts_val)
	except Exception:
		pass
	return row.id  # type: ignore


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
			detail = loop.run_until_complete(fetch_message_details(event))
			if isinstance(detail, dict):
				event = detail
			else:
				return None
		except Exception:
			return None
	message_obj = event.get("message") or {}
	if not message_obj:
		return None
	mid = message_obj.get("mid") or message_obj.get("id") or event.get("id")
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
	text_val = message_obj.get("message") or message_obj.get("text")
	attachments = message_obj.get("attachments")
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
	except Exception:
		ts_ms = None
	direction = "in"
	try:
		token, entity_id, is_page = __import__("app.services.instagram_api", fromlist=["_get_base_token_and_id"]).instagram_api._get_base_token_and_id()
		owner = entity_id
		if sender_id and str(sender_id) == str(owner):
			direction = "out"
	except Exception:
		pass
	conversation_id = f"dm:{(recipient_id if direction=='out' else sender_id)}" if ((recipient_id if direction=='out' else sender_id) is not None) else None
	# Ad/referral extraction (best-effort)
	ad_id = None
	ad_link = None
	ad_title = None
	ad_img = None
	ad_name = None
	referral_json_val = None
	try:
		ref = (event.get("referral") or message_obj.get("referral") or {})
		if isinstance(ref, dict):
			ad_id = str(ref.get("ad_id") or ref.get("ad_id_v2") or "") or None
			ad_link = ref.get("ad_link") or ref.get("url") or ref.get("link") or None
			ad_title = ref.get("headline") or ref.get("source") or ref.get("type") or None
			ad_img = ref.get("image_url") or ref.get("thumbnail_url") or ref.get("picture") or ref.get("media_url") or None
			ad_name = ref.get("name") or ref.get("title") or None
			referral_json_val = json.dumps(ref, ensure_ascii=False)
	except Exception:
		ad_id = ad_link = ad_title = ad_img = ad_name = None
		referral_json_val = None
	row = Message(
		ig_sender_id=str(sender_id) if sender_id is not None else None,
		ig_recipient_id=str(recipient_id) if recipient_id is not None else None,
		ig_message_id=str(mid),
		text=text_val,
		attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments is not None else None,
		timestamp_ms=int(ts_ms) if ts_ms is not None else None,
		raw_json=json.dumps(event, ensure_ascii=False),
		conversation_id=conversation_id,
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
	# Upsert ai_conversations last-* fields (safe on out-of-order via timestamp compare)
	try:
		if conversation_id and ts_ms is not None:
			from sqlalchemy import text as _t
			try:
				session.exec(_t("INSERT OR IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=str(conversation_id))
			except Exception:
				try:
					session.exec(_t("INSERT IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=str(conversation_id))
				except Exception:
					pass
			try:
				session.exec(
					_t(
						"""
						INSERT INTO ai_conversations(convo_id, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title)
						VALUES (:cid, :mid, :ts, :txt, :dir, :sun, :sid, :rid, :adid, :alink, :atitle)
						ON CONFLICT(convo_id) DO UPDATE SET
						  last_message_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_id ELSE ai_conversations.last_message_id END,
						  last_message_timestamp_ms=GREATEST(COALESCE(ai_conversations.last_message_timestamp_ms,0), excluded.last_message_timestamp_ms),
						  last_message_text=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_text ELSE ai_conversations.last_message_text END,
						  last_message_direction=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_direction ELSE ai_conversations.last_message_direction END,
						  last_sender_username=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_sender_username ELSE ai_conversations.last_sender_username END,
						  ig_sender_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.ig_sender_id ELSE ai_conversations.ig_sender_id END,
						  ig_recipient_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.ig_recipient_id ELSE ai_conversations.ig_recipient_id END,
						  last_ad_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_id ELSE ai_conversations.last_ad_id END,
						  last_ad_link=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_link ELSE ai_conversations.last_ad_link END,
						  last_ad_title=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_title ELSE ai_conversations.last_ad_title END
						"""
					).params(
						cid=str(conversation_id),
						mid=int(row.id),
						ts=int(ts_ms),
						txt=(text_val or ""),
						dir=(direction or "in"),
						sun=(sender_username or None),
						sid=(str(sender_id) if sender_id is not None else None),
						rid=(str(recipient_id) if recipient_id is not None else None),
						adid=(str(ad_id) if ad_id is not None else None),
						alink=ad_link,
						atitle=ad_title,
					)
				)
			except Exception as e:
				try:
					session.exec(
						_t(
							"""
							INSERT INTO ai_conversations(convo_id, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title)
							VALUES (:cid, :mid, :ts, :txt, :dir, :sun, :sid, :rid, :adid, :alink, :atitle)
							ON DUPLICATE KEY UPDATE
							  last_message_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_id), ai_conversations.last_message_id),
							  last_message_timestamp_ms=GREATEST(COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_timestamp_ms)),
							  last_message_text=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_text), ai_conversations.last_message_text),
							  last_message_direction=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_direction), ai_conversations.last_message_direction),
							  last_sender_username=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_sender_username), ai_conversations.last_sender_username),
							  ig_sender_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(ig_sender_id), ai_conversations.ig_sender_id),
							  ig_recipient_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(ig_recipient_id), ai_conversations.ig_recipient_id),
							  last_ad_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_id), ai_conversations.last_ad_id),
							  last_ad_link=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_link), ai_conversations.last_ad_link),
							  last_ad_title=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_title), ai_conversations.last_ad_title)
							"""
						).params(
							cid=str(conversation_id),
							mid=int(row.id),
							ts=int(ts_ms),
							txt=(text_val or ""),
							dir=(direction or "in"),
							sun=(sender_username or None),
							sid=(str(sender_id) if sender_id is not None else None),
							rid=(str(recipient_id) if recipient_id is not None else None),
							adid=(str(ad_id) if ad_id is not None else None),
							alink=ad_link,
							atitle=ad_title,
						)
					)
				except Exception as e2:
					import logging as _lg
					_lg.getLogger("ingest.upsert").warning("hydrate upsert ai_conversations failed cid=%s mid=%s ts=%s err=%s first_err=%s", str(conversation_id), str(row.id), str(ts_ms), str(e2)[:200], str(e)[:120])
	except Exception:
		pass
	# story cache
	try:
		if story_id:
			from sqlalchemy import text as _t
			try:
				session.exec(_t("INSERT OR IGNORE INTO stories(story_id, url, updated_at) VALUES (:id,:url,CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
			except Exception:
				try:
					session.exec(_t("INSERT IGNORE INTO stories(story_id, url, updated_at) VALUES (:id,:url,CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
				except Exception:
					exists = session.exec(_t("SELECT 1 FROM stories WHERE story_id=:id")).params(id=str(story_id)).first()
					if exists:
                        # update url if provided
						session.exec(_t("UPDATE stories SET url=COALESCE(:url,url), updated_at=CURRENT_TIMESTAMP WHERE story_id=:id")).params(id=str(story_id), url=(str(story_url) if story_url else None))
					else:
						session.exec(_t("INSERT INTO stories(story_id, url, updated_at) VALUES (:id,:url,CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
	except Exception:
		pass
	return int(row.id)


def handle(raw_event_id: int) -> int:
	"""Ingest one raw_event id. Return number of messages inserted."""
	inserted = 0
	with get_session() as session:
		# Load raw payload
		row = session.exec(text("SELECT id, payload FROM raw_events WHERE id = :id").params(id=raw_event_id)).first()
		if not row:
			return 0
		payload_text = row.payload if hasattr(row, "payload") else row[1]
		try:
			payload: Dict[str, Any] = json.loads(payload_text)
		except Exception:
			return 0
		entries: List[Dict[str, Any]] = payload.get("entry", [])
		with session.get_bind().begin() as conn:  # type: ignore
			for entry in entries:
				igba_id = str(entry.get("id")) if entry.get("id") is not None else ""
				if not igba_id:
					continue
				_ensure_ig_account(conn, igba_id)
				# Collect messaging events possibly nested
				messaging_events: List[Dict[str, Any]] = entry.get("messaging") or []
				if not messaging_events and entry.get("changes"):
					for change in entry.get("changes", []):
						val = change.get("value") or {}
						if isinstance(val, dict) and val.get("messaging"):
							messaging_events.extend(val.get("messaging", []))
				for event in messaging_events:
					message_obj = event.get("message") or {}
					if not message_obj or message_obj.get("is_echo") or message_obj.get("is_deleted"):
						continue
					sender_id = (event.get("sender") or {}).get("id")
					if sender_id:
						_ensure_ig_user(conn, str(sender_id))
					mid = message_obj.get("mid") or message_obj.get("id")
					# ensure ai_conversations row exists and possibly enqueue hydration
					try:
						other_party_id = (event.get("recipient") or {}).get("id") if ((event.get("sender") or {}).get("id") == igba_id) else (event.get("sender") or {}).get("id")
						cid = f"dm:{str(other_party_id)}"
						try:
							conn.exec_driver_sql("INSERT OR IGNORE INTO ai_conversations(convo_id) VALUES (?)", (cid,))
						except Exception:
							try:
								conn.exec_driver_sql("INSERT IGNORE INTO ai_conversations(convo_id) VALUES (%s)", (cid,))  # type: ignore
							except Exception:
								pass
						# one-time hydration enqueue if not hydrated (ai_conversations)
						row_h = session.exec(text("SELECT hydrated_at FROM ai_conversations WHERE convo_id=:cid").params(cid=cid)).first()
						need_hydrate = (not row_h) or (not (row_h.hydrated_at if hasattr(row_h, 'hydrated_at') else (row_h[0] if isinstance(row_h,(list,tuple)) else None)))
						if need_hydrate:
							enqueue("hydrate_conversation", key=cid, payload={"igba_id": str(igba_id), "ig_user_id": str(other_party_id), "max_messages": 200})
					except Exception:
						pass
					msg_id = _insert_message(session, event, igba_id)
					if msg_id:
						inserted += 1
						attachments = message_obj.get("attachments")
						if attachments:
							_create_attachment_stubs(session, msg_id, str(mid), attachments)
						# enrichers (idempotent via jobs table uniqueness)
						if sender_id:
							try:
								rowu = session.exec(text("SELECT fetch_status FROM ig_users WHERE ig_user_id=:id").params(id=str(sender_id))).first()
							except Exception:
								rowu = None
							fs = None
							try:
								fs = (rowu.fetch_status if hasattr(rowu, "fetch_status") else (rowu[0] if isinstance(rowu, (list, tuple)) else None)) if rowu else None
							except Exception:
								fs = None
							if fs is None or str(fs).lower() != "ok":
								enqueue("enrich_user", key=str(sender_id), payload={"ig_user_id": str(sender_id)})
						enqueue("enrich_page", key=str(igba_id), payload={"igba_id": str(igba_id)})
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
							# Determine conversation id (other party is the user id)
							other_party_id = sender_id if sender_id and str(sender_id) != str(igba_id) else recipient_id
							conversation_id = f"dm:{other_party_id}" if other_party_id else None
							if conversation_id:
								# Update the latest message in this conversation with ad metadata if missing
								rowm = session.exec(text(
									"SELECT id FROM message WHERE conversation_id=:cid ORDER BY timestamp_ms DESC, id DESC LIMIT 1"
								)).params(cid=conversation_id).first()
								if rowm:
									mid = rowm.id if hasattr(rowm, "id") else (rowm[0] if isinstance(rowm, (list, tuple)) else None)
									if mid:
										session.exec(text(
											"UPDATE message SET ad_id=COALESCE(ad_id, :adid), ad_link=COALESCE(ad_link, :link), ad_title=COALESCE(ad_title, :title), referral_json=COALESCE(referral_json, :ref) WHERE id=:id"
										)).params(id=int(mid), adid=ad_id, link=ad_link, title=ad_title, ref=referral_json_val)
						except Exception:
							pass
				except Exception:
					pass
		return inserted


