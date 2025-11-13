import hashlib
import hmac
import json
import os
from typing import Any, Dict, List, Optional
from pathlib import Path
import time

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from sqlmodel import select
from sqlalchemy.exc import IntegrityError

from ..db import get_session
from sqlalchemy import text
from ..services.queue import enqueue
from ..models import Message
from ..services.instagram_api import _get_base_token_and_id, fetch_user_username, GRAPH_VERSION, _get as graph_get
import httpx
from .ig import notify_new_message
import logging
from ..services.monitoring import increment_counter
from starlette.requests import ClientDisconnect


router = APIRouter()
_log = logging.getLogger("instagram.webhook")


@router.get("/webhooks/instagram")
async def verify_subscription(request: Request):
	params = request.query_params
	mode = params.get("hub.mode")
	verify_token = params.get("hub.verify_token")
	challenge = params.get("hub.challenge", "")
	expected = os.getenv("IG_WEBHOOK_VERIFY_TOKEN", "")
	# minimal debug without leaking full secrets
	try:
		_log.info(
			"IG verify: mode=%s recv_len=%d recv_sfx=%s expected_len=%d expected_sfx=%s",
			mode,
			len(verify_token or ""),
			(verify_token[-4:] if verify_token else None),
			len(expected or ""),
			(expected[-4:] if expected else None),
		)
	except Exception:
		pass
	if mode == "subscribe" and verify_token and verify_token == expected:
		return Response(content=str(challenge), media_type="text/plain")
	raise HTTPException(status_code=403, detail="Verification failed")


def _validate_signature(raw_body: bytes, signature: Optional[str]) -> None:
	secret = os.getenv("IG_APP_SECRET", "")
	if not secret or not signature:
		raise HTTPException(status_code=403, detail="Missing signature")
	# Expect header in form: 'sha256=<hex>'
	expected = "sha256=" + hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
	if not hmac.compare_digest(expected, signature):
		raise HTTPException(status_code=403, detail="Invalid signature")


@router.post("/webhooks/instagram")
async def receive_events(request: Request):
	# Read raw body to compute HMAC
	try:
		body = await request.body()
	except ClientDisconnect:
		# Client dropped connection; avoid error-level logs and exit fast
		try:
			_log.warning("IG webhook POST: client disconnected while reading body")
		except Exception:
			pass
		return Response(status_code=204)
	signature = request.headers.get("X-Hub-Signature-256") or request.headers.get("x-hub-signature-256")
	# lightweight arrival log (no secrets)
	try:
		_log.info(
			"IG webhook POST: sig_len=%d sig_sfx=%s body_len=%d",
			len(signature or ""),
			(signature[-6:] if signature else None),
			len(body or b""),
		)
	except Exception:
		pass
	_validate_signature(body, signature)

	# Persist raw payload for inspection (disabled by default to reduce disk I/O)
	# Enable via env var: SAVE_WEBHOOK_PAYLOADS=1
	if os.getenv("SAVE_WEBHOOK_PAYLOADS", "0") == "1":
		try:
			out_dir = Path("payloads")
			out_dir.mkdir(parents=True, exist_ok=True)
			fname = f"ig_{int(time.time()*1000)}_{len(body or b'')}" + (f"_{(signature[-6:] if signature else 'nosig')}" ) + ".json"
			(out_dir / fname).write_bytes(body)
		except Exception:
			pass

	try:
		payload: Dict[str, Any] = json.loads(body.decode("utf-8"))
	except Exception:
		try:
			_log.warning("IG webhook POST: invalid JSON, body_len=%d", len(body or b""))
		except Exception:
			pass
		raise HTTPException(status_code=400, detail="Invalid JSON")

	if payload.get("object") != "instagram":
		try:
			_log.info("IG webhook POST: ignored object=%s", payload.get("object"))
		except Exception:
			pass
		return {"status": "ignored"}

	# Insert raw_event once and enqueue ingestion. Idempotent on uniq_hash of full payload.
	entries: List[Dict[str, Any]] = payload.get("entry", [])
	saved_raw = 0
	uniq_hash = hashlib.sha256(body).hexdigest()
	with get_session() as session:
		for entry in entries:
			entry_id = str(entry.get("id")) if entry.get("id") is not None else ""
			try:
				session.exec(
					text(
						"""
						INSERT OR IGNORE INTO raw_events(object, entry_id, payload, sig256, uniq_hash)
						VALUES (:object, :entry_id, :payload, :sig256, :uniq_hash)
						"""
					)
				).params(
					object=str(payload.get("object") or "instagram"),
					entry_id=entry_id,
					payload=body.decode("utf-8"),
					sig256=signature or "",
					uniq_hash=uniq_hash,
				)
				row = session.exec(text("SELECT id FROM raw_events WHERE uniq_hash = :h").params(h=uniq_hash)).first()
				if row:
					saved_raw += 1
					try:
						enqueue("ingest", key=str(row.id), payload={"raw_event_id": int(row.id)})
					except Exception:
						pass
			except Exception:
				# ignore duplicates or insert errors to keep webhook fast
				pass
	try:
		_log.info("IG webhook POST: enqueued ingest for entries=%d raw_saved=%d", len(entries), saved_raw)
	except Exception:
		pass

	# Additionally, persist minimal Message rows immediately (no Graph calls)
	# so that inbox can render even if workers or Graph are unavailable.
	try:
		with get_session() as session:
			persisted = 0
			for entry in entries:
				messaging_events: List[Dict[str, Any]] = entry.get("messaging") or []
				if not messaging_events and entry.get("changes"):
					for change in entry.get("changes", []):
						val = change.get("value") or {}
						if isinstance(val, dict) and val.get("messaging"):
							messaging_events.extend(val.get("messaging", []))
				for event in messaging_events:
					mobj = event.get("message") or {}
					if not mobj or mobj.get("is_deleted"):
						continue
					mid = mobj.get("mid") or mobj.get("id")
					if not mid:
						continue
					# idempotent insert by ig_message_id
					exists = session.exec(select(Message).where(Message.ig_message_id == str(mid))).first()
					if exists:
						continue
					sender_id = (event.get("sender") or {}).get("id")
					recipient_id = (event.get("recipient") or {}).get("id")
					text_val = mobj.get("text")
					attachments = mobj.get("attachments")
					# Story reply (best-effort)
					story_id = None
					story_url = None
					try:
						story_obj = ((mobj.get("reply_to") or {}).get("story") or {})
						story_id = str(story_obj.get("id") or "") or None
						story_url = story_obj.get("url") or None
					except Exception:
						story_id = None
						story_url = None
					# Ad/referral (best-effort)
					ad_id = None
					ad_link = None
					ad_title = None
					ad_img = None
					ad_name = None
					try:
						ref = (event.get("referral") or mobj.get("referral") or {})
						if isinstance(ref, dict):
							ad_id = str(ref.get("ad_id") or ref.get("ad_id_v2") or "") or None
							ad_link = ref.get("ad_link") or ref.get("url") or ref.get("link") or None
							ad_title = ref.get("headline") or ref.get("source") or ref.get("type") or None
							ad_img = ref.get("image_url") or ref.get("thumbnail_url") or ref.get("picture") or ref.get("media_url") or None
							ad_name = ref.get("name") or ref.get("title") or None
						# Also try to parse Ads Library id from link query param
						if not ad_id and ad_link and "facebook.com/ads/library" in str(ad_link):
							try:
								from urllib.parse import urlparse, parse_qs
								q = parse_qs(urlparse(str(ad_link)).query)
								aid = (q.get("id") or [None])[0]
								ad_id = str(aid) if aid else None
							except Exception:
								pass
					except Exception:
						ad_id = ad_link = ad_title = None
					ts_ms = event.get("timestamp")
					# derive direction using entry.id when available
					igba_id = str(entry.get("id")) if entry.get("id") is not None else None
					direction = "in"
					try:
						if sender_id and igba_id and str(sender_id) == str(igba_id):
							direction = "out"
					except Exception:
						pass
					other_party_id = recipient_id if direction == "out" else sender_id
					conversation_id = (f"dm:{other_party_id}" if other_party_id is not None else None)
					row = Message(
						ig_sender_id=str(sender_id) if sender_id is not None else None,
						ig_recipient_id=str(recipient_id) if recipient_id is not None else None,
						ig_message_id=str(mid),
						text=text_val,
						attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments is not None else None,
						timestamp_ms=int(ts_ms) if isinstance(ts_ms, (int, float, str)) and str(ts_ms).isdigit() else None,
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
					)
					session.add(row)
					session.flush()  # Flush to get row.id
					persisted += 1

					ts_val = None
					try:
						if isinstance(ts_ms, (int, float)):
							ts_val = int(ts_ms)
						elif isinstance(ts_ms, str):
							digits = "".join(ch for ch in ts_ms if ch.isdigit())
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
							ts_val = int(time.time() * 1000)
						except Exception:
							ts_val = None
					if ts_val is not None and getattr(row, "timestamp_ms", None) is None:
						try:
							row.timestamp_ms = ts_val
						except Exception:
							pass

					sender_info = event.get("sender") or {}
					recipient_info = event.get("recipient") or {}
					sender_username_val = sender_info.get("username") or sender_info.get("name")
					recipient_username_val = recipient_info.get("username") or recipient_info.get("name")
					other_username_val = recipient_username_val if (direction == "out") else sender_username_val
					username_param = str(other_username_val).strip() if other_username_val else None
					if sender_username_val:
						try:
							row.sender_username = str(sender_username_val)
						except Exception:
							pass

					if other_party_id:
						try:
							from sqlalchemy import text as _t
							try:
								session.exec(_t("INSERT OR IGNORE INTO ig_users(ig_user_id, username, last_seen_at) VALUES (:id, :uname, CURRENT_TIMESTAMP)").params(id=str(other_party_id), uname=username_param))
							except Exception:
								try:
									session.exec(_t("INSERT IGNORE INTO ig_users(ig_user_id, username, last_seen_at) VALUES (:id, :uname, CURRENT_TIMESTAMP)").params(id=str(other_party_id), uname=username_param))
								except Exception:
									pass
							try:
								session.exec(_t("UPDATE ig_users SET username=COALESCE(:uname, username), last_seen_at=CURRENT_TIMESTAMP WHERE ig_user_id=:id").params(id=str(other_party_id), uname=username_param))
							except Exception:
								pass
						except Exception:
							pass
						try:
							enqueue("enrich_user", key=str(other_party_id), payload={"ig_user_id": str(other_party_id)})
						except Exception:
							pass
					if igba_id:
						try:
							enqueue("enrich_page", key=str(igba_id), payload={"igba_id": str(igba_id)})
						except Exception:
							pass
					# Ensure ai_conversations exists and update last-* fields atomically
					if conversation_id and ts_val is not None and row.id:
						try:
							from sqlalchemy import text as _t
							# create placeholder if missing (SQLite or MySQL)
							try:
								session.exec(_t("INSERT OR IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=str(conversation_id))
							except Exception:
								try:
                                    # MySQL
									session.exec(_t("INSERT IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=str(conversation_id))
								except Exception:
									pass
							# SQLite upsert
							try:
								session.exec(
									_t(
										"""
										INSERT INTO ai_conversations(convo_id, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title, hydrated_at)
										VALUES (:cid, :mid, :ts, :txt, :dir, :sun, :sid, :rid, :adid, :alink, :atitle, (SELECT hydrated_at FROM ai_conversations WHERE convo_id=:cid))
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
										ts=int(ts_val) if ts_val is not None else 0,
										txt=(text_val or ""),
										dir=(direction or "in"),
										sun=username_param,
										sid=(str(sender_id) if sender_id is not None else None),
										rid=(str(recipient_id) if recipient_id is not None else None),
										adid=(str(ad_id) if ad_id is not None else None),
										alink=ad_link,
										atitle=ad_title,
									)
								)
							except Exception:
								# MySQL upsert
								session.exec(
									_t(
										"""
										INSERT INTO ai_conversations(convo_id, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title, hydrated_at)
										VALUES (:cid, :mid, :ts, :txt, :dir, :sun, :sid, :rid, :adid, :alink, :atitle, (SELECT hydrated_at FROM ai_conversations WHERE convo_id=:cid))
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
										ts=int(ts_val) if ts_val is not None else 0,
										txt=(text_val or ""),
										dir=(direction or "in"),
										sun=username_param,
										sid=(str(sender_id) if sender_id is not None else None),
										rid=(str(recipient_id) if recipient_id is not None else None),
										adid=(str(ad_id) if ad_id is not None else None),
										alink=ad_link,
										atitle=ad_title,
									)
								)
						except Exception as e:
							try:
								_log.warning("webhook: failed to upsert ai_conversations cid=%s mid=%s ts=%s err=%s", conversation_id, row.id, ts_val, str(e)[:200])
							except Exception:
								pass
					# Start/refresh AI shadow debounce for inbound messages
					try:
						if (direction or "in") == "in" and conversation_id:
							from ..services.ai_shadow import touch_shadow_state
							tsv = int(ts_val) if ts_val is not None else None
							touch_shadow_state(str(conversation_id), tsv)
					except Exception:
						pass
					# Upsert ads and stories cache
					try:
						if ad_id:
							from sqlalchemy import text as _t
							try:
								session.exec(_t("INSERT OR IGNORE INTO ads(ad_id, name, image_url, link, updated_at) VALUES (:id,:n,:img,:lnk,CURRENT_TIMESTAMP)")).params(id=ad_id, n=ad_name, img=ad_img, lnk=ad_link)
							except Exception:
								session.exec(_t("INSERT IGNORE INTO ads(ad_id, name, image_url, link, updated_at) VALUES (:id,:n,:img,:lnk,CURRENT_TIMESTAMP)")).params(id=ad_id, n=ad_name, img=ad_img, lnk=ad_link)
							session.exec(_t("UPDATE ads SET name=COALESCE(:n,name), image_url=COALESCE(:img,image_url), link=COALESCE(:lnk,link), updated_at=CURRENT_TIMESTAMP WHERE ad_id=:id")).params(id=ad_id, n=ad_name, img=ad_img, lnk=ad_link)
							# If name still missing, enqueue hydrate job
							try:
								row_ad = session.exec(_t("SELECT name FROM ads WHERE ad_id=:id").params(id=ad_id)).first()
								nm = (row_ad.name if hasattr(row_ad, "name") else (row_ad[0] if row_ad else None)) if row_ad else None
								if not (nm and str(nm).strip()):
									from ..services.queue import enqueue
									enqueue("hydrate_ad", key=str(ad_id), payload={"ad_id": str(ad_id)})
							except Exception:
								pass
						# story cache
						if story_id:
							from sqlalchemy import text as _t
							try:
								session.exec(_t("INSERT OR IGNORE INTO stories(story_id, url, updated_at) VALUES (:id,:url,CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
							except Exception:
								try:
									session.exec(_t("INSERT IGNORE INTO stories(story_id, url, updated_at) VALUES (:id,:url,CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
								except Exception:
                                    # fall back to update or plain insert
									exists = session.exec(_t("SELECT 1 FROM stories WHERE story_id=:id LIMIT 1")).params(id=str(story_id)).first()
									if exists:
										session.exec(_t("UPDATE stories SET url=COALESCE(:url,url), updated_at=CURRENT_TIMESTAMP WHERE story_id=:id")).params(id=str(story_id), url=(str(story_url) if story_url else None))
									else:
										session.exec(_t("INSERT INTO stories(story_id, url, updated_at) VALUES (:id,:url,CURRENT_TIMESTAMP)")).params(id=str(story_id), url=(str(story_url) if story_url else None))
					except Exception:
						pass
					# ensure attachments are tracked per-message and fetch queued
					try:
						session.flush()  # obtain row.id
						if attachments:
							# reuse ingestion helper to normalize and enqueue fetch jobs
							try:
								from ..services.ingest import _create_attachment_stubs as _ins_atts
								_ins_atts(session, int(row.id), str(mid), attachments)  # type: ignore[arg-type]
							except Exception:
								# fallback: minimal inline insertion (same logic)
								items = []
								if isinstance(attachments, list):
									items = attachments
								elif isinstance(attachments, dict) and isinstance(attachments.get("data"), list):
									items = attachments.get("data") or []
								for idx, att in enumerate(items):
									kind = "file"
									try:
										ptype = (att.get("type") or att.get("mime_type") or "").lower()
										if "image" in ptype:
											kind = "image"
										elif "video" in ptype:
											kind = "video"
										elif "audio" in ptype:
											kind = "audio"
									except Exception:
										kind = "file"
									gid = None
									try:
										gid = att.get("id") or (att.get("payload") or {}).get("id")
									except Exception:
										gid = None
									session.exec(text(
										"INSERT INTO attachments(message_id, kind, graph_id, position, fetch_status) "
										"VALUES (:mid, :kind, :gid, :pos, 'pending')"
									)).params(mid=int(row.id), kind=kind, gid=gid, pos=idx)
									try:
										enqueue("fetch_media", key=f"{int(row.id)}:{idx}", payload={"message_id": int(row.id), "position": idx})
										try:
											_log.info("webhook: queued fetch_media mid=%s pos=%s msg_row=%s", str(mid), idx, int(row.id))
										except Exception:
											pass
									except Exception:
										pass
					except Exception:
						pass
					# ensure conversation row exists and enqueue one-time hydration (idempotent via jobs uniqueness)
					try:
						if other_party_id:
							# persist conversation row so visibility/debug works even before hydration
							session.exec(text(
								"""
								INSERT OR IGNORE INTO conversations(convo_id, igba_id, ig_user_id, last_message_at, unread_count)
								VALUES(:cid, :g, :u, CURRENT_TIMESTAMP, 0)
								"""
							)).params(cid=f"{igba_id}:{str(other_party_id)}", g=str(igba_id), u=str(other_party_id))
							# update last_message_at based on message timestamp when available
							try:
								from datetime import datetime
								if ts_val is not None:
									sec = int(ts_val // 1000) if ts_val > 10_000_000_000 else int(ts_val)
									ts_iso = datetime.utcfromtimestamp(sec).strftime('%Y-%m-%d %H:%M:%S')
									session.exec(text(
										"UPDATE conversations SET last_message_at=:ts WHERE convo_id=:cid"
									)).params(ts=ts_iso, cid=f"{igba_id}:{str(other_party_id)}")
								else:
									session.exec(text(
										"UPDATE conversations SET last_message_at=CURRENT_TIMESTAMP WHERE convo_id=:cid"
									)).params(cid=f"{igba_id}:{str(other_party_id)}")
							except Exception:
								pass
							enqueue("hydrate_conversation", key=f"{igba_id}:{str(other_party_id)}", payload={"igba_id": str(igba_id), "ig_user_id": str(other_party_id), "max_messages": 200})
							try:
								_log.info("webhook: enqueue hydrate convo=%s:%s", str(igba_id), str(other_party_id))
							except Exception:
								pass
					except Exception:
						pass
					# enqueue enrich jobs for user and page similar to ingest path
					try:
						if sender_id:
							enqueue("enrich_user", key=str(sender_id), payload={"ig_user_id": str(sender_id)})
						enqueue("enrich_page", key=str(entry.get("id") or ""), payload={"igba_id": str(entry.get("id") or "")})
					except Exception:
						pass
				# Best-effort: notify live clients via WebSocket about the new message
				try:
					await notify_new_message({
						"type": "ig_message",
						"conversation_id": conversation_id,
						"text": text_val,
						"timestamp_ms": int(ts_val) if ts_val is not None else None,
					})
				except Exception:
					pass
					# ensure attachments are tracked and fetched asynchronously
					try:
						session.flush()
						if attachments:
							items = []
							if isinstance(attachments, list):
								items = attachments
							elif isinstance(attachments, dict) and isinstance(attachments.get("data"), list):
								items = attachments.get("data") or []
							for idx, att in enumerate(items):
								kind = "file"
								try:
									ptype = (att.get("type") or att.get("mime_type") or "").lower()
									if "image" in ptype:
										kind = "image"
									elif "video" in ptype:
										kind = "video"
									elif "audio" in ptype:
										kind = "audio"
								except Exception:
									pass
								gid = None
								try:
									gid = att.get("id") or (att.get("payload") or {}).get("id")
								except Exception:
									gid = None
								session.exec(text(
									"INSERT INTO attachments(message_id, kind, graph_id, position, fetch_status) "
									"VALUES (:mid, :kind, :gid, :pos, 'pending')"
								)).params(mid=int(row.id), kind=kind, gid=gid, pos=idx)
								enqueue("fetch_media", key=f"{int(row.id)}:{idx}", payload={"message_id": int(row.id), "position": idx})
					except Exception:
						pass
		try:
			_log.info("IG webhook POST: inserted messages=%d (direct path)", persisted)
		except Exception:
			pass
		# Handle referral-only webhook events (messaging_referrals) to tag latest message with ad metadata
		try:
			for entry in entries:
				ref_events = entry.get("messaging_referrals") or []
				for rev in ref_events:
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
					igba_id = str(entry.get("id")) if entry.get("id") is not None else None
					other_party_id = sender_id if sender_id and (not igba_id or str(sender_id) != str(igba_id)) else recipient_id
					conversation_id = f"dm:{other_party_id}" if other_party_id else None
					if conversation_id:
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
		# increment rolling counter for messages to show up in NOC
		try:
			if persisted > 0:
				increment_counter("messages", int(persisted))
		except Exception:
			pass
	except Exception:
		# best-effort: ignore failures here; ingestion worker will backfill later
		pass
	return {"status": "ok", "raw_saved": saved_raw}


@router.get("/ig/media/{ig_message_id}/{idx}")
async def get_media(ig_message_id: str, idx: int):
	# Try to serve from attachments_json; otherwise query Graph attachments
	url: Optional[str] = None
	mime: Optional[str] = None
	with get_session() as session:
		rec = session.exec(select(Message).where(Message.ig_message_id == ig_message_id)).first()  # type: ignore
		if rec and rec.attachments_json:
			try:
				data = json.loads(rec.attachments_json)
				items = []
				if isinstance(data, list):
					items = data
				elif isinstance(data, dict) and isinstance(data.get("data"), list):
					items = data["data"]
				if idx < len(items):
					att = items[idx] or {}
					# attempt common shapes; prefer direct URL to avoid Graph requests
					url = None
					if isinstance(att, dict):
						url = att.get("file_url") or (att.get("payload") or {}).get("url")
						if not url and isinstance(att.get("image_data"), dict):
							url = att["image_data"].get("url") or att["image_data"].get("preview_url")
			except Exception:
				url = None
	if not url:
		# As a last resort, query Graph attachments for the message id (avoid when possible)
		token, _, _ = _get_base_token_and_id()
		base = f"https://graph.facebook.com/{GRAPH_VERSION}"
		path = f"/{ig_message_id}/attachments"
		params = {"access_token": token, "fields": "mime_type,file_url,image_data{url,preview_url},name"}
		async with httpx.AsyncClient() as client:
			try:
				data = await graph_get(client, base + path, params)
			except Exception:
				raise HTTPException(status_code=404, detail="Media unavailable")
			arr = data.get("data") or []
			if isinstance(arr, list) and idx < len(arr):
				att = arr[idx] or {}
				url = att.get("file_url") or ((att.get("image_data") or {}).get("url")) or ((att.get("image_data") or {}).get("preview_url"))
				mime = att.get("mime_type")
	if not url:
		raise HTTPException(status_code=404, detail="Media not found")
	# fetch and stream
	async with httpx.AsyncClient() as client:
		r = await client.get(url, timeout=30, follow_redirects=True)
		r.raise_for_status()
		media_type = mime or r.headers.get("content-type") or "image/jpeg"
		headers = {"Cache-Control": "public, max-age=86400"}
		return StreamingResponse(iter([r.content]), media_type=media_type, headers=headers)


