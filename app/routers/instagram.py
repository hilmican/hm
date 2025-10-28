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
	body = await request.body()
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

	# Persist raw payload for inspection
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
				row = session.exec(text("SELECT id FROM raw_events WHERE uniq_hash = :h")).params(h=uniq_hash).first()
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
					)
					session.add(row)
					persisted += 1
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
									enqueue("fetch_media", key=f"{int(row.id)}:{idx}", payload={"message_id": int(row.id), "position": idx})
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
						"timestamp_ms": int(ts_ms) if isinstance(ts_ms, (int, float, str)) and str(ts_ms).isdigit() else None,
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


