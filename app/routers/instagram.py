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
from .websocket_handlers import notify_new_message
import logging
from ..services.monitoring import increment_counter
from starlette.requests import ClientDisconnect


router = APIRouter()
_log = logging.getLogger("instagram.webhook")
PAYLOAD_DIR = Path(__file__).resolve().parents[2] / "payload"


def _persist_payload_to_disk(payload: Dict[str, Any], raw_body: bytes) -> Optional[Path]:
	ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
	uniq = hashlib.sha256(raw_body or b"").hexdigest()[:12]
	filename = f"ig_payload_{ts}_{uniq}.json"
	target = PAYLOAD_DIR / filename
	try:
		PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
		with target.open("w", encoding="utf-8") as fh:
			json.dump(payload, fh, ensure_ascii=False, indent=2)
		return target
	except Exception as exc:
		try:
			_log.warning("IG webhook POST: failed to persist payload: %s", str(exc))
		except Exception:
			pass
		return None


def _validate_signature(raw_body: bytes, signature: Optional[str]) -> None:
	secret = os.getenv("IG_APP_SECRET", "")
	if not secret or not signature:
		raise HTTPException(status_code=403, detail="Missing signature")
	# Expect header in form: 'sha256=<hex>'
	expected = "sha256=" + hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
	if not hmac.compare_digest(expected, signature):
		raise HTTPException(status_code=403, detail="Invalid signature")


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
	if mode == "subscribe" and verify_token == expected:
		return Response(content=str(challenge), media_type="text/plain")
	return Response(status_code=403)


@router.post("/webhooks/instagram")
async def receive_events(request: Request):
	try:
		body = await request.body()
	except ClientDisconnect:
		return {"status": "client_disconnected"}
	except Exception:
		try:
			_log.warning("IG webhook POST: failed to read request body")
		except Exception:
			pass
		raise HTTPException(status_code=400, detail="Failed to read request body")

	# Validate signature
	signature = request.headers.get("X-Hub-Signature-256")
	try:
		_validate_signature(body, signature)
	except Exception as e:
		try:
			_log.warning("IG webhook POST: signature validation failed: %s", str(e))
		except Exception:
			pass
		return {"status": "signature_invalid"}

	try:
		payload: Dict[str, Any] = json.loads(body.decode("utf-8"))
		_log.info(
			"IG webhook POST: parsed payload with object=%s, entries=%d",
			payload.get("object"),
			len(payload.get("entry", [])),
		)
		# Debug: log full payload JSON to help trace mapping issues (from/to/conversation IDs, etc.)
		try:
			_log.info("IG webhook POST: full payload JSON=%s", json.dumps(payload, ensure_ascii=False))
		except Exception:
			# Best-effort; never let debug logging break webhook handling
			pass
		payload_path = _persist_payload_to_disk(payload, body)
		if payload_path:
			try:
				_log.info("IG webhook POST: payload written to %s", payload_path)
			except Exception:
				pass
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
	raw_event_id = None

	with get_session() as session:
		try:
			_log.info("IG webhook POST: checking for existing raw event with hash=%s", uniq_hash[:16])
			row = session.exec(text("SELECT id FROM raw_events WHERE uniq_hash = :h").params(h=uniq_hash)).first()
			if row:
				raw_event_id = row.id if hasattr(row, "id") else row[0]
				saved_raw += 1
				_log.info("IG webhook POST: found existing raw event id=%s", raw_event_id)
			else:
				_log.info("IG webhook POST: inserting new raw event")
				# Insert raw event with all required columns
				session.exec(
					text("""
						INSERT INTO raw_events (object, entry_id, payload, sig256, uniq_hash)
						VALUES (:object, :entry_id, :payload, :sig256, :uniq_hash)
					""").params(
						object=str(payload.get("object") or "instagram"),
						entry_id="",  # Not per-entry since we store whole payload
						payload=json.dumps(payload),
						sig256=signature or "",
						uniq_hash=uniq_hash
					)
				)
				session.commit()
				_log.info("IG webhook POST: insert committed, querying for id")
				# Get the ID we just inserted
				row = session.exec(text("SELECT id FROM raw_events WHERE uniq_hash = :h").params(h=uniq_hash)).first()
				if row:
					raw_event_id = row.id if hasattr(row, "id") else row[0]
					saved_raw += 1
					_log.info("IG webhook POST: got raw_event_id=%s", raw_event_id)
				else:
					_log.error("IG webhook POST: failed to get id after insert")
		except Exception as e:
			# Log the actual error instead of ignoring
			_log.error("IG webhook POST: database error saving raw event: %s", str(e))
			pass

	try:
		_log.info("IG webhook POST: saved raw events for entries=%d raw_saved=%d raw_event_id=%s", len(entries), saved_raw, raw_event_id)
	except Exception:
		pass

	# Queue message processing for background handling
	if raw_event_id:
		try:
			_log.info("IG webhook POST: queuing message processing for raw_event_id=%s", raw_event_id)
			from ..services.queue import enqueue
			# Use raw_event_id as the job key and pass it in the payload for the worker
			enqueue("ingest", key=str(raw_event_id), payload={"raw_event_id": int(raw_event_id)})
		except Exception as e:
			# best-effort: ignore failures here; ingestion worker will backfill later
			try:
				_log.error("IG webhook POST: message processing failed with error: %s", str(e))
			except Exception:
				pass
	else:
		try:
			_log.warning("IG webhook POST: no raw_event_id to queue for processing")
		except Exception:
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
		try:
			r = await client.get(url, timeout=30, follow_redirects=True)
			r.raise_for_status()
		except httpx.HTTPStatusError as e:
			if getattr(e.response, "status_code", None) == 404:
				raise HTTPException(status_code=404, detail="Media not found")
			raise HTTPException(status_code=502, detail="Media fetch failed")
		media_type = mime or r.headers.get("content-type") or "image/jpeg"
		headers = {"Cache-Control": "public, max-age=86400"}
		return StreamingResponse(iter([r.content]), media_type=media_type, headers=headers)
