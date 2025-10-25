import hashlib
import hmac
import json
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from sqlmodel import select
from sqlalchemy.exc import IntegrityError

from ..db import get_session
from ..models import Message
from ..services.instagram_api import _get_base_token_and_id, fetch_user_username, GRAPH_VERSION, _get as graph_get
import httpx
from .ig import notify_new_message
import logging


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

	entries: List[Dict[str, Any]] = payload.get("entry", [])
	persisted = 0
	with get_session() as session:
		for entry in entries:
			messaging_events: List[Dict[str, Any]] = entry.get("messaging") or []
			# Some deliveries may nest inside 'changes' → 'value' → 'messaging'
			if not messaging_events and entry.get("changes"):
				for change in entry.get("changes", []):
					val = change.get("value") or {}
					if isinstance(val, dict) and val.get("messaging"):
						messaging_events.extend(val.get("messaging", []))

			for event in messaging_events:
				message_obj = event.get("message") or {}
				if not message_obj:
					continue
				# skip echoes/deleted to avoid duplicates
				try:
					if message_obj.get("is_echo") or message_obj.get("is_deleted"):
						continue
				except Exception:
					pass
				sender_id = (event.get("sender") or {}).get("id")
				recipient_id = (event.get("recipient") or {}).get("id")
				mid = message_obj.get("mid") or message_obj.get("id")
				text = message_obj.get("text")
				attachments = message_obj.get("attachments")
				timestamp_ms = event.get("timestamp")
				# optional username if webhook provides it (best-effort)
				sender_username = None
				try:
					sender_username = (event.get("sender") or {}).get("username") or (message_obj.get("from") or {}).get("username")
				except Exception:
					pass
				if not sender_username and sender_id:
					try:
						sender_username = await fetch_user_username(str(sender_id))
					except Exception:
						sender_username = None
				# determine direction and a stable conversation id using our owner id
				try:
					_, owner_id, _ = _get_base_token_and_id()
				except Exception:
					owner_id = None
				direction = "in"
				if owner_id is not None and sender_id is not None and str(sender_id) == str(owner_id):
					direction = "out"
				other_party_id = recipient_id if direction == "out" else sender_id
				conversation_id = (f"dm:{other_party_id}" if other_party_id is not None else None)
				# log a concise preview of the received message (no secrets, truncated)
				try:
					att_count = len(attachments) if isinstance(attachments, list) else (1 if attachments else 0)
					preview = (text or "").replace("\n", " ").replace("\r", " ")[:200]
					_log.info(
						"IG msg recv: from=%s to=%s mid_sfx=%s ts=%s text='%s' att=%d",
						(str(sender_id)[-4:] if sender_id else None),
						(str(recipient_id)[-4:] if recipient_id else None),
						(str(mid)[-6:] if mid else None),
						str(timestamp_ms),
						preview,
						att_count,
					)
				except Exception:
					pass
				# parse referral/ad metadata if present
				ad_id = None
				ad_link = None
				ad_title = None
				referral_json = None
				try:
					ref = event.get("referral") or {}
					if not ref and isinstance(event.get("change"), dict):
						ref = (event.get("change") or {}).get("value", {}).get("referral") or {}
					if ref:
						ad_id = ref.get("ad_id") or ref.get("adgroup_id") or ref.get("campaign_id")
						ad_link = ref.get("ad_link") or ref.get("source_url") or ref.get("link")
						ad_title = ref.get("ad_title") or ref.get("type")
						referral_json = json.dumps(ref, ensure_ascii=False)
				except Exception:
					pass

				# skip if already saved (idempotent on ig_message_id)
				try:
					exists = session.exec(select(Message).where(Message.ig_message_id == str(mid))).first()
					if exists:
						continue
				except Exception:
					pass

				row = Message(
					ig_sender_id=str(sender_id) if sender_id is not None else None,
					ig_recipient_id=str(recipient_id) if recipient_id is not None else None,
					ig_message_id=str(mid) if mid is not None else None,
					text=text,
					attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments is not None else None,
					timestamp_ms=int(timestamp_ms) if isinstance(timestamp_ms, (int, float, str)) and str(timestamp_ms).isdigit() else None,
					raw_json=json.dumps(event, ensure_ascii=False),
					conversation_id=conversation_id,
					direction=direction,
					sender_username=sender_username,
					ad_id=ad_id,
					ad_link=ad_link,
					ad_title=ad_title,
					referral_json=referral_json,
				)
				try:
					session.add(row)
					persisted += 1
				except IntegrityError:
					# concurrent duplicate; ignore
					try:
						session.rollback()
					except Exception:
						pass
				# fire websocket event for live UI update (best effort)
				try:
					await notify_new_message({
						"type": "ig_message",
						"conversation_id": conversation_id,
						"timestamp_ms": int(timestamp_ms) if isinstance(timestamp_ms, (int, float, str)) and str(timestamp_ms).isdigit() else None,
						"text": (text or "")[:200],
					})
				except Exception:
					pass

	try:
		_log.info("IG webhook POST: processed entries=%d saved=%d", len(entries), persisted)
	except Exception:
		pass
	return {"status": "ok", "saved": persisted}


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
					# attempt common shapes
					url = (
						(att.get("file_url") if isinstance(att, dict) else None)
						or (((att.get("payload") or {}).get("url")) if isinstance(att, dict) else None)
						or (((att.get("image_data") or {}).get("url")) if isinstance(att, dict) else None)
						or (((att.get("image_data") or {}).get("preview_url")) if isinstance(att, dict) else None)
					)
			except Exception:
				url = None
	if not url:
		# query Graph attachments for the message id
		token, _, _ = _get_base_token_and_id()
		base = f"https://graph.facebook.com/{GRAPH_VERSION}"
		path = f"/{ig_message_id}/attachments"
		params = {"access_token": token, "fields": "mime_type,file_url,image_data{url,preview_url},name"}
		async with httpx.AsyncClient() as client:
			data = await graph_get(client, base + path, params)
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


