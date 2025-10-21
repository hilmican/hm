import hashlib
import hmac
import json
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request, Response

from ..db import get_session
from ..models import Message
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
	_validate_signature(body, request.headers.get("X-Hub-Signature-256") or request.headers.get("x-hub-signature-256"))

	try:
		payload: Dict[str, Any] = json.loads(body.decode("utf-8"))
	except Exception:
		raise HTTPException(status_code=400, detail="Invalid JSON")

	if payload.get("object") != "instagram":
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
				sender_id = (event.get("sender") or {}).get("id")
				recipient_id = (event.get("recipient") or {}).get("id")
				mid = message_obj.get("mid") or message_obj.get("id")
				text = message_obj.get("text")
				attachments = message_obj.get("attachments")
				timestamp_ms = event.get("timestamp")
				row = Message(
					ig_sender_id=str(sender_id) if sender_id is not None else None,
					ig_recipient_id=str(recipient_id) if recipient_id is not None else None,
					ig_message_id=str(mid) if mid is not None else None,
					text=text,
					attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments is not None else None,
					timestamp_ms=int(timestamp_ms) if isinstance(timestamp_ms, (int, float, str)) and str(timestamp_ms).isdigit() else None,
					raw_json=json.dumps(event, ensure_ascii=False),
				)
				session.add(row)
				persisted += 1

	return {"status": "ok", "saved": persisted}


