import hashlib
import hmac
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Request, Response
from sqlalchemy import text
from starlette.requests import ClientDisconnect

from ..db import get_session
from ..services.queue import enqueue


router = APIRouter()
_log = logging.getLogger("whatsapp.webhook")
PAYLOAD_DIR = Path(__file__).resolve().parents[2] / "payload"


def _persist_payload_to_disk(payload: Dict[str, Any], raw_body: bytes) -> Optional[Path]:
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    uniq = hashlib.sha256(raw_body or b"").hexdigest()[:12]
    filename = f"wa_payload_{ts}_{uniq}.json"
    target = PAYLOAD_DIR / filename
    try:
        PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        return target
    except Exception as exc:
        try:
            _log.warning("WA webhook POST: failed to persist payload: %s", str(exc))
        except Exception:
            pass
        return None


def _validate_signature(raw_body: bytes, signature: Optional[str]) -> None:
    secret = os.getenv("IG_APP_SECRET", "")
    if not secret or not signature:
        raise HTTPException(status_code=403, detail="Missing signature")
    expected = "sha256=" + hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=403, detail="Invalid signature")


@router.get("/webhooks/whatsapp")
async def verify_subscription(request: Request):
    params = request.query_params
    mode = params.get("hub.mode")
    verify_token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge", "")
    expected = os.getenv("WA_WEBHOOK_VERIFY_TOKEN") or os.getenv("IG_WEBHOOK_VERIFY_TOKEN", "")
    if mode == "subscribe" and verify_token == expected:
        return Response(content=str(challenge), media_type="text/plain")
    return Response(status_code=403)


@router.post("/webhooks/whatsapp")
async def receive_events(request: Request):
    try:
        body = await request.body()
    except ClientDisconnect:
        return {"status": "client_disconnected"}
    except Exception:
        raise HTTPException(status_code=400, detail="Failed to read request body")

    signature = request.headers.get("X-Hub-Signature-256")
    try:
        _validate_signature(body, signature)
    except Exception:
        return {"status": "signature_invalid"}

    try:
        payload: Dict[str, Any] = json.loads(body.decode("utf-8"))
        payload_path = _persist_payload_to_disk(payload, body)
        if payload_path:
            try:
                _log.info("WA webhook POST: payload written to %s", payload_path)
            except Exception:
                pass
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if payload.get("object") != "whatsapp_business_account":
        return {"status": "ignored"}

    uniq_hash = hashlib.sha256(body).hexdigest()
    raw_event_id = None

    with get_session() as session:
        try:
            row = session.exec(text("SELECT id FROM raw_events WHERE uniq_hash = :h").params(h=uniq_hash)).first()
            if row:
                raw_event_id = row.id if hasattr(row, "id") else row[0]
            else:
                entry_id = ""
                try:
                    entries = payload.get("entry") or []
                    if entries and isinstance(entries[0], dict):
                        entry_id = str(entries[0].get("id") or "")
                except Exception:
                    entry_id = ""
                session.exec(
                    text(
                        """
                        INSERT INTO raw_events (object, entry_id, payload, sig256, uniq_hash)
                        VALUES (:object, :entry_id, :payload, :sig256, :uniq_hash)
                        """
                    ).params(
                        object="whatsapp_business_account",
                        entry_id=entry_id,
                        payload=json.dumps(payload),
                        sig256=signature or "",
                        uniq_hash=uniq_hash,
                    )
                )
                session.commit()
                row = session.exec(text("SELECT id FROM raw_events WHERE uniq_hash = :h").params(h=uniq_hash)).first()
                if row:
                    raw_event_id = row.id if hasattr(row, "id") else row[0]
        except Exception as exc:
            try:
                _log.error("WA webhook POST: database error saving raw event: %s", str(exc))
            except Exception:
                pass

    if raw_event_id:
        try:
            enqueue("ingest", key=str(raw_event_id), payload={"raw_event_id": int(raw_event_id)})
        except Exception:
            pass
    return {"status": "ok", "raw_event_id": raw_event_id}
