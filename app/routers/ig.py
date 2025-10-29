from fastapi import APIRouter, Request, HTTPException
from fastapi import WebSocket, WebSocketDisconnect
import logging
from sqlmodel import select

from ..db import get_session
from ..models import Message
from sqlmodel import select
from ..services.instagram_api import sync_latest_conversations
from ..services.instagram_api import _get_base_token_and_id, GRAPH_VERSION
import json
from pathlib import Path
from fastapi.responses import FileResponse
import time
import httpx
import os


router = APIRouter(prefix="/ig", tags=["instagram"])
_log = logging.getLogger("instagram.inbox")

# simple in-memory websocket registry (single-process)
connections: set[WebSocket] = set()


async def broadcast_event(data: dict) -> None:
    dead: list[WebSocket] = []
    for ws in list(connections):
        try:
            await ws.send_json(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            connections.discard(ws)
        except Exception:
            pass


async def notify_new_message(event: dict) -> None:
    # event example: {"type":"ig_message","conversation_id":"dm:123","text":"...","timestamp_ms":...}
    await broadcast_event(event)


@router.get("/inbox")
async def inbox(request: Request, limit: int = 25):
    with get_session() as session:
        # Latest messages across convs; pick most recent per conversation
        rows = session.exec(select(Message).order_by(Message.timestamp_ms.desc()).limit(300)).all()
        conv_map = {}
        other_ids: set[str] = set()
        for m in rows:
            cid = m.conversation_id
            if not cid:
                continue
            if cid not in conv_map:
                conv_map[cid] = m
            # Determine the other party id for this message
            other = None
            try:
                if (m.direction or "in") == "out":
                    other = m.ig_recipient_id
                else:
                    other = m.ig_sender_id
            except Exception:
                other = None
            if other:
                other_ids.add(str(other))
        conversations = list(conv_map.values())[:limit]
        # Resolve usernames preferring last inbound message's sender_username; fallback to ig_users
        labels = {}
        try:
            # Build map from conv -> latest inbound with sender_username
            inbound_named: dict[str, str] = {}
            for m in rows:
                cid = m.conversation_id
                if not cid:
                    continue
                if (m.direction or "in") == "in" and (m.sender_username or "").strip() and cid not in inbound_named:
                    inbound_named[cid] = str(m.sender_username).strip()
            for cid, un in inbound_named.items():
                labels[cid] = f"@{un}"
        except Exception:
            pass
        # Fallback via ig_users when inbox usernames missing
        if other_ids:
            try:
                missing = [cid for cid in conv_map.keys() if cid not in labels]
                if missing:
                    placeholders = ",".join([":p" + str(i) for i in range(len(other_ids))])
                    from sqlalchemy import text as _text
                    params = {("p" + str(i)): list(other_ids)[i] for i in range(len(other_ids))}
                    rows_u = session.exec(_text(f"SELECT ig_user_id, username FROM ig_users WHERE ig_user_id IN ({placeholders})")).params(**params).all()
                    id_to_username = {}
                    for r in rows_u:
                        uid = r.ig_user_id if hasattr(r, "ig_user_id") else r[0]
                        un = r.username if hasattr(r, "username") else r[1]
                        if uid and un:
                            id_to_username[str(uid)] = str(un)
                    for cid, m in conv_map.items():
                        if cid in labels:
                            continue
                        other = None
                        try:
                            if (m.direction or "in") == "out":
                                other = m.ig_recipient_id
                            else:
                                other = m.ig_sender_id
                        except Exception:
                            other = None
                        if other and str(other) in id_to_username:
                            labels[cid] = f"@{id_to_username[str(other)]}"
            except Exception:
                pass
        # Best-effort ad metadata from messages
        ad_map = {}
        for m in rows:
            cid = m.conversation_id
            if not cid:
                continue
            if (m.ad_link or m.ad_title) and cid not in ad_map:
                ad_map[cid] = {"link": m.ad_link, "title": m.ad_title}
        templates = request.app.state.templates
        return templates.TemplateResponse("ig_inbox.html", {"request": request, "conversations": conversations, "labels": labels, "ad_map": ad_map})


@router.post("/inbox/refresh")
async def refresh_inbox(limit: int = 25):
    # Temporarily bypass Graph API and rely solely on locally stored messages.
    # This endpoint now acts as a no-op refresh to keep the UI flow intact.
    try:
        return {"status": "ok", "saved": 0}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.websocket("/ws")
async def ws_inbox(websocket: WebSocket):
    await websocket.accept()
    connections.add(websocket)
    try:
        while True:
            # we do not use incoming messages; keep the socket open
            await websocket.receive_text()
    except WebSocketDisconnect:
        try:
            connections.discard(websocket)
        except Exception:
            pass
    except Exception:
        try:
            connections.discard(websocket)
        except Exception:
            pass


@router.get("/inbox/{conversation_id}")
def thread(request: Request, conversation_id: str):
    with get_session() as session:
        msgs = session.exec(select(Message).where(Message.conversation_id == conversation_id).order_by(Message.timestamp_ms.asc())).all()
        # Determine other party id from messages then resolve username
        other_label = None
        other_id = None
        for mm in msgs:
            try:
                other_id = (mm.ig_sender_id if (mm.direction or "in") == "in" else mm.ig_recipient_id)
                if other_id:
                    break
            except Exception:
                continue
        if other_id:
            try:
                from sqlalchemy import text as _text
                rowu = session.exec(_text("SELECT username FROM ig_users WHERE ig_user_id=:u").params(u=str(other_id))).first()
                if rowu:
                    un = rowu.username if hasattr(rowu, "username") else rowu[0]
                    if un:
                        other_label = f"@{un}"
            except Exception:
                pass
        # Build attachment indices so template can render images (fallback: legacy attachments_json)
        att_map = {}
        for mm in msgs:
            if not mm.attachments_json:
                continue
            try:
                data = json.loads(mm.attachments_json)
                items = []
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict) and isinstance(data.get("data"), list):
                    items = data["data"]
                if items:
                    att_map[mm.ig_message_id or ""] = list(range(len(items)))
            except Exception:
                pass
        # New: Build local attachment id map from attachments table
        att_ids_map = {}
        try:
            # Map message.id -> ig_message_id
            msgid_to_mid = {}
            msg_ids = []
            for mm in msgs:
                if mm.id:
                    msg_ids.append(mm.id)
                    msgid_to_mid[int(mm.id)] = mm.ig_message_id or ""
            if msg_ids:
                # Build a parameterized IN clause
                placeholders = ",".join([":p" + str(i) for i in range(len(msg_ids))])
                from sqlalchemy import text as _text
                params = {("p" + str(i)): int(msg_ids[i]) for i in range(len(msg_ids))}
                rows = session.exec(_text(f"SELECT id, message_id, position FROM attachments WHERE message_id IN ({placeholders}) ORDER BY position ASC")).params(**params).all()
                for r in rows:
                    att_id = r.id if hasattr(r, "id") else r[0]
                    m_id = r.message_id if hasattr(r, "message_id") else r[1]
                    pos = r.position if hasattr(r, "position") else r[2]
                    mid = msgid_to_mid.get(int(m_id)) or ""
                    if mid:
                        att_ids_map.setdefault(mid, []).append(int(att_id))
        except Exception:
            att_ids_map = {}
        templates = request.app.state.templates
        return templates.TemplateResponse("ig_thread.html", {"request": request, "conversation_id": conversation_id, "messages": msgs, "other_label": other_label, "att_map": att_map, "att_ids_map": att_ids_map})


@router.get("/media/local/{attachment_id}")
def serve_media_local(attachment_id: int):
    # Stream from local FS using attachments.storage_path
    from sqlalchemy import text
    with get_session() as session:
        row = session.exec(text("SELECT storage_path, mime FROM attachments WHERE id=:id")).params(id=attachment_id).first()
        if not row:
            raise HTTPException(status_code=404, detail="Attachment not found")
        storage_path = row.storage_path if hasattr(row, "storage_path") else row[0]
        mime = row.mime if hasattr(row, "mime") else (row[1] if len(row) > 1 else None)
        if not storage_path or not Path(storage_path).exists():
            raise HTTPException(status_code=404, detail="File not found")
        return FileResponse(storage_path, media_type=(mime or "application/octet-stream"))


@router.post("/inbox/{conversation_id}/refresh")
async def refresh_thread(conversation_id: str):
    # Reuse full sync for simplicity; it will upsert only new ones
    try:
        saved = await sync_latest_conversations(limit=25)
        return {"status": "ok", "saved": saved}
    except Exception as e:
        try:
            _log.exception("Thread refresh failed for %s: %s", conversation_id, e)
        except Exception:
            pass
        return {"status": "error", "error": str(e)}


@router.get("/debug/env")
def debug_env():
    """Lightweight diagnostics: show which token path is active (page vs user) and env presence.

    Does NOT return secrets; only booleans and token length/suffix for verification.
    """
    data: dict[str, object] = {
        "has_page_id": bool(os.getenv("IG_PAGE_ID")),
        "has_page_token": bool(os.getenv("IG_PAGE_ACCESS_TOKEN")),
        "has_user_id": bool(os.getenv("IG_USER_ID")),
        "has_user_token": bool(os.getenv("IG_ACCESS_TOKEN")),
        "graph_version": os.getenv("IG_GRAPH_API_VERSION", "v21.0"),
    }
    try:
        token, ident, is_page = _get_base_token_and_id()
        data["active_path"] = "page" if is_page else "user"
        data["id_in_use"] = str(ident)
        data["token_len"] = len(token or "")
        data["token_suffix"] = (token[-6:] if token else None)
    except Exception as e:
        data["resolve_error"] = str(e)
    return data

@router.post("/inbox/{conversation_id}/send")
async def send_message(conversation_id: str, body: dict):
    """Send a text reply to the other party in this conversation and persist locally.

    conversation_id formats supported:
    - "dm:<ig_user_id>" (preferred)
    - Graph conversation id: will resolve other party id from recent messages
    """
    text_val = (body or {}).get("text")
    if not text_val or not isinstance(text_val, str) or not text_val.strip():
        raise HTTPException(status_code=400, detail="Message text is required")
    text_val = text_val.strip()

    # Resolve recipient (other party IG user id)
    other_id: str | None = None
    if conversation_id.startswith("dm:"):
        other_id = conversation_id.split(":", 1)[1] or None
    else:
        # Fallback: infer from existing messages
        with get_session() as session:
            msgs = session.exec(
                select(Message).where(Message.conversation_id == conversation_id).order_by(Message.timestamp_ms.desc()).limit(50)
            ).all()
            for m in msgs:
                # other party is sender on inbound, recipient on outbound
                if (m.direction or "in") == "in" and m.ig_sender_id:
                    other_id = str(m.ig_sender_id)
                    break
                if (m.direction or "in") == "out" and m.ig_recipient_id:
                    other_id = str(m.ig_recipient_id)
                    break
    if not other_id:
        raise HTTPException(status_code=400, detail="Could not resolve recipient for this conversation")

    # Send via Messenger API for Instagram (requires Page token)
    token, entity_id, is_page = _get_base_token_and_id()
    if not is_page:
        raise HTTPException(status_code=400, detail="Sending requires a Page access token (IG_PAGE_ACCESS_TOKEN)")
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"
    url = base + "/me/messages"
    payload = {
        "recipient": {"id": other_id},
        "messaging_type": "RESPONSE",
        "message": {"text": text_val},
    }
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(url, params={"access_token": token}, json=payload, timeout=20)
            r.raise_for_status()
            resp = r.json()
        except httpx.HTTPStatusError as e:
            try:
                detail = e.response.text
            except Exception:
                detail = str(e)
            raise HTTPException(status_code=502, detail=f"Graph send failed: {detail}")
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Graph send failed: {e}")

    # Persist locally
    mid = str((resp or {}).get("message_id") or "")
    now_ms = int(time.time() * 1000)
    conv_id = conversation_id if conversation_id.startswith("dm:") else f"dm:{other_id}"
    with get_session() as session:
        row = Message(
            ig_sender_id=str(entity_id),
            ig_recipient_id=str(other_id),
            ig_message_id=(mid or None),
            text=text_val,
            attachments_json=None,
            timestamp_ms=now_ms,
            raw_json=json.dumps({"send_response": resp}, ensure_ascii=False),
            conversation_id=conv_id,
            direction="out",
        )
        session.add(row)
    try:
        await notify_new_message({"type": "ig_message", "conversation_id": conv_id, "text": text_val, "timestamp_ms": now_ms})
    except Exception:
        pass
    return {"status": "ok", "message_id": mid or None}

