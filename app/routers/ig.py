from fastapi import APIRouter, Request, HTTPException
from fastapi import WebSocket, WebSocketDisconnect
import logging
from sqlmodel import select

from ..db import get_session
from ..models import Message
from sqlmodel import select
from ..services.instagram_api import sync_latest_conversations, fetch_user_username
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
        # Fetch only lightweight columns for recent messages to reduce IO
        from sqlalchemy import text as _text
        sample_n = max(50, min(int(limit or 25) * 8, 400))
        rows_raw = session.exec(
            _text(
                """
                SELECT conversation_id, timestamp_ms, text, sender_username, direction,
                       ig_sender_id, ig_recipient_id, ad_link, ad_title
                FROM message
                WHERE conversation_id IS NOT NULL
                ORDER BY timestamp_ms DESC
                LIMIT :n
                """
            ).params(n=int(sample_n))
        ).all()
        # Normalize rows into dicts for template use
        rows = []
        for r in rows_raw:
            try:
                rows.append({
                    "conversation_id": (r.conversation_id if hasattr(r, "conversation_id") else r[0]),
                    "timestamp_ms": (r.timestamp_ms if hasattr(r, "timestamp_ms") else r[1]),
                    "text": (r.text if hasattr(r, "text") else r[2]),
                    "sender_username": (r.sender_username if hasattr(r, "sender_username") else r[3]),
                    "direction": (r.direction if hasattr(r, "direction") else r[4]),
                    "ig_sender_id": (r.ig_sender_id if hasattr(r, "ig_sender_id") else r[5]),
                    "ig_recipient_id": (r.ig_recipient_id if hasattr(r, "ig_recipient_id") else r[6]),
                    "ad_link": (r.ad_link if hasattr(r, "ad_link") else r[7]),
                    "ad_title": (r.ad_title if hasattr(r, "ad_title") else r[8]),
                })
            except Exception:
                continue
        conv_map = {}
        other_ids: set[str] = set()
        for m in rows:
            cid = m.get("conversation_id") if isinstance(m, dict) else m.conversation_id
            if not cid:
                continue
            if cid not in conv_map:
                conv_map[cid] = m
            # Determine the other party id for this message
            other = None
            try:
                direction = (m.get("direction") if isinstance(m, dict) else m.direction) or "in"
                if direction == "out":
                    other = m.get("ig_recipient_id") if isinstance(m, dict) else m.ig_recipient_id
                else:
                    other = m.get("ig_sender_id") if isinstance(m, dict) else m.ig_sender_id
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
                cid = m.get("conversation_id") if isinstance(m, dict) else m.conversation_id
                if not cid:
                    continue
                direction = (m.get("direction") if isinstance(m, dict) else m.direction) or "in"
                sender_username = (m.get("sender_username") if isinstance(m, dict) else m.sender_username) or ""
                if direction == "in" and sender_username.strip() and cid not in inbound_named:
                    inbound_named[cid] = str(sender_username).strip()
            for cid, un in inbound_named.items():
                labels[cid] = f"@{un}"
        except Exception:
            pass
        # Fallback via ig_users when inbox usernames missing; if missing there, resolve on-demand from Graph
        if other_ids:
            try:
                missing = [cid for cid in conv_map.keys() if cid not in labels]
                if missing:
                    placeholders = ",".join([":p" + str(i) for i in range(len(other_ids))])
                    from sqlalchemy import text as _text
                    params = {("p" + str(i)): list(other_ids)[i] for i in range(len(other_ids))}
                    rows_u = session.exec(_text(f"SELECT ig_user_id, username FROM ig_users WHERE ig_user_id IN ({placeholders})")).params(**params).all()
                    id_to_username: dict[str, str] = {}
                    ids_without_username: list[str] = []
                    for r in rows_u:
                        uid = r.ig_user_id if hasattr(r, "ig_user_id") else r[0]
                        un = r.username if hasattr(r, "username") else r[1]
                        if uid and un:
                            id_to_username[str(uid)] = str(un)
                        elif uid:
                            ids_without_username.append(str(uid))
                    # Fetch a small batch of missing usernames on-demand to improve UX
                    to_fetch = ids_without_username[: min(20, len(ids_without_username))]
                    if to_fetch:
                        import asyncio as _asyncio
                        async def _fetch_all(ids: list[str]) -> dict[str, str]:
                            out: dict[str, str] = {}
                            async def _one(uid: str) -> None:
                                try:
                                    uname = await fetch_user_username(uid)
                                    if uname:
                                        out[uid] = str(uname)
                                except Exception:
                                    pass
                            await _asyncio.gather(*[_one(u) for u in ids])
                            return out
                        resolved = await _fetch_all(to_fetch)
                        if resolved:
                            # persist and update map
                            try:
                                for uid, uname in resolved.items():
                                    session.exec(_text("UPDATE ig_users SET username=:u, fetched_at=CURRENT_TIMESTAMP, fetch_status='ok' WHERE ig_user_id=:id").params(u=uname, id=uid))
                                    id_to_username[uid] = uname
                            except Exception:
                                pass
                    for cid, m in conv_map.items():
                        if cid in labels:
                            continue
                        other = None
                        try:
                            direction = (m.get("direction") if isinstance(m, dict) else m.direction) or "in"
                            if direction == "out":
                                other = m.get("ig_recipient_id") if isinstance(m, dict) else m.ig_recipient_id
                            else:
                                other = m.get("ig_sender_id") if isinstance(m, dict) else m.ig_sender_id
                        except Exception:
                            other = None
                        if other and str(other) in id_to_username:
                            labels[cid] = f"@{id_to_username[str(other)]}"
            except Exception:
                pass
        # Best-effort ad metadata from messages
        ad_map = {}
        for m in rows:
            cid = m.get("conversation_id") if isinstance(m, dict) else m.conversation_id
            if not cid:
                continue
            ad_link = (m.get("ad_link") if isinstance(m, dict) else m.ad_link)
            ad_title = (m.get("ad_title") if isinstance(m, dict) else m.ad_title)
            if (ad_link or ad_title) and cid not in ad_map:
                ad_map[cid] = {"link": ad_link, "title": ad_title}
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
async def thread(request: Request, conversation_id: str, limit: int = 100):
    with get_session() as session:
        msgs = session.exec(
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.timestamp_ms.desc())
            .limit(min(max(limit, 1), 500))
        ).all()
        # chronological order for display
        msgs = list(reversed(msgs))
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
        # Resolve per-message sender usernames via ig_users as fallback for webhook-ingested rows.
        # If missing in DB, resolve on-demand from Graph to populate UI immediately.
        usernames: dict[str, str] = {}
        try:
            sender_ids: list[str] = []
            for mm in msgs:
                if mm.ig_sender_id:
                    sid = str(mm.ig_sender_id)
                    if sid not in sender_ids:
                        sender_ids.append(sid)
            if sender_ids:
                placeholders = ",".join([":p" + str(i) for i in range(len(sender_ids))])
                from sqlalchemy import text as _text
                params = {("p" + str(i)): sender_ids[i] for i in range(len(sender_ids))}
                rows_u = session.exec(_text(f"SELECT ig_user_id, username FROM ig_users WHERE ig_user_id IN ({placeholders})")).params(**params).all()
                ids_without_username: list[str] = []
                for r in rows_u:
                    uid = r.ig_user_id if hasattr(r, "ig_user_id") else r[0]
                    un = r.username if hasattr(r, "username") else r[1]
                    if uid and un:
                        usernames[str(uid)] = str(un)
                    elif uid:
                        ids_without_username.append(str(uid))
                # On-demand resolve a small batch for better UX
                to_fetch = ids_without_username[: min(20, len(ids_without_username))]
                if to_fetch:
                    import asyncio as _asyncio
                    async def _fetch_all(ids: list[str]) -> dict[str, str]:
                        out: dict[str, str] = {}
                        async def _one(uid: str) -> None:
                            try:
                                uname = await fetch_user_username(uid)
                                if uname:
                                    out[uid] = str(uname)
                            except Exception:
                                pass
                        await _asyncio.gather(*[_one(u) for u in ids])
                        return out
                    resolved = await _fetch_all(to_fetch)
                    if resolved:
                        try:
                            for uid, uname in resolved.items():
                                session.exec(_text("UPDATE ig_users SET username=:u, fetched_at=CURRENT_TIMESTAMP, fetch_status='ok' WHERE ig_user_id=:id").params(u=uname, id=uid))
                                usernames[uid] = uname
                        except Exception:
                            pass
        except Exception:
            usernames = {}

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
        return templates.TemplateResponse("ig_thread.html", {"request": request, "conversation_id": conversation_id, "messages": msgs, "other_label": other_label, "att_map": att_map, "att_ids_map": att_ids_map, "usernames": usernames})


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

