from fastapi import APIRouter, Request, HTTPException
from fastapi import WebSocket, WebSocketDisconnect
import logging
from sqlmodel import select

from ..db import get_session
from ..models import Message
from sqlmodel import select
from ..services.instagram_api import sync_latest_conversations
import json
from pathlib import Path
from fastapi.responses import FileResponse


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
        # Latest conversations by most recent message
        rows = session.exec(select(Message).order_by(Message.timestamp_ms.desc()).limit(200)).all()
        # Group by conversation_id
        conv_map = {}
        for m in rows:
            if not m.conversation_id:
                continue
            if m.conversation_id not in conv_map:
                conv_map[m.conversation_id] = m
        conversations = list(conv_map.values())[:limit]
        # Build labels and ad map
        labels = {}
        ad_map = {}
        for m in rows:
            cid = m.conversation_id
            if not cid:
                continue
            if (m.direction or "in") == "in" and m.sender_username and cid not in labels:
                labels[cid] = f"@{m.sender_username}"
            if not labels.get(cid) and m.sender_username:
                labels[cid] = f"@{m.sender_username}"
            if (m.ad_link or m.ad_title) and cid not in ad_map:
                ad_map[cid] = {"link": m.ad_link, "title": m.ad_title}
        # Do not perform any external lookups here to keep inbox fast.
        templates = request.app.state.templates
        return templates.TemplateResponse("ig_inbox.html", {"request": request, "conversations": conversations, "labels": labels, "ad_map": ad_map})


@router.post("/inbox/refresh")
async def refresh_inbox(limit: int = 25):
    try:
        saved = await sync_latest_conversations(limit=limit)
        try:
            _log.info("Inbox refresh: saved=%d", saved)
        except Exception:
            pass
        return {"status": "ok", "saved": saved}
    except Exception as e:
        try:
            _log.exception("Inbox refresh failed: %s", e)
        except Exception:
            pass
        # Return 200 so the frontend doesn't alert; page will reload regardless
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
        other_label = None
        for mm in msgs:
            if (mm.direction or "in") == "in" and mm.sender_username:
                other_label = f"@{mm.sender_username}"
        if not other_label:
            for mm in msgs:
                if mm.sender_username:
                    other_label = f"@{mm.sender_username}"
                    break
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


