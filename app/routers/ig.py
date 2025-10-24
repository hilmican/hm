from fastapi import APIRouter, Request
from fastapi import WebSocket, WebSocketDisconnect
import logging
from sqlmodel import select

from ..db import get_session
from ..models import Message
from ..services.instagram_api import sync_latest_conversations


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
def inbox(request: Request, limit: int = 25):
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
        templates = request.app.state.templates
        return templates.TemplateResponse("ig_inbox.html", {"request": request, "conversations": conversations})


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
        templates = request.app.state.templates
        return templates.TemplateResponse("ig_thread.html", {"request": request, "conversation_id": conversation_id, "messages": msgs})


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


