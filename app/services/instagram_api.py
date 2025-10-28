import os
import datetime as dt
import asyncio
import logging
from typing import Any, Dict, List, Optional

import httpx
from sqlmodel import select

from ..db import get_session
from ..models import Message


GRAPH_VERSION = os.getenv("IG_GRAPH_API_VERSION", "v21.0")


def _get_base_token_and_id() -> tuple[str, str, bool]:
    """Return (token, id, is_page) where id is Page ID if available else IG User ID.

    We prefer Page endpoints for conversations when available, otherwise fallback to IG User.
    """
    page_id = os.getenv("IG_PAGE_ID")
    page_token = os.getenv("IG_PAGE_ACCESS_TOKEN")
    if page_id and page_token:
        return page_token, page_id, True
    ig_user_id = os.getenv("IG_USER_ID")
    ig_token = os.getenv("IG_ACCESS_TOKEN")
    if not ig_user_id or not ig_token:
        raise RuntimeError("Missing IG_USER_ID/IG_ACCESS_TOKEN or IG_PAGE_ID/IG_PAGE_ACCESS_TOKEN")
    return ig_token, ig_user_id, False


async def _get(client: httpx.AsyncClient, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """GET with small retry/backoff to handle transient DNS/egress hiccups."""
    last_err: Optional[Exception] = None
    last_body: Optional[str] = None
    for attempt in range(3):
        try:
            r = await client.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            last_err = e
            try:
                last_body = e.response.text
            except Exception:
                last_body = None
            await asyncio.sleep(0.5 * (attempt + 1))
        except httpx.RequestError as e:
            last_err = e
            await asyncio.sleep(0.5 * (attempt + 1))
    detail = f"{type(last_err).__name__}: {last_err}"
    if last_body:
        # include a short snippet of the body for diagnostics
        snippet = last_body[:300].replace("\n", " ")
        detail = f"{detail}; body={snippet}"
    raise RuntimeError(f"Graph API request failed: {detail}")


async def fetch_conversations(limit: int = 25) -> List[Dict[str, Any]]:
    token, entity_id, is_page = _get_base_token_and_id()
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"
    fields = "id,updated_time,participants,unread_count"
    path = f"/{entity_id}/conversations"
    params = {"access_token": token, "limit": limit, "fields": fields}
    # Explicitly set platform for Instagram; some accounts require this even with IG User ID
    params["platform"] = "instagram"
    async with httpx.AsyncClient() as client:
        data = await _get(client, base + path, params)
        return data.get("data", [])


async def fetch_messages(conversation_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    token, _, _ = _get_base_token_and_id()
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"
    fields = "id,from{id,username},to,created_time,message,attachments"
    path = f"/{conversation_id}/messages"
    params = {"access_token": token, "limit": limit, "fields": fields}
    async with httpx.AsyncClient() as client:
        data = await _get(client, base + path, params)
        return data.get("data", [])


async def fetch_thread_messages(igba_id: str, ig_user_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    """Fetch latest N messages for a thread defined by page/user pair.

    Strategy:
    - First resolve the conversation id by listing page/user conversations and
      matching the participant set (best-effort and limited).
    - Then fetch messages for that conversation id.
    """
    try:
        convs = await fetch_conversations(limit=50)
    except Exception:
        convs = []
    conv_id: Optional[str] = None
    for c in convs:
        cid = str(c.get("id"))
        parts = ((c.get("participants") or {}).get("data") or [])
        ids = {str(p.get("id")) for p in parts if p.get("id")}
        if igba_id in ids and ig_user_id in ids:
            conv_id = cid
            break
    if not conv_id:
        return []
    try:
        msgs = await fetch_messages(conv_id, limit=min(max(limit, 1), 200))
    except Exception:
        msgs = []
    return msgs

async def fetch_user_username(user_id: str) -> Optional[str]:
    token, _, _ = _get_base_token_and_id()
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"
    path = f"/{user_id}"
    params = {"access_token": token, "fields": "username,name"}
    async with httpx.AsyncClient() as client:
        data = await _get(client, base + path, params)
        return data.get("username") or data.get("name")


async def sync_latest_conversations(limit: int = 25) -> int:
    conversations = await fetch_conversations(limit=limit)
    saved = 0
    with get_session() as session:
        for conv in conversations:
            cid = str(conv.get("id"))
            msgs = await fetch_messages(cid, limit=50)
            for m in reversed(msgs):  # oldest first for stable inserts
                mid = str(m.get("id")) if m.get("id") else None
                if not mid:
                    continue
                # dedupe by ig_message_id
                exists = session.exec(select(Message).where(Message.ig_message_id == mid)).first()
                if exists:
                    continue
                frm = (m.get("from") or {}).get("id")
                frm_username = (m.get("from") or {}).get("username")
                to = (m.get("to") or {}).get("data") or []
                recipient_id = to[0]["id"] if to else None
                direction = "in"
                token, entity_id, is_page = _get_base_token_and_id()
                owner_id = entity_id
                if frm and str(frm) == str(owner_id):
                    direction = "out"
                text = m.get("message")
                created_time = m.get("created_time")
                ts_ms = None
                try:
                    dt_obj = dt.datetime.fromisoformat(created_time.replace("+0000", "+00:00")) if created_time else None
                    ts_ms = int(dt_obj.timestamp() * 1000) if dt_obj else None
                except Exception:
                    ts_ms = None
                row = Message(
                    ig_sender_id=str(frm) if frm else None,
                    ig_recipient_id=str(recipient_id) if recipient_id else None,
                    ig_message_id=mid,
                    text=text,
                    attachments_json=None,
                    timestamp_ms=ts_ms,
                    raw_json=None,
                    conversation_id=cid,
                    direction=direction,
                    sender_username=frm_username,
                )
                session.add(row)
                saved += 1
    return saved


