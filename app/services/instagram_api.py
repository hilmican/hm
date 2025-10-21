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
    for attempt in range(3):
        try:
            r = await client.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            last_err = e
            # brief backoff: 0.5s, 1s
            await asyncio.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"Graph API request failed: {type(last_err).__name__}: {last_err}")


async def fetch_conversations(limit: int = 25) -> List[Dict[str, Any]]:
    token, entity_id, is_page = _get_base_token_and_id()
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"
    fields = "id,updated_time,participants,unread_count"
    path = f"/{entity_id}/conversations"
    params = {"access_token": token, "limit": limit, "fields": fields}
    if is_page:
        params["platform"] = "instagram"
    async with httpx.AsyncClient() as client:
        data = await _get(client, base + path, params)
        return data.get("data", [])


async def fetch_messages(conversation_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    token, _, _ = _get_base_token_and_id()
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"
    fields = "id,from,to,created_time,message,attachments"
    path = f"/{conversation_id}/messages"
    params = {"access_token": token, "limit": limit, "fields": fields}
    async with httpx.AsyncClient() as client:
        data = await _get(client, base + path, params)
        return data.get("data", [])


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
                    sender_username=None,
                )
                session.add(row)
                saved += 1
    return saved


