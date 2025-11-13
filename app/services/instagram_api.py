import os
import datetime as dt
import asyncio
import logging
from typing import Any, Dict, List, Optional

import httpx
import logging
from sqlmodel import select

from ..db import get_session
from ..models import Message


GRAPH_VERSION = os.getenv("IG_GRAPH_API_VERSION", "v21.0")
_log = logging.getLogger("graph.api")


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
    safe_url = url.split("?")[0]
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
            try:
                _log.warning("graph http %s attempt=%s code=%s url=%s body_snip=%s", type(e).__name__, attempt+1, getattr(e.response, 'status_code', None), safe_url, (last_body[:160] if last_body else None))
            except Exception:
                pass
            await asyncio.sleep(0.5 * (attempt + 1))
        except httpx.RequestError as e:
            last_err = e
            try:
                _log.warning("graph reqerr %s attempt=%s url=%s detail=%s", type(e).__name__, attempt+1, safe_url, str(e))
            except Exception:
                pass
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
    # Request referral metadata and richer attachments to enable ad-context + image rendering after hydrate
    fields = (
        "id,from{id,username},to,created_time,"
        "message,referral,"  # ad reply context (best-effort; ignored if unavailable)
        "attachments{"
        "id,mime_type,file_url,image_data{url,preview_url}"
        "}"
    )
    path = f"/{conversation_id}/messages"
    params = {"access_token": token, "limit": limit, "fields": fields}
    async with httpx.AsyncClient() as client:
        data = await _get(client, base + path, params)
        return data.get("data", [])


async def fetch_thread_messages(igba_id: str, ig_user_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    """Fetch latest N messages for a thread defined by page/user pair.

    Strategy:
    - First resolve the conversation id by listing page/user conversations and
      matching the participant set. Prefer cached mapping when available; otherwise
      page across conversations until found (best-effort).
    - Then fetch messages for that conversation id.
    """
    # 0) Try cached Graph conversation id from our DB
    cached_id: Optional[str] = None
    try:
        from sqlalchemy import text as _t
        from ..db import get_session as _gs
        with _gs() as session:
            row = session.exec(
                _t(
                    "SELECT graph_conversation_id FROM conversations WHERE igba_id=:g AND ig_user_id=:u ORDER BY last_message_at DESC LIMIT 1"
                ).params(g=str(igba_id), u=str(ig_user_id))
            ).first()
            if row:
                cached_id = (row.graph_conversation_id if hasattr(row, "graph_conversation_id") else (row[0] if len(row) > 0 else None)) or None
    except Exception:
        cached_id = None
    if cached_id:
        try:
            msgs = await fetch_messages(str(cached_id), limit=min(max(limit, 1), 200))
            # If we successfully fetched something (including empty because of recent-only), return it.
            return msgs
        except Exception:
            # Fall back to discovery if cached id is stale/invalid
            pass

    token, entity_id, is_page = _get_base_token_and_id()
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"
    fields = "id,updated_time,participants,unread_count"
    path = f"/{entity_id}/conversations"
    params = {"access_token": token, "limit": 50, "fields": fields, "platform": "instagram"}
    conv_id: Optional[str] = None
    # Page through up to max_pages of conversations to find the matching participant set
    max_pages = 10
    page_no = 0
    async with httpx.AsyncClient() as client:
        next_url: Optional[str] = base + path
        next_params: Dict[str, Any] = params
        while page_no < max_pages and next_url:
            page_no += 1
            try:
                data = await _get(client, next_url, next_params)
            except Exception:
                data = {}
            convs = data.get("data", []) or []
            for c in convs:
                cid = str(c.get("id"))
                parts = ((c.get("participants") or {}).get("data") or [])
                ids = {str(p.get("id")) for p in parts if p.get("id")}
                if igba_id in ids and ig_user_id in ids:
                    conv_id = cid
                    break
            if conv_id:
                break
            # advance pagination
            paging = data.get("paging") or {}
            nurl = paging.get("next")
            if nurl and isinstance(nurl, str) and nurl.strip():
                next_url = nurl
                next_params = {}  # the 'next' URL already contains the token and fields
            else:
                next_url = None
    if not conv_id:
        return []
    # Persist mapping for future runs
    try:
        from sqlalchemy import text as _t
        from ..db import get_session as _gs
        with _gs() as session:
            # Best-effort: update existing rows for this pair
            session.exec(
                _t(
                    """
                    UPDATE conversations
                    SET graph_conversation_id=:cid
                    WHERE igba_id=:g AND ig_user_id=:u
                    """
                ).params(cid=str(conv_id), g=str(igba_id), u=str(ig_user_id))
            )
    except Exception:
        pass
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


