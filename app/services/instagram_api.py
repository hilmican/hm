import os
import datetime as dt
import asyncio
import logging
from typing import Any, Dict, List, Optional

import httpx
import logging
from sqlalchemy import text as _text

from ..db import get_session


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
            status_code = getattr(e.response, 'status_code', None)
            try:
                last_body = e.response.text
            except Exception:
                last_body = None
            
            # Don't retry 403 errors - they're permission issues, not transient
            if status_code == 403:
                try:
                    _log.warning("graph http %s attempt=%s code=%s url=%s body_snip=%s (skipping retries)", 
                                type(e).__name__, attempt+1, status_code, safe_url, 
                                (last_body[:160] if last_body else None))
                except Exception:
                    pass
                # Break immediately - don't retry 403s
                break
            
            try:
                _log.warning("graph http %s attempt=%s code=%s url=%s body_snip=%s", 
                            type(e).__name__, attempt+1, status_code, safe_url, 
                            (last_body[:160] if last_body else None))
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
    # Ensure Instagram platform is selected; without this Graph may return only bare ids
    params["platform"] = "instagram"
    async with httpx.AsyncClient() as client:
        data = await _get(client, base + path, params)
        msgs = data.get("data", []) or []
        # Annotate each message with the Graph conversation id so downstream ingestion
        # can persist Message.conversation_id using this stable identifier.
        for m in msgs:
            if isinstance(m, dict):
                m["__graph_conversation_id"] = str(conversation_id)
        return msgs


async def fetch_message_details(message_id: str) -> Dict[str, Any]:
    """Fetch one message by id with full fields. Used when conversation fetch returns only IDs."""
    token, _, _ = _get_base_token_and_id()
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"
    fields = (
        "id,from{id,username},to,created_time,"
        "message,referral,"
        "attachments{id,mime_type,file_url,image_data{url,preview_url}}"
    )
    path = f"/{message_id}"
    params = {"access_token": token, "fields": fields, "platform": "instagram"}
    async with httpx.AsyncClient() as client:
        data = await _get(client, base + path, params)
        # Some responses wrap in {id:..., ...}; return the object itself
        return data


async def fetch_thread_messages(igba_id: str, ig_user_id: str, limit: int = 200, graph_conversation_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch latest N messages for a thread defined by page/user pair.

    Strategy:
    - If graph_conversation_id is provided, use it directly (1 request).
    - Otherwise, try cached Graph conversation id from DB (1 request if cached).
    - If no cached ID, discover by listing conversations (multiple requests - avoid if possible).

    Args:
        graph_conversation_id: If provided, use this directly instead of discovery (saves API calls).
    """
    try:
        _log.info("ftm.begin igba=%s ig_user_id=%s limit=%s graph_cid=%s", str(igba_id), str(ig_user_id), int(limit), "provided" if graph_conversation_id else "none")
    except Exception:
        pass
    
    # If graph_conversation_id is provided, use it directly (1 request)
    if graph_conversation_id:
        try:
            msgs = await fetch_messages(str(graph_conversation_id), limit=min(max(limit, 1), 200))
            try:
                _log.info("ftm.direct.fetch ok count=%s", len(msgs) if isinstance(msgs, list) else None)
            except Exception:
                pass
            return msgs
        except Exception as e:
            try:
                _log.warning("ftm.direct.fetch failed graph_cid=%s err=%s", str(graph_conversation_id)[:50], str(e)[:200])
            except Exception:
                pass
            # Fall through to cached/discovery if provided ID fails
    
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
            _log.info("ftm.cached conv_id=%s", str(cached_id))
        except Exception:
            pass
        try:
            msgs = await fetch_messages(str(cached_id), limit=min(max(limit, 1), 200))
            # If we successfully fetched something (including empty because of recent-only), return it.
            try:
                _log.info("ftm.cached.fetch ok count=%s", len(msgs) if isinstance(msgs, list) else None)
            except Exception:
                pass
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
        # Owner candidates: page id (entity_id) and IG business user id when available
        owner_candidates = {str(entity_id)}
        try:
            ig_business_id = os.getenv("IG_USER_ID")
            if ig_business_id:
                owner_candidates.add(str(ig_business_id))
        except Exception:
            pass
        # Potential candidates to deep-check via messages (fallback)
        convo_candidates: list[str] = []
        while page_no < max_pages and next_url:
            page_no += 1
            try:
                data = await _get(client, next_url, next_params)
            except Exception:
                data = {}
            convs = data.get("data", []) or []
            try:
                _log.info("ftm.page page=%s convs=%s", page_no, len(convs))
            except Exception:
                pass
            for c in convs:
                cid = str(c.get("id"))
                parts = ((c.get("participants") or {}).get("data") or [])
                ids = {str(p.get("id")) for p in parts if p.get("id")}
                # Prefer exact match: user + one of owner ids
                if (ig_user_id in ids) and (ids & owner_candidates):
                    try:
                        _log.info("ftm.match participants ids_size=%s found_cid=%s", len(ids), cid)
                    except Exception:
                        pass
                    conv_id = cid
                    break
                # Fallback candidate: at least the user is a participant; verify by fetching messages later
                if ig_user_id in ids:
                    try:
                        _log.info("ftm.candidate cid=%s ids_size=%s", cid, len(ids))
                    except Exception:
                        pass
                    convo_candidates.append(cid)
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
        # Fallback: scan a limited number of candidate conversations by checking their messages
        if not conv_id and convo_candidates:
            try:
                _log.info("ftm.scan candidates=%s", len(convo_candidates))
            except Exception:
                pass
            for cid in convo_candidates[:20]:
                try:
                    sample = await fetch_messages(cid, limit=10)
                except Exception:
                    sample = []
                found = False
                for m in (sample or []):
                    frm = ((m.get("from") or {}) or {}).get("id")
                    to = (((m.get("to") or {}) or {}).get("data") or [])
                    recips = {str(x.get("id")) for x in to if x.get("id")}
                    if str(ig_user_id) == str(frm) or str(ig_user_id) in recips:
                        try:
                            _log.info("ftm.scan found cid=%s", cid)
                        except Exception:
                            pass
                        conv_id = cid
                        found = True
                        break
                if found:
                    break
    if not conv_id:
        try:
            _log.info("ftm.end no_conversation_found igba=%s ig_user_id=%s", str(igba_id), str(ig_user_id))
        except Exception:
            pass
        return []
    # Persist mapping for future runs using the unified conversations table
    try:
        from sqlalchemy import text as _t
        from ..db import get_session as _gs
        from .ingest import _get_or_create_conversation_id as _get_conv_id  # lazy import to avoid cycles
        with _gs() as session:
            convo_pk = _get_conv_id(session, str(igba_id), str(ig_user_id))
            if convo_pk is not None:
                try:
                    session.exec(
                        _t(
                            """
                            UPDATE conversations
                            SET graph_conversation_id=:cid, hydrated_at=COALESCE(hydrated_at, CURRENT_TIMESTAMP)
                            WHERE id=:cid_int
                            """
                        ).params(cid=str(conv_id), cid_int=int(convo_pk))
                    )
                except Exception:
                    # Best-effort; mapping can also be established later by other flows
                    pass
    except Exception:
        pass
    try:
        _log.info("ftm.fetch_messages conv_id=%s", str(conv_id))
        msgs = await fetch_messages(conv_id, limit=min(max(limit, 1), 200))
    except Exception:
        msgs = []
    # Annotate messages with the Graph conversation id so downstream upsert can store under Graph id
    try:
        if isinstance(msgs, list):
            for m in msgs:
                if isinstance(m, dict):
                    m["__graph_conversation_id"] = str(conv_id)
    except Exception:
        pass
    try:
        _log.info("ftm.end ok count=%s", len(msgs) if isinstance(msgs, list) else None)
    except Exception:
        pass
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
    """
    Best-effort background sync of latest conversations from Graph.

    For MySQL/SQLite backends this uses dialect-specific *idempotent* inserts
    (`INSERT IGNORE` / `INSERT OR IGNORE`) so that concurrent webhook ingestion
    or other sync jobs do not raise duplicate-key errors on `ig_message_id`.
    """
    conversations = await fetch_conversations(limit=limit)
    saved = 0
    with get_session() as session:
        from .ingest import _get_or_create_conversation_id as _get_conv_id

        # Detect backend once per session to choose the appropriate INSERT style
        try:
            bind = session.get_bind()
            backend = getattr(bind.dialect, "name", "") or ""
        except Exception:
            backend = ""

        for conv in conversations:
            cid = str(conv.get("id"))
            msgs = await fetch_messages(cid, limit=50)
            for m in reversed(msgs):  # oldest first for stable inserts
                mid = str(m.get("id")) if m.get("id") else None
                if not mid:
                    continue

                frm = (m.get("from") or {}).get("id")
                frm_username = (m.get("from") or {}).get("username")
                to = (m.get("to") or {}).get("data") or []
                recipient_id = to[0]["id"] if to else None
                direction = "in"
                _, entity_id, _ = _get_base_token_and_id()
                owner_id = entity_id
                if frm and str(frm) == str(owner_id):
                    direction = "out"
                text_val = m.get("message")
                created_time = m.get("created_time")
                ts_ms = None
                try:
                    dt_obj = (
                        dt.datetime.fromisoformat(created_time.replace("+0000", "+00:00"))
                        if created_time
                        else None
                    )
                    ts_ms = int(dt_obj.timestamp() * 1000) if dt_obj else None
                except Exception:
                    ts_ms = None

                # Resolve or create internal Conversation row using (page, other_user)
                other_party_id = recipient_id if direction == "out" else frm
                convo_pk = None
                try:
                    convo_pk = _get_conv_id(
                        session,
                        str(owner_id),
                        str(other_party_id) if other_party_id is not None else None,
                    )
                except Exception:
                    convo_pk = None

                params = {
                    "ig_sender_id": str(frm) if frm else None,
                    "ig_recipient_id": str(recipient_id) if recipient_id else None,
                    "ig_message_id": mid,
                    "text": text_val,
                    "timestamp_ms": ts_ms,
                    "conversation_id": int(convo_pk) if convo_pk is not None else None,
                    "direction": direction,
                    "sender_username": frm_username,
                }

                # Use idempotent insert based on backend to avoid duplicate-key errors
                if backend == "mysql":
                    stmt = _text(
                        """
                        INSERT IGNORE INTO message (
                            ig_sender_id,
                            ig_recipient_id,
                            ig_message_id,
                            text,
                            timestamp_ms,
                            conversation_id,
                            direction,
                            sender_username
                        ) VALUES (
                            :ig_sender_id,
                            :ig_recipient_id,
                            :ig_message_id,
                            :text,
                            :timestamp_ms,
                            :conversation_id,
                            :direction,
                            :sender_username
                        )
                        """
                    )
                else:
                    # SQLite / others: best-effort ignore on duplicates
                    stmt = _text(
                        """
                        INSERT OR IGNORE INTO message (
                            ig_sender_id,
                            ig_recipient_id,
                            ig_message_id,
                            text,
                            timestamp_ms,
                            conversation_id,
                            direction,
                            sender_username
                        ) VALUES (
                            :ig_sender_id,
                            :ig_recipient_id,
                            :ig_message_id,
                            :text,
                            :timestamp_ms,
                            :conversation_id,
                            :direction,
                            :sender_username
                        )
                        """
                    )

                try:
                    result = session.exec(stmt.params(**params))
                    # rowcount > 0 only when a new row was actually inserted
                    if getattr(result, "rowcount", 0) and result.rowcount > 0:
                        saved += 1
                except Exception:
                    # Best-effort: skip problematic rows rather than failing the whole sync
                    continue

    return saved


