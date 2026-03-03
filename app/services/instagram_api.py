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
            
            # Don't retry 400 errors for nonexisting referral field - not transient, message type doesn't support it
            if status_code == 400 and last_body and "nonexisting field (referral)" in last_body:
                try:
                    _log.warning("graph http %s attempt=%s code=%s url=%s body_snip=%s (skipping retries - referral not supported)", 
                                type(e).__name__, attempt+1, status_code, safe_url, 
                                (last_body[:160] if last_body else None))
                except Exception:
                    pass
                # Break immediately - don't retry for this specific error
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
    # Use max limit per page (100 is Graph API max for messages)
    page_limit = min(limit, 100) if limit > 0 else 100
    params = {"access_token": token, "limit": page_limit, "fields": fields}
    # Ensure Instagram platform is selected; without this Graph may return only bare ids
    params["platform"] = "instagram"
    
    all_msgs: List[Dict[str, Any]] = []
    next_url: Optional[str] = None
    max_pages = 50  # Safety limit to prevent infinite loops
    page_count = 0
    
    async with httpx.AsyncClient() as client:
        while page_count < max_pages:
            if next_url:
                # Use the pagination URL directly
                try:
                    data = await _get(client, next_url, {})
                except Exception as e:
                    try:
                        _log.warning("fetch_messages pagination failed url=%s err=%s", next_url[:100], str(e)[:200])
                    except Exception:
                        pass
                    break
            else:
                # First request
                data = await _get(client, base + path, params)
            
            msgs = data.get("data", []) or []
            # Annotate each message with the Graph conversation id so downstream ingestion
            # can persist Message.conversation_id using this stable identifier.
            for m in msgs:
                if isinstance(m, dict):
                    m["__graph_conversation_id"] = str(conversation_id)
            all_msgs.extend(msgs)
            
            # Check for pagination
            paging = data.get("paging", {})
            next_url = paging.get("next")
            if not next_url:
                break
            
            # If limit was specified and we've reached it, stop
            if limit > 0 and len(all_msgs) >= limit:
                all_msgs = all_msgs[:limit]
                break
            
            page_count += 1
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.2)
        
        if page_count >= max_pages:
            try:
                _log.warning("fetch_messages hit max_pages limit conv_id=%s total_msgs=%s", str(conversation_id)[:50], len(all_msgs))
            except Exception:
                pass
        
        return all_msgs


async def fetch_message_details(message_id: str, include_referral: bool = True) -> Dict[str, Any]:
    """Fetch one message by id with full fields. Used when conversation fetch returns only IDs."""
    token, _, _ = _get_base_token_and_id()
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"
    fields = [
        "id",
        "from{id,username}",
        "to",
        "created_time",
        "message",
        "attachments{id,mime_type,file_url,image_data{url,preview_url}}",
    ]
    if include_referral:
        fields.insert(-1, "referral")
    path = f"/{message_id}"
    params = {"access_token": token, "fields": ",".join(fields), "platform": "instagram"}
    async with httpx.AsyncClient() as client:
        try:
            data = await _get(client, base + path, params)
            return data
        except RuntimeError as err:
            detail = str(err)
            if include_referral and "nonexisting field (referral)" in detail:
                try:
                    _log.warning("fetch_message_details: retrying without referral mid=%s", str(message_id)[:60])
                except Exception:
                    pass
                return await fetch_message_details(message_id, include_referral=False)
            raise


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

    Uses MySQL *idempotent* inserts (`INSERT IGNORE`) so that concurrent webhook ingestion
    or other sync jobs do not raise duplicate-key errors on `ig_message_id`.
    """
    conversations = await fetch_conversations(limit=limit)
    saved = 0
    with get_session() as session:
        from .ingest import _get_or_create_conversation_id as _get_conv_id

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

                # Use MySQL idempotent insert to avoid duplicate-key errors
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

                try:
                    result = session.exec(stmt.params(**params))
                    # rowcount > 0 only when a new row was actually inserted
                    if getattr(result, "rowcount", 0) and result.rowcount > 0:
                        saved += 1
                except Exception:
                    # Best-effort: skip problematic rows rather than failing the whole sync
                    continue

    return saved


def _make_absolute_url(url: str) -> str:
    """Convert relative URL to absolute; uses shared helper."""
    from .image_urls import make_absolute_image_url
    return make_absolute_image_url(url or "")


async def send_message(conversation_id: str, text: str, image_urls: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Send a message to an Instagram conversation via Graph API.
    
    Args:
        conversation_id: Graph conversation ID or dm:<ig_user_id> format
        text: Message text to send
        image_urls: Optional list of image URLs to send before the text
        
    Returns:
        Dict with message_id and status
    """
    token, entity_id, is_page = _get_base_token_and_id()
    if not is_page:
        raise RuntimeError("Sending requires a Page access token (IG_PAGE_ACCESS_TOKEN)")
    
    # Resolve recipient ID from conversation_id
    recipient_id: Optional[str] = None
    if conversation_id.startswith("dm:"):
        recipient_id = conversation_id.split(":", 1)[1] or None
    else:
        # For Graph conversation IDs, we need to resolve the other party
        # Try to get from conversations table
        from sqlalchemy import text as _text
        from ..db import get_session
        with get_session() as session:
            row = session.exec(
                _text(
                    "SELECT ig_user_id FROM conversations WHERE graph_conversation_id=:gc OR id=:gc LIMIT 1"
                ).params(gc=str(conversation_id))
            ).first()
            if row:
                recipient_id = (row.ig_user_id if hasattr(row, "ig_user_id") else (row[0] if len(row) > 0 else None)) or None
    
    if not recipient_id:
        raise RuntimeError(f"Could not resolve recipient for conversation_id={conversation_id}")
    
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"
    url = base + "/me/messages"
    
    image_urls = image_urls or []
    from .image_urls import normalize_image_urls_for_send
    absolute_image_urls = normalize_image_urls_for_send(image_urls)
    received_n = len(image_urls)
    after_filter_n = len(absolute_image_urls)
    _log.info(
        "Instagram send_message: image_urls received=%d after_absolute_filter=%d recipient_id=%s",
        received_n,
        after_filter_n,
        (recipient_id[:20] if recipient_id else ""),
    )
    if received_n and after_filter_n == 0:
        _log.warning(
            "Instagram send_message: all %d image URL(s) dropped (none absolute). First: %s. Set IMAGE_CDN_BASE_URL or APP_URL.",
            received_n,
            (image_urls[0][:120] if image_urls else ""),
        )
    results: Dict[str, Any] = {"message_ids": [], "status": "ok"}
    # Görseller arası gecikme (rate limit / "1 gönderiyor 1 göndermiyor" azaltmak için). Varsayılan 1.2s.
    image_delay_sec = float(os.getenv("IG_IMAGE_SEND_DELAY_SEC", "1.2"))
    # Başarısız gönderimden sonra ek bekleme (rate limit toparlanması)
    image_delay_after_fail_sec = float(os.getenv("IG_IMAGE_DELAY_AFTER_FAIL_SEC", "2.5"))

    async def _send_one_image(img_url: str) -> bool:
        img_payload = {
            "recipient": {"id": recipient_id},
            "messaging_type": "RESPONSE",
            "message": {
                "attachment": {
                    "type": "image",
                    "payload": {"url": img_url},
                }
            },
        }
        for attempt in range(2):  # İlk deneme + 1 retry
            try:
                r_img = await client.post(
                    url,
                    params={"access_token": token},
                    json=img_payload,
                    timeout=25,
                )
                r_img.raise_for_status()
                resp_img = r_img.json()
                if resp_img.get("message_id"):
                    results["message_ids"].append(resp_img["message_id"])
                    return True
            except httpx.HTTPStatusError as e:
                try:
                    body = (e.response.text or "")[:500]
                    code = getattr(e.response, "status_code", None)
                except Exception:
                    body = str(e)[:500]
                    code = None
                _log.warning(
                    "Instagram image API error attempt=%s status=%s url=%s body=%s",
                    attempt + 1,
                    code,
                    img_url[:80],
                    body,
                )
                if attempt == 0:
                    await asyncio.sleep(1.0)
            except Exception as e:
                _log.warning(
                    "Instagram image send exception attempt=%s url=%s err=%s",
                    attempt + 1,
                    img_url[:80],
                    str(e)[:300],
                )
                if attempt == 0:
                    await asyncio.sleep(1.0)
        return False

    async with httpx.AsyncClient() as client:
        # 1) Send TEXT first so the user always gets the welcome message even if images hit rate limit
        if text and text.strip():
            text_lines = [line.strip() for line in text.split('\n') if line.strip()]
            if not text_lines:
                text_lines = [text.strip()]
            _log.info(
                "Instagram send_message: sending %d text line(s) first recipient_id=%s",
                len(text_lines),
                recipient_id[:20] if recipient_id else "",
            )
            for idx, line_text in enumerate(text_lines):
                payload = {
                    "recipient": {"id": recipient_id},
                    "messaging_type": "RESPONSE",
                    "message": {"text": line_text},
                }
                try:
                    r = await client.post(url, params={"access_token": token}, json=payload, timeout=20)
                    r.raise_for_status()
                    resp = r.json()
                    if resp.get("message_id"):
                        if idx == 0:
                            results["message_id"] = resp["message_id"]
                        results["message_ids"].append(resp["message_id"])
                    if idx < len(text_lines) - 1:
                        await asyncio.sleep(0.3)
                except httpx.HTTPStatusError as e:
                    try:
                        detail = e.response.text
                    except Exception:
                        detail = str(e)
                    raise RuntimeError(f"Graph send failed (message {idx + 1}/{len(text_lines)}): {detail}")
                except Exception as e:
                    raise RuntimeError(f"Graph send failed (message {idx + 1}/{len(text_lines)}): {e}")
        elif (image_urls or []) and not (text or "").strip():
            _log.warning(
                "Instagram send_message: text empty, sending only images recipient_id=%s",
                recipient_id[:20] if recipient_id else "",
            )

        # 2) Then send images; short delay after text to reduce rate limit
        n_images = len(absolute_image_urls)
        if n_images:
            await asyncio.sleep(0.5)
            _log.info(
                "Instagram send_message: recipient_id=%s image_count=%d (from %d requested)",
                recipient_id[:20] if recipient_id else "",
                n_images,
                len(image_urls or []),
            )
            sent_count = 0
            for i, img_url in enumerate(absolute_image_urls):
                if i > 0 and image_delay_sec > 0:
                    await asyncio.sleep(image_delay_sec)
                ok = await _send_one_image(img_url)
                if ok:
                    sent_count += 1
                    mid = results["message_ids"][-1] if results["message_ids"] else None
                    _log.info(
                        "Instagram image sent idx=%d/%d message_id=%s url=%s",
                        i + 1,
                        n_images,
                        mid,
                        (img_url[:60] + "..." if len(img_url) > 60 else img_url),
                    )
                else:
                    _log.warning(
                        "Instagram image failed idx=%d/%d url=%s",
                        i + 1,
                        n_images,
                        (img_url[:60] + "..." if len(img_url) > 60 else img_url),
                    )
                    if image_delay_after_fail_sec > 0:
                        await asyncio.sleep(image_delay_after_fail_sec)
            results["image_message_count"] = sent_count
            _log.info(
                "Instagram send_message: image summary sent=%d requested=%d recipient_id=%s",
                sent_count,
                n_images,
                recipient_id[:20] if recipient_id else "",
            )
            if sent_count < n_images:
                _log.warning(
                    "Instagram images partial send: %d/%d succeeded (check logs above for API errors)",
                    sent_count,
                    n_images,
                )
    
    return results


