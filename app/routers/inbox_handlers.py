from fastapi import APIRouter, Request

from ..db import get_session
from ..services.queue import enqueue, _get_redis
from ..models import AdminMessage
from sqlmodel import select

router = APIRouter()


@router.get("/inbox")
async def inbox(
    request: Request,
    limit: int = 25,
    q: str | None = None,
    has_ad: str | None = None,
):
    with get_session() as session:
        # Use unified conversations table as single source for inbox list
        from sqlalchemy import text as _text
        # Check if new columns exist (for backward compatibility before migration)
        try:
            check_cols = session.exec(_text("""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'conversations' 
                AND COLUMN_NAME IN ('last_link_type', 'last_link_id')
            """)).all()
            has_new_cols = len(check_cols) >= 2
        except Exception:
            has_new_cols = False
        
        if has_new_cols:
            base_sql = """
                SELECT c.id AS convo_id,
                       c.last_message_timestamp_ms AS timestamp_ms,
                       c.last_message_text AS text,
                       c.last_sender_username AS sender_username,
                       c.last_message_direction AS direction,
                       c.ig_sender_id,
                       c.ig_recipient_id,
                       c.ig_user_id,
                       c.last_ad_link AS ad_link,
                       c.last_ad_title AS ad_title,
                       c.last_message_id AS message_id,
                       c.last_ad_id AS last_ad_id,
                       c.last_link_type AS last_link_type,
                       c.last_link_id AS last_link_id,
                       u.username AS other_username,
                       u.name AS other_name
                FROM conversations c
                LEFT JOIN ig_users u
                  ON u.ig_user_id = c.ig_user_id
                LEFT JOIN ads_products ap
                  ON ap.ad_id = COALESCE(c.last_link_id, c.last_ad_id) AND ap.link_type = COALESCE(c.last_link_type, 'ad')
            """
        else:
            # Fallback for databases that haven't run migration yet
            base_sql = """
                SELECT c.id AS convo_id,
                       c.last_message_timestamp_ms AS timestamp_ms,
                       c.last_message_text AS text,
                       c.last_sender_username AS sender_username,
                       c.last_message_direction AS direction,
                       c.ig_sender_id,
                       c.ig_recipient_id,
                       c.ig_user_id,
                       c.last_ad_link AS ad_link,
                       c.last_ad_title AS ad_title,
                       c.last_message_id AS message_id,
                       c.last_ad_id AS last_ad_id,
                       NULL AS last_link_type,
                       NULL AS last_link_id,
                       u.username AS other_username,
                       u.name AS other_name
                FROM conversations c
                LEFT JOIN ig_users u
                  ON u.ig_user_id = c.ig_user_id
                LEFT JOIN ads_products ap
                  ON ap.ad_id = c.last_ad_id AND (ap.link_type = 'ad' OR ap.link_type IS NULL)
            """
        where_parts: list[str] = []
        params: dict[str, object] = {}
        if q and isinstance(q, str) and q.strip():
            qq = f"%{q.lower().strip()}%"
            where_parts.append("""
                (
                    (c.last_message_text IS NOT NULL AND LOWER(c.last_message_text) LIKE :qq)
                    OR (c.last_sender_username IS NOT NULL AND LOWER(c.last_sender_username) LIKE :qq)
                    OR EXISTS (
                        SELECT 1 FROM ig_users u
                        WHERE (u.ig_user_id = c.ig_sender_id OR u.ig_user_id = c.ig_recipient_id)
                          AND (
                            (u.name IS NOT NULL AND LOWER(u.name) LIKE :qq)
                            OR (u.username IS NOT NULL AND LOWER(u.username) LIKE :qq)
                          )
                    )
                )
            """)
            params["qq"] = qq
        # Optional filter: restrict to conversations with ads/posts / unlinked ads/posts
        has_ad_s = (has_ad or "").strip().lower()
        if has_ad_s in ("yes", "true", "1", "any"):
            if has_new_cols:
                where_parts.append("(c.last_link_id IS NOT NULL OR c.last_ad_id IS NOT NULL)")
            else:
                where_parts.append("c.last_ad_id IS NOT NULL")
        elif has_ad_s in ("unlinked", "missing_product", "no_product"):
            # Conversations whose latest link (ad or post) has no product mapping yet
            if has_new_cols:
                where_parts.append("(c.last_link_id IS NOT NULL OR c.last_ad_id IS NOT NULL) AND ap.product_id IS NULL")
            else:
                where_parts.append("c.last_ad_id IS NOT NULL AND ap.product_id IS NULL")
        sample_n = max(50, min(int(limit or 25) * 4, 200))
        bind = session.get_bind()
        dialect_name = ""
        try:
            if bind is not None and getattr(bind, "dialect", None):
                dialect_name = bind.dialect.name.lower()
        except Exception:
            dialect_name = ""
        # Sort strictly by last message timestamp (ms since epoch), newest first
        order_sql = " ORDER BY c.last_message_timestamp_ms DESC LIMIT :n"
        params["n"] = int(sample_n)
        final_sql = base_sql + (" WHERE " + " AND ".join(where_parts) if where_parts else "") + order_sql
        rows_raw = session.exec(_text(final_sql).params(**params)).all()
        # Normalize rows into dicts for template use; fall back convo id when preview missing
        rows = []
        for r in rows_raw:
            try:
                cid = (r.convo_id if hasattr(r, "convo_id") else r[0])
                rows.append({
                    "conversation_id": cid,
                    "timestamp_ms": (getattr(r, "timestamp_ms", None) if hasattr(r, "timestamp_ms") else (r[1] if len(r) > 1 else None)),
                    "text": (getattr(r, "text", None) if hasattr(r, "text") else (r[2] if len(r) > 2 else None)),
                    "sender_username": (getattr(r, "sender_username", None) if hasattr(r, "sender_username") else (r[3] if len(r) > 3 else None)),
                    "direction": (getattr(r, "direction", None) if hasattr(r, "direction") else (r[4] if len(r) > 4 else None)),
                    "ig_sender_id": (getattr(r, "ig_sender_id", None) if hasattr(r, "ig_sender_id") else (r[5] if len(r) > 5 else None)),
                    "ig_recipient_id": (getattr(r, "ig_recipient_id", None) if hasattr(r, "ig_recipient_id") else (r[6] if len(r) > 6 else None)),
                    "ig_user_id": (getattr(r, "ig_user_id", None) if hasattr(r, "ig_user_id") else (r[7] if len(r) > 7 else None)),
                    "ad_link": (getattr(r, "ad_link", None) if hasattr(r, "ad_link") else (r[8] if len(r) > 8 else None)),
                    "ad_title": (getattr(r, "ad_title", None) if hasattr(r, "ad_title") else (r[9] if len(r) > 9 else None)),
                    "message_id": (getattr(r, "message_id", None) if hasattr(r, "message_id") else (r[10] if len(r) > 10 else None)),
                    "last_ad_id": (getattr(r, "last_ad_id", None) if hasattr(r, "last_ad_id") else (r[11] if len(r) > 11 else None)),
                    "last_link_type": (getattr(r, "last_link_type", None) if hasattr(r, "last_link_type") else (r[12] if len(r) > 12 else None)),
                    "last_link_id": (getattr(r, "last_link_id", None) if hasattr(r, "last_link_id") else (r[13] if len(r) > 13 else None)),
                    "other_username": (getattr(r, "other_username", None) if hasattr(r, "other_username") else (r[14] if len(r) > 14 else None)),
                    "other_name": (getattr(r, "other_name", None) if hasattr(r, "other_name") else (r[15] if len(r) > 15 else None)),
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
            # Determine the other party id for this conversation (use ig_user_id directly)
            other = None
            try:
                # Use ig_user_id from conversation (the canonical other party ID)
                other = m.get("ig_user_id") if isinstance(m, dict) else getattr(m, "ig_user_id", None)
                # Fallback to direction-based logic if ig_user_id is missing
                if not other:
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
        # Create display names prioritizing usernames over conversation IDs
        display_names: dict[str, str] = {}
        names: dict[str, str] = {}
        try:
            # Build map from conv -> display name (username preferred)
            for m in rows:
                cid = m.get("conversation_id") if isinstance(m, dict) else m.conversation_id
                if not cid:
                    continue
                # Prefer directly joined other_username/other_name if present
                try:
                    ou = (m.get("other_username") if isinstance(m, dict) else getattr(m, "other_username", None))
                    onm = (m.get("other_name") if isinstance(m, dict) else getattr(m, "other_name", None))
                    if ou:
                        display_names[cid] = f"@{str(ou)}"
                    else:
                        display_names[cid] = str(cid)
                    if onm:
                        names[cid] = str(onm)
                except Exception:
                    pass
        except Exception:
            pass
        # Fallback via ig_users when inbox usernames missing; if missing there, enqueue background enrich jobs
        if other_ids:
            try:
                missing = [cid for cid in conv_map.keys() if cid not in display_names]
                if missing:
                    placeholders = ",".join([":p" + str(i) for i in range(len(other_ids))])
                    from sqlalchemy import text as _text
                    params = {("p" + str(i)): list(other_ids)[i] for i in range(len(other_ids))}
                    rows_u = session.exec(_text(f"SELECT ig_user_id, username, name FROM ig_users WHERE ig_user_id IN ({placeholders})")).params(**params).all()
                    id_to_username: dict[str, str] = {}
                    id_to_name: dict[str, str] = {}
                    ids_without_username: list[str] = []
                    for r in rows_u:
                        uid = r.ig_user_id if hasattr(r, "ig_user_id") else r[0]
                        un = r.username if hasattr(r, "username") else r[1]
                        nm = r.name if hasattr(r, "name") else (r[2] if len(r) > 2 else None)
                        if uid and un:
                            id_to_username[str(uid)] = str(un)
                            if nm:
                                id_to_name[str(uid)] = str(nm)
                        elif uid:
                            ids_without_username.append(str(uid))
                    # Enqueue background enrichment instead of fetching inline
                    try:
                        for uid in ids_without_username[: min(50, len(ids_without_username))]:
                            enqueue("enrich_user", key=str(uid), payload={"ig_user_id": str(uid)})
                    except Exception:
                        pass
                    for cid, m in conv_map.items():
                        if cid in display_names:
                            continue
                        other = None
                        try:
                            # Use ig_user_id from conversation (the canonical other party ID)
                            other = m.get("ig_user_id") if isinstance(m, dict) else getattr(m, "ig_user_id", None)
                            # Fallback to direction-based logic if ig_user_id is missing
                            if not other:
                                direction = (m.get("direction") if isinstance(m, dict) else m.direction) or "in"
                                if direction == "out":
                                    other = m.get("ig_recipient_id") if isinstance(m, dict) else m.ig_recipient_id
                                else:
                                    other = m.get("ig_sender_id") if isinstance(m, dict) else m.ig_sender_id
                        except Exception:
                            other = None
                        if other:
                            sid = str(other)
                            if sid in id_to_username:
                                display_names[cid] = f"@{id_to_username[sid]}"
                            else:
                                display_names[cid] = str(cid)
                            if sid in id_to_name:
                                names[cid] = id_to_name[sid]
            except Exception:
                pass
        # Last-resort: conversations still missing a display name – leave as numeric id
        # Best-effort ad metadata using ai_conversations last_* fields
        ad_map = {}
        ad_ids: list[str] = []
        try:
            for m in rows:
                cid = m.get("conversation_id") if isinstance(m, dict) else m.conversation_id
                if not cid:
                    continue
                ad_link = (m.get("ad_link") if isinstance(m, dict) else getattr(m, "ad_link", None))
                ad_title = (m.get("ad_title") if isinstance(m, dict) else getattr(m, "ad_title", None))
                # Use new unified link_id, fallback to last_ad_id for backward compatibility
                link_id = (m.get("last_link_id") if isinstance(m, dict) else getattr(m, "last_link_id", None))
                link_type = (m.get("last_link_type") if isinstance(m, dict) else getattr(m, "last_link_type", None))
                ad_id_val = link_id or (m.get("last_ad_id") if isinstance(m, dict) else getattr(m, "last_ad_id", None))
                if ad_id_val:
                    sid = str(ad_id_val)
                    if sid not in ad_ids:
                        ad_ids.append(sid)
                if (ad_link or ad_title or ad_id_val) and cid not in ad_map:
                    ad_map[cid] = {"link": ad_link, "title": ad_title, "id": ad_id_val, "link_type": link_type}
        except Exception:
            # fallback to link/title only
            ad_map = {}
            ad_ids = []
            for m in rows:
                cid = m.get("conversation_id") if isinstance(m, dict) else m.conversation_id
                if not cid:
                    continue
                ad_link = (m.get("ad_link") if isinstance(m, dict) else getattr(m, "ad_link", None))
                ad_title = (m.get("ad_title") if isinstance(m, dict) else getattr(m, "ad_title", None))
                if (ad_link or ad_title) and cid not in ad_map:
                    ad_map[cid] = {"link": ad_link, "title": ad_title}
        # Enrich ad_map with linked product information from ads_products/product
        if ad_ids:
            try:
                from sqlalchemy import text as _text

                placeholders = ",".join([":a" + str(i) for i in range(len(ad_ids))])
                params_ads = {("a" + str(i)): ad_ids[i] for i in range(len(ad_ids))}
                stmt_ads = _text(
                    f"""
                    SELECT ap.ad_id, ap.product_id, p.name AS product_name, ap.link_type
                    FROM ads_products ap
                    LEFT JOIN product p ON ap.product_id = p.id
                    WHERE ap.ad_id IN ({placeholders})
                    """
                ).bindparams(**params_ads)
                rows_ap = session.exec(stmt_ads).all()
                ad_to_product: dict[str, dict[str, object]] = {}
                for r in rows_ap:
                    try:
                        aid = getattr(r, "ad_id", None) if hasattr(r, "ad_id") else (r[0] if len(r) > 0 else None)
                        pid = getattr(r, "product_id", None) if hasattr(r, "product_id") else (r[1] if len(r) > 1 else None)
                        pname = getattr(r, "product_name", None) if hasattr(r, "product_name") else (r[2] if len(r) > 2 else None)
                        if not aid:
                            continue
                        ad_to_product[str(aid)] = {"product_id": pid, "product_name": pname}
                    except Exception:
                        continue
                if ad_to_product:
                    for cid, meta in ad_map.items():
                        try:
                            ad_id_val = meta.get("id") if isinstance(meta, dict) else None
                            if not ad_id_val:
                                continue
                            ap = ad_to_product.get(str(ad_id_val))
                            if not ap:
                                continue
                            if isinstance(meta, dict):
                                meta["product_id"] = ap.get("product_id")
                                meta["product_name"] = ap.get("product_name")
                        except Exception:
                            continue
            except Exception:
                # best-effort; ignore product enrichment errors
                pass
        # For backward compatibility, set labels to display_names
        labels = display_names
        
        # Fetch unread admin messages
        admin_messages = []
        try:
            admin_msgs = session.exec(
                select(AdminMessage)
                .where(AdminMessage.is_read == False)
                .order_by(AdminMessage.created_at.desc())
                .limit(50)
            ).all()
            for msg in admin_msgs:
                admin_messages.append({
                    "id": msg.id,
                    "conversation_id": msg.conversation_id,
                    "message": msg.message,
                    "message_type": msg.message_type,
                    "created_at": msg.created_at,
                })
        except Exception:
            admin_messages = []
        
        templates = request.app.state.templates
        return templates.TemplateResponse("ig_inbox.html", {
            "request": request,
            "conversations": conversations,
            "labels": labels,
            "names": names,
            "ad_map": ad_map,
            "q": (q or ""),
            "admin_messages": admin_messages,
        })


@router.post("/inbox/refresh")
async def refresh_inbox(limit: int = 25):
    # Temporarily bypass Graph API and rely solely on locally stored messages.
    # This endpoint now acts as a no-op refresh to keep the UI flow intact.
    try:
        return {"status": "ok", "saved": 0}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/inbox/ai-replied")
async def ai_replied_messages(
    request: Request,
    limit: int = 50,
):
    """List of conversations where AI has automatically replied."""
    with get_session() as session:
        from sqlalchemy import text as _text
        rows = session.exec(
            _text(
                """
                SELECT DISTINCT
                    c.id AS convo_id,
                    c.last_message_timestamp_ms AS timestamp_ms,
                    c.last_message_text AS text,
                    c.last_sender_username AS sender_username,
                    c.last_message_direction AS direction,
                    c.ig_user_id,
                    u.username AS other_username,
                    u.name AS other_name,
                    (SELECT COUNT(*) FROM ai_shadow_reply r WHERE r.conversation_id = c.id AND r.status = 'sent') AS ai_reply_count,
                    (SELECT MAX(r.created_at) FROM ai_shadow_reply r WHERE r.conversation_id = c.id AND r.status = 'sent') AS last_ai_reply_at
                FROM conversations c
                INNER JOIN ai_shadow_reply r ON r.conversation_id = c.id AND r.status = 'sent'
                LEFT JOIN ig_users u ON u.ig_user_id = c.ig_user_id
                ORDER BY (SELECT MAX(r.created_at) FROM ai_shadow_reply r WHERE r.conversation_id = c.id AND r.status = 'sent') DESC
                LIMIT :n
                """
            ).params(n=int(limit))
        ).all()
        
        conversations = []
        for r in rows:
            try:
                conversations.append({
                    "conversation_id": (r.convo_id if hasattr(r, "convo_id") else r[0]),
                    "timestamp_ms": (getattr(r, "timestamp_ms", None) if hasattr(r, "timestamp_ms") else (r[1] if len(r) > 1 else None)),
                    "text": (getattr(r, "text", None) if hasattr(r, "text") else (r[2] if len(r) > 2 else None)),
                    "sender_username": (getattr(r, "sender_username", None) if hasattr(r, "sender_username") else (r[3] if len(r) > 3 else None)),
                    "direction": (getattr(r, "direction", None) if hasattr(r, "direction") else (r[4] if len(r) > 4 else None)),
                    "ig_user_id": (getattr(r, "ig_user_id", None) if hasattr(r, "ig_user_id") else (r[5] if len(r) > 5 else None)),
                    "other_username": (getattr(r, "other_username", None) if hasattr(r, "other_username") else (r[6] if len(r) > 6 else None)),
                    "other_name": (getattr(r, "other_name", None) if hasattr(r, "other_name") else (r[7] if len(r) > 7 else None)),
                    "ai_reply_count": (getattr(r, "ai_reply_count", None) if hasattr(r, "ai_reply_count") else (r[8] if len(r) > 8 else None)),
                    "last_ai_reply_at": (getattr(r, "last_ai_reply_at", None) if hasattr(r, "last_ai_reply_at") else (r[9] if len(r) > 9 else None)),
                })
            except Exception:
                continue
        
        # Build display names
        labels = {}
        names = {}
        for conv in conversations:
            cid = conv.get("conversation_id")
            if not cid:
                continue
            username = conv.get("other_username")
            if username:
                labels[cid] = f"@{username}"
            else:
                labels[cid] = str(cid)
            name = conv.get("other_name")
            if name:
                names[cid] = name
        
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "ig_inbox.html",
            {
                "request": request,
                "conversations": conversations,
                "labels": labels,
                "names": names,
                "ad_map": {},
                "q": "",
                "title": "AI Cevaplanan Mesajlar",
            },
        )


@router.post("/inbox/clear-enrich")
def clear_enrich_queues():
    # Purge Redis lists for enrich jobs and delete queued rows from jobs table
    cleared: dict[str, int] = {"redis": 0, "db": 0}
    try:
        r = _get_redis()
        # delete lists atomically; DEL returns number of keys removed
        n = int(r.delete("jobs:enrich_user", "jobs:enrich_page"))
        cleared["redis"] = n
    except Exception:
        pass
    try:
        from sqlalchemy import text as _text
        with get_session() as session:
            res = session.exec(_text("DELETE FROM jobs WHERE kind IN ('enrich_user','enrich_page')"))
            try:
                cleared["db"] = int(getattr(res, "rowcount", 0))
            except Exception:
                cleared["db"] = 0
    except Exception:
        pass
    return {"status": "ok", "cleared": cleared}
