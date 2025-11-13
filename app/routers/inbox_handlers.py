from fastapi import APIRouter, Request

from ..db import get_session
from ..services.queue import enqueue, _get_redis

router = APIRouter()


@router.get("/inbox")
async def inbox(request: Request, limit: int = 25, q: str | None = None):
    with get_session() as session:
        # Use ai_conversations as single source for inbox list
        from sqlalchemy import text as _text
        base_sql = """
            SELECT ac.convo_id,
                   ac.last_message_timestamp_ms AS timestamp_ms,
                   ac.last_message_text AS text,
                   ac.last_sender_username AS sender_username,
                   ac.last_message_direction AS direction,
                   ac.ig_sender_id,
                   ac.ig_recipient_id,
                   ac.last_ad_link AS ad_link,
                   ac.last_ad_title AS ad_title,
                   ac.last_message_id AS message_id,
                   ac.last_ad_id AS last_ad_id,
                   u.username AS other_username,
                   u.name AS other_name
            FROM ai_conversations ac
            LEFT JOIN ig_users u
              ON u.ig_user_id = CASE WHEN ac.last_message_direction='out' THEN ac.ig_recipient_id ELSE ac.ig_sender_id END
        """
        where_parts: list[str] = []
        params: dict[str, object] = {}
        if q and isinstance(q, str) and q.strip():
            qq = f"%{q.lower().strip()}%"
            where_parts.append("""
                (
                    (ac.last_message_text IS NOT NULL AND LOWER(ac.last_message_text) LIKE :qq)
                    OR (ac.last_sender_username IS NOT NULL AND LOWER(ac.last_sender_username) LIKE :qq)
                    OR EXISTS (
                        SELECT 1 FROM ig_users u
                        WHERE (u.ig_user_id = ac.ig_sender_id OR u.ig_user_id = ac.ig_recipient_id OR (ac.convo_id LIKE 'dm:%' AND u.ig_user_id = SUBSTR(ac.convo_id, 4)))
                          AND (
                            (u.name IS NOT NULL AND LOWER(u.name) LIKE :qq)
                            OR (u.username IS NOT NULL AND LOWER(u.username) LIKE :qq)
                          )
                    )
                )
            """)
            params["qq"] = qq
        sample_n = max(50, min(int(limit or 25) * 4, 200))
        bind = session.get_bind()
        dialect_name = ""
        try:
            if bind is not None and getattr(bind, "dialect", None):
                dialect_name = bind.dialect.name.lower()
        except Exception:
            dialect_name = ""
        # Sort strictly by last message timestamp (ms since epoch), newest first
        order_sql = " ORDER BY ac.last_message_timestamp_ms DESC LIMIT :n"
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
                    "ad_link": (getattr(r, "ad_link", None) if hasattr(r, "ad_link") else (r[7] if len(r) > 7 else None)),
                    "ad_title": (getattr(r, "ad_title", None) if hasattr(r, "ad_title") else (r[8] if len(r) > 8 else None)),
                    "message_id": (getattr(r, "message_id", None) if hasattr(r, "message_id") else (r[9] if len(r) > 9 else None)),
                    "last_ad_id": (getattr(r, "last_ad_id", None) if hasattr(r, "last_ad_id") else (r[10] if len(r) > 10 else None)),
                    "other_username": (getattr(r, "other_username", None) if hasattr(r, "other_username") else (r[11] if len(r) > 11 else None)),
                    "other_name": (getattr(r, "other_name", None) if hasattr(r, "other_name") else (r[12] if len(r) > 12 else None)),
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
        # Resolve usernames preferring last inbound message's sender_username; fallback to ig_users, also include full names
        labels: dict[str, str] = {}
        names: dict[str, str] = {}
        try:
            # Build map from conv -> latest inbound with sender_username
            inbound_named: dict[str, str] = {}
            for m in rows:
                cid = m.get("conversation_id") if isinstance(m, dict) else m.conversation_id
                if not cid:
                    continue
                # Prefer directly joined other_username/other_name if present
                try:
                    ou = (m.get("other_username") if isinstance(m, dict) else getattr(m, "other_username", None))
                    onm = (m.get("other_name") if isinstance(m, dict) else getattr(m, "other_name", None))
                    if ou and cid not in labels:
                        labels[cid] = f"@{str(ou)}"
                    if onm and cid not in names:
                        names[cid] = str(onm)
                except Exception:
                    pass
                direction = (m.get("direction") if isinstance(m, dict) else m.direction) or "in"
                sender_username = (m.get("sender_username") if isinstance(m, dict) else m.sender_username) or ""
                if direction == "in" and sender_username.strip() and cid not in inbound_named:
                    inbound_named[cid] = str(sender_username).strip()
            for cid, un in inbound_named.items():
                labels[cid] = f"@{un}"
        except Exception:
            pass
        # Fallback via ig_users when inbox usernames missing; if missing there, enqueue background enrich jobs
        if other_ids:
            try:
                missing = [cid for cid in conv_map.keys() if cid not in labels]
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
                        if other:
                            sid = str(other)
                            if sid in id_to_username:
                                labels[cid] = f"@{id_to_username[sid]}"
                            if sid in id_to_name:
                                names[cid] = id_to_name[sid]
            except Exception:
                pass
        # Last-resort: conversation ids that are dm:<ig_user_id> but still missing a label
        dm_missing = [cid for cid in conv_map.keys() if (cid not in labels and isinstance(cid, str) and cid.startswith("dm:"))]
        if dm_missing:
            dm_ids = []
            for cid in dm_missing:
                try:
                    dm_ids.append(cid.split(":", 1)[1])
                except Exception:
                    continue
            if dm_ids:
                placeholders = ",".join([":d" + str(i) for i in range(len(dm_ids))])
                from sqlalchemy import text as _text
                params = {("d" + str(i)): dm_ids[i] for i in range(len(dm_ids))}
                try:
                    rows_dm = session.exec(_text(f"SELECT ig_user_id, username, name FROM ig_users WHERE ig_user_id IN ({placeholders})")).params(**params).all()
                except Exception:
                    rows_dm = []
                for r in rows_dm:
                    uid = r.ig_user_id if hasattr(r, "ig_user_id") else r[0]
                    un = r.username if hasattr(r, "username") else r[1]
                    nm = r.name if hasattr(r, "name") else (r[2] if len(r) > 2 else None)
                    if uid and un:
                        cid = f"dm:{uid}"
                        if cid not in labels:
                            labels[cid] = f"@{str(un)}"
                        if nm and cid not in names:
                            names[cid] = str(nm)
        # Best-effort ad metadata using ai_conversations last_* fields
        ad_map = {}
        try:
            for m in rows:
                cid = m.get("conversation_id") if isinstance(m, dict) else m.conversation_id
                if not cid:
                    continue
                ad_link = (m.get("ad_link") if isinstance(m, dict) else getattr(m, "ad_link", None))
                ad_title = (m.get("ad_title") if isinstance(m, dict) else getattr(m, "ad_title", None))
                ad_id_val = (m.get("last_ad_id") if isinstance(m, dict) else getattr(m, "last_ad_id", None))
                if (ad_link or ad_title or ad_id_val) and cid not in ad_map:
                    ad_map[cid] = {"link": ad_link, "title": ad_title, "id": ad_id_val}
        except Exception:
            # fallback to link/title only
            ad_map = {}
            for m in rows:
                cid = m.get("conversation_id") if isinstance(m, dict) else m.conversation_id
                if not cid:
                    continue
                ad_link = (m.get("ad_link") if isinstance(m, dict) else getattr(m, "ad_link", None))
                ad_title = (m.get("ad_title") if isinstance(m, dict) else getattr(m, "ad_title", None))
                if (ad_link or ad_title) and cid not in ad_map:
                    ad_map[cid] = {"link": ad_link, "title": ad_title}
        templates = request.app.state.templates
        return templates.TemplateResponse("ig_inbox.html", {"request": request, "conversations": conversations, "labels": labels, "names": names, "ad_map": ad_map, "q": (q or "")})


@router.post("/inbox/refresh")
async def refresh_inbox(limit: int = 25):
    # Temporarily bypass Graph API and rely solely on locally stored messages.
    # This endpoint now acts as a no-op refresh to keep the UI flow intact.
    try:
        return {"status": "ok", "saved": 0}
    except Exception as e:
        return {"status": "error", "error": str(e)}


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
