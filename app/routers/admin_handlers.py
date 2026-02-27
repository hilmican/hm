from fastapi import APIRouter

from ..db import get_session
from ..services.queue import enqueue, _get_redis

router = APIRouter()


@router.post("/admin/backfill/ai_conversations")
def backfill_ai_conversations(limit: int = 1000):
    # Create missing ai_conversations rows from latest messages (safety backfill)
    from sqlalchemy import text as _text
    created = 0
    with get_session() as session:
        try:
            rows = session.exec(
                _text(
                    """
                    SELECT m.conversation_id, MAX(m.timestamp_ms) AS ts
                    FROM message m
                    WHERE m.conversation_id IS NOT NULL
                    GROUP BY m.conversation_id
                    ORDER BY ts DESC
                    LIMIT :n
                    """
                ).params(n=int(max(1, min(limit, 5000))))
            ).all()
        except Exception:
            rows = []
        for r in rows:
            try:
                cid = getattr(r, "conversation_id", None) if hasattr(r, "conversation_id") else (r[0] if len(r) > 0 else None)
                if not cid:
                    continue
                session.exec(
                    _text(
                        "INSERT IGNORE INTO ai_conversations(convo_id) VALUES (:cid)"
                    ).params(cid=str(cid))
                )
                created += 1
            except Exception:
                pass
    return {"status": "ok", "created": created}


@router.post("/admin/backfill/ads")
def backfill_ads():
	"""Create missing ads rows from messages with ad metadata or Ads Library links."""
	from sqlalchemy import text as _text
	created = 0
	updated = 0
	with get_session() as session:
		# 1) Parse ad_id from ad_link when missing on message, best-effort
		try:
			rows = session.exec(
				_text("SELECT id, ad_link FROM message WHERE ad_id IS NULL AND ad_link IS NOT NULL LIMIT 5000")
			).all()
			for r in rows:
				try:
					mid = r.id if hasattr(r, "id") else r[0]
					lnk = r.ad_link if hasattr(r, "ad_link") else r[1]
					if lnk and "facebook.com/ads/library" in str(lnk):
						from urllib.parse import urlparse, parse_qs
						q = parse_qs(urlparse(str(lnk)).query)
						aid = (q.get("id") or [None])[0]
						if aid:
							session.exec(_text("UPDATE message SET ad_id=:aid WHERE id=:id")).params(aid=str(aid), id=int(mid))
				except Exception:
					continue
		except Exception:
			pass
		# 2) Insert or update ads table from distinct message ad_id/link/title
		try:
			rows2 = session.exec(
				_text(
					"""
					SELECT DISTINCT ad_id, MAX(ad_link) AS link, MAX(ad_title) AS title
					FROM message
					WHERE ad_id IS NOT NULL
					GROUP BY ad_id
					"""
				)
			).all()
			for r in rows2:
				aid = r.ad_id if hasattr(r, "ad_id") else r[0]
				lnk = r.link if hasattr(r, "link") else (r[1] if len(r) > 1 else None)
				title = r.title if hasattr(r, "title") else (r[2] if len(r) > 2 else None)
				if not aid:
					continue
				try:
					session.exec(_text("INSERT IGNORE INTO ads(ad_id, name, image_url, link, updated_at) VALUES (:id, :n, NULL, :lnk, CURRENT_TIMESTAMP)")).params(id=str(aid), n=(title or None), lnk=(lnk or ("https://www.facebook.com/ads/library/?id=" + str(aid))))
					created += 1
				except Exception:
					pass
				try:
					rc = session.exec(_text("UPDATE ads SET name=COALESCE(:n,name), link=COALESCE(:lnk,link), updated_at=CURRENT_TIMESTAMP WHERE ad_id=:id")).params(id=str(aid), n=(title or None), lnk=(lnk or ("https://www.facebook.com/ads/library/?id=" + str(aid))))
					updated += int(getattr(rc, "rowcount", 0) or 0)
				except Exception:
					pass
		except Exception:
			pass
	return {"status": "ok", "created": int(created), "updated": int(updated)}


@router.post("/admin/reset/instagram")
def reset_instagram_data():
    """Dangerous: Clear Instagram-related data only.

    Tables affected:
      - attachments, message
      - ai_conversations, ai_shadow_state, ai_shadow_reply
      - ig_users, ig_accounts
      - conversations
      - ads, stories (and MySQL mapping tables ads_products, stories_products) if present
      - ig_ai_run, ig_ai_result
      - jobs rows for kinds: ingest, hydrate_conversation, hydrate_ad, enrich_user, enrich_page, fetch_media
    Also clears Redis queues for the same kinds (best-effort).
    """
    cleared: dict[str, int] = {"redis": 0}
    # Clear Redis queues (best-effort)
    try:
        r = _get_redis()
        n = int(r.delete(
            "jobs:ingest",
            "jobs:hydrate_conversation",
            "jobs:hydrate_ad",
            "jobs:enrich_user",
            "jobs:enrich_page",
            "jobs:fetch_media",
        ))
        cleared["redis"] = n
    except Exception:
        pass
    # Delete rows in dependency-safe order
    from sqlalchemy import text as _text
    counts: dict[str, int] = {}
    with get_session() as session:
        def run(q: str, params: dict | None = None, key: str | None = None) -> None:
            try:
                res = session.exec(_text(q).params(**(params or {})))
                if key is not None:
                    try:
                        counts[key] = int(getattr(res, "rowcount", 0) or 0)
                    except Exception:
                        counts[key] = 0
            except Exception:
                if key is not None:
                    counts[key] = 0
        # attachments before message
        run("DELETE FROM attachments", key="attachments")
        run("DELETE FROM message", key="message")
        # AI shadow and summaries
        run("DELETE FROM ai_shadow_reply", key="ai_shadow_reply")
        run("DELETE FROM ai_shadow_state", key="ai_shadow_state")
        run("DELETE FROM ai_conversations", key="ai_conversations")
        # Instagram entities
        run("DELETE FROM ig_users", key="ig_users")
        run("DELETE FROM ig_accounts", key="ig_accounts")
        # Conversations (IG cache)
        run("DELETE FROM conversations", key="conversations")
        # Ads / Stories caches
        run("DELETE FROM ads", key="ads")
        run("DELETE FROM stories", key="stories")
        # Optional mapping tables
        run("DELETE FROM ads_products", key="ads_products")
        run("DELETE FROM stories_products", key="stories_products")
        # AI run history
        run("DELETE FROM ig_ai_result", key="ig_ai_result")
        run("DELETE FROM ig_ai_run", key="ig_ai_run")
        # Jobs by kind
        run("DELETE FROM jobs WHERE kind IN ('ingest','hydrate_conversation','hydrate_ad','enrich_user','enrich_page','fetch_media')", key="jobs")
    return {"status": "ok", "cleared": {**counts, **cleared}}


@router.post("/admin/normalize_dm_conversation_ids")
def normalize_dm_conversation_ids(limit: int = 20000):
    """Normalize message.conversation_id to 'dm:<ig_user_id>' for legacy rows.

    Strategy:
    - Scan up to :limit rows where conversation_id is NULL or not starting with 'dm:'.
    - Compute other party id based on direction:
      - in  -> sender is the other party
      - out -> recipient is the other party
    - Update message.conversation_id to dm:<other_id> when resolvable.
    - Finally, backfill ai_conversations last-* fields using existing admin helper.
    """
    from sqlalchemy import text as _text
    updated = 0
    considered = 0
    with get_session() as session:
        # Fetch candidates
        try:
            rows = session.exec(
                _text(
                    """
                    SELECT id, conversation_id, direction, ig_sender_id, ig_recipient_id
                    FROM message
                    WHERE (conversation_id IS NULL OR conversation_id NOT LIKE 'dm:%')
                    ORDER BY id ASC
                    LIMIT :n
                    """
                ).params(n=int(max(1, min(limit, 100000))))
            ).all()
        except Exception:
            rows = []
        for r in rows:
            try:
                considered += 1
                mid = getattr(r, "id", None) if hasattr(r, "id") else (r[0] if len(r) > 0 else None)
                conv = getattr(r, "conversation_id", None) if hasattr(r, "conversation_id") else (r[1] if len(r) > 1 else None)
                direction = getattr(r, "direction", None) if hasattr(r, "direction") else (r[2] if len(r) > 2 else None)
                sid = getattr(r, "ig_sender_id", None) if hasattr(r, "ig_sender_id") else (r[3] if len(r) > 3 else None)
                rid = getattr(r, "ig_recipient_id", None) if hasattr(r, "ig_recipient_id") else (r[4] if len(r) > 4 else None)
                d = (str(direction) if direction else "in").lower()
                other = (rid if d == "out" else sid)
                if other and (not (isinstance(conv, str) and conv.startswith("dm:"))):
                    session.exec(
                        _text("UPDATE message SET conversation_id=:cid WHERE id=:id").params(cid=f"dm:{other}", id=int(mid))
                    )
                    updated += 1
            except Exception:
                continue
        # Backfill ai_conversations with latest message meta for normalized threads
        try:
            # Reuse existing backfill endpoint logic in-process
            from .admin_handlers import backfill_ai_latest
            res = backfill_ai_latest(limit=50000)
            return {"status": "ok", "considered": int(considered), "normalized": int(updated), "ai_backfill": res}
        except Exception:
            return {"status": "ok", "considered": int(considered), "normalized": int(updated)}


@router.post("/admin/merge_to_graph_conversation_ids")
def merge_to_graph_conversation_ids(limit: int = 5000):
    """Migrate legacy dm:<ig_user_id> threads to Graph conversation ids.

    Actions per mapping (conversations.igba_id + ig_user_id -> graph_conversation_id):
    - UPDATE message SET conversation_id=<graph_id> WHERE conversation_id='dm:<ig_user_id>'
    - UPDATE order SET ig_conversation_id=<graph_id> WHERE ig_conversation_id='dm:<ig_user_id>'
    - Upsert ai_conversations row under <graph_id> from existing dm:<ig_user_id> row, then delete the dm row
    """
    from sqlalchemy import text as _text
    migrated = 0
    considered = 0
    with get_session() as session:
        # Fetch mappings with known Graph id
        try:
            rows = session.exec(
                _text(
                    """
                    SELECT ig_user_id, graph_conversation_id
                    FROM conversations
                    WHERE graph_conversation_id IS NOT NULL
                    ORDER BY last_message_at DESC
                    LIMIT :n
                    """
                ).params(n=int(max(1, min(limit, 50000))))
            ).all()
        except Exception:
            rows = []
        for r in rows:
            try:
                considered += 1
                ig_user_id = getattr(r, "ig_user_id", None) if hasattr(r, "ig_user_id") else (r[0] if len(r) > 0 else None)
                graph_id = getattr(r, "graph_conversation_id", None) if hasattr(r, "graph_conversation_id") else (r[1] if len(r) > 1 else None)
                if not (ig_user_id and graph_id):
                    continue
                dm_id = f"dm:{ig_user_id}"
                # Messages
                session.exec(_text("UPDATE message SET conversation_id=:g WHERE conversation_id=:d").params(g=str(graph_id), d=str(dm_id)))
                # Orders
                try:
                    session.exec(_text('UPDATE "order" SET ig_conversation_id=:g WHERE ig_conversation_id=:d').params(g=str(graph_id), d=str(dm_id)))
                except Exception:
                    # MySQL backticks
                    try:
                        session.exec(_text("UPDATE `order` SET ig_conversation_id=:g WHERE ig_conversation_id=:d").params(g=str(graph_id), d=str(dm_id)))
                    except Exception:
                        pass
                # ai_conversations upsert copy (MySQL)
                try:
                    session.exec(
                        _text(
                            """
                            INSERT INTO ai_conversations(convo_id, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title, hydrated_at)
                            SELECT :g, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title, hydrated_at
                            FROM ai_conversations WHERE convo_id=:d
                            ON DUPLICATE KEY UPDATE
                              last_message_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_id), ai_conversations.last_message_id),
                              last_message_timestamp_ms=GREATEST(COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_timestamp_ms)),
                              last_message_text=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_text), ai_conversations.last_message_text),
                              last_message_direction=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_direction), ai_conversations.last_message_direction),
                              last_sender_username=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_sender_username), ai_conversations.last_sender_username),
                              ig_sender_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(ig_sender_id), ai_conversations.ig_sender_id),
                              ig_recipient_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(ig_recipient_id), ai_conversations.ig_recipient_id),
                              last_ad_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_id), ai_conversations.last_ad_id),
                              last_ad_link=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_link), ai_conversations.last_ad_link),
                              last_ad_title=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_title), ai_conversations.last_ad_title),
                              hydrated_at=COALESCE(ai_conversations.hydrated_at, VALUES(hydrated_at))
                            """
                        ).params(g=str(graph_id), d=str(dm_id))
                    )
                except Exception:
                    pass
                # Remove old dm row if exists
                try:
                    session.exec(_text("DELETE FROM ai_conversations WHERE convo_id=:d").params(d=str(dm_id)))
                except Exception:
                    pass
                migrated += 1
            except Exception:
                continue
    return {"status": "ok", "considered": int(considered), "migrated": int(migrated)}


@router.post("/admin/enrich/users-errors")
def enrich_users_with_errors(limit: int = 2000):
	from sqlalchemy import text as _text
	enqueued = 0
	with get_session() as session:
		try:
			rows = session.exec(_text("SELECT ig_user_id FROM ig_users WHERE fetch_status='error' LIMIT :n")).params(n=int(max(1, min(limit, 10000)))).all()
		except Exception:
			rows = []
		for r in rows:
			try:
				uid = r.ig_user_id if hasattr(r, "ig_user_id") else (r[0] if len(r) > 0 else None)
				if not uid:
					continue
				enqueue("enrich_user", key=str(uid), payload={"ig_user_id": str(uid)})
				enqueued += 1
			except Exception:
				continue
	return {"status": "ok", "enqueued": int(enqueued)}


@router.post("/admin/backfill/ai_latest")
def backfill_ai_latest(limit: int = 50000):
    # Populate ai_conversations last-* fields from message table; migrate hydrated_at best-effort
    from sqlalchemy import text as _text
    updated = 0
    considered = 0
    with get_session() as session:
        # Fetch conversation ids with their latest timestamp (ordered)
        try:
            rows = session.exec(
                _text(
                    """
                    SELECT m.conversation_id, MAX(m.timestamp_ms) AS ts
                    FROM message m
                    WHERE m.conversation_id IS NOT NULL
                    GROUP BY m.conversation_id
                    ORDER BY ts DESC
                    LIMIT :n
                    """
                ).params(n=int(max(1, min(limit, 100000))))
            ).all()
        except Exception:
            rows = []
        for r in rows:
            try:
                cid = r.conversation_id if hasattr(r, "conversation_id") else (r[0] if len(r) > 0 else None)
                if not cid:
                    continue
                considered += 1
                # Load last message for this conversation
                rm = session.exec(
                    _text(
                        """
                        SELECT id, timestamp_ms, text, direction, sender_username, ig_sender_id, ig_recipient_id, ad_id, ad_link, ad_title
                        FROM message
                        WHERE conversation_id=:cid
                        ORDER BY timestamp_ms DESC, id DESC
                        LIMIT 1
                        """
                    ).params(cid=str(cid))
                ).first()
                if not rm:
                    continue
                mid = getattr(rm, "id", None) if hasattr(rm, "id") else (rm[0] if len(rm) > 0 else None)
                ts = getattr(rm, "timestamp_ms", None) if hasattr(rm, "timestamp_ms") else (rm[1] if len(rm) > 1 else None)
                txt = getattr(rm, "text", None) if hasattr(rm, "text") else (rm[2] if len(rm) > 2 else None)
                dirn = getattr(rm, "direction", None) if hasattr(rm, "direction") else (rm[3] if len(rm) > 3 else None)
                sun = getattr(rm, "sender_username", None) if hasattr(rm, "sender_username") else (rm[4] if len(rm) > 4 else None)
                sid = getattr(rm, "ig_sender_id", None) if hasattr(rm, "ig_sender_id") else (rm[5] if len(rm) > 5 else None)
                rid = getattr(rm, "ig_recipient_id", None) if hasattr(rm, "ig_recipient_id") else (rm[6] if len(rm) > 6 else None)
                adid = getattr(rm, "ad_id", None) if hasattr(rm, "ad_id") else (rm[7] if len(rm) > 7 else None)
                alink = getattr(rm, "ad_link", None) if hasattr(rm, "ad_link") else (rm[8] if len(rm) > 8 else None)
                atitle = getattr(rm, "ad_title", None) if hasattr(rm, "ad_title") else (rm[9] if len(rm) > 9 else None)
                # ensure ai_conversations row exists
                try:
                    session.exec(_text("INSERT IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=str(cid))
                except Exception:
                    pass
                # migrate hydrated_at best-effort from conversations where ig_user_id matches dm:<id>
                try:
                    dm_id = None
                    if isinstance(cid, str) and cid.startswith("dm:"):
                        try:
                            dm_id = cid.split(":", 1)[1]
                        except Exception:
                            dm_id = None
                    if dm_id:
                        row_h = session.exec(_text("SELECT MAX(hydrated_at) FROM conversations WHERE ig_user_id=:u")).params(u=str(dm_id)).first()
                        hyd_at = None
                        if row_h is not None:
                            hyd_at = row_h[0] if isinstance(row_h, (list, tuple)) else getattr(row_h, "MAX(hydrated_at)", None)
                        if hyd_at:
                            session.exec(_text("UPDATE ai_conversations SET hydrated_at=COALESCE(hydrated_at, :h) WHERE convo_id=:cid")).params(cid=str(cid), h=hyd_at)
                except Exception:
                    pass
                # upsert last-* fields (MySQL)
                try:
                    session.exec(
                        _text(
                            """
                            INSERT INTO ai_conversations(convo_id, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title)
                            VALUES (:cid, :mid, :ts, :txt, :dir, :sun, :sid, :rid, :adid, :alink, :atitle)
                            ON DUPLICATE KEY UPDATE
                              last_message_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_id), ai_conversations.last_message_id),
                              last_message_timestamp_ms=GREATEST(COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_timestamp_ms)),
                              last_message_text=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_text), ai_conversations.last_message_text),
                              last_message_direction=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_message_direction), ai_conversations.last_message_direction),
                              last_sender_username=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_sender_username), ai_conversations.last_sender_username),
                              ig_sender_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(ig_sender_id), ai_conversations.ig_sender_id),
                              ig_recipient_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(ig_recipient_id), ai_conversations.ig_recipient_id),
                              last_ad_id=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_id), ai_conversations.last_ad_id),
                              last_ad_link=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_link), ai_conversations.last_ad_link),
                              last_ad_title=IF(VALUES(last_message_timestamp_ms) >= COALESCE(ai_conversations.last_message_timestamp_ms,0), VALUES(last_ad_title), ai_conversations.last_ad_title)
                            """
                        ).params(
                            cid=str(cid),
                            mid=int(mid) if mid is not None else None,
                            ts=int(ts) if ts is not None else None,
                            txt=(txt or ""),
                            dir=(dirn or "in"),
                            sun=(sun or None),
                            sid=(str(sid) if sid is not None else None),
                            rid=(str(rid) if rid is not None else None),
                            adid=(str(adid) if adid is not None else None),
                            alink=alink,
                            atitle=atitle,
                        )
                    )
                    updated += 1
                except Exception:
                    pass
            except Exception:
                continue
    return {"status": "ok", "updated": int(updated), "considered": int(considered)}


@router.post("/admin/backfill/latest")
def backfill_latest_messages(limit: int = 50000):
	# Populate latest_messages from message table (MySQL)
	from sqlalchemy import text as _text
	created = 0
	try:
		with get_session() as session:
			sql_mysql = """
				INSERT INTO latest_messages(convo_id, message_id, timestamp_ms, text, sender_username, direction, ig_sender_id, ig_recipient_id, ad_link, ad_title)
				SELECT t.conversation_id, t.id, t.ts, t.text, t.sender_username, t.direction, t.ig_sender_id, t.ig_recipient_id, t.ad_link, t.ad_title
				FROM (
					SELECT m.conversation_id, m.id, COALESCE(m.timestamp_ms,0) AS ts, m.text, m.sender_username, m.direction, m.ig_sender_id, m.ig_recipient_id, m.ad_link, m.ad_title
					FROM message m
					JOIN (
						SELECT conversation_id, MAX(COALESCE(timestamp_ms,0)) AS ts
						FROM message
						WHERE conversation_id IS NOT NULL
						GROUP BY conversation_id
					) lm ON lm.conversation_id = m.conversation_id AND lm.ts = COALESCE(m.timestamp_ms,0)
					ORDER BY ts DESC
					LIMIT :n
				) AS t
				ON DUPLICATE KEY UPDATE
				  message_id=VALUES(message_id),
				  timestamp_ms=VALUES(timestamp_ms),
				  text=VALUES(text),
				  sender_username=VALUES(sender_username),
				  direction=VALUES(direction),
				  ig_sender_id=VALUES(ig_sender_id),
				  ig_recipient_id=VALUES(ig_recipient_id),
				  ad_link=VALUES(ad_link),
				  ad_title=VALUES(ad_title)
			"""
			res = session.exec(_text(sql_mysql).params(n=int(max(1000, min(limit, 200000)))) )
			created = int(getattr(res, "rowcount", 0) or 0)
	except Exception:
		created = 0
	return {"status": "ok", "upserted": created}


@router.post("/admin/backfill/raw_events")
def backfill_raw_events(since: str = "2025-01-22", limit: int = 50000):
	"""Re-enqueue ingest jobs for raw_events received after a given date.

	Use when the ingest worker was down and webhooks were saved to raw_events but
	never processed. Worker will process each raw_event idempotently (duplicate
	messages are skipped). Inbox list is built from conversations updated by ingest.
	"""
	from sqlalchemy import text as _text
	enqueued = 0
	try:
		with get_session() as session:
			# raw_events.received_at is set by DB default on insert
			rows = session.exec(
				_text(
					"""
					SELECT id FROM raw_events
					WHERE received_at >= :since
					ORDER BY id ASC
					LIMIT :n
					"""
				).params(since=since, n=int(max(1, min(limit, 100000))))
			).all()
			for r in rows:
				rid = r.id if hasattr(r, "id") else r[0]
				try:
					enqueue("ingest", key=str(rid), payload={"raw_event_id": int(rid)})
					enqueued += 1
				except Exception:
					pass
	except Exception as e:
		return {"status": "error", "error": str(e), "enqueued": enqueued}
	return {"status": "ok", "enqueued": enqueued, "since": since}


@router.post("/admin/ingest/clear_backlog")
def clear_ingest_backlog():
	"""Tüm bekleyen ingest işlerini siler. Geçmiş mesajlar işlenmemiş sayılır; sadece bundan sonra gelen webhook'lar işlenecek.

	- Redis jobs:ingest listesini temizler.
	- jobs tablosunda kind='ingest' kayıtlarını siler.
	"""
	from sqlalchemy import text as _text
	redis_removed = 0
	jobs_deleted = 0
	try:
		r = _get_redis()
		redis_removed = r.delete("jobs:ingest")
	except Exception:
		pass
	try:
		with get_session() as session:
			res = session.exec(_text("DELETE FROM jobs WHERE kind = 'ingest'"))
			jobs_deleted = getattr(res, "rowcount", None) or 0
	except Exception as e:
		return {"status": "error", "error": str(e), "redis_removed": redis_removed, "jobs_deleted": jobs_deleted}
	return {"status": "ok", "redis_removed": redis_removed, "jobs_deleted": jobs_deleted}


@router.post("/admin/ai_reply/exhaust_old_pending")
def exhaust_old_pending_ai_replies(minutes: int = 30):
	"""30 dakikadan eski, cevap bekleyen (pending/paused) AI kuyruğundaki konuşmaları 'exhausted' yapar.

	Böylece worker sadece yeni/taze konuşmalara odaklanır; eski backlog işlenmez.
	"""
	from sqlalchemy import text as _text
	updated = 0
	try:
		with get_session() as session:
			# Son gelen mesaj 30+ dakika önceyse artık otomatik cevap denemeyi bırak
			# last_inbound_ms: ms since epoch; 30 min ago = (UNIX_TIMESTAMP() - minutes*60) * 1000
			res = session.exec(
				_text(
					"""
					UPDATE ai_shadow_state
					SET status = 'exhausted', updated_at = CURRENT_TIMESTAMP
					WHERE (status = 'pending' OR status = 'paused')
					  AND last_inbound_ms > 0
					  AND last_inbound_ms < ((UNIX_TIMESTAMP() - :mins * 60) * 1000)
					"""
				).params(mins=int(max(1, min(minutes, 1440))))
			)
			updated = getattr(res, "rowcount", None) or 0
	except Exception as e:
		return {"status": "error", "error": str(e), "exhausted": 0}
	return {"status": "ok", "exhausted": updated, "minutes": minutes}
