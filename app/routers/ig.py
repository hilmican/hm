from fastapi import APIRouter, Request, HTTPException
from fastapi import WebSocket, WebSocketDisconnect
import logging
from sqlmodel import select
from typing import Any

from ..db import get_session
from ..models import Message, IGAiDebugRun
from sqlmodel import select
from ..services.instagram_api import sync_latest_conversations
from ..services.queue import enqueue
from ..services.instagram_api import _get_base_token_and_id, GRAPH_VERSION
from ..services.queue import _get_redis
import json
from pathlib import Path
from fastapi.responses import FileResponse, RedirectResponse
from starlette.status import HTTP_303_SEE_OTHER
import time
import httpx
import os
from ..services.ai_ig import _detect_focus_product
from fastapi import Form
from fastapi import Request


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


@router.get("/inbox/{conversation_id}/debug")
def debug_conversation(request: Request, conversation_id: str, limit: int = 25):
    templates = request.app.state.templates
    n = max(1, min(int(limit or 25), 100))
    with get_session() as session:
        runs = session.exec(
            select(IGAiDebugRun)
            .where(IGAiDebugRun.conversation_id == conversation_id)
            .order_by(IGAiDebugRun.id.desc())
            .limit(n)
        ).all()
        ai_run_meta: dict[int, dict[str, Any]] = {}
        run_ids = [int(r.ai_run_id) for r in runs if r.ai_run_id]
        if run_ids:
            from sqlalchemy import text as _text

            placeholders = ",".join([":p" + str(i) for i in range(len(run_ids))])
            params = {"p" + str(i): run_ids[i] for i in range(len(run_ids))}
            rows = session.exec(
                _text(
                    f"SELECT id, started_at, completed_at, errors_json, conversations_considered, conversations_processed, purchases_detected, purchases_unlinked FROM ig_ai_run WHERE id IN ({placeholders})"
                ).params(**params)
            ).all()
            for row in rows:
                rid = row.id if hasattr(row, "id") else row[0]
                ai_run_meta[int(rid)] = {
                    "started_at": getattr(row, "started_at", None) if hasattr(row, "started_at") else row[1],
                    "completed_at": getattr(row, "completed_at", None) if hasattr(row, "completed_at") else row[2],
                    "errors_json": getattr(row, "errors_json", None) if hasattr(row, "errors_json") else row[3],
                    "considered": getattr(row, "conversations_considered", None) if hasattr(row, "conversations_considered") else row[4],
                    "processed": getattr(row, "conversations_processed", None) if hasattr(row, "conversations_processed") else row[5],
                    "purchases": getattr(row, "purchases_detected", None) if hasattr(row, "purchases_detected") else row[6],
                    "unlinked": getattr(row, "purchases_unlinked", None) if hasattr(row, "purchases_unlinked") else row[7],
                }
    formatted: list[dict[str, Any]] = []  # type: ignore[type-arg]
    for run in runs:
        try:
            extracted_obj = json.loads(run.extracted_json) if run.extracted_json else None
        except Exception:
            extracted_obj = None
        try:
            logs_obj = json.loads(run.logs_json) if run.logs_json else None
        except Exception:
            logs_obj = None
        formatted.append({
            "run": run,
            "extracted": extracted_obj,
            "extracted_pretty": json.dumps(extracted_obj, ensure_ascii=False, indent=2) if extracted_obj else None,
            "logs": logs_obj,
            "logs_pretty": json.dumps(logs_obj, ensure_ascii=False, indent=2) if logs_obj else None,
            "ai_run": ai_run_meta.get(int(run.ai_run_id)) if run.ai_run_id else None,
        })
    return templates.TemplateResponse(
        "ig_debug.html",
        {
            "request": request,
            "conversation_id": conversation_id,
            "runs": formatted,
        },
    )


@router.post("/inbox/{conversation_id}/debug/run")
def trigger_debug_conversation(conversation_id: str):
    with get_session() as session:
        # 1) Create a debug run row (UI anchor)
        run = IGAiDebugRun(conversation_id=conversation_id, status="queued")
        session.add(run)
        session.flush()
        debug_id = int(run.id or 0)

        # 2) Create a corresponding ig_ai_run row to collect summary stats
        try:
            from sqlalchemy import text as _text

            session.exec(
                _text(
                    """
                    INSERT INTO ig_ai_run(started_at, date_from, date_to, min_age_minutes)
                    VALUES (CURRENT_TIMESTAMP, NULL, NULL, :age)
                    """
                ).params(age=0)
            )
            run_id = None
            # MySQL LAST_INSERT_ID
            try:
                rid_row = session.exec(_text("SELECT LAST_INSERT_ID() AS id")).first()
                if rid_row is not None:
                    run_id = int(getattr(rid_row, "id", rid_row[0]))
            except Exception:
                pass
            # SQLite fallback
            if run_id is None:
                try:
                    rid_row = session.exec(_text("SELECT last_insert_rowid() AS id")).first()
                    if rid_row is not None:
                        run_id = int(getattr(rid_row, "id", rid_row[0]))
                except Exception:
                    pass
            if run_id is None:
                raise RuntimeError("Could not create ig_ai_run")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"create_ai_run_failed: {e}")

        # 3) Enqueue processing job with real run_id and debug_run_id
        try:
            payload = {
                "run_id": run_id,
                "date_from": None,
                "date_to": None,
                "min_age_minutes": 0,
                "limit": 200,
                "reprocess": False,
                "conversation_id": conversation_id,
                "debug_run_id": debug_id,
            }
            job_id = enqueue("ig_ai_process_run", key=str(run_id), payload=payload)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"enqueue_failed: {e}")

        # 4) Link debug row to ai_run and persist job id
        run.job_id = job_id
        run.ai_run_id = run_id
        session.add(run)
        session.commit()
    return RedirectResponse(url=f"/ig/inbox/{conversation_id}/debug", status_code=HTTP_303_SEE_OTHER)


@router.get("/inbox/{conversation_id}")
def thread(request: Request, conversation_id: str, limit: int = 100):
    # If this is a legacy dm:<ig_user_id> and we have a known Graph conversation id, redirect
    if isinstance(conversation_id, str) and conversation_id.startswith("dm:"):
        try:
            other_id = conversation_id.split(":", 1)[1]
        except Exception:
            other_id = None
        if other_id:
            try:
                from sqlalchemy import text as _text
                with get_session() as session:
                    rowc = session.exec(
                        _text(
                            "SELECT graph_conversation_id FROM conversations WHERE ig_user_id=:u AND graph_conversation_id IS NOT NULL ORDER BY last_message_at DESC LIMIT 1"
                        ).params(u=str(other_id))
                    ).first()
                    if rowc:
                        gcid = rowc.graph_conversation_id if hasattr(rowc, "graph_conversation_id") else (rowc[0] if len(rowc) > 0 else None)
                        if gcid and str(gcid) != conversation_id:
                            return RedirectResponse(url=f"/ig/inbox/{gcid}", status_code=HTTP_303_SEE_OTHER)
            except Exception:
                pass
    with get_session() as session:
        msgs = session.exec(
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.timestamp_ms.desc())
            .limit(min(max(limit, 1), 500))
        ).all()
        # chronological order for display
        msgs = list(reversed(msgs))
        # Determine other party id from messages then resolve username
        other_label = None
        other_id = None
        contact_name = None
        contact_phone = None
        contact_address = None
        enrich_status: dict[str, Any] | None = None
        linked_order_id = None
        ai_status = None
        ai_json = None
        try:
            from sqlalchemy import text as _text
            row_convo = session.exec(
                _text(
                    """
                    SELECT contact_name, contact_phone, contact_address, linked_order_id, ai_status, ai_json
                    FROM conversations WHERE convo_id=:cid LIMIT 1
                    """
                ).params(cid=str(conversation_id))
            ).first()
            if row_convo:
                contact_name = (row_convo.contact_name if hasattr(row_convo, "contact_name") else row_convo[0]) or None
                contact_phone = (row_convo.contact_phone if hasattr(row_convo, "contact_phone") else row_convo[1]) or None
                contact_address = (row_convo.contact_address if hasattr(row_convo, "contact_address") else row_convo[2]) or None
                linked_order_id = (row_convo.linked_order_id if hasattr(row_convo, "linked_order_id") else row_convo[3]) or None
                ai_status = (row_convo.ai_status if hasattr(row_convo, "ai_status") else row_convo[4]) or None
                ai_json = (row_convo.ai_json if hasattr(row_convo, "ai_json") else row_convo[5]) or None
        except Exception:
            pass

        # Fallback: even if conversations row is missing or ai_json empty, try latest historical result
        if not ai_json:
            try:
                from sqlalchemy import text as _text
                row_hist = session.exec(
                    _text(
                        "SELECT ai_json FROM ig_ai_result WHERE convo_id=:cid ORDER BY id DESC LIMIT 1"
                    ).params(cid=str(conversation_id))
                ).first()
                if row_hist:
                    ai_json = (row_hist.ai_json if hasattr(row_hist, "ai_json") else row_hist[0]) or None
            except Exception:
                pass
        for mm in msgs:
            try:
                other_id = (mm.ig_sender_id if (mm.direction or "in") == "in" else mm.ig_recipient_id)
                if other_id:
                    break
            except Exception:
                continue
        # Fallback: derive other id from conversation_id format "dm:<ig_user_id>"
        if not other_id:
            try:
                if isinstance(conversation_id, str) and conversation_id.startswith("dm:"):
                    other_id = conversation_id.split(":", 1)[1] or None
            except Exception:
                other_id = None
        # Additional fallback: check ai_conversations for this convo id
        if not other_id:
            try:
                from sqlalchemy import text as _text
                # ai_conversations stores only convo_id; when it's dm:<id> we already handled above. Keep as safety if formats evolve.
                row_ac = session.exec(_text("SELECT convo_id FROM ai_conversations WHERE convo_id=:cid LIMIT 1").params(cid=str(conversation_id))).first()
                if row_ac:
                    conv = row_ac.convo_id if hasattr(row_ac, "convo_id") else (row_ac[0] if isinstance(row_ac, (list, tuple)) else None)
                    if isinstance(conv, str) and conv.startswith("dm:"):
                        other_id = conv.split(":", 1)[1] or None
            except Exception:
                pass
        if other_id:
            # Username for page header
            try:
                from sqlalchemy import text as _text
                rowu = session.exec(_text("SELECT username FROM ig_users WHERE ig_user_id=:u").params(u=str(other_id))).first()
                if rowu:
                    un = rowu.username if hasattr(rowu, "username") else rowu[0]
                    if un:
                        other_label = f"@{un}"
            except Exception:
                pass
            # Collect enrichment status and queue info (build piecemeal, never fail the whole block)
            estatus: dict[str, Any] = {"ig_user_id": str(other_id)}
            # Row from ig_users
            try:
                from sqlalchemy import text as _text
                rowe = session.exec(
                    _text("SELECT username, name, fetched_at, fetch_status, fetch_error FROM ig_users WHERE ig_user_id=:u LIMIT 1")
                ).params(u=str(other_id)).first()
                if rowe:
                    estatus["username"] = getattr(rowe, "username", None) if hasattr(rowe, "username") else (rowe[0] if len(rowe) > 0 else None)
                    estatus["name"] = getattr(rowe, "name", None) if hasattr(rowe, "name") else (rowe[1] if len(rowe) > 1 else None)
                    estatus["fetched_at"] = getattr(rowe, "fetched_at", None) if hasattr(rowe, "fetched_at") else (rowe[2] if len(rowe) > 2 else None)
                    estatus["fetch_status"] = getattr(rowe, "fetch_status", None) if hasattr(rowe, "fetch_status") else (rowe[3] if len(rowe) > 3 else None)
                    estatus["fetch_error"] = getattr(rowe, "fetch_error", None) if hasattr(rowe, "fetch_error") else (rowe[4] if len(rowe) > 4 else None)
            except Exception:
                pass
            # Pending job (if any)
            try:
                dialect = str(session.get_bind().dialect.name)
            except Exception:
                dialect = ""
            try:
                from sqlalchemy import text as _text
                if dialect == "mysql":
                    qry_job = "SELECT `id`, `attempts`, `run_after` FROM `jobs` WHERE `kind`='enrich_user' AND `key`=:u LIMIT 1"
                else:
                    qry_job = "SELECT id, attempts, run_after FROM jobs WHERE kind='enrich_user' AND key=:u LIMIT 1"
                rowj = session.exec(_text(qry_job).params(u=str(other_id))).first()
                if rowj:
                    estatus["job"] = {
                        "id": getattr(rowj, "id", None) if hasattr(rowj, "id") else (rowj[0] if len(rowj) > 0 else None),
                        "attempts": getattr(rowj, "attempts", None) if hasattr(rowj, "attempts") else (rowj[1] if len(rowj) > 1 else None),
                        "run_after": getattr(rowj, "run_after", None) if hasattr(rowj, "run_after") else (rowj[2] if len(rowj) > 2 else None),
                    }
            except Exception:
                pass
            # Queue depth
            try:
                r = _get_redis()
                estatus["queue_depth"] = int(r.llen("jobs:enrich_user"))
            except Exception:
                pass
            enrich_status = estatus
            # Try to fetch contact info from conversations table
            try:
                from sqlalchemy import text as _text
                rowc = session.exec(_text("""
                    SELECT contact_name, contact_phone, contact_address, linked_order_id
                    FROM conversations
                    WHERE ig_user_id = :u ORDER BY last_message_at DESC LIMIT 1
                """).params(u=str(other_id))).first()
                if rowc:
                    if contact_name is None:
                        contact_name = (rowc.contact_name if hasattr(rowc, 'contact_name') else rowc[0]) or None
                    if contact_phone is None:
                        contact_phone = (rowc.contact_phone if hasattr(rowc, 'contact_phone') else rowc[1]) or None
                    if contact_address is None:
                        contact_address = (rowc.contact_address if hasattr(rowc, 'contact_address') else rowc[2]) or None
                    if linked_order_id is None:
                        val = rowc.linked_order_id if hasattr(rowc, 'linked_order_id') else None
                        if val is None:
                            try:
                                val = rowc[3]
                            except Exception:
                                val = None
                        linked_order_id = val or None
            except Exception:
                pass
        # Resolve per-message sender usernames via ig_users only.
        # Enqueue missing ones for background enrichment instead of fetching inline.
        usernames: dict[str, str] = {}
        ad_ids: list[str] = []
        try:
            sender_ids: list[str] = []
            for mm in msgs:
                if mm.ig_sender_id:
                    sid = str(mm.ig_sender_id)
                    if sid not in sender_ids:
                        sender_ids.append(sid)
                try:
                    if mm.ad_id:
                        aid = str(mm.ad_id)
                        if aid not in ad_ids:
                            ad_ids.append(aid)
                except Exception:
                    pass
            if sender_ids:
                placeholders = ",".join([":p" + str(i) for i in range(len(sender_ids))])
                from sqlalchemy import text as _text
                params = {("p" + str(i)): sender_ids[i] for i in range(len(sender_ids))}
                rows_u = session.exec(_text(f"SELECT ig_user_id, username FROM ig_users WHERE ig_user_id IN ({placeholders})")).params(**params).all()
                ids_without_username: list[str] = []
                for r in rows_u:
                    uid = r.ig_user_id if hasattr(r, "ig_user_id") else r[0]
                    un = r.username if hasattr(r, "username") else r[1]
                    if uid and un:
                        usernames[str(uid)] = str(un)
                    elif uid:
                        ids_without_username.append(str(uid))
                try:
                    for uid in ids_without_username[: min(50, len(ids_without_username))]:
                        enqueue("enrich_user", key=str(uid), payload={"ig_user_id": str(uid)})
                except Exception:
                    pass
        except Exception:
            usernames = {}

        # Fetch cached ads for messages in this thread
        ads_cache: dict[str, dict[str, Any]] = {}
        try:
            if ad_ids:
                placeholders = ",".join([":a" + str(i) for i in range(len(ad_ids))])
                from sqlalchemy import text as _text
                params = {("a" + str(i)): ad_ids[i] for i in range(len(ad_ids))}
                rows_ad = session.exec(_text(f"SELECT ad_id, name, image_url, link FROM ads WHERE ad_id IN ({placeholders})")).params(**params).all()
                for r in rows_ad:
                    aid = r.ad_id if hasattr(r, "ad_id") else r[0]
                    name = r.name if hasattr(r, "name") else (r[1] if len(r) > 1 else None)
                    img = r.image_url if hasattr(r, "image_url") else (r[2] if len(r) > 2 else None)
                    lnk = r.link if hasattr(r, "link") else (r[3] if len(r) > 3 else None)
                    ads_cache[str(aid)] = {"name": name, "image_url": img, "link": lnk}
        except Exception:
            ads_cache = {}

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
                # Only include attachments that are already fetched to avoid 404s on /ig/media/local/*
                rows = session.exec(_text(f"SELECT id, message_id, position, storage_path, fetch_status FROM attachments WHERE message_id IN ({placeholders}) ORDER BY position ASC")).params(**params).all()
                for r in rows:
                    att_id = r.id if hasattr(r, "id") else r[0]
                    m_id = r.message_id if hasattr(r, "message_id") else r[1]
                    pos = r.position if hasattr(r, "position") else r[2]
                    sp = r.storage_path if hasattr(r, "storage_path") else (r[3] if len(r) > 3 else None)
                    fs = r.fetch_status if hasattr(r, "fetch_status") else (r[4] if len(r) > 4 else None)
                    mid = msgid_to_mid.get(int(m_id)) or ""
                    # Only map to local ids when we actually have a file on disk
                    if mid and sp and str(sp).strip() and str(fs or "").lower() == "ok":
                        att_ids_map.setdefault(mid, []).append(int(att_id))
        except Exception:
            att_ids_map = {}
        # Additionally, if attachments_json is missing but we have attachment rows,
        # build a positions list so template can stream directly from Graph.
        try:
            if msg_ids:
                from sqlalchemy import text as _text
                placeholders = ",".join([":q" + str(i) for i in range(len(msg_ids))])
                params = {("q" + str(i)): int(msg_ids[i]) for i in range(len(msg_ids))}
                rows_pos = session.exec(_text(f"SELECT message_id, position FROM attachments WHERE message_id IN ({placeholders}) ORDER BY position ASC")).params(**params).all()
                # message_id -> [positions...]
                tmp: dict[int, list[int]] = {}
                for r in rows_pos:
                    m_id = r.message_id if hasattr(r, "message_id") else r[0]
                    pos = r.position if hasattr(r, "position") else (r[1] if len(r) > 1 else None)
                    if m_id is None or pos is None:
                        continue
                    tmp.setdefault(int(m_id), []).append(int(pos))
                # convert to ig_message_id -> positions only when attachments_json did not already provide mapping
                for mid_internal, positions in tmp.items():
                    mid = msgid_to_mid.get(int(mid_internal)) or ""
                    if not mid:
                        continue
                    if mid not in att_map and positions:
                        att_map[mid] = positions
        except Exception:
            pass
        templates = request.app.state.templates
        # Fetch latest AI shadow draft (suggested) for this conversation
        shadow = None
        # Fetch ALL AI suggestions for this thread (not just the latest)
        try:
            from sqlalchemy import text as _text
            rows_shadow = session.exec(
                _text(
                    "SELECT id, reply_text, model, confidence, reason, created_at, status FROM ai_shadow_reply WHERE convo_id=:cid ORDER BY id ASC LIMIT 200"
                ).params(cid=str(conversation_id))
            ).all()
            # Represent the last one (if any) in 'shadow' for legacy panel rendering
            if rows_shadow:
                rlast = rows_shadow[-1]
                shadow = {
                    "id": getattr(rlast, "id", None) if hasattr(rlast, "id") else (rlast[0] if len(rlast) > 0 else None),
                    "text": getattr(rlast, "reply_text", None) if hasattr(rlast, "reply_text") else (rlast[1] if len(rlast) > 1 else None),
                    "model": getattr(rlast, "model", None) if hasattr(rlast, "model") else (rlast[2] if len(rlast) > 2 else None),
                    "confidence": getattr(rlast, "confidence", None) if hasattr(rlast, "confidence") else (rlast[3] if len(rlast) > 3 else None),
                    "reason": getattr(rlast, "reason", None) if hasattr(rlast, "reason") else (rlast[4] if len(rlast) > 4 else None),
                    "created_at": getattr(rlast, "created_at", None) if hasattr(rlast, "created_at") else (rlast[5] if len(rlast) > 5 else None),
                }
                try:
                    ca = shadow.get("created_at")
                    if ca:
                        from datetime import datetime as _d
                        dtv = _d.fromisoformat(ca.replace("Z","+00:00")) if isinstance(ca, str) and "Z" in ca else (_d.fromisoformat(ca) if isinstance(ca, str) else ca)
                        if dtv:
                            shadow["timestamp_ms"] = int(dtv.timestamp() * 1000)
                except Exception:
                    pass
            else:
                shadow = None
        except Exception:
            rows_shadow = []
            shadow = None
        # If inline drafts enabled, merge all suggestions as virtual messages and resort by timestamp
        import os as _os
        inline_drafts = (_os.getenv("IG_INLINE_DRAFTS", "1") not in ("0", "false", "False"))
        if inline_drafts and rows_shadow:
            # Determine product focus once for this thread (fallback to None)
            try:
                focus_slug, _ = _detect_focus_product(conversation_id)
            except Exception:
                focus_slug = None
            vms: list[dict] = []
            from datetime import datetime as _d
            for rr in rows_shadow:
                try:
                    txt = getattr(rr, "reply_text", None) if hasattr(rr, "reply_text") else (rr[1] if len(rr) > 1 else None)
                    if not (txt and str(txt).strip()):
                        continue
                    ca = getattr(rr, "created_at", None) if hasattr(rr, "created_at") else (rr[5] if len(rr) > 5 else None)
                    ts = None
                    if ca:
                        try:
                            ts = _d.fromisoformat(ca.replace("Z","+00:00")).timestamp()*1000 if isinstance(ca, str) else (ca.timestamp()*1000)
                            ts = int(ts)
                        except Exception:
                            ts = None
                    vm = {
                        "direction": "out",
                        "text": str(txt),
                        "timestamp_ms": ts or 0,
                        "sender_username": "AI",
                        "ig_message_id": None,
                        "ig_sender_id": None,
                        "ig_recipient_id": None,
                        "is_ai_draft": True,
                        "ai_model": getattr(rr, "model", None) if hasattr(rr, "model") else (rr[2] if len(rr) > 2 else None),
                        "ai_reason": getattr(rr, "reason", None) if hasattr(rr, "reason") else (rr[4] if len(rr) > 4 else None),
                        "product_slug": focus_slug or "default",
                    }
                    vms.append(vm)
                except Exception:
                    continue
            msgs = list(msgs) + vms
            try:
                msgs.sort(key=lambda m: (getattr(m, "timestamp_ms", None) if hasattr(m, "timestamp_ms") else (m.get("timestamp_ms") if isinstance(m, dict) else 0)) or 0)
            except Exception:
                pass
        return templates.TemplateResponse(
            "ig_thread.html",
            {
                "request": request,
                "conversation_id": conversation_id,
                "messages": msgs,
                "other_label": other_label,
                "enrich": enrich_status,
                "att_map": att_map,
                "att_ids_map": att_ids_map,
                "usernames": usernames,
                "ads_cache": ads_cache,
                "contact_name": contact_name,
                "contact_phone": contact_phone,
                "contact_address": contact_address,
                "linked_order_id": linked_order_id,
                "ai_status": ai_status,
                "ai_json": ai_json,
                "shadow": shadow,
                "inline_drafts": inline_drafts,
            },
        )


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

@router.post("/inbox/{conversation_id}/shadow/dismiss")
def dismiss_shadow(conversation_id: str):
	# Mark the latest suggested shadow draft as dismissed
	try:
		from sqlalchemy import text as _text
		from ..services.monitoring import increment_counter
		with get_session() as session:
			row = session.exec(
				_text("SELECT id FROM ai_shadow_reply WHERE convo_id=:cid AND (status IS NULL OR status='suggested') ORDER BY id DESC LIMIT 1")
			).params(cid=str(conversation_id)).first()
			if not row:
				return {"status": "ok", "changed": 0}
			rid = getattr(row, "id", None) if hasattr(row, "id") else (row[0] if isinstance(row, (list, tuple)) else None)
			if not rid:
				return {"status": "ok", "changed": 0}
			session.exec(_text("UPDATE ai_shadow_reply SET status='dismissed' WHERE id=:id").params(id=int(rid)))
			try:
				increment_counter("ai_draft_dismissed", 1)
			except Exception:
				pass
		return {"status": "ok", "changed": 1}
	except Exception as e:
		return {"status": "error", "error": str(e)}

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
                        "INSERT OR IGNORE INTO ai_conversations(convo_id) VALUES (:cid)"
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
					# fallback for SQLite
					session.exec(_text("INSERT OR IGNORE INTO ads(ad_id, name, image_url, link, updated_at) VALUES (:id, :n, NULL, :lnk, CURRENT_TIMESTAMP)")).params(id=str(aid), n=(title or None), lnk=(lnk or ("https://www.facebook.com/ads/library/?id=" + str(aid))))
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
        # Ads / Stories caches (tables exist in both SQLite/MySQL with same names)
        run("DELETE FROM ads", key="ads")
        run("DELETE FROM stories", key="stories")
        # Optional mapping tables (MySQL-only); ignore errors on SQLite
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
                # ai_conversations upsert copy
                # SQLite path
                try:
                    session.exec(
                        _text(
                            """
                            INSERT INTO ai_conversations(convo_id, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title, hydrated_at)
                            SELECT :g, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title, hydrated_at
                            FROM ai_conversations WHERE convo_id=:d
                            ON CONFLICT(convo_id) DO UPDATE SET
                              last_message_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_id ELSE ai_conversations.last_message_id END,
                              last_message_timestamp_ms=GREATEST(COALESCE(ai_conversations.last_message_timestamp_ms,0), excluded.last_message_timestamp_ms),
                              last_message_text=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_text ELSE ai_conversations.last_message_text END,
                              last_message_direction=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_direction ELSE ai_conversations.last_message_direction END,
                              last_sender_username=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_sender_username ELSE ai_conversations.last_sender_username END,
                              ig_sender_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.ig_sender_id ELSE ai_conversations.ig_sender_id END,
                              ig_recipient_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.ig_recipient_id ELSE ai_conversations.ig_recipient_id END,
                              last_ad_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_id ELSE ai_conversations.last_ad_id END,
                              last_ad_link=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_link ELSE ai_conversations.last_ad_link END,
                              last_ad_title=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_title ELSE ai_conversations.last_ad_title END,
                              hydrated_at=COALESCE(ai_conversations.hydrated_at, excluded.hydrated_at)
                            """
                        ).params(g=str(graph_id), d=str(dm_id))
                    )
                except Exception:
                    # MySQL path
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


@router.post("/inbox/{conversation_id}/merge-to-graph")
def merge_this_thread_to_graph(conversation_id: str, max_messages: int = 50):
    """Resolve Graph conversation id for this thread and migrate legacy dm:<id> rows to it."""
    # Only meaningful for dm:<ig_user_id>
    if not (isinstance(conversation_id, str) and conversation_id.startswith("dm:")):
        return {"status": "ok", "message": "already_graph_or_unsupported"}
    try:
        other_id = conversation_id.split(":", 1)[1]
    except Exception:
        return {"status": "error", "error": "invalid_dm_conversation_id"}
    # Resolve active page/user id for fetching
    try:
        _, entity_id, _ = _get_base_token_and_id()
        igba_id = str(entity_id)
    except Exception as e:
        return {"status": "error", "error": f"resolve_token_failed: {e}"}
    # Ensure mapping exists by fetching a small sample (also updates conversations.graph_conversation_id best-effort)
    try:
        import asyncio as _aio
        loop = _aio.get_event_loop()
        from ..services.instagram_api import fetch_thread_messages as _ftm
        loop.run_until_complete(_ftm(igba_id, str(other_id), limit=max(1, min(int(max_messages or 50), 200))))
    except Exception:
        pass
    # Read mapping
    graph_id = None
    try:
        from sqlalchemy import text as _text
        with get_session() as session:
            rowc = session.exec(
                _text(
                    "SELECT graph_conversation_id FROM conversations WHERE igba_id=:g AND ig_user_id=:u ORDER BY last_message_at DESC LIMIT 1"
                ).params(g=str(igba_id), u=str(other_id))
            ).first()
            if rowc:
                graph_id = rowc.graph_conversation_id if hasattr(rowc, "graph_conversation_id") else (rowc[0] if len(rowc) > 0 else None)
    except Exception:
        graph_id = None
    # Fallback: infer graph conversation id from latest message rows if mapping missing
    if not graph_id:
        try:
            from sqlalchemy import text as _text
            with get_session() as session:
                rowm = session.exec(
                    _text(
                        """
                        SELECT conversation_id
                        FROM message
                        WHERE (ig_sender_id=:u OR ig_recipient_id=:u) AND conversation_id IS NOT NULL AND conversation_id NOT LIKE 'dm:%'
                        ORDER BY timestamp_ms DESC, id DESC
                        LIMIT 1
                        """
                    ).params(u=str(other_id))
                ).first()
                if rowm:
                    graph_id = rowm.conversation_id if hasattr(rowm, "conversation_id") else (rowm[0] if len(rowm) > 0 else None)
        except Exception:
            graph_id = None
    if not graph_id:
        return {"status": "error", "error": "graph_conversation_id_not_found"}
    # Perform targeted migration using same logic as bulk endpoint
    migrated = 0
    try:
        from sqlalchemy import text as _text
        with get_session() as session:
            dm_id = str(conversation_id)
            # Messages
            session.exec(_text("UPDATE message SET conversation_id=:g WHERE conversation_id=:d").params(g=str(graph_id), d=str(dm_id)))
            # Orders
            try:
                session.exec(_text('UPDATE "order" SET ig_conversation_id=:g WHERE ig_conversation_id=:d').params(g=str(graph_id), d=str(dm_id)))
            except Exception:
                try:
                    session.exec(_text("UPDATE `order` SET ig_conversation_id=:g WHERE ig_conversation_id=:d").params(g=str(graph_id), d=str(dm_id)))
                except Exception:
                    pass
            # ai_conversations upsert copy
            try:
                session.exec(
                    _text(
                        """
                        INSERT INTO ai_conversations(convo_id, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title, hydrated_at)
                        SELECT :g, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title, hydrated_at
                        FROM ai_conversations WHERE convo_id=:d
                        ON CONFLICT(convo_id) DO UPDATE SET
                          last_message_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_id ELSE ai_conversations.last_message_id END,
                          last_message_timestamp_ms=GREATEST(COALESCE(ai_conversations.last_message_timestamp_ms,0), excluded.last_message_timestamp_ms),
                          last_message_text=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_text ELSE ai_conversations.last_message_text END,
                          last_message_direction=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_direction ELSE ai_conversations.last_message_direction END,
                          last_sender_username=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_sender_username ELSE ai_conversations.last_sender_username END,
                          ig_sender_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.ig_sender_id ELSE ai_conversations.ig_sender_id END,
                          ig_recipient_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.ig_recipient_id ELSE ai_conversations.ig_recipient_id END,
                          last_ad_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_id ELSE ai_conversations.last_ad_id END,
                          last_ad_link=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_link ELSE ai_conversations.last_ad_link END,
                          last_ad_title=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_title ELSE ai_conversations.last_ad_title END,
                          hydrated_at=COALESCE(ai_conversations.hydrated_at, excluded.hydrated_at)
                        """
                    ).params(g=str(graph_id), d=str(dm_id))
                )
            except Exception:
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
            migrated = 1
    except Exception as e:
        return {"status": "error", "error": f"migrate_failed: {e}"}
    return {"status": "ok", "graph_conversation_id": str(graph_id), "migrated": int(migrated)}

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
                    session.exec(_text("INSERT OR IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=str(cid))
                except Exception:
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
                # upsert last-* fields (SQLite then MySQL)
                try:
                    session.exec(
                        _text(
                            """
                            INSERT INTO ai_conversations(convo_id, last_message_id, last_message_timestamp_ms, last_message_text, last_message_direction, last_sender_username, ig_sender_id, ig_recipient_id, last_ad_id, last_ad_link, last_ad_title)
                            VALUES (:cid, :mid, :ts, :txt, :dir, :sun, :sid, :rid, :adid, :alink, :atitle)
                            ON CONFLICT(convo_id) DO UPDATE SET
                              last_message_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_id ELSE ai_conversations.last_message_id END,
                              last_message_timestamp_ms=GREATEST(COALESCE(ai_conversations.last_message_timestamp_ms,0), excluded.last_message_timestamp_ms),
                              last_message_text=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_text ELSE ai_conversations.last_message_text END,
                              last_message_direction=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_message_direction ELSE ai_conversations.last_message_direction END,
                              last_sender_username=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_sender_username ELSE ai_conversations.last_sender_username END,
                              ig_sender_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.ig_sender_id ELSE ai_conversations.ig_sender_id END,
                              ig_recipient_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.ig_recipient_id ELSE ai_conversations.ig_recipient_id END,
                              last_ad_id=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_id ELSE ai_conversations.last_ad_id END,
                              last_ad_link=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_link ELSE ai_conversations.last_ad_link END,
                              last_ad_title=CASE WHEN excluded.last_message_timestamp_ms >= COALESCE(ai_conversations.last_message_timestamp_ms,0) THEN excluded.last_ad_title ELSE ai_conversations.last_ad_title END
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
	# Populate latest_messages from message table (MySQL/SQLite compatible)
	from sqlalchemy import text as _text
	created = 0
	try:
		with get_session() as session:
			# MySQL 8 and SQLite both support CTE; but also provide fallback
			sql_cte = """
				WITH lm AS (
					SELECT conversation_id, MAX(COALESCE(timestamp_ms,0)) AS ts
					FROM message
					WHERE conversation_id IS NOT NULL
					GROUP BY conversation_id
					ORDER BY ts DESC
					LIMIT :n
				)
				INSERT INTO latest_messages(convo_id, message_id, timestamp_ms, text, sender_username, direction, ig_sender_id, ig_recipient_id, ad_link, ad_title)
				SELECT m.conversation_id, m.id, COALESCE(m.timestamp_ms,0), m.text, m.sender_username, m.direction, m.ig_sender_id, m.ig_recipient_id, m.ad_link, m.ad_title
				FROM message m
				JOIN lm ON lm.conversation_id = m.conversation_id AND lm.ts = COALESCE(m.timestamp_ms,0)
				ON CONFLICT(convo_id) DO UPDATE SET
				  message_id=excluded.message_id,
				  timestamp_ms=excluded.timestamp_ms,
				  text=excluded.text,
				  sender_username=excluded.sender_username,
				  direction=excluded.direction,
				  ig_sender_id=excluded.ig_sender_id,
				  ig_recipient_id=excluded.ig_recipient_id,
				  ad_link=excluded.ad_link,
				  ad_title=excluded.ad_title
			"""
			try:
				res = session.exec(_text(sql_cte).params(n=int(max(1000, min(limit, 200000)))) )
				created = int(getattr(res, "rowcount", 0) or 0)
			except Exception:
				# MySQL fallback using ON DUPLICATE KEY UPDATE without CTE
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
@router.post("/inbox/{conversation_id}/enrich")
def enqueue_enrich(conversation_id: str):
    # Enqueue enrich_user for the other party and enrich_page for the active page/user id
    other_id: str | None = None
    igba_id: str | None = None
    with get_session() as session:
        # Try conversations table first (reliable)
        try:
            from sqlalchemy import text as _text
            row = session.exec(
                _text("SELECT igba_id, ig_user_id FROM conversations WHERE convo_id=:cid LIMIT 1")
            ).params(cid=str(conversation_id)).first()
            if row:
                igba_id = str(getattr(row, "igba_id", None) or (row[0] if len(row) > 0 else "") or "")
                other_id = str(getattr(row, "ig_user_id", None) or (row[1] if len(row) > 1 else "") or "")
        except Exception:
            pass
        # If convo_id wasn't found, try resolving by Graph conversation id mapping
        if not other_id:
            try:
                from sqlalchemy import text as _text
                rowg = session.exec(
                    _text("SELECT igba_id, ig_user_id FROM conversations WHERE graph_conversation_id=:gc LIMIT 1")
                ).params(gc=str(conversation_id)).first()
                if rowg:
                    igba_id = str(getattr(rowg, "igba_id", None) or (rowg[0] if len(rowg) > 0 else "") or "")
                    other_id = str(getattr(rowg, "ig_user_id", None) or (rowg[1] if len(rowg) > 1 else "") or "")
            except Exception:
                pass
        # Fallback for legacy conversation_id formats
        if not other_id and conversation_id.startswith("dm:"):
            try:
                other_id = conversation_id.split(":", 1)[1] or None
            except Exception:
                other_id = None
        # Last-resort: infer other_id from latest message rows for this conversation id
        if not other_id:
            try:
                from sqlalchemy import text as _text
                rmsg = session.exec(
                    _text("SELECT ig_sender_id, ig_recipient_id FROM message WHERE conversation_id=:cid ORDER BY timestamp_ms DESC, id DESC LIMIT 1")
                ).params(cid=str(conversation_id)).first()
                if rmsg:
                    sid = getattr(rmsg, "ig_sender_id", None) if hasattr(rmsg, "ig_sender_id") else (rmsg[0] if len(rmsg) > 0 else None)
                    rid = getattr(rmsg, "ig_recipient_id", None) if hasattr(rmsg, "ig_recipient_id") else (rmsg[1] if len(rmsg) > 1 else None)
                    try:
                        _, owner_id, _ = _get_base_token_and_id()
                        if sid and str(sid) == str(owner_id):
                            other_id = str(rid) if rid else None
                        else:
                            other_id = str(sid) if sid else None
                    except Exception:
                        other_id = str(sid) if sid else (str(rid) if rid else None)
            except Exception:
                pass
        # Final fallback: derive by fetching recent messages from Graph for this conversation id
        if not other_id:
            try:
                import asyncio as _aio
                loop = _aio.get_event_loop()
                from ..services.instagram_api import fetch_messages as _fm, _get_base_token_and_id as _gb
                _, owner_id, _ = _gb()
                msgs = loop.run_until_complete(_fm(str(conversation_id), limit=10))
                try:
                    _log.info("hydrate.resolve.graph_scan msgs_len=%s owner=%s", (len(msgs) if isinstance(msgs, list) else None), str(owner_id))
                    if isinstance(msgs, list) and msgs:
                        m0 = msgs[0] if isinstance(msgs[0], dict) else {}
                        _log.info("hydrate.resolve.graph_scan first.from=%s first.to_count=%s", str(((m0.get('from') or {}) or {}).get('id')), len((((m0.get('to') or {}) or {}).get('data') or [])))
                except Exception:
                    pass
                uid: str | None = None
                for m in (msgs or []):
                    try:
                        frm = (m.get("from") or {}).get("id")
                        if frm and str(frm) != str(owner_id):
                            uid = str(frm)
                            break
                        to = (((m.get("to") or {}) or {}).get("data") or [])
                        for t in to:
                            tid = t.get("id")
                            if tid and str(tid) != str(owner_id):
                                uid = str(tid)
                                break
                        if uid:
                            break
                    except Exception:
                        continue
                if uid:
                    other_id = uid
                else:
                    try:
                        _log.info("hydrate.resolve.graph_scan_no_uid cid=%s owner=%s", str(conversation_id), str(owner_id))
                    except Exception:
                        pass
            except Exception as ex_gs:
                try:
                    _log.info("hydrate.resolve.graph_scan_error cid=%s err=%s", str(conversation_id), str(ex_gs)[:160])
                except Exception:
                    pass
        if not igba_id:
            try:
                _, entity_id, _ = _get_base_token_and_id()
                igba_id = str(entity_id)
            except Exception:
                igba_id = None
    if not other_id:
        raise HTTPException(status_code=400, detail="Could not resolve IG user id for conversation")
    queued = {"enrich_user": False, "enrich_page": False}
    try:
        enqueue("enrich_user", key=str(other_id), payload={"ig_user_id": str(other_id)})
        queued["enrich_user"] = True
    except Exception:
        pass
    if igba_id:
        try:
            enqueue("enrich_page", key=str(igba_id), payload={"igba_id": str(igba_id)})
            queued["enrich_page"] = True
        except Exception:
            pass
    return {"status": "ok", "queued": queued, "ig_user_id": other_id, "igba_id": igba_id}

@router.post("/inbox/{conversation_id}/hydrate")
def enqueue_hydrate(conversation_id: str, max_messages: int = 200):
    # Enqueue hydrate_conversation for this thread (igba_id + ig_user_id)
    other_id: str | None = None
    igba_id: str | None = None
    try:
        _log.info("hydrate.begin cid=%s", str(conversation_id))
    except Exception:
        pass
    with get_session() as session:
        try:
            from sqlalchemy import text as _text
            row = session.exec(
                _text("SELECT igba_id, ig_user_id FROM conversations WHERE convo_id=:cid LIMIT 1")
            ).params(cid=str(conversation_id)).first()
            if row:
                igba_id = str(getattr(row, "igba_id", None) or (row[0] if len(row) > 0 else "") or "")
                other_id = str(getattr(row, "ig_user_id", None) or (row[1] if len(row) > 1 else "") or "")
        except Exception:
            pass
        # Resolve via Graph conversation id mapping if convo_id row missing
        if not other_id:
            try:
                from sqlalchemy import text as _text
                rowg = session.exec(
                    _text("SELECT igba_id, ig_user_id FROM conversations WHERE graph_conversation_id=:gc LIMIT 1")
                ).params(gc=str(conversation_id)).first()
                if rowg:
                    igba_id = str(getattr(rowg, "igba_id", None) or (rowg[0] if len(rowg) > 0 else "") or "")
                    other_id = str(getattr(rowg, "ig_user_id", None) or (rowg[1] if len(rowg) > 1 else "") or "")
            except Exception:
                pass
        if not other_id and conversation_id.startswith("dm:"):
            try:
                other_id = conversation_id.split(":", 1)[1] or None
            except Exception:
                other_id = None
        # Last-resort: infer other_id from latest messages when viewing by Graph CID
        if not other_id:
            try:
                from sqlalchemy import text as _text
                rmsg = session.exec(
                    _text("SELECT ig_sender_id, ig_recipient_id FROM message WHERE conversation_id=:cid ORDER BY timestamp_ms DESC, id DESC LIMIT 1")
                ).params(cid=str(conversation_id)).first()
                if rmsg:
                    sid = getattr(rmsg, "ig_sender_id", None) if hasattr(rmsg, "ig_sender_id") else (rmsg[0] if len(rmsg) > 0 else None)
                    rid = getattr(rmsg, "ig_recipient_id", None) if hasattr(rmsg, "ig_recipient_id") else (rmsg[1] if len(rmsg) > 1 else None)
                    try:
                        _, owner_id, _ = _get_base_token_and_id()
                        if sid and str(sid) == str(owner_id):
                            other_id = str(rid) if rid else None
                        else:
                            other_id = str(sid) if sid else None
                    except Exception:
                        other_id = str(sid) if sid else (str(rid) if rid else None)
            except Exception:
                pass
        # Final fallback: derive by fetching recent messages from Graph for this conversation id
        if not other_id:
            try:
                import asyncio as _aio
                loop = _aio.get_event_loop()
                from ..services.instagram_api import fetch_messages as _fm, _get_base_token_and_id as _gb
                _, owner_id, _ = _gb()
                msgs = loop.run_until_complete(_fm(str(conversation_id), limit=10))
                uid: str | None = None
                for m in (msgs or []):
                    try:
                        frm = (m.get("from") or {}).get("id")
                        if frm and str(frm) != str(owner_id):
                            uid = str(frm)
                            break
                        to = (((m.get("to") or {}) or {}).get("data") or [])
                        for t in to:
                            tid = t.get("id")
                            if tid and str(tid) != str(owner_id):
                                uid = str(tid)
                                break
                        if uid:
                            break
                    except Exception:
                        continue
                if uid:
                    other_id = uid
            except Exception:
                pass
        if not igba_id:
            try:
                _, entity_id, _ = _get_base_token_and_id()
                igba_id = str(entity_id)
            except Exception:
                igba_id = None
    try:
        _log.info("hydrate.resolve.final cid=%s igba_id=%s other_id=%s", str(conversation_id), str(igba_id), str(other_id))
    except Exception:
        pass
    if not (igba_id and other_id):
        raise HTTPException(status_code=400, detail=f"Could not resolve identifiers to hydrate; cid={conversation_id} igba_id={igba_id} other_id={other_id}")
    # Best-effort: persist conversations mapping for future actions
    try:
        from sqlalchemy import text as _text
        with get_session() as session:
            conv_key = f"{str(igba_id)}:{str(other_id)}"
            try:
                session.exec(_text("INSERT OR IGNORE INTO conversations(convo_id, igba_id, ig_user_id, last_message_at, unread_count) VALUES (:cv, :g, :u, CURRENT_TIMESTAMP, 0)")).params(cv=conv_key, g=str(igba_id), u=str(other_id))
            except Exception:
                try:
                    session.exec(_text("INSERT IGNORE INTO conversations(convo_id, igba_id, ig_user_id, last_message_at, unread_count) VALUES (:cv, :g, :u, CURRENT_TIMESTAMP, 0)")).params(cv=conv_key, g=str(igba_id), u=str(other_id))
                except Exception:
                    pass
            try:
                session.exec(_text("UPDATE conversations SET graph_conversation_id=:gc WHERE convo_id=:cv")).params(gc=str(conversation_id), cv=conv_key)
            except Exception:
                pass
    except Exception:
        pass
    key = f"{igba_id}:{other_id}"
    try:
        enqueue("hydrate_conversation", key=key, payload={"igba_id": str(igba_id), "ig_user_id": str(other_id), "max_messages": int(max_messages)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"enqueue_failed: {e}")
    return {"status": "ok", "queued": True, "key": key, "igba_id": igba_id, "ig_user_id": other_id, "max_messages": int(max_messages)}


@router.get("/queue/status")
def queue_status():
	"""Return approximate queue sizes for related workers."""
	out = {"ingest": None, "hydrate_conversation": None, "enrich_user": None, "enrich_page": None}
	try:
		r = _get_redis()
		out["ingest"] = int(r.llen("jobs:ingest"))
		out["hydrate_conversation"] = int(r.llen("jobs:hydrate_conversation"))
		out["enrich_user"] = int(r.llen("jobs:enrich_user"))
		out["enrich_page"] = int(r.llen("jobs:enrich_page"))
	except Exception:
		# keep None to indicate unavailable
		pass
	return {"status": "ok", "queues": out}


@router.get("/debug/env")
def debug_env():
    """Lightweight diagnostics: show which token path is active (page vs user) and env presence.

    Does NOT return secrets; only booleans and token length/suffix for verification.
    """
    data: dict[str, object] = {
        "has_page_id": bool(os.getenv("IG_PAGE_ID")),
        "has_page_token": bool(os.getenv("IG_PAGE_ACCESS_TOKEN")),
        "has_user_id": bool(os.getenv("IG_USER_ID")),
        "has_user_token": bool(os.getenv("IG_ACCESS_TOKEN")),
        "graph_version": os.getenv("IG_GRAPH_API_VERSION", "v21.0"),
    }
    try:
        token, ident, is_page = _get_base_token_and_id()
        data["active_path"] = "page" if is_page else "user"
        data["id_in_use"] = str(ident)
        data["token_len"] = len(token or "")
        data["token_suffix"] = (token[-6:] if token else None)
    except Exception as e:
        data["resolve_error"] = str(e)
    return data

@router.post("/inbox/{conversation_id}/send")
async def send_message(conversation_id: str, body: dict):
    """Send a text reply to the other party in this conversation and persist locally.

    conversation_id formats supported:
    - "dm:<ig_user_id>" (preferred)
    - Graph conversation id: will resolve other party id from recent messages
    """
    text_val = (body or {}).get("text")
    if not text_val or not isinstance(text_val, str) or not text_val.strip():
        raise HTTPException(status_code=400, detail="Message text is required")
    text_val = text_val.strip()

    # Resolve recipient (other party IG user id)
    other_id: str | None = None
    if conversation_id.startswith("dm:"):
        other_id = conversation_id.split(":", 1)[1] or None
    else:
        # Fallback: infer from existing messages
        with get_session() as session:
            msgs = session.exec(
                select(Message).where(Message.conversation_id == conversation_id).order_by(Message.timestamp_ms.desc()).limit(50)
            ).all()
            for m in msgs:
                # other party is sender on inbound, recipient on outbound
                if (m.direction or "in") == "in" and m.ig_sender_id:
                    other_id = str(m.ig_sender_id)
                    break
                if (m.direction or "in") == "out" and m.ig_recipient_id:
                    other_id = str(m.ig_recipient_id)
                    break
    if not other_id:
        raise HTTPException(status_code=400, detail="Could not resolve recipient for this conversation")

    # Send via Messenger API for Instagram (requires Page token)
    token, entity_id, is_page = _get_base_token_and_id()
    if not is_page:
        raise HTTPException(status_code=400, detail="Sending requires a Page access token (IG_PAGE_ACCESS_TOKEN)")
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"
    url = base + "/me/messages"
    payload = {
        "recipient": {"id": other_id},
        "messaging_type": "RESPONSE",
        "message": {"text": text_val},
    }
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(url, params={"access_token": token}, json=payload, timeout=20)
            r.raise_for_status()
            resp = r.json()
        except httpx.HTTPStatusError as e:
            try:
                detail = e.response.text
            except Exception:
                detail = str(e)
            raise HTTPException(status_code=502, detail=f"Graph send failed: {detail}")
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Graph send failed: {e}")

    # Persist locally
    mid = str((resp or {}).get("message_id") or "")
    now_ms = int(time.time() * 1000)
    conv_id = conversation_id if conversation_id.startswith("dm:") else f"dm:{other_id}"
    with get_session() as session:
        # Idempotency: avoid duplicate insert when the same message_id was already saved
        if mid:
            try:
                exists = session.exec(select(Message).where(Message.ig_message_id == mid)).first()
                if exists:
                    # still bump last_message_at for the conversation to reflect the send time
                    try:
                        from datetime import datetime as _dt
                        ts_iso = _dt.utcfromtimestamp(int(now_ms/1000)).strftime('%Y-%m-%d %H:%M:%S')
                        from sqlalchemy import text as _text
                        session.exec(_text("UPDATE conversations SET last_message_at=:ts WHERE convo_id=:cid").params(ts=ts_iso, cid=conv_id))
                    except Exception:
                        pass
                    return {"status": "ok", "message_id": mid}
            except Exception:
                # proceed with best-effort insert
                pass
        row = Message(
            ig_sender_id=str(entity_id),
            ig_recipient_id=str(other_id),
            ig_message_id=(mid or None),
            text=text_val,
            attachments_json=None,
            timestamp_ms=now_ms,
            raw_json=json.dumps({"send_response": resp}, ensure_ascii=False),
            conversation_id=conv_id,
            direction="out",
        )
        session.add(row)
        # update conversations.last_message_at using this timestamp
        try:
            from datetime import datetime as _dt
            ts_iso = _dt.utcfromtimestamp(int(now_ms/1000)).strftime('%Y-%m-%d %H:%M:%S')
            from sqlalchemy import text as _text
            session.exec(_text("UPDATE conversations SET last_message_at=:ts WHERE convo_id=:cid").params(ts=ts_iso, cid=conv_id))
        except Exception:
            pass
    try:
        from ..services.monitoring import increment_counter
        increment_counter("sent_messages", 1)
        await notify_new_message({"type": "ig_message", "conversation_id": conv_id, "text": text_val, "timestamp_ms": now_ms})
    except Exception:
        pass
    return {"status": "ok", "message_id": mid or None}

