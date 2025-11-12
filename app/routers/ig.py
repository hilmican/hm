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
			# Use materialized latest_messages to avoid heavy CTE scans
			from sqlalchemy import text as _text
			base_sql = """
				SELECT ac.convo_id, lm.timestamp_ms, lm.text, lm.sender_username, lm.direction,
				       lm.ig_sender_id, lm.ig_recipient_id, lm.ad_link, lm.ad_title
				FROM ai_conversations ac
				LEFT JOIN latest_messages lm ON lm.convo_id = ac.convo_id
			"""
			where_parts: list[str] = []
			params: dict[str, object] = {}
			if q and isinstance(q, str) and q.strip():
				qq = f\"%{q.lower().strip()}%\"
				where_parts.append("""
					(
						(lm.text IS NOT NULL AND LOWER(lm.text) LIKE :qq)
						OR (lm.sender_username IS NOT NULL AND LOWER(lm.sender_username) LIKE :qq)
						OR EXISTS (
							SELECT 1 FROM ig_users u
							WHERE (u.ig_user_id = lm.ig_sender_id OR u.ig_user_id = lm.ig_recipient_id OR (ac.convo_id LIKE 'dm:%' AND u.ig_user_id = SUBSTR(ac.convo_id, 4)))
							  AND (
								(u.name IS NOT NULL AND LOWER(u.name) LIKE :qq)
								OR (u.username IS NOT NULL AND LOWER(u.username) LIKE :qq)
							  )
						)
					)
				""")
				params["qq"] = qq
			sample_n = max(50, min(int(limit or 25) * 4, 200))
			order_sql = " ORDER BY COALESCE(ac.ai_process_time, lm.timestamp_ms) DESC LIMIT :n"
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
        # Best-effort ad metadata from messages
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
                rows = session.exec(_text(f"SELECT id, message_id, position FROM attachments WHERE message_id IN ({placeholders}) ORDER BY position ASC")).params(**params).all()
                for r in rows:
                    att_id = r.id if hasattr(r, "id") else r[0]
                    m_id = r.message_id if hasattr(r, "message_id") else r[1]
                    pos = r.position if hasattr(r, "position") else r[2]
                    mid = msgid_to_mid.get(int(m_id)) or ""
                    if mid:
                        att_ids_map.setdefault(mid, []).append(int(att_id))
        except Exception:
            att_ids_map = {}
		templates = request.app.state.templates
		# Fetch latest AI shadow draft (suggested) for this conversation
		shadow = None
		try:
			from sqlalchemy import text as _text
			row_shadow = session.exec(
				_text(
					"SELECT id, reply_text, model, confidence, reason, created_at FROM ai_shadow_reply WHERE convo_id=:cid AND (status IS NULL OR status='suggested') ORDER BY id DESC LIMIT 1"
				).params(cid=str(conversation_id))
			).first()
			if row_shadow:
				shadow = {
					"id": getattr(row_shadow, "id", None) if hasattr(row_shadow, "id") else (row_shadow[0] if len(row_shadow) > 0 else None),
					"text": getattr(row_shadow, "reply_text", None) if hasattr(row_shadow, "reply_text") else (row_shadow[1] if len(row_shadow) > 1 else None),
					"model": getattr(row_shadow, "model", None) if hasattr(row_shadow, "model") else (row_shadow[2] if len(row_shadow) > 2 else None),
					"confidence": getattr(row_shadow, "confidence", None) if hasattr(row_shadow, "confidence") else (row_shadow[3] if len(row_shadow) > 3 else None),
					"reason": getattr(row_shadow, "reason", None) if hasattr(row_shadow, "reason") else (row_shadow[4] if len(row_shadow) > 4 else None),
					"created_at": getattr(row_shadow, "created_at", None) if hasattr(row_shadow, "created_at") else (row_shadow[5] if len(row_shadow) > 5 else None),
				}
		except Exception:
			shadow = None
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
        # Fallback for legacy conversation_id formats
        if not other_id and conversation_id.startswith("dm:"):
            try:
                other_id = conversation_id.split(":", 1)[1] or None
            except Exception:
                other_id = None
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
        if not other_id and conversation_id.startswith("dm:"):
            try:
                other_id = conversation_id.split(":", 1)[1] or None
            except Exception:
                other_id = None
        if not igba_id:
            try:
                _, entity_id, _ = _get_base_token_and_id()
                igba_id = str(entity_id)
            except Exception:
                igba_id = None
    if not (igba_id and other_id):
        raise HTTPException(status_code=400, detail="Could not resolve identifiers to hydrate")
    key = f"{igba_id}:{other_id}"
    try:
        enqueue("hydrate_conversation", key=key, payload={"igba_id": str(igba_id), "ig_user_id": str(other_id), "max_messages": int(max_messages)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"enqueue_failed: {e}")
    return {"status": "ok", "queued": True, "key": key, "igba_id": igba_id, "ig_user_id": other_id, "max_messages": int(max_messages)}


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
        await notify_new_message({"type": "ig_message", "conversation_id": conv_id, "text": text_val, "timestamp_ms": now_ms})
    except Exception:
        pass
    return {"status": "ok", "message_id": mid or None}

