from fastapi import APIRouter, Request, HTTPException, Form, Body
from fastapi.responses import FileResponse, RedirectResponse
from starlette.status import HTTP_303_SEE_OTHER
from pathlib import Path
from typing import Any
import json
import time
import httpx
import os as _os
from datetime import datetime as _d

from ..db import get_session
from ..models import Message, IGAiDebugRun, Conversation, AdminMessage
from sqlalchemy import text as _text

from ..services.instagram_api import sync_latest_conversations, _get_base_token_and_id, GRAPH_VERSION
from ..services.queue import enqueue
from ..services.ai_shadow import touch_shadow_state
from ..services.ai_ig import _detect_focus_product
from ..services.monitoring import increment_counter
from ..services.ai_reply import _load_history, _sanitize_reply_text
from ..services.ingest import _extract_graph_conversation_id_from_message_id
from ..services.admin_notifications import create_admin_notification
from ..services.channel_sender import send_message as send_channel_message

router = APIRouter()


def _resolve_graph_conversation_id_for_hydrate(conversation_identifier: str) -> tuple[str | None, dict[str, Any]]:
    """
    Best-effort lookup/derivation of the Graph conversation ID used by Meta APIs.
    Returns (graph_id or None, debug_info dict for logging/UX).
    """
    info: dict[str, Any] = {"input": str(conversation_identifier)}
    cid_str = str(conversation_identifier or "").strip()
    if not cid_str:
        info["status"] = "missing_identifier"
        return None, info
    if cid_str.startswith("dm:"):
        info["status"] = "dm_identifier"
        return None, info

    with get_session() as session:
        conv_pk: int | None = None
        graph_cid: str | None = None

        # If identifier is numeric, treat it as conversations.id
        try:
            conv_pk = int(cid_str)
        except (TypeError, ValueError):
            conv_pk = None

        # If identifier isn't a numeric PK, try resolving via convo_id
        if conv_pk is None:
            try:
                row = session.exec(
                    _text("SELECT id, graph_conversation_id FROM conversations WHERE convo_id=:cid LIMIT 1").params(cid=cid_str)
                ).first()
            except Exception as exc:
                info["convo_lookup_error"] = str(exc)[:160]
                row = None
            if row:
                conv_pk = (
                    getattr(row, "id", None)
                    if hasattr(row, "id")
                    else (row[0] if isinstance(row, (list, tuple)) and len(row) > 0 else None)
                )
                graph_cid = (
                    getattr(row, "graph_conversation_id", None)
                    if hasattr(row, "graph_conversation_id")
                    else (row[1] if isinstance(row, (list, tuple)) and len(row) > 1 else None)
                )
                if graph_cid:
                    info["status"] = "resolved_from_convo_id"
                    info["conversation_pk"] = int(conv_pk) if conv_pk is not None else None
                    info["graph_id"] = str(graph_cid)
                    return str(graph_cid), info

        if conv_pk is not None:
            # Direct lookup on conversations.id
            row = session.exec(
                _text("SELECT graph_conversation_id FROM conversations WHERE id=:cid LIMIT 1").params(cid=int(conv_pk))
            ).first()
            graph_cid = (
                getattr(row, "graph_conversation_id", None)
                if row is not None and hasattr(row, "graph_conversation_id")
                else (row[0] if isinstance(row, (list, tuple)) and len(row) > 0 else None)
                if row
                else None
            )
            if graph_cid:
                info["status"] = "resolved_from_pk"
                info["conversation_pk"] = int(conv_pk)
                info["graph_id"] = str(graph_cid)
                return str(graph_cid), info

            # Derive from the latest ig_message_id if Graph ID is missing
            row_msg = session.exec(
                _text(
                    "SELECT ig_message_id FROM message WHERE conversation_id=:cid AND ig_message_id IS NOT NULL ORDER BY timestamp_ms DESC, id DESC LIMIT 1"
                ).params(cid=int(conv_pk))
            ).first()
            ig_mid = (
                getattr(row_msg, "ig_message_id", None)
                if row_msg and hasattr(row_msg, "ig_message_id")
                else (row_msg[0] if isinstance(row_msg, (list, tuple)) and len(row_msg) > 0 else None)
            )
            if ig_mid:
                derived = _extract_graph_conversation_id_from_message_id(str(ig_mid))
                if derived:
                    try:
                        session.exec(
                            _text(
                                "UPDATE conversations SET graph_conversation_id=:gc WHERE id=:cid AND (graph_conversation_id IS NULL OR graph_conversation_id=:gc)"
                            ).params(gc=str(derived), cid=int(conv_pk))
                        )
                    except Exception as exc:
                        info["persist_error"] = str(exc)[:160]
                    info["status"] = "derived_from_message"
                    info["conversation_pk"] = int(conv_pk)
                    info["graph_id"] = str(derived)
                    return str(derived), info

            # If we have igba_id and ig_user_id but no graph_conversation_id, try fetching from Graph API
            if conv_pk is not None:
                # Get igba_id and ig_user_id for this conversation
                conv_row = session.exec(
                    _text("SELECT igba_id, ig_user_id FROM conversations WHERE id=:cid LIMIT 1").params(cid=int(conv_pk))
                ).first()
                if conv_row:
                    igba_id = getattr(conv_row, "igba_id", None) or (conv_row[0] if len(conv_row) > 0 else None)
                    ig_user_id = getattr(conv_row, "ig_user_id", None) or (conv_row[1] if len(conv_row) > 1 else None)
                    
                    if igba_id and ig_user_id:
                        # Try to fetch from Graph API conversations endpoint
                        try:
                            import asyncio as _aio
                            try:
                                loop = _aio.get_event_loop()
                            except RuntimeError:
                                loop = _aio.new_event_loop()
                                _aio.set_event_loop(loop)
                            from ..services.instagram_api import fetch_conversations as _fc
                            # Fetch conversations and match by participants
                            all_convs = loop.run_until_complete(_fc(limit=100))
                            for conv in (all_convs or []):
                                conv_id = conv.get("id")
                                participants = conv.get("participants", {}).get("data", [])
                                participant_ids = [p.get("id") for p in participants if p.get("id")]
                                # Check if this conversation includes our ig_user_id
                                if str(ig_user_id) in [str(p) for p in participant_ids] and conv_id:
                                    # Found it! Store and return
                                    try:
                                        session.exec(
                                            _text(
                                                "UPDATE conversations SET graph_conversation_id=:gc WHERE id=:cid AND (graph_conversation_id IS NULL OR graph_conversation_id=:gc)"
                                            ).params(gc=str(conv_id), cid=int(conv_pk))
                                        )
                                    except Exception:
                                        pass
                                    info["status"] = "fetched_from_graph_api"
                                    info["conversation_pk"] = int(conv_pk)
                                    info["graph_id"] = str(conv_id)
                                    return str(conv_id), info
                            info["graph_api_no_match"] = True
                            info["conversations_checked"] = len(all_convs or [])
                        except Exception as e:
                            info["graph_api_fetch_error"] = str(e)[:160]
        
        # As a final heuristic, treat alphanumeric identifiers as Graph IDs
        if any(ch.isalpha() for ch in cid_str):
            info["status"] = "assumed_graph"
            info["graph_id"] = cid_str
            return cid_str, info

    info["status"] = "not_resolved"
    return None, info


@router.get("/inbox/{conversation_id}/debug")
def debug_conversation(request: Request, conversation_id: int, limit: int = 25):
    templates = request.app.state.templates
    n = max(1, min(int(limit or 25), 100))
    with get_session() as session:
        from sqlmodel import select
        runs = session.exec(
            select(IGAiDebugRun)
            .where(IGAiDebugRun.conversation_id == str(conversation_id))
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


@router.get("/inbox/shadow/{draft_id}")
def shadow_debug(request: Request, draft_id: int):
    """
    Show details of a single shadow draft:
    - System prompt used
    - Prompt/user payload JSON
    - Raw model response (if available)
    """
    from sqlalchemy import text as _text
    templates = request.app.state.templates
    conversation_info: dict[str, Any] | None = None
    history_preview: list[dict[str, Any]] = []
    with get_session() as session:
        row = session.exec(
            _text(
                """
                SELECT id, conversation_id, reply_text, model, confidence, reason, json_meta, actions_json, status, created_at
                FROM ai_shadow_reply
                WHERE id = :id
                LIMIT 1
                """
            ).params(id=int(draft_id))
        ).first()
        if not row:
            raise HTTPException(status_code=404, detail="Shadow draft not found")
        # Support both row objects and tuples
        did = getattr(row, "id", None) if hasattr(row, "id") else (row[0] if len(row) > 0 else None)
        convo_id = getattr(row, "conversation_id", None) if hasattr(row, "conversation_id") else (row[1] if len(row) > 1 else None)
        reply_text = getattr(row, "reply_text", None) if hasattr(row, "reply_text") else (row[2] if len(row) > 2 else None)
        model = getattr(row, "model", None) if hasattr(row, "model") else (row[3] if len(row) > 3 else None)
        confidence = getattr(row, "confidence", None) if hasattr(row, "confidence") else (row[4] if len(row) > 4 else None)
        reason = getattr(row, "reason", None) if hasattr(row, "reason") else (row[5] if len(row) > 5 else None)
        json_meta = getattr(row, "json_meta", None) if hasattr(row, "json_meta") else (row[6] if len(row) > 6 else None)
        actions_json = getattr(row, "actions_json", None) if hasattr(row, "actions_json") else (row[7] if len(row) > 7 else None)
        status = getattr(row, "status", None) if hasattr(row, "status") else (row[8] if len(row) > 8 else None)
        created_at = getattr(row, "created_at", None) if hasattr(row, "created_at") else (row[9] if len(row) > 9 else None)
        if convo_id:
            try:
                conv = session.get(Conversation, int(convo_id))
                user_row = None
                if conv and conv.ig_user_id:
                    user_row = session.exec(
                        _text("SELECT username, name FROM ig_users WHERE ig_user_id=:uid LIMIT 1").params(uid=str(conv.ig_user_id))
                    ).first()
                conversation_info = {
                    "ig_user_id": str(conv.ig_user_id) if conv and conv.ig_user_id else None,
                    "username": getattr(user_row, "username", None) if user_row and hasattr(user_row, "username") else (user_row[0] if user_row and len(user_row) > 0 else None),
                    "name": getattr(user_row, "name", None) if user_row and hasattr(user_row, "name") else (user_row[1] if user_row and len(user_row) > 1 else None),
                    "is_mock": bool(conv and conv.ig_user_id and str(conv.ig_user_id).startswith("mock_")),
                    "last_message_at": getattr(conv, "last_message_at", None) if conv else None,
                }
            except Exception:
                conversation_info = None
    if convo_id:
        try:
            history_preview, _ = _load_history(int(convo_id), limit=12)
        except Exception:
            history_preview = []
    debug_meta = None
    if json_meta:
        try:
            import json as _json

            debug_meta = _json.loads(json_meta)
            if isinstance(debug_meta, dict):
                # pretty-print nested fields for template
                try:
                    user_payload = debug_meta.get("user_payload")
                    if user_payload is not None:
                        debug_meta["user_payload_pretty"] = _json.dumps(user_payload, ensure_ascii=False, indent=2)
                except Exception:
                    debug_meta["user_payload_pretty"] = None
                try:
                    raw_resp = debug_meta.get("raw_response")
                    if raw_resp is not None:
                        debug_meta["raw_response_pretty"] = _json.dumps(raw_resp, ensure_ascii=False, indent=2) if not isinstance(raw_resp, str) else raw_resp
                except Exception:
                    debug_meta["raw_response_pretty"] = None
                try:
                    api_req = debug_meta.get("api_request_payload")
                    if api_req is not None:
                        debug_meta["api_request_payload_pretty"] = _json.dumps(api_req, ensure_ascii=False, indent=2)
                except Exception:
                    debug_meta["api_request_payload_pretty"] = None
        except Exception:
            debug_meta = None
    draft = {
        "id": did,
        "reply_text": reply_text,
        "model": model,
        "confidence": confidence,
        "reason": reason,
        "status": status,
        "created_at": created_at,
    }
    actions = None
    if actions_json:
        try:
            actions = json.loads(actions_json)
        except Exception:
            actions = None
    return templates.TemplateResponse(
        "ig_shadow_debug.html",
        {
            "request": request,
            "conversation_id": convo_id,
            "draft": draft,
            "debug_meta": debug_meta,
            "actions": actions,
            "conversation_info": conversation_info,
            "history_preview": history_preview,
        },
    )


@router.post("/inbox/{conversation_id}/debug/run")
def trigger_debug_conversation(conversation_id: int):
    with get_session() as session:
        # 1) Create a debug run row (UI anchor)
        run = IGAiDebugRun(conversation_id=str(conversation_id), status="queued")
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
            # Backend fallback
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
                "conversation_id": int(conversation_id),
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
def thread(request: Request, conversation_id: int, limit: int = 500):
    with get_session() as session:
        from sqlmodel import select
        # Load conversation row (for basic metadata) and messages for this internal id
        convo = session.get(Conversation, int(conversation_id))
        if convo is None:
            raise HTTPException(status_code=404, detail="Conversation not found")
        msgs = session.exec(
            select(Message)
            .where(Message.conversation_id == int(conversation_id))
            .order_by(Message.timestamp_ms.desc())
            .limit(min(max(limit, 1), 500))
        ).all()
        # chronological order for display
        msgs = list(reversed(msgs))
        admin_alerts: list[dict[str, Any]] = []
        escalation_map: dict[int, list[dict[str, Any]]] = {}
        try:
            admin_rows = session.exec(
                select(AdminMessage)
                .where(AdminMessage.conversation_id == int(conversation_id))
                .order_by(AdminMessage.created_at.desc())
                .limit(50)
            ).all()
            for row in admin_rows:
                try:
                    meta = json.loads(row.metadata_json) if getattr(row, "metadata_json", None) else {}
                except Exception:
                    meta = {}
                created_at = getattr(row, "created_at", None)
                try:
                    ts_ms = int(created_at.timestamp() * 1000) if isinstance(created_at, _d) else None
                except Exception:
                    ts_ms = None
                alert_entry = {
                    "id": getattr(row, "id", None),
                    "message": getattr(row, "message", None),
                    "message_type": getattr(row, "message_type", None),
                    "is_read": getattr(row, "is_read", False),
                    "created_at": created_at,
                    "timestamp_ms": ts_ms,
                    "metadata": meta,
                }
                admin_alerts.append(alert_entry)
                trigger_id = meta.get("trigger_message_id")
                if trigger_id:
                    try:
                        trigger_key = int(trigger_id)
                    except (TypeError, ValueError):
                        continue
                    escalation_map.setdefault(trigger_key, []).append(
                        {
                            "message": getattr(row, "message", None),
                            "message_type": getattr(row, "message_type", None),
                            "created_at": getattr(row, "created_at", None),
                        }
                    )
        except Exception:
            admin_alerts = []
            escalation_map = {}
        # Determine other party id from messages then resolve username
        other_label = None
        other_username = None
        other_id = None
        contact_name = None
        contact_phone = None
        contact_address = None
        enrich_status: dict[str, Any] | None = None
        linked_order_id = None
        ai_status = None
        ai_json = None
        focus_product: dict[str, Any] | None = None
        # Fetch user-level contact / AI info from ig_users (single source of truth)
        from sqlalchemy import text as _text
        try:
            row_user = session.exec(
                _text(
                    """
                    SELECT contact_name, contact_phone, contact_address, linked_order_id, ai_status, ai_json
                    FROM ig_users
                    WHERE ig_user_id = :u
                    LIMIT 1
                    """
                ).params(u=str(convo.ig_user_id))
            ).first()
            if row_user:
                contact_name = (row_user.contact_name if hasattr(row_user, "contact_name") else row_user[0]) or None
                contact_phone = (row_user.contact_phone if hasattr(row_user, "contact_phone") else row_user[1]) or None
                contact_address = (row_user.contact_address if hasattr(row_user, "contact_address") else row_user[2]) or None
                linked_order_id = (row_user.linked_order_id if hasattr(row_user, "linked_order_id") else row_user[3]) or None
                ai_status = (row_user.ai_status if hasattr(row_user, "ai_status") else row_user[4]) or None
                ai_json = (row_user.ai_json if hasattr(row_user, "ai_json") else row_user[5]) or None
        except Exception:
            pass

        # Fallback: even if ig_users row is missing or ai_json empty, try latest historical result
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
                        other_username = str(un)
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
                from ..services.queue import _get_redis
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
        # Basic link/ad context from conversation row
        link_context: dict[str, Any] = {
            "link_type": getattr(convo, "last_link_type", None),
            "link_id": getattr(convo, "last_link_id", None),
            "ad_id": getattr(convo, "last_ad_id", None),
            "ad_title": getattr(convo, "last_ad_title", None),
            "ad_link": getattr(convo, "last_ad_link", None),
        }
        if not link_context["link_id"] and link_context["ad_id"]:
            link_context["link_id"] = link_context["ad_id"]
        link_context["has_link"] = bool(link_context.get("link_id"))
        link_context["product_id"] = None
        link_context["product_name"] = None
        link_context["product_slug"] = None
        if link_context.get("link_id"):
            try:
                from sqlalchemy import text as _text

                row_link = session.exec(
                    _text(
                        """
                        SELECT ap.product_id, p.name AS product_name, p.slug
                        FROM ads_products ap
                        LEFT JOIN product p ON ap.product_id = p.id
                        WHERE ap.ad_id = :aid
                          AND (:lt IS NULL OR ap.link_type = :lt)
                        LIMIT 1
                        """
                    ).params(aid=str(link_context["link_id"]), lt=(link_context.get("link_type") or None))
                ).first()
                if row_link:
                    link_context["product_id"] = getattr(row_link, "product_id", None) if hasattr(row_link, "product_id") else (row_link[0] if len(row_link) > 0 else None)
                    link_context["product_name"] = getattr(row_link, "product_name", None) if hasattr(row_link, "product_name") else (row_link[1] if len(row_link) > 1 else None)
                    link_context["product_slug"] = getattr(row_link, "slug", None) if hasattr(row_link, "slug") else (row_link[2] if len(row_link) > 2 else None)
            except Exception:
                pass
        if link_context.get("link_id"):
            try:
                link_context["ad_edit_url"] = f"/ads/{link_context['link_id']}/edit"
            except Exception:
                link_context["ad_edit_url"] = None
        else:
            link_context["ad_edit_url"] = None

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

        # Detect focus product for this conversation (for UI + AI hints)
        try:
            focus_slug, focus_conf = _detect_focus_product(str(conversation_id))
        except Exception:
            focus_slug, focus_conf = (None, 0.0)
        if focus_slug:
            try:
                from sqlalchemy import text as _text
                stmt_fp = _text(
                    "SELECT id, name, ai_system_msg, ai_prompt_msg, default_price, slug FROM product WHERE slug=:s OR name=:s LIMIT 1"
                ).bindparams(s=str(focus_slug))
                row_fp = session.exec(stmt_fp).first()
                if row_fp:
                    pid = getattr(row_fp, "id", None) if hasattr(row_fp, "id") else (row_fp[0] if len(row_fp) > 0 else None)
                    pname = getattr(row_fp, "name", None) if hasattr(row_fp, "name") else (row_fp[1] if len(row_fp) > 1 else None)
                    psys = getattr(row_fp, "ai_system_msg", None) if hasattr(row_fp, "ai_system_msg") else (row_fp[2] if len(row_fp) > 2 else None)
                    pprompt = getattr(row_fp, "ai_prompt_msg", None) if hasattr(row_fp, "ai_prompt_msg") else (row_fp[3] if len(row_fp) > 3 else None)
                    pprice = getattr(row_fp, "default_price", None) if hasattr(row_fp, "default_price") else (row_fp[4] if len(row_fp) > 4 else None)
                    pslug_real = getattr(row_fp, "slug", None) if hasattr(row_fp, "slug") else (row_fp[5] if len(row_fp) > 5 else None)
                    focus_product = {
                        "id": pid,
                        "name": pname,
                        "slug": pslug_real or focus_slug,
                        "system": psys,
                        "prompt": pprompt,
                        "price": pprice,
                        "confidence": float(focus_conf or 0.0),
                    }
                    if not link_context.get("product_name"):
                        link_context["product_name"] = pname
                        link_context["product_slug"] = focus_product["slug"]
                        link_context["product_id"] = pid
            except Exception:
                focus_product = None

        # Fetch cached ads for messages in this thread
        ads_cache: dict[str, dict[str, Any]] = {}
        ad_products: dict[str, dict[str, Any]] = {}
        try:
            if ad_ids:
                placeholders = ",".join([":a" + str(i) for i in range(len(ad_ids))])
                from sqlalchemy import text as _text
                params = {("a" + str(i)): ad_ids[i] for i in range(len(ad_ids))}
                stmt_ads = _text(f"SELECT ad_id, name, image_url, link FROM ads WHERE ad_id IN ({placeholders})").bindparams(**params)
                rows_ad = session.exec(stmt_ads).all()
                for r in rows_ad:
                    aid = r.ad_id if hasattr(r, "ad_id") else r[0]
                    name = r.name if hasattr(r, "name") else (r[1] if len(r) > 1 else None)
                    img = r.image_url if hasattr(r, "image_url") else (r[2] if len(r) > 2 else None)
                    lnk = r.link if hasattr(r, "link") else (r[3] if len(r) > 3 else None)
                    ads_cache[str(aid)] = {"name": name, "image_url": img, "link": lnk}
                # Enrich with linked product info
                try:
                    stmt_ap = _text(
                        f"""
                        SELECT ap.ad_id, ap.product_id, p.name AS product_name
                        FROM ads_products ap
                        LEFT JOIN product p ON ap.product_id = p.id
                        WHERE ap.ad_id IN ({placeholders})
                        """
                    ).bindparams(**params)
                    rows_ap = session.exec(stmt_ap).all()
                    for r in rows_ap:
                        try:
                            aid = getattr(r, "ad_id", None) if hasattr(r, "ad_id") else (r[0] if len(r) > 0 else None)
                            pid = getattr(r, "product_id", None) if hasattr(r, "product_id") else (r[1] if len(r) > 1 else None)
                            pname = getattr(r, "product_name", None) if hasattr(r, "product_name") else (r[2] if len(r) > 2 else None)
                            if not aid:
                                continue
                            ad_products[str(aid)] = {"product_id": pid, "product_name": pname}
                        except Exception:
                            continue
                except Exception:
                    ad_products = {}
        except Exception:
            ads_cache = {}
            ad_products = {}

        # Build attachment indices so template can render images (fallback: legacy attachments_json)
        att_map = {}
        template_cards: dict[str, list[dict[str, Any]]] = {}
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
                    mid_key = mm.ig_message_id or ""
                    att_map[mid_key] = list(range(len(items)))
                    cards: list[dict[str, Any]] = []
                    for att in items:
                        if not isinstance(att, dict):
                            continue
                        payload = att.get("payload") or {}
                        generic = payload.get("generic") or {}
                        elements = None
                        if isinstance(generic, dict) and isinstance(generic.get("elements"), list):
                            elements = generic.get("elements")
                        elif isinstance(payload.get("elements"), list):
                            elements = payload.get("elements")
                        elif isinstance(payload.get("cards"), list):
                            elements = payload.get("cards")
                        if not isinstance(elements, list):
                            continue
                        for el in elements:
                            if not isinstance(el, dict):
                                continue
                            cards.append(
                                {
                                    "title": el.get("title") or el.get("header"),
                                    "subtitle": el.get("subtitle") or el.get("description"),
                                    "image_url": el.get("image_url") or el.get("image") or el.get("media_url"),
                                    "buttons": el.get("buttons") or [],
                                    "default_action": el.get("default_action") or {},
                                }
                            )
                    if cards:
                        template_cards[mid_key] = cards
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
                    "SELECT id, reply_text, model, confidence, reason, created_at, status, actions_json, state_json, json_meta FROM ai_shadow_reply WHERE conversation_id=:cid ORDER BY created_at ASC LIMIT 200"
                ).params(cid=int(conversation_id))
            ).all()
            # Represent the last one (if any) in 'shadow' for legacy panel rendering
            def _parse_actions(raw_val: Any) -> list[dict[str, Any]]:
                if not raw_val:
                    return []
                try:
                    if isinstance(raw_val, str):
                        parsed = json.loads(raw_val)
                    else:
                        parsed = raw_val
                    if isinstance(parsed, list):
                        return parsed  # type: ignore[return-value]
                except Exception:
                    pass
                return []
            
            def _parse_function_callbacks(raw_val: Any) -> list[dict[str, Any]]:
                """Parse function_callbacks from json_meta."""
                if not raw_val:
                    return []
                try:
                    if isinstance(raw_val, str):
                        parsed = json.loads(raw_val)
                    else:
                        parsed = raw_val
                    if isinstance(parsed, dict):
                        # json_meta is a dict, look for function_callbacks inside it
                        callbacks = parsed.get("function_callbacks", [])
                        if isinstance(callbacks, list):
                            return callbacks  # type: ignore[return-value]
                    elif isinstance(parsed, list):
                        # json_meta might be a list directly (unlikely but handle it)
                        return parsed  # type: ignore[return-value]
                except Exception:
                    pass
                return []
            
            if rows_shadow:
                rlast = rows_shadow[-1]
                reply_text_raw = getattr(rlast, "reply_text", None) if hasattr(rlast, "reply_text") else (rlast[1] if len(rlast) > 1 else None)
                # Decode any escape sequences that might still be in the stored text
                if reply_text_raw:
                    # Import decode function locally
                    from ..services.ai_reply import _decode_escape_sequences
                    reply_text_raw = _decode_escape_sequences(str(reply_text_raw))
                    reply_text_raw = _sanitize_reply_text(str(reply_text_raw))
                # Split reply text by newlines for display as separate messages
                reply_text_lines = []
                if reply_text_raw:
                    # Split by actual newlines (not literal \n sequences)
                    lines = [line.strip() for line in str(reply_text_raw).split('\n') if line.strip()]
                    # If no non-empty lines, use the original text (might be single line)
                    if not lines:
                        lines = [str(reply_text_raw).strip()] if str(reply_text_raw).strip() else []
                    reply_text_lines = lines
                def _parse_state(raw_val: Any) -> dict[str, Any]:
                    if not raw_val:
                        return {}
                    if isinstance(raw_val, dict):
                        return raw_val  # type: ignore[return-value]
                    try:
                        if isinstance(raw_val, str):
                            parsed = json.loads(raw_val)
                            if isinstance(parsed, dict):
                                return parsed  # type: ignore[return-value]
                    except Exception:
                        return {}
                    return {}

                json_meta_raw = getattr(rlast, "json_meta", None) if hasattr(rlast, "json_meta") else (rlast[9] if len(rlast) > 9 else None)
                function_callbacks_list = _parse_function_callbacks(json_meta_raw)

                shadow = {
                    "id": getattr(rlast, "id", None) if hasattr(rlast, "id") else (rlast[0] if len(rlast) > 0 else None),
                    "text": reply_text_raw,
                    "text_lines": reply_text_lines,  # Split lines for display
                    "model": getattr(rlast, "model", None) if hasattr(rlast, "model") else (rlast[2] if len(rlast) > 2 else None),
                    "confidence": getattr(rlast, "confidence", None) if hasattr(rlast, "confidence") else (rlast[3] if len(rlast) > 3 else None),
                    "reason": getattr(rlast, "reason", None) if hasattr(rlast, "reason") else (rlast[4] if len(rlast) > 4 else None),
                    "created_at": getattr(rlast, "created_at", None) if hasattr(rlast, "created_at") else (rlast[5] if len(rlast) > 5 else None),
                    "status": getattr(rlast, "status", None) if hasattr(rlast, "status") else (rlast[6] if len(rlast) > 6 else None),
                    "actions": _parse_actions(getattr(rlast, "actions_json", None) if hasattr(rlast, "actions_json") else (rlast[7] if len(rlast) > 7 else None)),
                    "state": _parse_state(getattr(rlast, "state_json", None) if hasattr(rlast, "state_json") else (rlast[8] if len(rlast) > 8 else None)),
                    "function_callbacks": function_callbacks_list,
                }
                try:
                    ca = shadow.get("created_at")
                    if ca:
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
        inline_drafts = (_os.getenv("IG_INLINE_DRAFTS", "1") not in ("0", "false", "False"))
        if inline_drafts and rows_shadow:
            # Determine product focus once for this thread (fallback to None)
            try:
                focus_slug, _ = _detect_focus_product(conversation_id)
            except Exception:
                focus_slug = None
            vms: list[dict] = []
            for rr in rows_shadow:
                try:
                    txt = getattr(rr, "reply_text", None) if hasattr(rr, "reply_text") else (rr[1] if len(rr) > 1 else None)
                    status = getattr(rr, "status", None) if hasattr(rr, "status") else (rr[6] if len(rr) > 6 else None)
                    actions_val = getattr(rr, "actions_json", None) if hasattr(rr, "actions_json") else (rr[7] if len(rr) > 7 else None)
                    state_val = getattr(rr, "state_json", None) if hasattr(rr, "state_json") else (rr[8] if len(rr) > 8 else None)
                    json_meta_val = getattr(rr, "json_meta", None) if hasattr(rr, "json_meta") else (rr[9] if len(rr) > 9 else None)
                    confidence_val = getattr(rr, "confidence", None) if hasattr(rr, "confidence") else (rr[3] if len(rr) > 3 else None)
                    state_dict = _parse_state(state_val)
                    actions_list = _parse_actions(actions_val)
                    function_callbacks_vm = _parse_function_callbacks(json_meta_val)
                    # Include all records, even if empty text (for no_reply decisions)
                    if not txt:
                        txt = ""  # Will be handled in template
                    
                    # Decode any escape sequences that might still be in the stored text
                    if txt:
                        from ..services.ai_reply import _decode_escape_sequences
                        txt = _decode_escape_sequences(str(txt))
                        txt = _sanitize_reply_text(str(txt))
                    # Split text by newlines to show as separate messages
                    text_lines = []
                    if txt:
                        lines = [line.strip() for line in str(txt).split('\n') if line.strip()]
                        if not lines:
                            lines = [str(txt).strip()]
                        text_lines = lines
                    else:
                        text_lines = [""]
                    
                    did = getattr(rr, "id", None) if hasattr(rr, "id") else (rr[0] if len(rr) > 0 else None)
                    ca = getattr(rr, "created_at", None) if hasattr(rr, "created_at") else (rr[5] if len(rr) > 5 else None)
                    ts = None
                    if ca:
                        try:
                            ts = _d.fromisoformat(ca.replace("Z","+00:00")).timestamp()*1000 if isinstance(ca, str) else (ca.timestamp()*1000)
                            ts = int(ts)
                        except Exception:
                            ts = None
                    # Normalize status for template
                    normalized_status = (status or "suggested").lower()
                    if normalized_status not in ["sent", "error", "no_reply", "suggested", "dismissed", "expired"]:
                        normalized_status = "suggested"
                    
                    # Create a separate virtual message for each line
                    for line_idx, line_text in enumerate(text_lines):
                        vm = {
                            "direction": "out",
                            "text": line_text,
                            "timestamp_ms": ts or 0,
                            "sender_username": "AI",
                            "ig_message_id": None,
                            "ig_sender_id": None,
                            "ig_recipient_id": None,
                            "is_ai_draft": True,
                            "ai_decision_status": normalized_status,  # Pass status to template
                            "draft_id": int(did) if did is not None else None,
                            "ai_model": getattr(rr, "model", None) if hasattr(rr, "model") else (rr[2] if len(rr) > 2 else None),
                            "ai_reason": getattr(rr, "reason", None) if hasattr(rr, "reason") else (rr[4] if len(rr) > 4 else None),
                            "ai_confidence": confidence_val,
                            "product_slug": focus_slug or None,
                            "ai_actions": actions_list if line_idx == 0 else [],  # Only show actions on first message
                            "ai_state": state_dict if state_dict else None,
                            "ai_function_callbacks": function_callbacks_vm if line_idx == 0 else [],  # Only show function_callbacks on first message
                        }
                        vms.append(vm)
                except Exception:
                    continue
            msgs = list(msgs) + vms
            try:
                msgs.sort(key=lambda m: (getattr(m, "timestamp_ms", None) if hasattr(m, "timestamp_ms") else (m.get("timestamp_ms") if isinstance(m, dict) else 0)) or 0)
            except Exception:
                pass
        # Determine last message direction (for AI retry button) - check after inline drafts merge
        last_message_direction = None
        if msgs:
            last_msg = msgs[-1]
            if hasattr(last_msg, "direction"):
                last_message_direction = last_msg.direction
            elif isinstance(last_msg, dict):
                last_message_direction = last_msg.get("direction")
        # AI shadow state indicators
        ai_state = None
        try:
            from sqlalchemy import text as _text

            row_state = session.exec(
                _text(
                    """
                    SELECT status, next_attempt_at, postpone_count, last_inbound_ms
                    FROM ai_shadow_state
                    WHERE conversation_id=:cid
                    LIMIT 1
                    """
                ).params(cid=int(conversation_id))
            ).first()
            if row_state:
                status_val = getattr(row_state, "status", None) if hasattr(row_state, "status") else (row_state[0] if len(row_state) > 0 else None)
                next_at = getattr(row_state, "next_attempt_at", None) if hasattr(row_state, "next_attempt_at") else (row_state[1] if len(row_state) > 1 else None)
                postpone_val = getattr(row_state, "postpone_count", None) if hasattr(row_state, "postpone_count") else (row_state[2] if len(row_state) > 2 else None)
                status = (status_val or "pending").lower()
                status_labels = {
                    "pending": "Sırada",
                    "running": "Üretiliyor",
                    "paused": "Beklemede",
                    "needs_link": "Ürün bekleniyor",
                    "needs_admin": "Yönetici Bekleniyor",
                    "exhausted": "Durdu",
                    "error": "Hata",
                }
                status_desc = {
                    "needs_link": "AI çalışmadan önce konuşmayı ilgili reklam/ürüne bağlayın.",
                    "paused": "Kısa süre sonra yeniden denenecek.",
                    "needs_admin": "AI konuşmayı yöneticilere eskale etti. Manuel yanıt bekleniyor.",
                    "exhausted": "Üst üste denendi, manuel tetiklenmeli.",
                    "error": "AI cevabı üretilirken hata oluştu.",
                }
                ai_state = {
                    "status": status,
                    "label": status_labels.get(status, status.title()),
                    "description": status_desc.get(status),
                    "next_attempt_at": next_at,
                    "postpone_count": postpone_val,
                    "needs_link": status == "needs_link",
                    "needs_admin": status == "needs_admin",
                }
        except Exception:
            ai_state = None

        if ai_state and ai_state.get("needs_link"):
            link_context["needs_link"] = True
        else:
            link_context["needs_link"] = False

        public_conversation_id = (
            getattr(convo, "graph_conversation_id", None)
            or (f"dm:{convo.ig_user_id}" if getattr(convo, "ig_user_id", None) else None)
        )

        user_context = {
            "platform": str(getattr(convo, "platform", None) or "instagram"),
            "username": other_username,
            "ig_user_id": str(convo.ig_user_id) if getattr(convo, "ig_user_id", None) else None,
            "contact_name": contact_name,
            "contact_phone": contact_phone,
            "contact_address": contact_address,
            "linked_order_id": linked_order_id,
        }

        # Brand snapshot + assignment helpers (lazy imports to avoid heavy modules at top-level)
        brand_profile = None
        try:
            from ..services.ig_profile import ensure_profile_snapshot as _ensure_profile_snapshot

            snap = _ensure_profile_snapshot()
            brand_profile = {
                "username": snap.username,
                "name": snap.name,
                "profile_picture_url": snap.profile_picture_url,
                "biography": snap.biography,
                "followers_count": snap.followers_count,
                "follows_count": snap.follows_count,
                "media_count": snap.media_count,
                "website": snap.website,
                "refreshed_at": snap.refreshed_at.isoformat() if snap.refreshed_at else None,
            }
        except Exception:
            brand_profile = None

        assignable_users = []
        current_assignment = None
        try:
            from ..services import ig_inbox as _ig_inbox

            assignable_users = _ig_inbox.list_assignable_users()
            current_assignment = _ig_inbox.get_assignment(int(conversation_id))
        except Exception:
            assignable_users = []
            current_assignment = None

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
                "ad_products": ad_products,
                "focus_product": focus_product,
                "contact_name": contact_name,
                "contact_phone": contact_phone,
                "contact_address": contact_address,
                "linked_order_id": linked_order_id,
                "ai_status": ai_status,
                "ai_json": ai_json,
                "shadow": shadow,
                "inline_drafts": inline_drafts,
                "link_context": link_context,
                "ai_state": ai_state,
				"admin_alerts": admin_alerts,
				"escalation_map": escalation_map,
                "user_context": user_context,
                "template_cards": template_cards,
                "public_conversation_id": public_conversation_id,
                "conversation_platform": str(getattr(convo, "platform", None) or "instagram"),
                "brand_profile": brand_profile,
                "assignable_users": assignable_users,
                "current_assignment": current_assignment,
                "last_message_direction": last_message_direction,
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
            import logging
            _log = logging.getLogger("instagram.inbox")
            _log.exception("Thread refresh failed for %s: %s", conversation_id, e)
        except Exception:
            pass
        return {"status": "error", "error": str(e)}


@router.get("/inbox/{conversation_id}/ai/queue_position")
def ai_queue_position(conversation_id: int):
    """Worker kuyruğunda bu konuşmadan önce kaç konuşma var (bu mesaja kadar işlenecek sayı)."""
    from sqlalchemy import text as _text
    total_due = 0
    ahead_count = 0
    try:
        # Worker ile aynı koşul: pending/paused ve next_attempt_at due
        due_where = """
            (
                (status = 'pending' OR status IS NULL)
                AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
            )
            OR (
                status = 'paused'
                AND postpone_count > 0 AND postpone_count <= 8
                AND next_attempt_at IS NOT NULL AND next_attempt_at <= CURRENT_TIMESTAMP
            )
        """
        with get_session() as session:
            # Toplam kuyruktaki (due) konuşma sayısı
            total_row = session.exec(
                _text(f"SELECT COUNT(*) FROM ai_shadow_state WHERE {due_where}")
            ).first()
            if total_row is not None:
                try:
                    total_due = int(total_row[0] if hasattr(total_row, "__getitem__") else list(total_row)[0])
                except (TypeError, ValueError, IndexError, KeyError):
                    total_due = 0
            # Bu konuşmadan önce kaç tane var
            ahead_row = session.exec(
                _text(
                    """
                    SELECT COUNT(*) FROM ai_shadow_state A
                    INNER JOIN (SELECT next_attempt_at AS na FROM ai_shadow_state WHERE conversation_id=:cid LIMIT 1) B ON 1=1
                    WHERE (
                        (A.status = 'pending' OR A.status IS NULL) AND (A.next_attempt_at IS NULL OR A.next_attempt_at <= CURRENT_TIMESTAMP)
                    ) OR (
                        A.status = 'paused' AND A.postpone_count > 0 AND A.postpone_count <= 8
                        AND A.next_attempt_at IS NOT NULL AND A.next_attempt_at <= CURRENT_TIMESTAMP
                    )
                    AND A.conversation_id <> :cid
                    AND (
                        (A.next_attempt_at IS NULL AND B.na IS NOT NULL)
                        OR (A.next_attempt_at IS NOT NULL AND B.na IS NOT NULL AND A.next_attempt_at < B.na)
                        OR (A.next_attempt_at IS NOT NULL AND B.na IS NOT NULL AND A.next_attempt_at = B.na AND A.conversation_id < :cid)
                        OR (A.next_attempt_at IS NULL AND B.na IS NULL AND A.conversation_id < :cid)
                    )
                    """
                ).params(cid=int(conversation_id))
            ).first()
            if ahead_row is not None:
                try:
                    ahead_count = int(ahead_row[0] if hasattr(ahead_row, "__getitem__") else list(ahead_row)[0])
                except (TypeError, ValueError, IndexError, KeyError):
                    ahead_count = 0
    except Exception:
        pass
    return {
        "conversation_id": conversation_id,
        "ahead_count": ahead_count,
        "queue_position": ahead_count + 1,
        "total_pending": total_due,
    }


@router.post("/inbox/{conversation_id}/ai/retry")
def retry_ai_for_thread(conversation_id: int):
    """Force AI reply to be attempted now: set status=pending and next_attempt_at in the past so worker picks this conversation first (ORDER BY next_attempt_at ASC LIMIT 20)."""
    last_ms = None
    from sqlalchemy import text as _text

    with get_session() as session:
        row_ts = session.exec(
            _text("SELECT last_message_timestamp_ms FROM conversations WHERE id=:cid LIMIT 1").params(cid=int(conversation_id))
        ).first()
        if row_ts:
            last_ms = getattr(row_ts, "last_message_timestamp_ms", None) if hasattr(row_ts, "last_message_timestamp_ms") else (row_ts[0] if len(row_ts) > 0 else None)
        # Ensure row exists and set next_attempt_at to past so this cid is first in worker's ORDER BY next_attempt_at ASC LIMIT 20
        session.exec(
            _text(
                """
                INSERT INTO ai_shadow_state(conversation_id, last_inbound_ms, next_attempt_at, postpone_count, status, ai_images_sent, updated_at)
                VALUES (:cid, :ms, '2000-01-01 00:00:00', 0, 'pending', 0, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                  status = 'pending',
                  next_attempt_at = '2000-01-01 00:00:00',
                  updated_at = CURRENT_TIMESTAMP
                """
            ).params(cid=int(conversation_id), ms=int(last_ms or 0))
        )
        # Refresh last_inbound_ms so worker doesn't postpone (old message = no debounce)
        session.exec(
            _text(
                "UPDATE ai_shadow_state SET last_inbound_ms=COALESCE(:ms, last_inbound_ms) WHERE conversation_id=:cid"
            ).params(cid=int(conversation_id), ms=int(last_ms or 0))
        )
    return {"status": "ok"}


def _fetch_last_inbound_message_for_escalation(conversation_id: int) -> dict[str, Any] | None:
    with get_session() as session:
        try:
            row = session.exec(
                _text(
                    """
                    SELECT id, text, timestamp_ms
                    FROM message
                    WHERE conversation_id=:cid AND direction='in'
                    ORDER BY timestamp_ms DESC
                    LIMIT 1
                    """
                ).params(cid=int(conversation_id))
            ).first()
        except Exception:
            return None
        if not row:
            return None
        try:
            return {
                "id": getattr(row, "id", None) if hasattr(row, "id") else (row[0] if len(row) > 0 else None),
                "text": getattr(row, "text", None) if hasattr(row, "text") else (row[1] if len(row) > 1 else None),
                "timestamp_ms": getattr(row, "timestamp_ms", None)
                if hasattr(row, "timestamp_ms")
                else (row[2] if len(row) > 2 else None),
            }
        except Exception:
            return None


@router.get("/inbox/{conversation_id}/ads")
def list_available_ads_for_conversation(conversation_id: int, limit: int = 100):
	"""List ads that can be assigned to this conversation, including their linked products."""
	ads: list[dict[str, Any]] = []
	with get_session() as session:
		try:
			rows = session.exec(
				_text(
					"""
					SELECT a.ad_id, a.name, a.image_url, a.link, a.link_type,
					       MAX(ap.product_id) AS product_id, 
					       MAX(p.name) AS product_name, 
					       MAX(p.slug) AS product_slug
					FROM ads a
					LEFT JOIN ads_products ap ON a.ad_id = ap.ad_id
					LEFT JOIN product p ON ap.product_id = p.id
					GROUP BY a.ad_id, a.name, a.image_url, a.link, a.link_type
					ORDER BY a.updated_at DESC
					LIMIT :lim
					"""
				).params(lim=int(limit))
			).all()
		except Exception:
			rows = []
		
		for r in rows:
			try:
				ad_id = getattr(r, "ad_id", None) if hasattr(r, "ad_id") else (r[0] if len(r) > 0 else None)
				ad_name = getattr(r, "name", None) if hasattr(r, "name") else (r[1] if len(r) > 1 else None)
				image_url = getattr(r, "image_url", None) if hasattr(r, "image_url") else (r[2] if len(r) > 2 else None)
				link = getattr(r, "link", None) if hasattr(r, "link") else (r[3] if len(r) > 3 else None)
				link_type = getattr(r, "link_type", None) if hasattr(r, "link_type") else (r[4] if len(r) > 4 else None)
				product_id = getattr(r, "product_id", None) if hasattr(r, "product_id") else (r[5] if len(r) > 5 else None)
				product_name = getattr(r, "product_name", None) if hasattr(r, "product_name") else (r[6] if len(r) > 6 else None)
				product_slug = getattr(r, "product_slug", None) if hasattr(r, "product_slug") else (r[7] if len(r) > 7 else None)
				
				if ad_id:
					ads.append({
						"ad_id": str(ad_id),
						"name": ad_name or str(ad_id),
						"image_url": image_url,
						"link": link,
						"link_type": link_type or "ad",
						"product_id": int(product_id) if product_id is not None else None,
						"product_name": product_name,
						"product_slug": product_slug,
					})
			except Exception:
				continue
	
	return {"status": "ok", "ads": ads}


@router.post("/inbox/{conversation_id}/assign-ad")
def assign_ad_to_conversation(conversation_id: int, body: dict | None = Body(default=None)):
	"""Manually assign an ad to a conversation so AI can use product context."""
	payload = body or {}
	ad_id = str(payload.get("ad_id") or "").strip()
	if not ad_id:
		raise HTTPException(status_code=400, detail="ad_id_required")
	
	try:
		conv_id_int = int(conversation_id)
	except (TypeError, ValueError):
		raise HTTPException(status_code=400, detail="invalid_conversation_id")
	
	with get_session() as session:
		# Verify conversation exists
		convo = session.get(Conversation, conv_id_int)
		if not convo:
			raise HTTPException(status_code=404, detail="conversation_not_found")
		
		# Fetch ad details
		row_ad = session.exec(
			_text(
				"""
				SELECT ad_id, name, link, link_type
				FROM ads
				WHERE ad_id = :aid
				LIMIT 1
				"""
			).params(aid=str(ad_id))
		).first()
		
		if not row_ad:
			raise HTTPException(status_code=404, detail="ad_not_found")
		
		ad_name = getattr(row_ad, "name", None) if hasattr(row_ad, "name") else (row_ad[1] if len(row_ad) > 1 else None)
		ad_link = getattr(row_ad, "link", None) if hasattr(row_ad, "link") else (row_ad[2] if len(row_ad) > 2 else None)
		link_type = getattr(row_ad, "link_type", None) if hasattr(row_ad, "link_type") else (row_ad[3] if len(row_ad) > 3 else None)
		
		# Update conversation with ad assignment
		session.exec(
			_text(
				"""
				UPDATE conversations
				SET last_ad_id = :aid,
				    last_link_id = :aid,
				    last_link_type = :lt,
				    last_ad_link = :link,
				    last_ad_title = :title
				WHERE id = :cid
				"""
			).params(
				cid=conv_id_int,
				aid=str(ad_id),
				lt=(link_type or "ad"),
				link=ad_link,
				title=ad_name,
			)
		)
		
		# Check if ad has a linked product
		row_product = session.exec(
			_text(
				"""
				SELECT ap.product_id, p.name AS product_name, p.slug
				FROM ads_products ap
				LEFT JOIN product p ON ap.product_id = p.id
				WHERE ap.ad_id = :aid
				LIMIT 1
				"""
			).params(aid=str(ad_id))
		).first()
		
		product_id = None
		product_name = None
		product_slug = None
		if row_product:
			product_id = getattr(row_product, "product_id", None) if hasattr(row_product, "product_id") else (row_product[0] if len(row_product) > 0 else None)
			product_name = getattr(row_product, "product_name", None) if hasattr(row_product, "product_name") else (row_product[1] if len(row_product) > 1 else None)
			product_slug = getattr(row_product, "slug", None) if hasattr(row_product, "slug") else (row_product[2] if len(row_product) > 2 else None)
		
		# If ad has a product, also trigger AI shadow state update to allow AI to work
		if product_id:
			try:
				from ..services.ai_shadow import touch_shadow_state
				touch_shadow_state(conv_id_int, None, debounce_seconds=0)
			except Exception:
				pass
	
	return {
		"status": "ok",
		"ad_id": ad_id,
		"product_id": product_id,
		"product_name": product_name,
		"product_slug": product_slug,
	}


@router.post("/inbox/{conversation_id}/escalate")
def escalate_conversation(conversation_id: str, body: dict | None = Body(default=None)):
    payload = body or {}
    message_text = str(payload.get("message") or "").strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="message_required")
    message_type = str(payload.get("message_type") or "warning").strip().lower()
    if message_type not in ("info", "warning", "urgent"):
        message_type = "warning"
    auto_block = bool(payload.get("auto_block", True))
    pause_ai = bool(payload.get("pause_ai", True))
    manual_reason = str(payload.get("reason") or "").strip() or message_text

    try:
        conv_id_int = int(conversation_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="invalid_conversation_id")

    trigger_info = _fetch_last_inbound_message_for_escalation(conv_id_int)

    metadata = {
        "manual_escalation": True,
        "auto_blocked": auto_block,
        "source": "manual_ui",
    }
    if manual_reason:
        metadata["manual_reason"] = manual_reason
    if trigger_info:
        metadata["trigger_message_id"] = trigger_info.get("id")
        metadata["trigger_message_text"] = trigger_info.get("text")
        metadata["trigger_message_ts"] = trigger_info.get("timestamp_ms")

    admin_msg_id = create_admin_notification(
        conv_id_int,
        message_text,
        message_type=message_type,
        metadata=metadata,
    )

    ai_paused = False
    if pause_ai:
        ai_paused = True
        state_snapshot = {
            "needs_admin": True,
            "manual_escalation": True,
            "manual_reason": manual_reason,
        }
        state_json = json.dumps(state_snapshot, ensure_ascii=False)
        with get_session() as session:
            res = session.exec(
                _text(
                    """
                    UPDATE ai_shadow_state
                    SET status='needs_admin',
                        state_json=:state,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE conversation_id=:cid
                    """
                ).params(cid=conv_id_int, state=state_json)
            )
            rowcount = getattr(res, "rowcount", None)
            if not rowcount:
                session.exec(
                    _text(
                        """
                        INSERT INTO ai_shadow_state(conversation_id, status, state_json, postpone_count, last_inbound_ms, ai_images_sent, updated_at)
                        VALUES(:cid, 'needs_admin', :state, 0, 0, 0, CURRENT_TIMESTAMP)
                        """
                    ).params(cid=conv_id_int, state=state_json)
                )

    return {
        "status": "ok",
        "admin_message_id": admin_msg_id,
        "ai_paused": ai_paused,
    }


@router.post("/inbox/{conversation_id}/shadow/dismiss")
def dismiss_shadow(conversation_id: int):
    # Mark the latest suggested shadow draft as dismissed
    try:
        from sqlalchemy import text as _text
        with get_session() as session:
            row = session.exec(
                _text(
                    "SELECT id FROM ai_shadow_reply WHERE conversation_id=:cid AND (status IS NULL OR status='suggested') ORDER BY id DESC LIMIT 1"
                )
            ).params(cid=int(conversation_id)).first()
            if not row:
                return {"status": "ok", "changed": 0}
            rid = getattr(row, "id", None) if hasattr(row, "id") else (
                row[0] if isinstance(row, (list, tuple)) and len(row) > 0 else None
            )
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
        # Additional fallback: infer from ai_conversations (last known sender/recipient)
        if not other_id:
            try:
                from sqlalchemy import text as _text
                rowac = session.exec(
                    _text("SELECT ig_sender_id, ig_recipient_id FROM ai_conversations WHERE convo_id=:cid ORDER BY last_message_timestamp_ms DESC LIMIT 1")
                ).params(cid=str(conversation_id)).first()
                if rowac:
                    sid = getattr(rowac, "ig_sender_id", None) if hasattr(rowac, "ig_sender_id") else (rowac[0] if len(rowac) > 0 else None)
                    rid = getattr(rowac, "ig_recipient_id", None) if hasattr(rowac, "ig_recipient_id") else (rowac[1] if len(rowac) > 1 else None)
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
                try:
                    loop = _aio.get_event_loop()
                except RuntimeError:
                    loop = _aio.new_event_loop()
                    _aio.set_event_loop(loop)
                from ..services.instagram_api import fetch_messages as _fm, _get_base_token_and_id as _gb
                _, owner_id, _ = _gb()
                msgs = loop.run_until_complete(_fm(str(conversation_id), limit=10))
                try:
                    import logging
                    _log = logging.getLogger("instagram.inbox")
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
                        import logging
                        _log = logging.getLogger("instagram.inbox")
                        _log.info("hydrate.resolve.graph_scan_no_uid cid=%s owner=%s", str(conversation_id), str(owner_id))
                    except Exception:
                        pass
            except Exception as ex_gs:
                try:
                    import logging
                    _log = logging.getLogger("instagram.inbox")
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
        # Fallback: enqueue GCID hydrate and best-effort enrich_page; return ok with debug
        debug: dict[str, object] = {}
        try:
            token, ident, is_page = _get_base_token_and_id()
            debug["active_path"] = ("page" if is_page else "user")
            debug["owner_id"] = str(ident)
        except Exception as e:
            debug["env_resolve_error"] = str(e)
        debug["cid_kind"] = ("dm" if conversation_id.startswith("dm:") else "graph")
        debug["igba_id"] = (str(igba_id) if igba_id else None)
        # enqueue hydrate_by_conversation_id for Graph CIDs
        fallback_enqueued = False
        graph_cid, lookup_info = _resolve_graph_conversation_id_for_hydrate(str(conversation_id))
        debug["graph_lookup"] = lookup_info
        if graph_cid:
            try:
                enqueue(
                    "hydrate_by_conversation_id",
                    key=str(graph_cid),
                    payload={"conversation_id": str(graph_cid), "max_messages": 50},
                )
                fallback_enqueued = True
            except Exception:
                fallback_enqueued = False
        # best-effort: enrich_page even if user unresolved
        try:
            if igba_id:
                enqueue("enrich_page", key=str(igba_id), payload={"igba_id": str(igba_id)})
                debug["page_enrich_enqueued"] = True
        except Exception:
            debug["page_enrich_enqueued"] = False
        return {"status": "ok", "queued": {"hydrate_by_cid": fallback_enqueued, "enrich_user": False, "enrich_page": bool(igba_id)}, "conversation_id": conversation_id, "debug": debug}
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
    # conversation_id can be:
    # - Database primary key (numeric: conversations.id)
    # - Graph conversation ID (base64 string: graph_conversation_id)
    # - convo_id format (string like "page_id:user_id")
    other_id: str | None = None
    igba_id: str | None = None
    graph_conversation_id: str | None = None
    
    try:
        import logging
        _log = logging.getLogger("instagram.inbox")
        _log.info("hydrate.begin cid=%s", str(conversation_id))
    except Exception:
        pass
    
    with get_session() as session:
        # First, check if conversation_id is a numeric database ID
        is_db_id = False
        try:
            int(conversation_id)
            is_db_id = True
            try:
                _log.info("hydrate.lookup: cid=%s is database ID", str(conversation_id))
            except Exception:
                pass
        except (ValueError, TypeError):
            try:
                _log.info("hydrate.lookup: cid=%s is NOT database ID (treating as Graph/convo_id)", str(conversation_id))
            except Exception:
                pass
        
        if is_db_id:
            # Look up by database primary key (conversations.id)
            try:
                from sqlalchemy import text as _text
                row = session.exec(
                    _text("SELECT id, igba_id, ig_user_id, graph_conversation_id FROM conversations WHERE id=:cid LIMIT 1"),
                    {"cid": int(conversation_id)}
                ).first()
                if row:
                    igba_id_raw = getattr(row, "igba_id", None) or (row[1] if len(row) > 1 else None)
                    other_id_raw = getattr(row, "ig_user_id", None) or (row[2] if len(row) > 2 else None)
                    graph_conversation_id_raw = getattr(row, "graph_conversation_id", None) or (row[3] if len(row) > 3 else None)
                    
                    igba_id = str(igba_id_raw) if igba_id_raw else None
                    other_id = str(other_id_raw) if other_id_raw else None
                    graph_conversation_id = str(graph_conversation_id_raw) if graph_conversation_id_raw else None
                    
                    try:
                        _log.info("hydrate.lookup.db_id: found row id=%s igba_id=%s ig_user_id=%s graph_cid=%s", 
                                 int(conversation_id), igba_id, other_id, (graph_conversation_id[:40] + "..." if graph_conversation_id and len(graph_conversation_id) > 40 else graph_conversation_id))
                    except Exception:
                        pass
                else:
                    try:
                        _log.warning("hydrate.lookup.db_id: no row found for id=%s", int(conversation_id))
                    except Exception:
                        pass
            except Exception as e:
                try:
                    _log.warning("hydrate.lookup.db_id: error looking up id=%s err=%s", int(conversation_id), str(e)[:200])
                except Exception:
                    pass
        else:
            # Try looking up by convo_id (format: "page_id:user_id")
            try:
                from sqlalchemy import text as _text
                row = session.exec(
                    _text("SELECT igba_id, ig_user_id, graph_conversation_id FROM conversations WHERE convo_id=:cid LIMIT 1"),
                    {"cid": str(conversation_id)}
                ).first()
                if row:
                    igba_id = str(getattr(row, "igba_id", None) or (row[0] if len(row) > 0 else "") or "")
                    other_id = str(getattr(row, "ig_user_id", None) or (row[1] if len(row) > 1 else "") or "")
                    graph_conversation_id = str(getattr(row, "graph_conversation_id", None) or (row[2] if len(row) > 2 else None) or "") or None
            except Exception:
                pass
            
            # Also try looking up by Graph conversation ID
            if not other_id:
                try:
                    from sqlalchemy import text as _text
                    rowg = session.exec(
                        _text("SELECT igba_id, ig_user_id, graph_conversation_id FROM conversations WHERE graph_conversation_id=:gc LIMIT 1"),
                        {"gc": str(conversation_id)}
                    ).first()
                    if rowg:
                        igba_id = str(getattr(rowg, "igba_id", None) or (rowg[0] if len(rowg) > 0 else "") or "")
                        other_id = str(getattr(rowg, "ig_user_id", None) or (rowg[1] if len(rowg) > 1 else "") or "")
                        graph_conversation_id = str(getattr(rowg, "graph_conversation_id", None) or (rowg[2] if len(rowg) > 2 else None) or "") or None
                        if not graph_conversation_id:
                            graph_conversation_id = str(conversation_id)  # Use the input if it matched graph_conversation_id
                except Exception:
                    pass
        if not other_id and conversation_id.startswith("dm:"):
            try:
                other_id = conversation_id.split(":", 1)[1] or None
            except Exception:
                other_id = None
        # Last-resort: infer other_id from latest messages when viewing by Graph CID or database ID
        if not other_id:
            try:
                from sqlalchemy import text as _text
                # Use database ID if numeric, otherwise try conversation_id as string
                lookup_cid = int(conversation_id) if is_db_id else str(conversation_id)
                rmsg = session.exec(
                    _text("SELECT ig_sender_id, ig_recipient_id FROM message WHERE conversation_id=:cid ORDER BY timestamp_ms DESC, id DESC LIMIT 1"),
                    {"cid": lookup_cid}
                ).first()
                if rmsg:
                    sid = getattr(rmsg, "ig_sender_id", None) if hasattr(rmsg, "ig_sender_id") else (rmsg[0] if len(rmsg) > 0 else None)
                    rid = getattr(rmsg, "ig_recipient_id", None) if hasattr(rmsg, "ig_recipient_id") else (rmsg[1] if len(rmsg) > 1 else None)
                    try:
                        _, owner_id, _ = _get_base_token_and_id()
                        if sid and str(sid) == str(owner_id):
                            other_id = str(rid) if rid else None
                        else:
                            other_id = str(sid) if sid else None
                        try:
                            _log.info("hydrate.infer_message: cid=%s sid=%s rid=%s owner=%s inferred_other_id=%s", 
                                     str(conversation_id), str(sid), str(rid), str(owner_id), str(other_id))
                        except Exception:
                            pass
                    except Exception:
                        other_id = str(sid) if sid else (str(rid) if rid else None)
                else:
                    try:
                        _log.info("hydrate.infer_message: no messages found with conversation_id=%s (lookup_cid=%s, is_db_id=%s)", 
                                 str(conversation_id), lookup_cid, is_db_id)
                        # Also log how many messages exist for this conversation_id
                        msg_count = session.exec(
                            _text("SELECT COUNT(*) FROM message WHERE conversation_id=:cid"),
                            {"cid": lookup_cid}
                        ).first()
                        if msg_count:
                            count = getattr(msg_count, "COUNT(*)", None) or (msg_count[0] if len(msg_count) > 0 else None) or 0
                            _log.info("hydrate.infer_message: message count for conversation_id=%s is %s", lookup_cid, count)
                    except Exception:
                        pass
            except Exception as e:
                try:
                    _log.warning("hydrate.infer_message: error looking up messages cid=%s lookup_cid=%s err=%s", 
                                str(conversation_id), lookup_cid if 'lookup_cid' in locals() else 'N/A', str(e)[:200])
                except Exception:
                    pass
        # Additional fallback: infer from ai_conversations (last known sender/recipient)
        if not other_id:
            try:
                from sqlalchemy import text as _text
                rowac = session.exec(
                    _text("SELECT ig_sender_id, ig_recipient_id FROM ai_conversations WHERE convo_id=:cid ORDER BY last_message_timestamp_ms DESC LIMIT 1")
                ).params(cid=str(conversation_id)).first()
                if rowac:
                    sid = getattr(rowac, "ig_sender_id", None) if hasattr(rowac, "ig_sender_id") else (rowac[0] if len(rowac) > 0 else None)
                    rid = getattr(rowac, "ig_recipient_id", None) if hasattr(rowac, "ig_recipient_id") else (rowac[1] if len(rowac) > 1 else None)
                    try:
                        _, owner_id, _ = _get_base_token_and_id()
                        if sid and str(sid) == str(owner_id):
                            other_id = str(rid) if rid else None
                        else:
                            other_id = str(sid) if sid else None
                    except Exception:
                        other_id = str(sid) if sid else (str(rid) if rid else None)
                try:
                    import logging
                    _log = logging.getLogger("instagram.inbox")
                    _log.info("hydrate.resolve.ai_conversations cid=%s other_id=%s", str(conversation_id), str(other_id))
                except Exception:
                    pass
            except Exception:
                pass
        # Final fallback: derive by fetching recent messages from Graph for this conversation id
        # Only attempt if we have a Graph conversation ID (not a database ID)
        if not other_id and graph_conversation_id and not conversation_id.startswith("dm:"):
            # Use the resolved Graph conversation ID to fetch messages
            try:
                import asyncio as _aio
                try:
                    loop = _aio.get_event_loop()
                except RuntimeError:
                    loop = _aio.new_event_loop()
                    _aio.set_event_loop(loop)
                from ..services.instagram_api import fetch_messages as _fm, _get_base_token_and_id as _gb
                _, owner_id, _ = _gb()
                msgs = loop.run_until_complete(_fm(str(graph_conversation_id), limit=10))
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
        # If we have a database ID but no graph_conversation_id stored yet, try to resolve it
        if not graph_conversation_id and is_db_id:
            try:
                try:
                    _log.info("hydrate.resolve_graph: attempting to resolve graph_cid for db_id=%s", int(conversation_id))
                except Exception:
                    pass
                graph_cid, lookup_info = _resolve_graph_conversation_id_for_hydrate(str(conversation_id))
                try:
                    _log.info("hydrate.resolve_graph: result graph_cid=%s lookup_info=%s", 
                             (graph_cid[:40] + "..." if graph_cid and len(graph_cid) > 40 else graph_cid) if graph_cid else None,
                             str(lookup_info)[:200] if lookup_info else None)
                except Exception:
                    pass
                if graph_cid:
                    graph_conversation_id = graph_cid
                    # Store it for future use
                    try:
                        from sqlalchemy import text as _t
                        session.exec(
                            _t("UPDATE conversations SET graph_conversation_id=:gc WHERE id=:cid AND (graph_conversation_id IS NULL OR graph_conversation_id=:gc)")
                        ).params(gc=str(graph_conversation_id), cid=int(conversation_id))
                        try:
                            _log.info("hydrate.resolve_graph: stored graph_cid=%s for db_id=%s", 
                                     (graph_conversation_id[:40] + "..." if len(graph_conversation_id) > 40 else graph_conversation_id),
                                     int(conversation_id))
                        except Exception:
                            pass
                    except Exception as e:
                        try:
                            _log.warning("hydrate.resolve_graph: failed to store graph_cid err=%s", str(e)[:200])
                        except Exception:
                            pass
            except Exception as e:
                try:
                    _log.warning("hydrate.resolve_graph: resolution failed err=%s", str(e)[:200])
                except Exception:
                    pass
        
        # If we have igba_id and ig_user_id but no graph_conversation_id, try fetching from Graph API
        if not graph_conversation_id and igba_id and other_id:
            try:
                try:
                    _log.info("hydrate.fetch_graph_cid: fetching from Graph API for igba_id=%s ig_user_id=%s", igba_id, other_id)
                except Exception:
                    pass
                import asyncio as _aio
                try:
                    loop = _aio.get_event_loop()
                except RuntimeError:
                    loop = _aio.new_event_loop()
                    _aio.set_event_loop(loop)
                from ..services.instagram_api import fetch_conversations as _fc
                all_convs = loop.run_until_complete(_fc(limit=100))
                for conv in (all_convs or []):
                    conv_id = conv.get("id")
                    participants = conv.get("participants", {})
                    participant_list = participants.get("data", []) if isinstance(participants, dict) else (participants if isinstance(participants, list) else [])
                    participant_ids = [str(p.get("id")) for p in participant_list if p and p.get("id")]
                    # Check if this conversation includes our user
                    if str(other_id) in participant_ids and conv_id:
                        graph_conversation_id = str(conv_id)
                        # Store it for future use
                        try:
                            from sqlalchemy import text as _t
                            if is_db_id:
                                session.exec(
                                    _t("UPDATE conversations SET graph_conversation_id=:gc WHERE id=:cid AND (graph_conversation_id IS NULL OR graph_conversation_id=:gc)")
                                ).params(gc=str(graph_conversation_id), cid=int(conversation_id))
                            else:
                                session.exec(
                                    _t("UPDATE conversations SET graph_conversation_id=:gc WHERE igba_id=:g AND ig_user_id=:u AND (graph_conversation_id IS NULL OR graph_conversation_id=:gc)")
                                ).params(gc=str(graph_conversation_id), g=str(igba_id), u=str(other_id))
                            try:
                                _log.info("hydrate.fetch_graph_cid: found and stored graph_cid=%s", 
                                         (graph_conversation_id[:40] + "..." if len(graph_conversation_id) > 40 else graph_conversation_id))
                            except Exception:
                                pass
                        except Exception as e:
                            try:
                                _log.warning("hydrate.fetch_graph_cid: failed to store err=%s", str(e)[:200])
                            except Exception:
                                pass
                        break
            except Exception as e:
                try:
                    _log.warning("hydrate.fetch_graph_cid: fetch failed err=%s", str(e)[:200])
                except Exception:
                    pass
        
        if not igba_id:
            try:
                _, entity_id, _ = _get_base_token_and_id()
                igba_id = str(entity_id)
            except Exception:
                igba_id = None
    
    try:
        import logging
        _log = logging.getLogger("instagram.inbox")
        _log.info("hydrate.resolve.final cid=%s igba_id=%s other_id=%s graph_cid=%s is_db_id=%s", 
                 str(conversation_id), str(igba_id), str(other_id), 
                 (graph_conversation_id[:40] + "..." if graph_conversation_id and len(graph_conversation_id) > 40 else graph_conversation_id),
                 is_db_id)
    except Exception:
        pass
    
    if not (igba_id and other_id):
        # If we at least have a Graph conversation id, enqueue a GCID-based hydrate as a fallback
        fallback_enqueued = False
        debug: dict[str, object] = {}
        try:
            token, ident, is_page = _get_base_token_and_id()
            debug["active_path"] = ("page" if is_page else "user")
            debug["owner_id"] = str(ident)
        except Exception as e:
            debug["env_resolve_error"] = str(e)
        debug["cid_kind"] = ("dm" if conversation_id.startswith("dm:") else ("db_id" if is_db_id else "graph"))
        debug["igba_id"] = (str(igba_id) if igba_id else None)
        debug["other_id"] = (str(other_id) if other_id else None)
        debug["graph_conversation_id"] = (str(graph_conversation_id) if graph_conversation_id else None)
        
        # If we have a Graph conversation ID but missing igba_id/other_id, use Graph API to hydrate
        if graph_conversation_id and not (igba_id and other_id) and not conversation_id.startswith("dm:"):
            try:
                try:
                    _log.info("hydrate.fallback: enqueuing hydrate_by_conversation_id with graph_cid=%s", 
                             (graph_conversation_id[:40] + "..." if len(graph_conversation_id) > 40 else graph_conversation_id))
                except Exception:
                    pass
                enqueue(
                    "hydrate_by_conversation_id",
                    key=str(graph_conversation_id),
                    payload={"conversation_id": str(graph_conversation_id), "max_messages": int(max_messages)},
                )
                fallback_enqueued = True
            except Exception as e:
                try:
                    _log.warning("hydrate.fallback: failed to enqueue hydrate_by_conversation_id err=%s", str(e)[:200])
                except Exception:
                    pass
                fallback_enqueued = False
        if fallback_enqueued:
            try:
                _log.info("hydrate.fallback: successfully enqueued hydrate_by_conversation_id")
            except Exception:
                pass
            return {"status": "ok", "queued": True, "fallback": "hydrate_by_conversation_id", "conversation_id": conversation_id, "debug": debug}
        try:
            _log.error("hydrate.failed: cannot resolve identifiers cid=%s igba_id=%s other_id=%s graph_cid=%s debug=%s", 
                      str(conversation_id), str(igba_id), str(other_id), 
                      (graph_conversation_id[:40] + "..." if graph_conversation_id and len(graph_conversation_id) > 40 else graph_conversation_id),
                      str(debug)[:500])
        except Exception:
            pass
        raise HTTPException(
            status_code=400,
            detail=f"Could not resolve identifiers to hydrate; cid={conversation_id} igba_id={igba_id} other_id={other_id}; debug={debug}"
        )
    # Best-effort: persist conversations mapping for future actions
    try:
        from sqlalchemy import text as _text
        with get_session() as session:
            conv_key = f"{str(igba_id)}:{str(other_id)}"
            try:
                session.exec(
                    _text(
                        "INSERT IGNORE INTO conversations(convo_id, igba_id, ig_user_id, last_message_at, unread_count) "
                        "VALUES (:cv, :g, :u, CURRENT_TIMESTAMP, 0)"
                    )
                ).params(cv=conv_key, g=str(igba_id), u=str(other_id))
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


@router.post("/inbox/{conversation_id}/send")
async def send_message(conversation_id: str, body: dict):
    """Send a reply (optional images + text) to the other party and persist locally.

    Body format:
      {
        "text": "Merhabalar abim ...",
        "image_urls": ["https://.../image-1.jpg", "https://.../image-2.jpg"]
      }

    conversation_id formats supported:
    - "dm:<ig_user_id>" (preferred)
    - Graph conversation id: will resolve other party id from recent messages
    """
    text_val = (body or {}).get("text")
    if not text_val or not isinstance(text_val, str) or not text_val.strip():
        raise HTTPException(status_code=400, detail="Message text is required")
    text_val = text_val.strip()
    import logging

    _log = logging.getLogger("instagram.inbox")

    # Optional list of image URLs to send before the text
    image_urls_raw = (body or {}).get("image_urls") or []
    if isinstance(image_urls_raw, str):
        image_urls_raw = [image_urls_raw]
    image_urls: list[str] = []
    if isinstance(image_urls_raw, list):
        for u in image_urls_raw:
            if isinstance(u, str) and u.strip():
                image_urls.append(u.strip())

    # Resolve recipient (other party IG user id) and canonical conversation identifiers
    other_id: str | None = None
    conv_internal_id: int | None = None
    public_conv_id: str | None = None
    conversation_platform: str = "instagram"
    with get_session() as session:
        from sqlmodel import select

        convo_row = None
        try:
            convo_row = session.get(Conversation, int(conversation_id))
        except (TypeError, ValueError):
            convo_row = None
        if convo_row is None:
            if conversation_id.startswith("dm:"):
                suffix = conversation_id.split(":", 1)[1] or None
                if suffix:
                    convo_row = (
                        session.exec(
                            select(Conversation)
                            .where(Conversation.ig_user_id == suffix)
                            .order_by(Conversation.id.desc())
                        ).first()
                    )
                    other_id = suffix
            else:
                convo_row = (
                    session.exec(
                        select(Conversation)
                        .where(Conversation.graph_conversation_id == conversation_id)
                        .order_by(Conversation.id.desc())
                    ).first()
                )
        if convo_row:
            conv_internal_id = int(convo_row.id)
            conversation_platform = str(getattr(convo_row, "platform", None) or "instagram")
            if not other_id and getattr(convo_row, "ig_user_id", None):
                other_id = str(convo_row.ig_user_id)
            public_conv_id = (
                getattr(convo_row, "graph_conversation_id", None)
                or (f"dm:{convo_row.ig_user_id}" if getattr(convo_row, "ig_user_id", None) else None)
            )
        msg_filter_id: int | None = conv_internal_id
        if msg_filter_id is None:
            try:
                msg_filter_id = int(conversation_id)
            except (TypeError, ValueError):
                msg_filter_id = None
        if not other_id and msg_filter_id is not None:
            msgs = session.exec(
                select(Message)
                .where(Message.conversation_id == msg_filter_id)
                .order_by(Message.timestamp_ms.desc())
                .limit(50)
            ).all()
            for m in msgs:
                if (m.direction or "in") == "in" and m.ig_sender_id:
                    other_id = str(m.ig_sender_id)
                    break
                if (m.direction or "in") == "out" and m.ig_recipient_id:
                    other_id = str(m.ig_recipient_id)
                    break
    if not public_conv_id and other_id:
        public_conv_id = f"dm:{other_id}"
    if not other_id:
        raise HTTPException(status_code=400, detail="Could not resolve recipient for this conversation")
    if conv_internal_id is None:
        raise HTTPException(status_code=404, detail="Conversation not found")

    # Resolve sender account id for local persistence
    entity_id = ""
    if conversation_platform == "instagram":
        _, entity_id, is_page = _get_base_token_and_id()
        if not is_page:
            raise HTTPException(status_code=400, detail="Sending requires a Page access token (IG_PAGE_ACCESS_TOKEN)")
    else:
        entity_id = _os.getenv("WA_PHONE_NUMBER_ID", "")

    try:
        resp = await send_channel_message(
            platform=conversation_platform,
            recipient_id=str(other_id),
            conversation_id=(public_conv_id or conversation_id),
            text=text_val,
            image_urls=image_urls if image_urls else None,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Message send failed: {e}")

    # Persist locally
    message_ids = (resp or {}).get("message_ids") or []
    mid = str((resp or {}).get("message_id") or (message_ids[0] if message_ids else "") or "")
    now_ms = int(time.time() * 1000)
    if not public_conv_id:
        public_conv_id = conversation_id if conversation_id.startswith("dm:") else f"dm:{other_id}"

    with get_session() as session:
        # Idempotency: avoid duplicate insert when the same message_id was already saved
        if mid:
            try:
                exists = session.exec(
                    select(Message).where(
                        Message.ig_message_id == mid,
                        Message.platform == str(conversation_platform or "instagram"),
                    )
                ).first()
                if exists:
                    # still bump last_message fields for the conversation to reflect the send time
                    try:
                        from datetime import datetime as _dt
                        convo = session.get(Conversation, int(conv_internal_id))
                        if convo:
                            convo.last_message_at = _dt.utcfromtimestamp(int(now_ms / 1000))
                            convo.last_message_timestamp_ms = now_ms
                            convo.last_message_text = text_val
                            convo.last_message_direction = "out"
                            convo.ig_sender_id = str(entity_id)
                            convo.ig_recipient_id = str(other_id)
                            session.add(convo)
                    except Exception:
                        pass
                    return {"status": "ok", "message_id": mid}
            except Exception:
                # proceed with best-effort insert
                pass
        row = Message(
            platform=str(conversation_platform or "instagram"),
            ig_sender_id=str(entity_id),
            ig_recipient_id=str(other_id),
            ig_message_id=(mid or None),
            text=text_val,
            attachments_json=None,
            timestamp_ms=now_ms,
            raw_json=json.dumps({"send_response": resp}, ensure_ascii=False),
            conversation_id=int(conv_internal_id),
            direction="out",
        )
        session.add(row)
        try:
            from datetime import datetime as _dt
            session.flush()
            convo = session.get(Conversation, int(conv_internal_id))
            if convo:
                if row.id:
                    convo.last_message_id = int(row.id)
                convo.last_message_at = _dt.utcfromtimestamp(int(now_ms / 1000))
                convo.last_message_timestamp_ms = now_ms
                convo.last_message_text = text_val
                convo.last_message_direction = "out"
                convo.ig_sender_id = str(entity_id)
                convo.ig_recipient_id = str(other_id)
                session.add(convo)
        except Exception:
            pass
    try:
        increment_counter("sent_messages", 1)
        from .websocket_handlers import notify_new_message
        notify_id = public_conv_id or (f"dm:{other_id}" if other_id else str(conv_internal_id))
        await notify_new_message({
            "type": "ig_message",
            "conversation_id": notify_id,
            "conversation_pk": int(conv_internal_id),
            "text": text_val,
            "timestamp_ms": now_ms,
        })
    except Exception:
        pass
    return {"status": "ok", "message_id": mid or None}
