from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, Optional, Tuple, List
import os

from sqlmodel import select

from ..db import get_session
from ..models import Message
from .ai import AIClient
from .prompts import IG_PURCHASE_SYSTEM_PROMPT
from .monitoring import ai_run_log
import logging


log = logging.getLogger("ig_ai.process")


def _format_transcript(messages: List[Any], max_chars: int = 15000) -> str:
    parts: List[str] = []
    for m in messages:
        # Support SQLModel instances or plain dicts
        try:
            role = ((m.direction if hasattr(m, "direction") else (m.get("direction") if isinstance(m, dict) else "in")) or "in").lower()
        except Exception:
            role = "in"
        try:
            ts = (m.timestamp_ms if hasattr(m, "timestamp_ms") else (m.get("timestamp_ms") if isinstance(m, dict) else 0)) or 0
        except Exception:
            ts = 0
        try:
            txt = ((m.text if hasattr(m, "text") else (m.get("text") if isinstance(m, dict) else "")) or "").strip()
        except Exception:
            txt = ""
        parts.append(f"[{role}] {ts}: {txt}")
    txt = "\n".join(parts)
    # trim to model budget if needed
    if len(txt) > max_chars:
        return txt[-max_chars:]
    return txt


def analyze_conversation(conversation_id: str, *, limit: int = 200, run_id: Optional[int] = None, include_meta: bool = False) -> Dict[str, Any]:
    """Run AI over a single conversation to detect purchase and extract contacts.

    Returns a dict with keys: purchase_detected, buyer_name, phone, address, notes,
    product_mentions (list), possible_order_ids (list)
    """
    client = AIClient()
    if not client.enabled:
        raise RuntimeError("AI client is not configured. Set OPENAI_API_KEY.")
    with get_session() as session:
        msgs = session.exec(
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.timestamp_ms.asc())
            .limit(min(max(limit, 1), 500))
        ).all()
        # Serialize messages into simple dicts to avoid detached/lazy load issues
        simple_msgs: List[Dict[str, Any]] = []
        for m in msgs:
            try:
                simple_msgs.append({
                    "direction": (m.direction or "in"),
                    "timestamp_ms": (m.timestamp_ms or 0),
                    "text": (m.text or ""),
                })
            except Exception:
                continue
    transcript = _format_transcript(simple_msgs)
    # Log context preparation
    try:
        ai_run_log(int(run_id), "debug", "prepare_context", {
            "conversation_id": conversation_id,
            "messages": len(msgs),
            "transcript_len": len(transcript),
        }) if run_id is not None else None
    except Exception:
        pass
    schema_hint = (
        '{"purchase_detected": true|false, "buyer_name": "str|null", "phone": "str|null", '
        '"address": "str|null", "notes": "str|null", "product_mentions": ["str"], '
        '"possible_order_ids": ["str"]}'
    )
    user_prompt = (
        "Aşağıda bir DM konuşması transkripti var. \n"
        "Lütfen kesin satın alma olup olmadığını belirle ve bilgileri çıkar.\n\n"
        f"Şema: {schema_hint}\n\n"
        f"Transkript:\n{transcript}"
    )
    # Optional prompt logging (truncated) when enabled
    try:
        if os.getenv("AI_LOG_PROMPT", "0") not in ("0", "false", "False", "") and run_id is not None:
            ai_run_log(int(run_id), "debug", "ai_prompt", {
                "system_prompt": IG_PURCHASE_SYSTEM_PROMPT[:800],
                "user_prompt": user_prompt[:1200],
                "conversation_id": conversation_id,
            })
    except Exception:
        pass
    if include_meta:
        data, raw_response = client.generate_json(
            system_prompt=IG_PURCHASE_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            include_raw=True,
        )
    else:
        data = client.generate_json(system_prompt=IG_PURCHASE_SYSTEM_PROMPT, user_prompt=user_prompt)
        raw_response = None
    try:
        if run_id is not None:
            ai_run_log(int(run_id), "info", "ai_response", {
                "conversation_id": conversation_id,
                "purchase_detected": bool(data.get("purchase_detected")),
                "has_phone": bool(data.get("phone")),
                "has_address": bool(data.get("address")),
                "mentions": len(data.get("product_mentions") or []),
            })
    except Exception:
        pass
    if not isinstance(data, dict):
        raise RuntimeError("AI returned non-dict JSON")
    # Normalize keys presence
    out: Dict[str, Any] = {
        "purchase_detected": bool(data.get("purchase_detected", False)),
        "buyer_name": data.get("buyer_name"),
        "phone": data.get("phone"),
        "address": data.get("address"),
        "notes": data.get("notes"),
        "product_mentions": data.get("product_mentions") or [],
        "possible_order_ids": data.get("possible_order_ids") or [],
    }
    if include_meta:
        out["meta"] = {
            "ai_model": client.model,
            "system_prompt": IG_PURCHASE_SYSTEM_PROMPT,
            "user_prompt": user_prompt,
            "raw_response": raw_response,
        }
    return out


def process_run(
    *,
    run_id: int,
    date_from: Optional[dt.date],
    date_to: Optional[dt.date],
    min_age_minutes: int = 60,
    limit: int = 200,
    reprocess: bool = False,
    conversation_id: Optional[str] = None,
    debug_run_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Process eligible conversations for a run id and persist results.

    Returns counters summary.
    """
    from sqlalchemy import text as _text
    from .matching import link_order_for_extraction  # lazy import

    now = dt.datetime.utcnow()
    min_age_delta = dt.timedelta(minutes=max(0, int(min_age_minutes)))
    cutoff_dt = now - min_age_delta

    considered = 0
    processed = 0
    linked = 0
    purchases = 0
    purchases_unlinked = 0
    errors: List[str] = []

    with get_session() as session:
        # Optional reprocess: clear processed markers for in-scope conversations
        if reprocess:
            try:
                params_clear: Dict[str, Any] = {"cutoff": cutoff_dt.isoformat(" ")}
                where_clear = ["last_message_at <= :cutoff"]
                if date_from and date_to and date_from <= date_to:
                    dt_end = date_to + dt.timedelta(days=1)
                    params_clear["df"] = f"{date_from.isoformat()} 00:00:00"
                    params_clear["dte"] = f"{dt_end.isoformat()} 00:00:00"
                    where_clear.append("last_message_at >= :df AND last_message_at < :dte")
                elif date_from:
                    params_clear["df"] = f"{date_from.isoformat()} 00:00:00"
                    where_clear.append("last_message_at >= :df")
                elif date_to:
                    dt_end = date_to + dt.timedelta(days=1)
                    params_clear["dte"] = f"{dt_end.isoformat()} 00:00:00"
                    where_clear.append("last_message_at < :dte")
                set_cols_full = "ai_processed_at=NULL, ai_status=NULL, ai_run_id=NULL, linked_order_id=NULL"
                set_cols_nostatus = "ai_processed_at=NULL, ai_run_id=NULL, linked_order_id=NULL"
                sql_clear_full = ("UPDATE conversations SET " + set_cols_full + " WHERE " + " AND ".join(where_clear))
                sql_clear_nostatus = ("UPDATE conversations SET " + set_cols_nostatus + " WHERE " + " AND ".join(where_clear))
                rc = None
                try:
                    rc = session.exec(_text(sql_clear_full).params(**params_clear)).rowcount
                except Exception as e:
                    # If ai_status column is missing, retry without it
                    if "Unknown column 'ai_status'" in str(e):
                        try:
                            rc = session.exec(_text(sql_clear_nostatus).params(**params_clear)).rowcount
                        except Exception:
                            rc = 0
                    else:
                        raise
                try:
                    ai_run_log(run_id, "info", "reprocess_clear", {"cleared": int(rc or 0)})
                except Exception:
                    pass
            except Exception as e:
                try:
                    ai_run_log(run_id, "error", "reprocess_clear_error", {"error": str(e)})
                except Exception:
                    pass
        # Select eligible conversations by last_message_at and ai_processed_at is NULL
        params: Dict[str, Any] = {"cutoff": cutoff_dt.isoformat(" ")}
        if conversation_id:
            sql = "SELECT convo_id FROM conversations WHERE convo_id = :single LIMIT 1"
            row = session.exec(_text(sql).params(single=conversation_id)).first()
            if row:
                cid_val = row.convo_id if hasattr(row, "convo_id") else row[0]
                convo_ids = [str(cid_val)]
            else:
                convo_ids = [conversation_id]
            considered = len(convo_ids)
        else:
            where = ["ai_processed_at IS NULL", "last_message_at <= :cutoff"]
            if date_from and date_to and date_from <= date_to:
                dt_end = date_to + dt.timedelta(days=1)
                params["df"] = f"{date_from.isoformat()} 00:00:00"
                params["dte"] = f"{dt_end.isoformat()} 00:00:00"
                where.append("last_message_at >= :df AND last_message_at < :dte")
            elif date_from:
                params["df"] = f"{date_from.isoformat()} 00:00:00"
                where.append("last_message_at >= :df")
            elif date_to:
                dt_end = date_to + dt.timedelta(days=1)
                params["dte"] = f"{dt_end.isoformat()} 00:00:00"
                where.append("last_message_at < :dte")
            sql = (
                "SELECT convo_id FROM conversations WHERE "
                + " AND ".join(where)
                + f" ORDER BY last_message_at DESC LIMIT {int(limit)}"
            )
            rows = session.exec(_text(sql).params(**params)).all()
            convo_ids = [r.convo_id if hasattr(r, "convo_id") else r[0] for r in rows]
            considered = len(convo_ids)
        # Fallback: if conversations table yields 0, select distinct conversation_id
        # from messages by timestamp window so we can still process
        if not conversation_id and considered == 0:
            try:
                cutoff_ms = int(cutoff_dt.timestamp() * 1000)
                msg_where = ["(m.timestamp_ms IS NULL OR m.timestamp_ms <= :cutoff_ms)", "m.conversation_id IS NOT NULL"]
                msg_params: Dict[str, Any] = {"cutoff_ms": int(cutoff_ms)}
                if date_from and date_to and date_from <= date_to:
                    ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
                    ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
                    msg_where.append("(m.timestamp_ms IS NULL OR (m.timestamp_ms >= :ms_from AND m.timestamp_ms < :ms_to))")
                    msg_params["ms_from"] = int(ms_from)
                    msg_params["ms_to"] = int(ms_to)
                elif date_from:
                    ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
                    msg_where.append("(m.timestamp_ms IS NULL OR m.timestamp_ms >= :ms_from)")
                    msg_params["ms_from"] = int(ms_from)
                elif date_to:
                    ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
                    msg_where.append("(m.timestamp_ms IS NULL OR m.timestamp_ms < :ms_to)")
                    msg_params["ms_to"] = int(ms_to)
                sql_msg = (
                    "SELECT DISTINCT m.conversation_id FROM message m WHERE " + " AND ".join(msg_where) + f" ORDER BY 1 DESC LIMIT {int(limit)}"
                )
                rows_m = session.exec(_text(sql_msg).params(**msg_params)).all()
                convo_ids = [r.conversation_id if hasattr(r, "conversation_id") else r[0] for r in rows_m]
                considered = len(convo_ids)
                ai_run_log(run_id, "info", "fallback_candidates", {"count": int(considered)})
            except Exception as e:
                ai_run_log(run_id, "error", "fallback_error", {"error": str(e)})
    try:
        log.info(
            "ig_ai run start rid=%s df=%s dt=%s min_age=%s limit=%s considered=%s",
            run_id,
            (date_from.isoformat() if date_from else None),
            (date_to.isoformat() if date_to else None),
            int(min_age_minutes),
            int(limit),
            considered,
        )
    except Exception:
        pass
    ai_run_log(run_id, "info", "run_start", {
        "date_from": (date_from.isoformat() if date_from else None),
        "date_to": (date_to.isoformat() if date_to else None),
        "min_age_minutes": int(min_age_minutes),
        "limit": int(limit),
        "considered": int(considered),
    })

    # Helper to check cancellation flag quickly
    def _is_cancelled() -> bool:
        try:
            with get_session() as s2:
                row = s2.exec(_text("SELECT cancelled_at FROM ig_ai_run WHERE id=:id").params(id=int(run_id))).first()
                if not row:
                    return False
                val = row.cancelled_at if hasattr(row, 'cancelled_at') else (row[0] if isinstance(row, (list, tuple)) else None)
                return bool(val)
        except Exception:
            return False

    if _is_cancelled():
        errors.append("cancelled")
        with get_session() as session:
            try:
                session.exec(_text("UPDATE ig_ai_run SET completed_at=CURRENT_TIMESTAMP WHERE id=:id").params(id=int(run_id)))
            except Exception:
                pass
        return {
            "considered": 0,
            "processed": 0,
            "linked": 0,
            "purchases": 0,
            "purchases_unlinked": 0,
            "errors": errors,
        }

    debug_entries: list[dict[str, Any]] = []
    include_meta = bool(debug_run_id)

    for cid in convo_ids:
        ai_run_log(run_id, "debug", "analyze_start", {"conversation_id": cid})
        try:
            data = analyze_conversation(cid, limit=limit, run_id=run_id, include_meta=include_meta)
        except Exception as e:
            errors.append(f"{cid}: {e}")
            ai_run_log(run_id, "error", "analyze_error", {"conversation_id": cid, "error": str(e)})
            # persist error status so we don't spin forever; keep ai_json for debugging
            with get_session() as session:
                try:
                    session.exec(
                        _text(
                            "UPDATE conversations SET ai_status=:s, ai_json=:j, ai_processed_at=CURRENT_TIMESTAMP, ai_run_id=:rid WHERE convo_id=:cid"
                        ).params(s="error", j=json.dumps({"error": str(e)}), rid=run_id, cid=cid)
                    )
                except Exception:
                    pass
            if include_meta and conversation_id and cid == conversation_id:
                debug_entries.append({
                    "conversation_id": cid,
                    "status": "error",
                    "error": str(e),
                })
            continue

        def _clean_field(val: Any) -> Optional[str]:
            if val is None:
                return None
            try:
                if isinstance(val, str):
                    stripped = val.strip()
                    return stripped or None
                stripped = str(val).strip()
                return stripped or None
            except Exception:
                return None

        meta_info = None
        if include_meta and isinstance(data, dict):
            meta_info = data.get("meta")
            if isinstance(data, dict):
                data = dict(data)
                data.pop("meta", None)

        status = "no_purchase"
        linked_order_id: Optional[int] = None
        buyer_name_clean = _clean_field(data.get("buyer_name"))
        phone_clean = _clean_field(data.get("phone"))
        address_clean = _clean_field(data.get("address"))
        data["buyer_name"] = buyer_name_clean
        data["phone"] = phone_clean
        data["address"] = address_clean
        if bool(data.get("purchase_detected")):
            purchases += 1
            try:
                with get_session() as session:
                    linked_order_id = link_order_for_extraction(session, data, date_from=date_from, date_to=date_to)
            except Exception as le:
                errors.append(f"{cid}: match_err {le}")
                linked_order_id = None
            if linked_order_id:
                linked += 1
                status = "ok"
                try:
                    log.info("ig_ai convo=%s purchase=1 linked_order_id=%s", cid, int(linked_order_id))
                except Exception:
                    pass
                ai_run_log(run_id, "info", "purchase_linked", {"conversation_id": cid, "order_id": int(linked_order_id)})
            else:
                purchases_unlinked += 1
                status = "ambiguous"
                try:
                    log.info("ig_ai convo=%s purchase=1 linked_order_id=null", cid)
                except Exception:
                    pass
                ai_run_log(run_id, "info", "purchase_unlinked", {"conversation_id": cid})
        else:
            try:
                log.debug("ig_ai convo=%s purchase=0", cid)
            except Exception:
                pass
            ai_run_log(run_id, "debug", "no_purchase", {"conversation_id": cid})

        with get_session() as session:
            try:
                ai_run_log(run_id, "debug", "persist_start", {"conversation_id": cid, "status": status, "linked_order_id": linked_order_id})
                # Persist conversations fields
                session.exec(
                    _text(
                        """
                        UPDATE conversations SET
                          contact_name = COALESCE(NULLIF(TRIM(contact_name), ''), :name),
                          contact_phone = COALESCE(NULLIF(TRIM(contact_phone), ''), :phone),
                          contact_address = COALESCE(NULLIF(TRIM(contact_address), ''), :addr),
                          ai_status = :st,
                          ai_json = :js,
                          ai_processed_at = CURRENT_TIMESTAMP,
                          linked_order_id = COALESCE(linked_order_id, :oid),
                          ai_run_id = :rid
                        WHERE convo_id = :cid
                        """
                    ).params(
                        name=buyer_name_clean,
                        phone=phone_clean,
                        addr=address_clean,
                        st=status,
                        js=json.dumps(data, ensure_ascii=False),
                        oid=linked_order_id,
                        rid=run_id,
                        cid=cid,
                    )
                )
                # Also back-fill order.ig_conversation_id if linked
                if linked_order_id and cid:
                    session.exec(
                        _text('UPDATE `order` SET ig_conversation_id = COALESCE(ig_conversation_id, :cid) WHERE id=:oid').params(cid=cid, oid=int(linked_order_id))
                    )
                # Optional: write history row
                try:
                    session.exec(
                        _text(
                            """
                            INSERT INTO ig_ai_result(convo_id, run_id, status, ai_json, linked_order_id, created_at)
                            VALUES(:cid, :rid, :st, :js, :oid, CURRENT_TIMESTAMP)
                            """
                        ).params(cid=cid, rid=run_id, st=status, js=json.dumps(data, ensure_ascii=False), oid=linked_order_id)
                    )
                except Exception:
                    pass
                processed += 1
                ai_run_log(run_id, "info", "persist_done", {"conversation_id": cid, "status": status, "linked_order_id": linked_order_id})
            except Exception as pe:
                errors.append(f"{cid}: persist_err {pe}")
                ai_run_log(run_id, "error", "persist_error", {"conversation_id": cid, "error": str(pe)})
                ai_run_log(run_id, "error", "persist_error", {"conversation_id": cid, "error": str(pe)})

        # cancellation check between items
        if _is_cancelled():
            errors.append("cancelled")
            break

        if include_meta and conversation_id and cid == conversation_id:
            debug_entries.append({
                "conversation_id": cid,
                "status": status,
                "linked_order_id": linked_order_id,
                "result": data,
                "meta": meta_info,
                "errors": list(errors),
            })

    # Update run row
    with get_session() as session:
        try:
            session.exec(
                _text(
                    """
                    UPDATE ig_ai_run SET
                      completed_at = CURRENT_TIMESTAMP,
                      conversations_considered = :cns,
                      conversations_processed = :prs,
                      orders_linked = :lnk,
                      purchases_detected = :pur,
                      purchases_unlinked = :pun,
                      errors_json = :err
                    WHERE id = :rid
                    """
                ).params(
                    cns=considered,
                    prs=processed,
                    lnk=linked,
                    pur=purchases,
                    pun=purchases_unlinked,
                    err=json.dumps(errors, ensure_ascii=False) if errors else None,
                    rid=run_id,
                )
            )
        except Exception:
            # Fallback: at least mark completion timestamp
            try:
                session.exec(_text("UPDATE ig_ai_run SET completed_at=CURRENT_TIMESTAMP WHERE id=:rid").params(rid=run_id))
            except Exception:
                pass
    try:
        log.info(
            "ig_ai run done rid=%s considered=%s processed=%s linked=%s purchases=%s unlinked=%s errors=%s",
            run_id,
            considered,
            processed,
            linked,
            purchases,
            purchases_unlinked,
            len(errors),
        )
    except Exception:
        pass
    ai_run_log(run_id, "info", "run_done", {
        "considered": int(considered),
        "processed": int(processed),
        "linked": int(linked),
        "purchases": int(purchases),
        "unlinked": int(purchases_unlinked),
        "errors": len(errors),
    })

    result_summary: Dict[str, Any] = {
        "considered": considered,
        "processed": processed,
        "linked": linked,
        "purchases": purchases,
        "purchases_unlinked": purchases_unlinked,
        "errors": errors,
    }
    if debug_entries:
        result_summary["debug_entries"] = debug_entries

    return result_summary


