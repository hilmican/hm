from __future__ import annotations

import datetime as dt
from typing import Optional

from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import text
from sqlmodel import select
import datetime as dt

from ..db import get_session
from ..services.queue import enqueue, delete_job
from ..services.monitoring import get_ai_run_logs


router = APIRouter(prefix="/ig/ai", tags=["instagram-ai"])


@router.get("/process")
def process_page(request: Request):
    templates = request.app.state.templates
    # Render immediately; client will fetch runs via /ig/ai/process/runs
    return templates.TemplateResponse("ig_ai_process.html", {"request": request, "runs": []})


@router.post("/process/run")
def start_process(body: dict):
    # Parse inputs
    date_from_s: Optional[str] = (body or {}).get("date_from")
    date_to_s: Optional[str] = (body or {}).get("date_to")
    min_age_minutes: int = int((body or {}).get("min_age_minutes") or 60)
    limit: int = int((body or {}).get("limit") or 200)
    reprocess: bool = bool((body or {}).get("reprocess") not in (False, 0, "0", "false", "False", None))

    def _parse_date(v: Optional[str]) -> Optional[dt.date]:
        try:
            return dt.date.fromisoformat(str(v)) if v else None
        except Exception:
            return None

    date_from = _parse_date(date_from_s)
    date_to = _parse_date(date_to_s)

    # Create run row
    with get_session() as session:
        stmt = text(
            """
            INSERT INTO ig_ai_run(started_at, date_from, date_to, min_age_minutes)
            VALUES (CURRENT_TIMESTAMP, :df, :dt, :age)
            """
        ).bindparams(
            df=(date_from.isoformat() if date_from else None),
            dt=(date_to.isoformat() if date_to else None),
            age=int(min_age_minutes),
        )
        session.exec(stmt)
        run_id = None
        # Try MySQL first
        try:
            rid_row = session.exec(text("SELECT LAST_INSERT_ID() AS id")).first()
            if rid_row is not None:
                run_id = int(getattr(rid_row, "id", rid_row[0]))
        except Exception:
            pass
        # Fallback to SQLite-style
        if run_id is None:
            try:
                rid_row = session.exec(text("SELECT last_insert_rowid() AS id")).first()
                if rid_row is not None:
                    run_id = int(getattr(rid_row, "id", rid_row[0]))
            except Exception:
                pass
        if run_id is None:
            raise HTTPException(status_code=500, detail="Could not create run")

    # Enqueue background job to process
    job_id = enqueue("ig_ai_process_run", key=str(run_id), payload={
        "run_id": run_id,
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,
        "min_age_minutes": min_age_minutes,
        "limit": limit,
        "reprocess": reprocess,
    })
    with get_session() as session:
        session.exec(text("UPDATE ig_ai_run SET job_id=:jid WHERE id=:id").params(jid=int(job_id), id=int(run_id)))
    return {"status": "ok", "run_id": run_id}


@router.get("/process/runs")
def list_runs(limit: int = 50):
    with get_session() as session:
        nint = int(max(1, min(limit, 200)))
        # Embed LIMIT as a literal integer to avoid driver param binding issues
        rows = session.exec(text(f"""
            SELECT id, started_at, completed_at, cancelled_at, job_id, date_from, date_to, min_age_minutes,
                   conversations_considered, conversations_processed, orders_linked,
                   purchases_detected, purchases_unlinked, errors_json
            FROM ig_ai_run ORDER BY id DESC LIMIT {nint}
        """)).all()
        out = []
        for r in rows:
            out.append({
                "id": getattr(r, "id", r[0]),
                "started_at": getattr(r, "started_at", r[1]),
                "completed_at": getattr(r, "completed_at", r[2]),
                "cancelled_at": getattr(r, "cancelled_at", r[3]),
                "job_id": getattr(r, "job_id", r[4]),
                "date_from": getattr(r, "date_from", r[5]),
                "date_to": getattr(r, "date_to", r[6]),
                "min_age_minutes": getattr(r, "min_age_minutes", r[7]),
                "conversations_considered": getattr(r, "conversations_considered", r[8]),
                "conversations_processed": getattr(r, "conversations_processed", r[9]),
                "orders_linked": getattr(r, "orders_linked", r[10]),
                "purchases_detected": getattr(r, "purchases_detected", r[11]),
                "purchases_unlinked": getattr(r, "purchases_unlinked", r[12]),
                "errors_json": getattr(r, "errors_json", r[13]),
            })
        return {"runs": out}


@router.get("/process/run/{run_id}")
def run_details(run_id: int):
    with get_session() as session:
        stmt = text(
            """
            SELECT id, started_at, completed_at, cancelled_at, job_id, date_from, date_to, min_age_minutes,
                   conversations_considered, conversations_processed, orders_linked,
                   purchases_detected, purchases_unlinked, errors_json
            FROM ig_ai_run WHERE id = :id
            """
        ).bindparams(id=int(run_id))
        row = session.exec(stmt).first()
        if not row:
            raise HTTPException(status_code=404, detail="Run not found")
        return {
            "id": getattr(row, "id", row[0]),
            "started_at": getattr(row, "started_at", row[1]),
            "completed_at": getattr(row, "completed_at", row[2]),
            "cancelled_at": getattr(row, "cancelled_at", row[3]),
            "job_id": getattr(row, "job_id", row[4]),
            "date_from": getattr(row, "date_from", row[5]),
            "date_to": getattr(row, "date_to", row[6]),
            "min_age_minutes": getattr(row, "min_age_minutes", row[7]),
            "conversations_considered": getattr(row, "conversations_considered", row[8]),
            "conversations_processed": getattr(row, "conversations_processed", row[9]),
            "orders_linked": getattr(row, "orders_linked", row[10]),
            "purchases_detected": getattr(row, "purchases_detected", row[11]),
            "purchases_unlinked": getattr(row, "purchases_unlinked", row[12]),
            "errors_json": getattr(row, "errors_json", row[13]),
        }


@router.post("/process/run/{run_id}/cancel")
def cancel_run(run_id: int):
    with get_session() as session:
        row = session.exec(text("SELECT job_id FROM ig_ai_run WHERE id=:id").params(id=int(run_id))).first()
        session.exec(text("UPDATE ig_ai_run SET cancelled_at=CURRENT_TIMESTAMP, completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP) WHERE id=:id").params(id=int(run_id)))
        jid = None
        if row:
            jid = getattr(row, 'job_id', None) if hasattr(row, 'job_id') else (row[0] if isinstance(row, (list, tuple)) else None)
    try:
        if jid:
            delete_job(int(jid))
    except Exception:
        pass
    return {"status": "ok"}


@router.post("/process/preview")
def preview_process(body: dict):
    # Parse inputs like start_process, but only compute counts
    date_from_s: Optional[str] = (body or {}).get("date_from")
    date_to_s: Optional[str] = (body or {}).get("date_to")
    min_age_minutes: int = int((body or {}).get("min_age_minutes") or 60)
    reprocess: bool = bool((body or {}).get("reprocess") not in (False, 0, "0", "false", "False", None))

    def _parse_date(v: Optional[str]) -> Optional[dt.date]:
        try:
            return dt.date.fromisoformat(str(v)) if v else None
        except Exception:
            return None

    date_from = _parse_date(date_from_s)
    date_to = _parse_date(date_to_s)

    # Compute cutoff
    now = dt.datetime.utcnow()
    cutoff_dt = now - dt.timedelta(minutes=max(0, min_age_minutes))
    # For message timestamp_ms comparisons (ms since epoch)
    cutoff_ms = int(cutoff_dt.timestamp() * 1000)

    with get_session() as session:
        # Conversations count derived from messages grouped by conversation_id, filtered by ai_conversations.ai_process_time
        cutoff_ms = int(cutoff_dt.timestamp() * 1000)
        msg_where = ["m.conversation_id IS NOT NULL", "(m.timestamp_ms IS NULL OR m.timestamp_ms <= :cutoff_ms)"]
        msg_params: dict[str, object] = {"cutoff_ms": int(cutoff_ms)}
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
        try:
            backend = getattr(session.get_bind().engine.url, "get_backend_name", lambda: "")()
        except Exception:
            backend = ""
        # Use conversations.ai_process_time as the watermark instead of ai_conversations
        ts_expr = "COALESCE(UNIX_TIMESTAMP(c.ai_process_time),0)*1000" if backend == "mysql" else "COALESCE(strftime('%s', c.ai_process_time),0)*1000"
        sql_conv = (
            "SELECT COUNT(1) AS c FROM ("
            " SELECT m.conversation_id, MAX(COALESCE(m.timestamp_ms,0)) AS last_ts"
            " FROM message m WHERE " + " AND ".join(msg_where) +
            " GROUP BY m.conversation_id"
            ") t LEFT JOIN conversations c ON c.id = t.conversation_id "
            + ("WHERE (c.ai_process_time IS NULL OR t.last_ts > " + ts_expr + ")" if not reprocess else "")
        )
        rowc = session.exec(text(sql_conv).params(**msg_params)).first()
        conv_count = int((getattr(rowc, "c", None) if rowc is not None else 0) or (rowc[0] if rowc else 0) or 0)

        # Messages count aligned with eligibility: only messages newer than ai_process_time when not reprocessing
        msg_where = ["m.conversation_id IS NOT NULL", "COALESCE(m.timestamp_ms,0) <= :cutoff_ms"]
        msg_params = {"cutoff_ms": int(cutoff_ms)}
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
            "SELECT COUNT(1) AS mc, SUM(CASE WHEN m.timestamp_ms IS NULL THEN 1 ELSE 0 END) AS mt0 "
            "FROM message m LEFT JOIN conversations c ON c.id = m.conversation_id WHERE "
            + " AND ".join(msg_where)
            + (f" AND (c.ai_process_time IS NULL OR COALESCE(m.timestamp_ms,0) > {ts_expr})" if not reprocess else "")
        )
        rowm = session.exec(text(sql_msg).params(**msg_params)).first()
        msg_count = int((getattr(rowm, "mc", None) if rowm is not None else 0) or (rowm[0] if rowm else 0) or 0)
        msg_ts_missing = int((getattr(rowm, "mt0", None) if rowm is not None else 0) or (rowm[1] if rowm and len(rowm) > 1 else 0) or 0)

        # Fallbacks when conversations table filters produce 0 due to missing/old data
        if conv_count == 0:
            ms_from = None
            ms_to = None
            if date_from:
                ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
            if date_to:
                ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
            where_msg = ["m.conversation_id IS NOT NULL", "(m.timestamp_ms IS NULL OR m.timestamp_ms <= :cutoff_ms)"]
            params_f = {"cutoff_ms": int(cutoff_ms)}
            if ms_from is not None and ms_to is not None:
                where_msg.append("(m.timestamp_ms IS NULL OR (m.timestamp_ms >= :ms_from AND m.timestamp_ms < :ms_to))")
                params_f["ms_from"] = int(ms_from)
                params_f["ms_to"] = int(ms_to)
            elif ms_from is not None:
                where_msg.append("(m.timestamp_ms IS NULL OR m.timestamp_ms >= :ms_from)")
                params_f["ms_from"] = int(ms_from)
            elif ms_to is not None:
                where_msg.append("(m.timestamp_ms IS NULL OR m.timestamp_ms < :ms_to)")
                params_f["ms_to"] = int(ms_to)
            sql_conv_fb = "SELECT COUNT(DISTINCT m.conversation_id) AS c FROM message m WHERE " + " AND ".join(where_msg)
            rowfb = session.exec(text(sql_conv_fb).params(**params_f)).first()
            conv_count = int((getattr(rowfb, "c", None) if rowfb is not None else 0) or (rowfb[0] if rowfb else 0) or 0)

    return {
        "eligible_conversations": conv_count,
        "messages_in_scope": msg_count,
        "messages_without_timestamp": msg_ts_missing,
        "cutoff": cutoff_dt.isoformat(),
    }


@router.get("/run/{run_id}/logs")
def run_logs(run_id: int, limit: int = 200):
    n = int(max(1, min(limit, 2000)))
    logs = get_ai_run_logs(int(run_id), n)
    return {"logs": logs}


@router.get("/process/run/{run_id}/results")
def run_results(request: Request, run_id: int, limit: int = 200, status: str | None = None, linked: str | None = None, q: str | None = None, start: str | None = None, end: str | None = None):
    """List per-conversation results for a given AI run."""
    n = int(max(1, min(limit or 200, 1000)))
    # Build filters
    where = ["r.run_id = :rid"]
    params: dict[str, object] = {"rid": int(run_id), "lim": int(n)}
    st = (status or "").strip().lower()
    if st and st not in ("all", "*"):
        where.append("r.status = :st")
        params["st"] = st
    lk = (linked or "").strip().lower()
    if lk in ("yes", "true", "1"):
        where.append("r.linked_order_id IS NOT NULL")
    elif lk in ("no", "false", "0"):
        where.append("r.linked_order_id IS NULL")
    qq = (q or "").strip()
    if qq:
        where.append("("
                     "LOWER(COALESCE(r.convo_id,'')) LIKE :qq OR "
                     "LOWER(COALESCE(r.ai_json,'')) LIKE :qq OR "
                     "LOWER(COALESCE(u.contact_name,'')) LIKE :qq OR "
                     "COALESCE(u.contact_phone,'') LIKE :qp"
                     ")")
        params["qq"] = f"%{qq.lower()}%"
        # for phone, do not lower or strip digits only; a simple contains helps
        params["qp"] = f"%{qq}%"
    # Date filter on last_ts using HAVING after aggregation
    having: list[str] = []
    def _parse_date(s: str | None):
        try:
            return dt.date.fromisoformat(str(s)) if s else None
        except Exception:
            return None
    sd = _parse_date(start)
    ed = _parse_date(end)
    if sd:
        ms_from = int(dt.datetime.combine(sd, dt.time.min).timestamp() * 1000)
        having.append("last_ts >= :ms_from")
        params["ms_from"] = int(ms_from)
    if ed:
        ms_to = int(dt.datetime.combine(ed + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
        having.append("last_ts < :ms_to")
        params["ms_to"] = int(ms_to)
    sql = (
        "SELECT r.convo_id, r.status, r.linked_order_id, r.ai_json, r.created_at, "
        "       MAX(COALESCE(m.timestamp_ms,0)) AS last_ts, "
        "       u.contact_name, u.contact_phone "
        "FROM ig_ai_result r "
        "LEFT JOIN message m ON m.conversation_id = r.convo_id "
        "LEFT JOIN ig_users u "
        "  ON u.ig_user_id = COALESCE("
        "       CASE WHEN m.direction = 'in' THEN m.ig_sender_id ELSE m.ig_recipient_id END, "
        "       m.ig_sender_id, m.ig_recipient_id"
        "     ) "
        "WHERE " + " AND ".join(where) + " "
        "GROUP BY r.convo_id, r.status, r.linked_order_id, r.ai_json, r.created_at, "
        "         u.contact_name, u.contact_phone "
        + ("HAVING " + " AND ".join(having) + " " if having else "")
        + "ORDER BY last_ts DESC, r.convo_id DESC "
        "LIMIT :lim"
    )
    with get_session() as session:
        rows = session.exec(text(sql).params(**params)).all()
        items: list[dict] = []
        for r in rows:
            try:
                items.append({
                    "convo_id": getattr(r, "convo_id", r[0]),
                    "status": getattr(r, "status", r[1]),
                    "linked_order_id": getattr(r, "linked_order_id", r[2]),
                    "ai_json": getattr(r, "ai_json", r[3]),
                    "created_at": getattr(r, "created_at", r[4]),
                    "last_ts": getattr(r, "last_ts", r[5]),
                    "contact_name": getattr(r, "contact_name", r[6]) if len(r) > 6 else None,
                    "contact_phone": getattr(r, "contact_phone", r[7]) if len(r) > 7 else None,
                })
            except Exception:
                continue
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "ig_ai_run_results.html",
        {
            "request": request,
            "run_id": int(run_id),
            "rows": items,
            "status": status or "",
            "linked": linked or "",
            "q": q or "",
            "start": start or "",
            "end": end or "",
        },
    )


@router.get("/process/run/{run_id}/result/{convo_id}")
def run_result_detail(request: Request, run_id: int, convo_id: str, limit: int = 120):
    """Detail view for a conversation result in a given run, including recent messages and bind UI."""
    n = int(max(20, min(limit or 120, 500)))
    with get_session() as session:
        # latest result for run+convo
        row = session.exec(
            text(
                """
                SELECT id, status, ai_json, linked_order_id, created_at
                FROM ig_ai_result
                WHERE run_id=:rid AND convo_id=:cid
                ORDER BY id DESC LIMIT 1
                """
            ).params(rid=int(run_id), cid=str(convo_id))
        ).first()
        if not row:
            raise HTTPException(status_code=404, detail="Result not found for this run/conversation")
        res = {
            "id": getattr(row, "id", row[0]),
            "status": getattr(row, "status", row[1]),
            "ai_json": getattr(row, "ai_json", row[2]),
            "linked_order_id": getattr(row, "linked_order_id", row[3]),
            "created_at": getattr(row, "created_at", row[4]) if len(row) > 4 else None,
        }
        # messages (chronological)
        msgs = session.exec(
            text(
                """
                SELECT timestamp_ms, direction, text
                FROM message
                WHERE conversation_id=:cid
                ORDER BY COALESCE(timestamp_ms,0) ASC, id ASC
                LIMIT :lim
                """
            ).params(cid=str(convo_id), lim=int(n))
        ).all()
        messages: list[dict] = []
        for m in msgs:
            try:
                ts_ms = getattr(m, "timestamp_ms", m[0])
                ts_h = None
                try:
                    if ts_ms and int(ts_ms) > 0:
                        from datetime import datetime as _dt
                        ts_h = _dt.utcfromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    ts_h = None
                messages.append({
                    "timestamp_ms": ts_ms,
                    "ts": ts_h,
                    "direction": getattr(m, "direction", m[1]),
                    "text": getattr(m, "text", m[2]),
                })
            except Exception:
                continue
        # contact info (from ig_users via latest message for this conversation)
        contact = {}
        try:
            rowu = session.exec(
                text(
                    """
                    SELECT
                      CASE
                        WHEN m.direction = 'in' THEN m.ig_sender_id
                        ELSE m.ig_recipient_id
                      END AS ig_user_id
                    FROM message m
                    WHERE m.conversation_id=:cid
                    ORDER BY COALESCE(m.timestamp_ms,0) DESC, m.id DESC
                    LIMIT 1
                    """
                ).params(cid=str(convo_id))
            ).first()
            ig_user_id = rowu.ig_user_id if rowu and hasattr(rowu, "ig_user_id") else (rowu[0] if rowu else None)
            if ig_user_id:
                rc = session.exec(
                    text(
                        "SELECT contact_name, contact_phone, contact_address "
                        "FROM ig_users WHERE ig_user_id=:uid LIMIT 1"
                    ).params(uid=str(ig_user_id))
                ).first()
                if rc:
                    contact = {
                        "name": getattr(rc, "contact_name", rc[0]) or None,
                        "phone": getattr(rc, "contact_phone", rc[1]) or None,
                        "address": getattr(rc, "contact_address", rc[2]) or None,
                    }
        except Exception:
            contact = {}
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "ig_ai_run_result_detail.html",
        {
            "request": request,
            "run_id": int(run_id),
            "convo_id": str(convo_id),
            "result": res,
            "messages": messages,
            "contact": contact,
        },
    )


@router.get("/link-suggest")
def link_suggest_page(request: Request, start: str | None = None, end: str | None = None, limit: int = 200):
    """Suggest linking orders to IG conversations by matching client phone in conversations/messages.

    - Only orders without ig_conversation_id
    - Default date window: last 7 days
    - Date criterion: shipment_date if present else data_date
    """
    n = int(max(1, min(limit or 200, 1000)))
    def _parse_date(s: str | None) -> dt.date | None:
        try:
            return dt.date.fromisoformat(str(s)) if s else None
        except Exception:
            return None
    today = dt.date.today()
    start_d = _parse_date(start) or (today - dt.timedelta(days=7))
    end_d = _parse_date(end) or today
    if end_d < start_d:
        start_d, end_d = end_d, start_d
    from ..models import Order, Client, Item
    with get_session() as session:
        # Orders without link in date window
        rows = session.exec(
            select(Order, Client, Item)
            .where(Order.ig_conversation_id.is_(None))
            .where(
                (
                    ((Order.shipment_date.is_not(None)) & (Order.shipment_date >= start_d) & (Order.shipment_date <= end_d))
                    | ((Order.shipment_date.is_(None)) & (Order.data_date.is_not(None)) & (Order.data_date >= start_d) & (Order.data_date <= end_d))
                )
            )
            .where(Order.client_id == Client.id)
            .where((Order.item_id.is_(None)) | (Order.item_id == Item.id))
            .order_by(Order.id.desc())
            .limit(n)
        ).all()
        suggestions: list[dict] = []
        for o, c, it in rows:
            phone = (c.phone or "").strip() if c.phone else ""
            # normalize digits, prefer last 10
            digits = "".join([ch for ch in phone if ch.isdigit()])
            last10 = digits[-10:] if len(digits) >= 10 else digits
            convo_id = None
            msg_preview = None
            # Try ig_users.contact_phone first (via messages to identify ig_user_id)
            if last10:
                try:
                    rowc = session.exec(
                        text(
                            """
                            SELECT m.conversation_id, u.contact_phone
                            FROM message m
                            JOIN ig_users u
                              ON u.ig_user_id = COALESCE(
                                   CASE WHEN m.direction='in' THEN m.ig_sender_id ELSE m.ig_recipient_id END,
                                   m.ig_sender_id, m.ig_recipient_id
                                 )
                            WHERE u.contact_phone IS NOT NULL
                              AND REPLACE(REPLACE(REPLACE(u.contact_phone,' ',''),'-',''),'+','') LIKE :p
                            ORDER BY COALESCE(m.timestamp_ms,0) DESC, m.id DESC
                            LIMIT 1
                            """
                        ).params(p=f\"%{last10}%\")
                    ).first()
                    if rowc:
                        convo_id = rowc.conversation_id if hasattr(rowc, \"conversation_id\") else rowc[0]
                except Exception:
                    convo_id = None
            # Fallback: search messages.text for digits
            if (convo_id is None) and last10:
                try:
                    rowm = session.exec(
                        text(
                            """
                            SELECT conversation_id, text, timestamp_ms
                            FROM message
                            WHERE text IS NOT NULL AND REPLACE(REPLACE(REPLACE(text,' ',''),'-',''),'+','') LIKE :p
                            ORDER BY COALESCE(timestamp_ms,0) DESC LIMIT 1
                            """
                        ).params(p=f"%{last10}%")
                    ).first()
                    if rowm:
                        convo_id = rowm.conversation_id if hasattr(rowm, "conversation_id") else rowm[0]
                        msg_preview = rowm.text if hasattr(rowm, "text") else (rowm[1] if len(rowm) > 1 else None)
                except Exception:
                    pass
            suggestions.append({
                "order_id": int(o.id or 0),
                "client_id": int(c.id or 0),
                "client_name": c.name,
                "client_phone": phone,
                "item_name": (it.name if it else None),
                "total": float(o.total_amount or 0.0) if o.total_amount is not None else None,
                "date": (o.shipment_date or o.data_date),
                "convo_id": convo_id,
                "msg_preview": msg_preview,
            })
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "ig_link_suggest.html",
        {"request": request, "rows": suggestions, "start": start_d, "end": end_d},
    )


@router.post("/link-suggest/apply")
async def link_suggest_apply(request: Request):
    """Apply linking selections from the UI."""
    form = await request.form()
    # Expect arrays: sel[] (order_id strings), and conv[order_id]=convo_id
    selected = form.getlist("sel[]") if hasattr(form, "getlist") else []
    # Build conv map
    conv_map: dict[int, str] = {}
    try:
        for k, v in form.multi_items():  # type: ignore[attr-defined]
            if str(k).startswith("conv[") and str(k).endswith("]"):
                try:
                    oid = int(str(k)[5:-1])
                    conv_map[oid] = str(v)
                except Exception:
                    continue
    except Exception:
        # fallback: scan all keys
        for k in form.keys():  # type: ignore
            if str(k).startswith("conv[") and str(k).endswith("]"):
                try:
                    val = form.get(k)  # type: ignore
                    oid = int(str(k)[5:-1])
                    conv_map[oid] = str(val or "")
                except Exception:
                    continue
    updated = 0
    with get_session() as session:
        from ..models import Order
        for s in selected:
            try:
                oid = int(s)
            except Exception:
                continue
            cv = conv_map.get(oid)
            if not cv:
                continue
            o = session.exec(select(Order).where(Order.id == oid)).first()
            if not o:
                continue
            # Resolve ig_user_id for this conversation from latest message
            ig_user_id = None
            try:
                rowu = session.exec(
                    text(
                        """
                        SELECT
                          CASE
                            WHEN m.direction = 'in' THEN m.ig_sender_id
                            ELSE m.ig_recipient_id
                          END AS ig_user_id
                        FROM message m
                        WHERE m.conversation_id=:cid
                        ORDER BY COALESCE(m.timestamp_ms,0) DESC, m.id DESC
                        LIMIT 1
                        """
                    ).params(cid=str(cv))
                ).first()
                ig_user_id = rowu.ig_user_id if rowu and hasattr(rowu, "ig_user_id") else (rowu[0] if rowu else None)
            except Exception:
                ig_user_id = None
            if ig_user_id:
                try:
                    session.exec(
                        text(
                            "UPDATE ig_users SET linked_order_id=:oid "
                            "WHERE ig_user_id=:uid AND linked_order_id IS NULL"
                        ).params(oid=int(oid), uid=str(ig_user_id))
                    )
                except Exception:
                    pass
            if not o.ig_conversation_id:
                o.ig_conversation_id = str(cv)
                session.add(o)
            updated += 1
    return {"status": "ok", "linked": updated}


@router.get("/unlinked")
def unlinked_purchases(request: Request, q: str | None = None, start: str | None = None, end: str | None = None, limit: int = 200):
    """List conversations where a purchase was detected but not linked to an order.

    Uses ig_users AI status and orders by latest message timestamp.
    """
    n = int(max(1, min(limit or 200, 1000)))
    where = ["(u.ai_status = 'ambiguous' OR u.ai_status IS NULL)"]
    params: dict[str, object] = {}
    # Parse start/end as dates and convert to ms window over last_ts
    def _parse_date(s: str | None):
        try:
            return dt.date.fromisoformat(str(s)) if s else None
        except Exception:
            return None
    start_d = _parse_date(start)
    end_d = _parse_date(end)
    ms_from = None
    ms_to = None
    if start_d:
        ms_from = int(dt.datetime.combine(start_d, dt.time.min).timestamp() * 1000)
    if end_d:
        ms_to = int(dt.datetime.combine(end_d + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
    if q:
        # Search in contact fields (when present) or AI JSON (on ig_users)
        where.append("(LOWER(COALESCE(u.contact_name,'')) LIKE :qq OR COALESCE(u.contact_phone,'') LIKE :qp OR LOWER(COALESCE(u.ai_json,'')) LIKE :qa)")
        qs = f\"%{q.lower()}%\"
        params.update({\"qq\": qs, \"qa\": qs, \"qp\": f\"%{q}%\"} )
    # Build via subquery to filter on last_ts window
    sql = (
        "SELECT t.convo_id, u.contact_name, u.contact_phone, u.contact_address, "
        "       u.ai_status, u.ai_json, NULL AS linked_order_id, t.last_ts "
        "FROM ("
        "  SELECT m.conversation_id AS convo_id, "
        "         MAX(COALESCE(m.timestamp_ms,0)) AS last_ts, "
        "         MAX(CASE WHEN m.direction='in' THEN m.ig_sender_id ELSE m.ig_recipient_id END) AS ig_user_id "
        "  FROM message m "
        "  GROUP BY m.conversation_id"
        ") t "
        "JOIN ig_users u ON u.ig_user_id = t.ig_user_id "
        "WHERE " + " AND ".join(where)
        + (" AND t.last_ts >= :ms_from" if ms_from is not None else "")
        + (" AND t.last_ts < :ms_to" if ms_to is not None else "")
        + " ORDER BY t.last_ts DESC, t.convo_id DESC "
        "LIMIT :lim"
    )
    params["lim"] = int(n)
    if ms_from is not None:
        params["ms_from"] = int(ms_from)
    if ms_to is not None:
        params["ms_to"] = int(ms_to)
    with get_session() as session:
        rows = session.exec(text(sql).params(**params)).all()
        items = []
        for r in rows:
            try:
                convo_id = getattr(r, "convo_id", r[0])
                contact_name = getattr(r, "contact_name", None if len(r) < 2 else r[1])
                contact_phone = getattr(r, "contact_phone", None if len(r) < 3 else r[2])
                contact_address = getattr(r, "contact_address", None if len(r) < 4 else r[3])
                ai_status = getattr(r, "ai_status", None if len(r) < 5 else r[4])
                ai_json = getattr(r, "ai_json", None if len(r) < 6 else r[5])
                last_ts = getattr(r, "last_ts", None if len(r) < 8 else r[7])
                # Fallback contact info from AI JSON if conversations row is missing/empty
                if (not contact_name or not contact_phone or not contact_address) and ai_json:
                    try:
                        data = __import__("json").loads(ai_json)
                        if isinstance(data, dict):
                            contact_name = contact_name or data.get("buyer_name")
                            contact_phone = contact_phone or data.get("phone")
                            contact_address = contact_address or data.get("address")
                    except Exception:
                        pass
                # Convert last_ts ms to ISO string
                last_dt = None
                try:
                    if last_ts and int(last_ts) > 0:
                        last_dt = __import__("datetime").datetime.utcfromtimestamp(int(last_ts) / 1000).isoformat()
                except Exception:
                    last_dt = None
                items.append({
                    "convo_id": convo_id,
                    "contact_name": contact_name,
                    "contact_phone": contact_phone,
                    "contact_address": contact_address,
                    "ai_status": ai_status,
                    "ai_json": ai_json,
                    "linked_order_id": None,
                    "last_message_at": last_dt,
                })
            except Exception:
                continue
    templates = request.app.state.templates
    return templates.TemplateResponse("ig_ai_unlinked.html", {"request": request, "rows": items, "q": q or "", "start": start or "", "end": end or ""})


@router.get("/unlinked/search")
def search_orders(q: str, limit: int = 20):
    """Search orders by client name or phone digits."""
    if not q or not isinstance(q, str):
        raise HTTPException(status_code=400, detail="q required")
    q = q.strip()
    from ..models import Client, Order
    with get_session() as session:
        # Normalize phone digits
        phone_digits = "".join([c for c in q if c.isdigit()])
        # Build base query
        qry = select(Order, Client).where(Order.client_id == Client.id)
        if phone_digits:
            qry = qry.where((Client.phone.is_not(None)) & (Client.phone.contains(phone_digits)))
        else:
            from sqlalchemy import func as _func
            qry = qry.where(_func.lower(Client.name).like(f"%{q.lower()}%"))
        rows = session.exec(qry.order_by(Order.id.desc()).limit(max(1, min(limit, 50)))).all()
        out = []
        for o, c in rows:
            out.append({
                "order_id": int(o.id or 0),
                "client_id": int(c.id or 0),
                "client_name": c.name,
                "client_phone": c.phone,
                "total": float(o.total_amount or 0.0) if o.total_amount is not None else None,
                "shipment_date": o.shipment_date.isoformat() if o.shipment_date else None,
                "data_date": o.data_date.isoformat() if o.data_date else None,
                "source": o.source,
            })
        return {"results": out}


@router.post("/unlinked/bind")
def bind_conversation(body: dict):
    """Bind a conversation to an order: sets conversations.linked_order_id and order.ig_conversation_id if empty."""
    convo_id = (body or {}).get("conversation_id")
    order_id = (body or {}).get("order_id")
    if not convo_id or not order_id:
        raise HTTPException(status_code=400, detail="conversation_id and order_id required")
    try:
        oid = int(order_id)
    except Exception:
        raise HTTPException(status_code=400, detail="order_id must be integer")
    with get_session() as session:
        # Ensure order exists
        from ..models import Order
        row = session.exec(select(Order).where(Order.id == oid)).first()
        if not row:
            raise HTTPException(status_code=404, detail="Order not found")
        # Update conversations and order
        try:
            session.exec(text("UPDATE conversations SET linked_order_id=:oid WHERE convo_id=:cid").params(oid=oid, cid=str(convo_id)))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"link_failed: {e}")
        # Reflect link in ai_conversations as well
        try:
            session.exec(text("UPDATE ai_conversations SET linked_order_id=:oid WHERE convo_id=:cid").params(oid=oid, cid=str(convo_id)))
        except Exception:
            pass
        try:
            if not row.ig_conversation_id:
                row.ig_conversation_id = str(convo_id)
                session.add(row)
        except Exception:
            pass
    return {"status": "ok"}


@router.post("/unlinked/mark")
def mark_unlinked(body: dict):
    """Mark an unlinked conversation's AI status (e.g., no_purchase) so it no longer appears in the list."""
    convo_id = (body or {}).get("conversation_id")
    status = (body or {}).get("status") or "no_purchase"
    allowed = {"no_purchase", "ambiguous", "ok"}
    if not convo_id:
        raise HTTPException(status_code=400, detail="conversation_id required")
    if status not in allowed:
        raise HTTPException(status_code=400, detail=f"invalid status; allowed: {', '.join(sorted(allowed))}")
    with get_session() as session:
        # Resolve ig_user_id for this conversation and update ig_users.ai_status
        ig_user_id = None
        try:
            rowu = session.exec(
                text(
                    """
                    SELECT
                      CASE
                        WHEN m.direction = 'in' THEN m.ig_sender_id
                        ELSE m.ig_recipient_id
                      END AS ig_user_id
                    FROM message m
                    WHERE m.conversation_id=:cid
                    ORDER BY COALESCE(m.timestamp_ms,0) DESC, m.id DESC
                    LIMIT 1
                    """
                ).params(cid=str(convo_id))
            ).first()
            ig_user_id = rowu.ig_user_id if rowu and hasattr(rowu, "ig_user_id") else (rowu[0] if rowu else None)
        except Exception:
            ig_user_id = None
        if ig_user_id:
            try:
                session.exec(
                    text(
                        "UPDATE ig_users SET ai_status=:st WHERE ig_user_id=:uid"
                    ).params(st=status, uid=str(ig_user_id))
                )
            except Exception:
                pass
    return {"status": "ok"}


