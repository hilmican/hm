from __future__ import annotations

import datetime as dt
from typing import Optional

from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import text

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
        # Conversations count with same eligibility as processor
        where = ["ai_processed_at IS NULL", "last_message_at <= :cutoff"]
        params: dict[str, object] = {"cutoff": cutoff_dt.isoformat(" ")}
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
        sql_conv = "SELECT COUNT(1) AS c FROM conversations WHERE " + " AND ".join(where)
        rowc = session.exec(text(sql_conv).params(**params)).first()
        conv_count = int((getattr(rowc, "c", None) if rowc is not None else 0) or (rowc[0] if rowc else 0) or 0)

        # Messages count by timestamp window only (robust even if conversation links are missing)
        msg_where = ["(m.timestamp_ms IS NULL OR m.timestamp_ms <= :cutoff_ms)"]
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
            "SELECT COUNT(1) AS mc, SUM(CASE WHEN m.timestamp_ms IS NULL THEN 1 ELSE 0 END) AS mt0 FROM message m WHERE "
            + " AND ".join(msg_where)
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


