from __future__ import annotations

import datetime as dt
from typing import Optional

from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import text

from ..db import get_session
from ..services.queue import enqueue


router = APIRouter(prefix="/ig/ai", tags=["instagram-ai"])


@router.get("/process")
def process_page(request: Request):
    # Load last runs for summary
    with get_session() as session:
        rows = session.exec(text("""
            SELECT id, started_at, completed_at, date_from, date_to, min_age_minutes,
                   conversations_considered, conversations_processed, orders_linked,
                   purchases_detected, purchases_unlinked
            FROM ig_ai_run ORDER BY id DESC LIMIT 20
        """)).all()
        runs = []
        for r in rows:
            runs.append({
                "id": getattr(r, "id", r[0]),
                "started_at": getattr(r, "started_at", r[1]),
                "completed_at": getattr(r, "completed_at", r[2]),
                "date_from": getattr(r, "date_from", r[3]),
                "date_to": getattr(r, "date_to", r[4]),
                "min_age_minutes": getattr(r, "min_age_minutes", r[5]),
                "conversations_considered": getattr(r, "conversations_considered", r[6]),
                "conversations_processed": getattr(r, "conversations_processed", r[7]),
                "orders_linked": getattr(r, "orders_linked", r[8]),
                "purchases_detected": getattr(r, "purchases_detected", r[9]),
                "purchases_unlinked": getattr(r, "purchases_unlinked", r[10]),
            })
    templates = request.app.state.templates
    return templates.TemplateResponse("ig_ai_process.html", {"request": request, "runs": runs})


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
        row = session.exec(text("""
            INSERT INTO ig_ai_run(started_at, date_from, date_to, min_age_minutes)
            VALUES (CURRENT_TIMESTAMP, :df, :dt, :age)
        """)).params(df=date_from.isoformat() if date_from else None, dt=date_to.isoformat() if date_to else None, age=min_age_minutes)
        rid_row = session.exec(text("SELECT last_insert_rowid() AS id")).first()
        if not rid_row:
            raise HTTPException(status_code=500, detail="Could not create run")
        run_id = int(getattr(rid_row, "id", rid_row[0]))

    # Enqueue background job to process
    enqueue("ig_ai_process_run", key=str(run_id), payload={
        "run_id": run_id,
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,
        "min_age_minutes": min_age_minutes,
        "limit": limit,
    })
    return {"status": "ok", "run_id": run_id}


@router.get("/process/runs")
def list_runs(limit: int = 50):
    with get_session() as session:
        rows = session.exec(text("""
            SELECT id, started_at, completed_at, date_from, date_to, min_age_minutes,
                   conversations_considered, conversations_processed, orders_linked,
                   purchases_detected, purchases_unlinked, errors_json
            FROM ig_ai_run ORDER BY id DESC LIMIT :n
        """)).params(n=int(max(1, min(limit, 200)))).all()
        out = []
        for r in rows:
            out.append({
                "id": getattr(r, "id", r[0]),
                "started_at": getattr(r, "started_at", r[1]),
                "completed_at": getattr(r, "completed_at", r[2]),
                "date_from": getattr(r, "date_from", r[3]),
                "date_to": getattr(r, "date_to", r[4]),
                "min_age_minutes": getattr(r, "min_age_minutes", r[5]),
                "conversations_considered": getattr(r, "conversations_considered", r[6]),
                "conversations_processed": getattr(r, "conversations_processed", r[7]),
                "orders_linked": getattr(r, "orders_linked", r[8]),
                "purchases_detected": getattr(r, "purchases_detected", r[9]),
                "purchases_unlinked": getattr(r, "purchases_unlinked", r[10]),
                "errors_json": getattr(r, "errors_json", r[11]),
            })
        return {"runs": out}


@router.get("/process/run/{run_id}")
def run_details(run_id: int):
    with get_session() as session:
        row = session.exec(text("""
            SELECT id, started_at, completed_at, date_from, date_to, min_age_minutes,
                   conversations_considered, conversations_processed, orders_linked,
                   purchases_detected, purchases_unlinked, errors_json
            FROM ig_ai_run WHERE id = :id
        """)).params(id=run_id).first()
        if not row:
            raise HTTPException(status_code=404, detail="Run not found")
        return {
            "id": getattr(row, "id", row[0]),
            "started_at": getattr(row, "started_at", row[1]),
            "completed_at": getattr(row, "completed_at", row[2]),
            "date_from": getattr(row, "date_from", row[3]),
            "date_to": getattr(row, "date_to", row[4]),
            "min_age_minutes": getattr(row, "min_age_minutes", row[5]),
            "conversations_considered": getattr(row, "conversations_considered", row[6]),
            "conversations_processed": getattr(row, "conversations_processed", row[7]),
            "orders_linked": getattr(row, "orders_linked", row[8]),
            "purchases_detected": getattr(row, "purchases_detected", row[9]),
            "purchases_unlinked": getattr(row, "purchases_unlinked", row[10]),
            "errors_json": getattr(row, "errors_json", row[11]),
        }


