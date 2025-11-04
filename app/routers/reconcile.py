from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from sqlmodel import select
import json
import datetime as dt

from ..db import get_session
from ..models import ReconcileTask, ImportRow, Order, OrderItem, Item
from ..services.inventory import adjust_stock

router = APIRouter()


@router.get("/queue")
def get_queue():
    with get_session() as session:
        tasks = session.exec(select(ReconcileTask).where(ReconcileTask.resolved_at == None).order_by(ReconcileTask.id.desc())).all()
        return {
            "tasks": [
                {"id": t.id, "import_row_id": t.import_row_id, "candidates": json.loads(t.candidates_json or "[]"), "chosen_id": t.chosen_id}
                for t in tasks
            ]
        }


@router.get("/returns")
def list_returns_tasks(request: Request):
    with get_session() as session:
        tasks = session.exec(select(ReconcileTask).where(ReconcileTask.resolved_at == None).order_by(ReconcileTask.id.desc())).all()
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "reconcile_returns.html",
            {"request": request, "tasks": tasks},
        )


@router.post("/returns/{task_id}/choose")
def choose_return(task_id: int, body: dict):
    """Resolve a returns reconcile task by choosing an order id.

    Applies returns logic: restock items, set status/date, set total_amount from returns amount.
    """
    chosen_id = body.get("order_id")
    if not chosen_id:
        raise HTTPException(status_code=400, detail="order_id required")
    with get_session() as session:
        task = session.exec(select(ReconcileTask).where(ReconcileTask.id == task_id)).first()
        if not task:
            raise HTTPException(status_code=404, detail="task not found")
        ir = session.exec(select(ImportRow).where(ImportRow.id == task.import_row_id)).first()
        if not ir:
            raise HTTPException(status_code=404, detail="import row not found")
        try:
            rec = eval(ir.mapped_json) if ir.mapped_json else {}
        except Exception:
            rec = {}
        order = session.exec(select(Order).where(Order.id == int(chosen_id))).first()
        if not order:
            raise HTTPException(status_code=404, detail="order not found")
        # idempotency guards
        if (order.status or "") in ("refunded", "switched", "stitched"):
            task.chosen_id = int(chosen_id)
            task.resolved_at = dt.datetime.utcnow()
            return {"status": "ok", "message": "already_processed"}
        # restock linked items
        oitems = session.exec(select(OrderItem).where(OrderItem.order_id == order.id)).all()
        for oi in oitems:
            if oi.item_id is None:
                continue
            qty = int(oi.quantity or 0)
            if qty > 0:
                adjust_stock(session, item_id=int(oi.item_id), delta=qty, related_order_id=order.id)
        # set status/date and top-level amount
        action = (rec.get("action") or "").strip()
        if action == "refund":
            order.status = "refunded"
        elif action == "switch":
            order.status = "switched"
        # date
        rdate = rec.get("date")
        try:
            if rdate and not isinstance(rdate, dt.date):
                import datetime as _dt
                # handle iso string fallback
                rdate = _dt.date.fromisoformat(str(rdate))
        except Exception:
            rdate = None
        order.return_or_switch_date = rdate or order.return_or_switch_date or dt.date.today()
        # amount to Toplam
        try:
            amt = float(rec.get("amount") or 0.0)
            order.total_amount = round(amt, 2)
        except Exception:
            pass
        # resolve
        task.chosen_id = int(chosen_id)
        task.resolved_at = dt.datetime.utcnow()
        return {"status": "ok", "order_id": order.id}
