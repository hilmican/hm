from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from sqlmodel import select
import json
import datetime as dt

from ..db import get_session
from ..models import ReconcileTask, ImportRow, Order, OrderItem, Item, ImportRun, Client, Payment
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
def review_returns(request: Request, run_id: int | None = None):
    """Interactive review panel: list unmatched returns rows for a run and allow choosing orders.

    If run_id is not given, picks the latest ImportRun with source='returns'.
    """
    with get_session() as session:
        if run_id is None:
            run = session.exec(select(ImportRun).where(ImportRun.source == "returns").order_by(ImportRun.id.desc())).first()
            if not run:
                rows = []
                run_id_val = None
            else:
                run_id_val = run.id
                rows = session.exec(select(ImportRow).where(ImportRow.import_run_id == run.id, ImportRow.status == "unmatched")).all()
        else:
            run_id_val = run_id
            rows = session.exec(select(ImportRow).where(ImportRow.import_run_id == run_id, ImportRow.status == "unmatched")).all()
        # build candidate orders per row (by matched_client_id)
        data: list[dict] = []
        for ir in rows:
            try:
                rec = eval(ir.mapped_json) if ir.mapped_json else {}
            except Exception:
                rec = {}
            cid = ir.matched_client_id
            cands: list[dict] = []
            if cid:
                order_rows = session.exec(select(Order).where(Order.client_id == cid).order_by(Order.id.desc())).all()
                for o in order_rows[:20]:
                    itname = None
                    if o.item_id:
                        itobj = session.exec(select(Item).where(Item.id == o.item_id)).first()
                        itname = itobj.name if itobj else None
                    cands.append({
                        "id": o.id,
                        "date": str(o.shipment_date or o.data_date),
                        "status": o.status,
                        "total": float(o.total_amount or 0.0),
                        "item_name": itname,
                    })
            data.append({
                "import_row_id": ir.id,
                "record": rec,
                "candidates": cands,
            })
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "reconcile_returns.html",
            {"request": request, "rows": data, "run_id": run_id_val},
        )


@router.post("/returns/apply")
def apply_returns_choices(body: dict):
    """Apply chosen orders for unmatched returns rows.

    body: { selections: [{ import_row_id, order_id }] }
    """
    sels = body.get("selections") or []
    if not isinstance(sels, list) or not sels:
        raise HTTPException(status_code=400, detail="selections[] required")
    updated = 0
    with get_session() as session:
        for sel in sels:
            try:
                ir_id = int(sel.get("import_row_id"))
                order_id = int(sel.get("order_id"))
            except Exception:
                continue
            ir = session.exec(select(ImportRow).where(ImportRow.id == ir_id)).first()
            if not ir:
                continue
            try:
                rec = eval(ir.mapped_json) if ir.mapped_json else {}
            except Exception:
                rec = {}
            order = session.exec(select(Order).where(Order.id == order_id)).first()
            if not order:
                continue
            if (order.status or "") in ("refunded", "switched", "stitched"):
                continue
            # restock items
            oitems = session.exec(select(OrderItem).where(OrderItem.order_id == order.id)).all()
            for oi in oitems:
                if oi.item_id is None:
                    continue
                qty = int(oi.quantity or 0)
                if qty > 0:
                    adjust_stock(session, item_id=int(oi.item_id), delta=qty, related_order_id=order.id)
            # set status/date/amount
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
                    rdate = _dt.date.fromisoformat(str(rdate))
            except Exception:
                rdate = None
            order.return_or_switch_date = rdate or dt.date.today()
            try:
                amt = float(rec.get("amount") or 0.0)
                order.total_amount = round(amt, 2)
            except Exception:
                pass
            ir.matched_order_id = order.id
            ir.status = "updated"  # reflect resolution
            updated += 1
    return {"status": "ok", "updated": updated}


@router.get("/returns/search-orders")
def search_orders(q: str, limit: int = 20):
    """Search recent orders by client phone or name.

    - q: free text. If it contains >=3 digits, match phone contains those digits.
         Otherwise, match client name contains text (case-insensitive by SQLite collation).
    - Returns at most `limit` orders, newest first, across matched clients.
    """
    q = (q or "").strip()
    if not q or len(q) < 2:
        return {"orders": []}
    # Extract digits for phone search heuristic
    digits = "".join(ch for ch in q if ch.isdigit())
    results: list[dict] = []
    with get_session() as session:
        clients: list[Client] = []
        if digits and len(digits) >= 3:
            clients = session.exec(select(Client).where(Client.phone != None).where(Client.phone.contains(digits)).limit(50)).all()
        # If no phone hits or no significant digits, fall back to name search
        if not clients:
            clients = session.exec(select(Client).where(Client.name.contains(q)).limit(50)).all()
        if not clients:
            return {"orders": []}
        # Fetch recent orders per client until we fill limit
        remaining = max(1, min(int(limit or 20), 100))
        for c in clients:
            if remaining <= 0:
                break
            orders = session.exec(
                select(Order)
                .where(Order.client_id == c.id)
                .order_by(Order.id.desc())
                .limit(remaining)
            ).all()
            for o in orders:
                itname = None
                if o.item_id:
                    itobj = session.exec(select(Item).where(Item.id == o.item_id)).first()
                    itname = itobj.name if itobj else None
                results.append(
                    {
                        "id": o.id,
                        "date": str(o.shipment_date or o.data_date),
                        "status": o.status,
                        "total": float(o.total_amount or 0.0),
                        "item_name": itname,
                        "client_name": c.name,
                        "client_phone": c.phone,
                    }
                )
                remaining -= 1
                if remaining <= 0:
                    break
    return {"orders": results}


@router.get("/payments")
def reconcile_payments(request: Request, q: str | None = None, client_id: int | None = None, amount: float | None = None, date: str | None = None):
    """Interactive reconciliation page for mis-assigned payments between a client's orders.

    - q: free text for client search (name or phone, like orders table search)
    - client_id: direct client selection (takes precedence over q)
    - amount/date: optional hints from import pages to highlight a specific payment row
    """
    from datetime import date as _date

    with get_session() as session:
        client = None
        if client_id:
            client = session.exec(select(Client).where(Client.id == client_id)).first()
        elif q:
            q_norm = (q or "").strip()
            if q_norm:
                digits = "".join(ch for ch in q_norm if ch.isdigit())
                if digits and len(digits) >= 3:
                    client = session.exec(
                        select(Client)
                        .where(Client.phone != None)
                        .where(Client.phone.contains(digits))
                    ).first()
                if not client:
                    client = session.exec(
                        select(Client).where(Client.name.contains(q_norm))
                    ).first()

        orders: list[Order] = []
        payments: list[Payment] = []
        highlight_amount: float | None = None
        highlight_date: _date | None = None

        if client:
            orders = session.exec(
                select(Order)
                .where(Order.client_id == client.id)
                .order_by(Order.id.desc())
            ).all()
            order_ids = [o.id for o in orders if o.id]
            payments = session.exec(
                select(Payment).where(Payment.order_id.in_(order_ids))
            ).all() if order_ids else []

        try:
            if amount is not None:
                highlight_amount = float(amount)
        except Exception:
            highlight_amount = None
        try:
            if date:
                import datetime as _dt
                highlight_date = _dt.date.fromisoformat(str(date))
        except Exception:
            highlight_date = None

        # Build simple maps for template
        order_map = {o.id: o for o in orders if o.id is not None}
        rows: list[dict] = []
        for p in sorted(payments, key=lambda x: (x.date or _date.min, x.id or 0)):
            oid = p.order_id
            o = order_map.get(oid)
            rows.append(
                {
                    "payment": p,
                    "order": o,
                }
            )

        templates = request.app.state.templates
        return templates.TemplateResponse(
            "reconcile_payments.html",
            {
                "request": request,
                "client": client,
                "orders": orders,
                "rows": rows,
                "q": q or "",
                "highlight_amount": highlight_amount,
                "highlight_date": str(highlight_date) if highlight_date else None,
            },
        )


@router.post("/payments/apply")
def reconcile_payments_apply(body: dict):
    """Apply manual payment moves between a client's orders.

    body: { moves: [{ payment_id, new_order_id }] }
    """
    moves = body.get("moves") or []
    if not isinstance(moves, list) or not moves:
        raise HTTPException(status_code=400, detail="moves[] required")

    changed = 0
    with get_session() as session:
        for mv in moves:
            try:
                pid = int(mv.get("payment_id"))
                new_oid = int(mv.get("new_order_id"))
            except Exception:
                continue
            if not pid or not new_oid:
                continue
            p = session.exec(select(Payment).where(Payment.id == pid)).first()
            if not p:
                continue
            current_order = session.exec(select(Order).where(Order.id == p.order_id)).first() if p.order_id else None
            target_order = session.exec(select(Order).where(Order.id == new_oid)).first()
            if not target_order:
                continue
            # Safety: require same client when both orders exist
            if current_order and current_order.client_id and target_order.client_id and (current_order.client_id != target_order.client_id):
                continue
            p.order_id = new_oid
            changed += 1
        return {"status": "ok", "changed": changed}
