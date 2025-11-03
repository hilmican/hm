from fastapi import APIRouter, Query, Request, HTTPException
from sqlmodel import select
from sqlalchemy import or_, and_
import datetime as dt
from typing import Optional

from ..db import get_session
from ..models import Order, Payment, OrderItem, Item
from ..services.inventory import adjust_stock
from ..services.shipping import compute_shipping_fee

router = APIRouter()


@router.get("")
@router.get("/")
def list_orders(limit: int = Query(default=100, ge=1, le=1000)):
	with get_session() as session:
		rows = session.exec(select(Order).order_by(Order.id.desc()).limit(limit)).all()
		return {
			"orders": [
				{
					"id": o.id or 0,
					"tracking_no": o.tracking_no,
					"client_id": o.client_id,
					"item_id": o.item_id,
					"quantity": o.quantity,
					"total_amount": o.total_amount,
					"total_cost": o.total_cost,
					"shipment_date": o.shipment_date.isoformat() if o.shipment_date else None,
                    "data_date": o.data_date.isoformat() if o.data_date else None,
					"source": o.source,
				}
				for o in rows
			]
		}


@router.get("/table")
def list_orders_table(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    date_field: str = Query(default="shipment", regex="^(shipment|data|both)$"),
    source: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
):
    def _parse_date_or_default(value: Optional[str], fallback: dt.date) -> dt.date:
        try:
            if value:
                return dt.date.fromisoformat(value)
        except Exception:
            pass
        return fallback

    with get_session() as session:
        # default to last 7 days inclusive
        today = dt.date.today()
        default_start = today - dt.timedelta(days=6)
        start_date = _parse_date_or_default(start, default_start)
        end_date = _parse_date_or_default(end, today)
        if end_date < start_date:
            start_date, end_date = end_date, start_date

        # Build base query with date filter logic similar to reports
        if date_field == "both":
            q = (
                select(Order)
                .where(
                    or_(
                        and_(Order.shipment_date.is_not(None), Order.shipment_date >= start_date, Order.shipment_date <= end_date),
                        and_(Order.data_date.is_not(None), Order.data_date >= start_date, Order.data_date <= end_date),
                    )
                )
                .order_by(Order.id.desc())
            )
        else:
            date_col = Order.shipment_date if date_field == "shipment" else Order.data_date
            alt_date_col = Order.data_date if date_field == "shipment" else Order.shipment_date
            q = (
                select(Order)
                .where(
                    or_(
                        and_(date_col.is_not(None), date_col >= start_date, date_col <= end_date),
                        and_(date_col.is_(None), alt_date_col.is_not(None), alt_date_col >= start_date, alt_date_col <= end_date),
                    )
                )
                .order_by(Order.id.desc())
            )

        # Optional source filter (bizim|kargo)
        if source in ("bizim", "kargo"):
            q = q.where(Order.source == source)

        rows = session.exec(q).all()

        # Payments and status map for paid/unpaid classification and refund/stitch display
        order_ids = [o.id for o in rows if o.id]
        pays = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all() if order_ids else []
        paid_map: dict[int, float] = {}
        for p in pays:
            if p.order_id is None:
                continue
            paid_map[p.order_id] = paid_map.get(p.order_id, 0.0) + float(p.amount or 0.0)
        status_map: dict[int, str] = {}
        for o in rows:
            oid = o.id or 0
            total = float(o.total_amount or 0.0)
            paid = paid_map.get(oid, 0.0)
        # refunded/switched take precedence for row classes
        if (o.status or "") in ("refunded", "switched", "stitched"):
                status_map[oid] = str(o.status)
            else:
                status_map[oid] = "paid" if (paid > 0 and paid >= total) else "unpaid"

        # Optional status filter
        if status in ("paid", "unpaid", "refunded", "switched"):
            rows = [o for o in rows if status_map.get(o.id or 0) == status]

        # build simple maps for names based on filtered rows
        client_ids = sorted({o.client_id for o in rows if o.client_id})
        item_ids = sorted({o.item_id for o in rows if o.item_id})
        from ..models import Client, Item  # local import to avoid circulars
        clients = session.exec(select(Client).where(Client.id.in_(client_ids))).all() if client_ids else []
        items = session.exec(select(Item).where(Item.id.in_(item_ids))).all() if item_ids else []
        client_map = {c.id: c.name for c in clients if c.id is not None}
        item_map = {it.id: it.name for it in items if it.id is not None}

        # shipping_map based on Order.total_amount for display
        shipping_map: dict[int, float] = {}
        for o in rows:
            oid = o.id or 0
            if o.shipping_fee is not None:
                shipping_map[oid] = float(o.shipping_fee or 0.0)
            else:
                amt = float(o.total_amount or 0.0)
                shipping_map[oid] = compute_shipping_fee(amt)

        # Use stored total_cost; if missing, compute via batch-loaded OrderItems and Items
        from sqlmodel import select as _select
        cost_map: dict[int, float] = {}
        for o in rows:
            if o.total_cost is not None:
                cost_map[o.id or 0] = float(o.total_cost or 0.0)
        missing_ids = [o.id for o in rows if (o.id and (o.id not in cost_map))]
        if missing_ids:
            oitems = session.exec(_select(OrderItem).where(OrderItem.order_id.in_(missing_ids))).all()
            order_item_map: dict[int, list[tuple[int, int]]] = {}
            item_ids_needed: set[int] = set()
            for oi in oitems:
                if oi.order_id is None or oi.item_id is None:
                    continue
                order_item_map.setdefault(int(oi.order_id), []).append((int(oi.item_id), int(oi.quantity or 0)))
                item_ids_needed.add(int(oi.item_id))
            item_cost_map: dict[int, float] = {}
            if item_ids_needed:
                cost_items = session.exec(_select(Item).where(Item.id.in_(sorted(item_ids_needed)))).all()
                for it in cost_items:
                    if it.id is not None:
                        item_cost_map[int(it.id)] = float(it.cost or 0.0)
            for oid in missing_ids:
                acc = 0.0
                for (iid, qty) in order_item_map.get(int(oid), []):
                    acc += float(item_cost_map.get(int(iid), 0.0)) * int(qty or 0)
                cost_map[int(oid)] = round(acc, 2)

        templates = request.app.state.templates
        return templates.TemplateResponse(
            "orders_table.html",
            {
                "request": request,
                "rows": rows,
                "client_map": client_map,
                "item_map": item_map,
                "status_map": status_map,
                "shipping_map": shipping_map,
                "cost_map": cost_map,
                # totals over filtered rows
                "sum_qty": sum(int(o.quantity or 0) for o in rows),
                "sum_total": sum(float(o.total_amount or 0.0) for o in rows),
                "sum_cost": sum(float(cost_map.get(o.id or 0, 0.0)) for o in rows),
                "sum_shipping": sum(float(shipping_map.get(o.id or 0, 0.0)) for o in rows),
                # current filters
                "start": start_date,
                "end": end_date,
                "date_field": date_field,
                "source": source,
                "status": status,
            },
        )


@router.post("/recalc-financials")
def recalc_financials():
    with get_session() as session:
        from sqlmodel import select as _select
        rows = session.exec(select(Order)).all()
        updated = 0
        for o in rows:
            # shipping from toplam; zero totals => base only
            amt = float(o.total_amount or 0.0)
            o.shipping_fee = compute_shipping_fee(amt)
            # cost from order items * item.cost
            total_cost = 0.0
            oitems = session.exec(_select(OrderItem).where(OrderItem.order_id == (o.id or 0))).all()
            for oi in oitems:
                it = session.exec(_select(Item).where(Item.id == oi.item_id)).first()
                total_cost += float(oi.quantity or 0) * float((it.cost or 0.0) if it else 0.0)
            o.total_cost = round(total_cost, 2)
            updated += 1
        return {"status": "ok", "orders_updated": updated}


@router.post("/recalc-costs")
def recalc_costs():
    with get_session() as session:
        rows = session.exec(select(Order)).all()
        from sqlmodel import select as _select
        updated = 0
        for o in rows:
            if not o.id:
                continue
            oitems = session.exec(_select(OrderItem).where(OrderItem.order_id == o.id)).all()
            total_cost = 0.0
            for oi in oitems:
                it = session.exec(_select(Item).where(Item.id == oi.item_id)).first()
                total_cost += float(oi.quantity or 0) * float((it.cost or 0.0) if it else 0.0)
            if o.total_cost != round(total_cost, 2):
                o.total_cost = round(total_cost, 2)
                updated += 1
        return {"status": "ok", "orders_updated": updated}


@router.post("/{order_id}/refund")
def refund_order(order_id: int):
    with get_session() as session:
        o = session.exec(select(Order).where(Order.id == order_id)).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")
        # idempotent: if already refunded/switched, do nothing
        if (o.status or "") in ("refunded", "switched", "stitched"):
            return {"status": "ok", "message": "already_processed"}
        # add stock back for all order items
        oitems = session.exec(select(OrderItem).where(OrderItem.order_id == order_id)).all()
        for oi in oitems:
            if oi.item_id is None:
                continue
            qty = int(oi.quantity or 0)
            if qty > 0:
                adjust_stock(session, item_id=int(oi.item_id), delta=qty, related_order_id=order_id)
        o.status = "refunded"
        o.return_or_switch_date = dt.date.today()
        return {"status": "ok"}


@router.post("/{order_id}/switch")
def switch_order(order_id: int):
    with get_session() as session:
        o = session.exec(select(Order).where(Order.id == order_id)).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")
        # idempotent: if already refunded/switched, do nothing
        if (o.status or "") in ("refunded", "switched", "stitched"):
            return {"status": "ok", "message": "already_processed"}
        # add stock back for all order items
        oitems = session.exec(select(OrderItem).where(OrderItem.order_id == order_id)).all()
        for oi in oitems:
            if oi.item_id is None:
                continue
            qty = int(oi.quantity or 0)
            if qty > 0:
                adjust_stock(session, item_id=int(oi.item_id), delta=qty, related_order_id=order_id)
        o.status = "switched"
        o.return_or_switch_date = dt.date.today()
        return {"status": "ok"}


# Backward-compatible alias: /stitch maps to switch
@router.post("/{order_id}/stitch")
def stitch_order(order_id: int):
    return switch_order(order_id)


@router.post("/{order_id}/update-total")
def update_total(order_id: int, body: dict):
    try:
        new_total_raw = body.get("total")
        new_total = float(new_total_raw)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid total")
    with get_session() as session:
        o = session.exec(select(Order).where(Order.id == order_id)).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")
        o.total_amount = round(new_total, 2)
        return {"status": "ok"}
