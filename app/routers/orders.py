from fastapi import APIRouter, Query, Request
from sqlmodel import select

from ..db import get_session
from ..models import Order, Payment, OrderItem, Item
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
def list_orders_table(request: Request):
    with get_session() as session:
        rows = session.exec(select(Order).order_by(Order.id.desc())).all()
        # build simple maps for names
        client_ids = sorted({o.client_id for o in rows if o.client_id})
        item_ids = sorted({o.item_id for o in rows if o.item_id})
        from ..models import Client, Item  # local import to avoid circulars
        clients = session.exec(select(Client).where(Client.id.in_(client_ids))).all() if client_ids else []
        items = session.exec(select(Item).where(Item.id.in_(item_ids))).all() if item_ids else []
        client_map = {c.id: c.name for c in clients if c.id is not None}
        item_map = {it.id: it.name for it in items if it.id is not None}
        # compute paid/unpaid using TahsilatTutari (gross collected) amounts
        order_ids = [o.id for o in rows if o.id]
        pays = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all() if order_ids else []
        paid_map: dict[int, float] = {}
        # shipping_map based on Order.total_amount for all orders (display only)
        shipping_map: dict[int, float] = {}
        for p in pays:
            if p.order_id is None:
                continue
            paid_map[p.order_id] = paid_map.get(p.order_id, 0.0) + float(p.amount or 0.0)
        # Use stored shipping_fee if present; else compute from toplam and show base for zero
        for o in rows:
            oid = o.id or 0
            if o.shipping_fee is not None:
                shipping_map[oid] = float(o.shipping_fee or 0.0)
            else:
                amt = float(o.total_amount or 0.0)
                shipping_map[oid] = compute_shipping_fee(amt)
        status_map: dict[int, str] = {}
        for o in rows:
            oid = o.id or 0
            total = float(o.total_amount or 0.0)
            paid = paid_map.get(oid, 0.0)
            status_map[oid] = "paid" if (paid > 0 and paid >= total) else "unpaid"
        # Use stored total_cost; if missing, compute on the fly for display
        from sqlmodel import select as _select
        cost_map: dict[int, float] = {}
        for o in rows:
            if o.total_cost is not None:
                cost_map[o.id or 0] = float(o.total_cost or 0.0)
            else:
                if not o.id:
                    continue
                oitems = session.exec(_select(OrderItem).where(OrderItem.order_id == o.id)).all()
                total_cost = 0.0
                for oi in oitems:
                    it = session.exec(_select(Item).where(Item.id == oi.item_id)).first()
                    total_cost += float(oi.quantity or 0) * float((it.cost or 0.0) if it else 0.0)
                cost_map[o.id] = round(total_cost, 2)
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "orders_table.html",
            {"request": request, "rows": rows, "client_map": client_map, "item_map": item_map, "status_map": status_map, "shipping_map": shipping_map, "cost_map": cost_map,
            "sum_qty": sum(int(o.quantity or 0) for o in rows),
            "sum_total": sum(float(o.total_amount or 0.0) for o in rows),
            "sum_cost": sum(float(cost_map.get(o.id or 0, 0.0)) for o in rows),
            "sum_shipping": sum(float(shipping_map.get(o.id or 0, 0.0)) for o in rows)},
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
