from fastapi import APIRouter, Query, Request
from sqlmodel import select

from ..db import get_session
from ..models import Order, Payment

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
					"shipment_date": o.shipment_date.isoformat() if o.shipment_date else None,
                    "data_date": o.data_date.isoformat() if o.data_date else None,
					"source": o.source,
				}
				for o in rows
			]
		}


@router.get("/table")
def list_orders_table(request: Request, limit: int = Query(default=100, ge=1, le=2000)):
    with get_session() as session:
        rows = session.exec(select(Order).order_by(Order.id.desc()).limit(limit)).all()
        # build simple maps for names
        client_ids = sorted({o.client_id for o in rows if o.client_id})
        item_ids = sorted({o.item_id for o in rows if o.item_id})
        from ..models import Client, Item  # local import to avoid circulars
        clients = session.exec(select(Client).where(Client.id.in_(client_ids))).all() if client_ids else []
        items = session.exec(select(Item).where(Item.id.in_(item_ids))).all() if item_ids else []
        client_map = {c.id: c.name for c in clients if c.id is not None}
        item_map = {it.id: it.name for it in items if it.id is not None}
        # compute paid/unpaid using net amounts
        order_ids = [o.id for o in rows if o.id]
        pays = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all() if order_ids else []
        paid_map: dict[int, float] = {}
        for p in pays:
            if p.order_id is None:
                continue
            paid_map[p.order_id] = paid_map.get(p.order_id, 0.0) + float(p.net_amount or 0.0)
        status_map: dict[int, str] = {}
        for o in rows:
            oid = o.id or 0
            total = float(o.total_amount or 0.0)
            net = paid_map.get(oid, 0.0)
            status_map[oid] = "paid" if (net > 0 and net >= total) else "unpaid"
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "orders_table.html",
            {"request": request, "rows": rows, "limit": limit, "client_map": client_map, "item_map": item_map, "status_map": status_map},
        )
