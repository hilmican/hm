from fastapi import APIRouter, Query, Request
from sqlmodel import select

from ..db import get_session
from ..models import Payment, Client, Order

router = APIRouter()


@router.get("")
@router.get("/")
def list_payments(limit: int = Query(default=100, ge=1, le=1000)):
	with get_session() as session:
		rows = session.exec(select(Payment).order_by(Payment.id.desc()).limit(limit)).all()
		return {
			"payments": [
				{
					"id": p.id or 0,
					"client_id": p.client_id,
					"order_id": p.order_id,
					"amount": p.amount,
					"date": p.date.isoformat() if p.date else None,
					"method": p.method,
				}
				for p in rows
			]
		}


@router.get("/table")
def list_payments_table(request: Request, limit: int = Query(default=1000000, ge=1, le=1000000)):
	with get_session() as session:
		rows = session.exec(select(Payment).order_by(Payment.id.desc()).limit(limit)).all()
		client_ids = sorted({p.client_id for p in rows if p.client_id})
		order_ids = sorted({p.order_id for p in rows if p.order_id})
		clients = session.exec(select(Client).where(Client.id.in_(client_ids))).all() if client_ids else []
		orders = session.exec(select(Order).where(Order.id.in_(order_ids))).all() if order_ids else []
		client_map = {c.id: c.name for c in clients if c.id is not None}
		order_map = {o.id: o.tracking_no for o in orders if o.id is not None}
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"payments_table.html",
			{"request": request, "rows": rows, "client_map": client_map, "order_map": order_map, "limit": limit},
		)