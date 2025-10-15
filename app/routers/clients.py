from fastapi import APIRouter, Query, Request
from sqlmodel import select
from sqlalchemy import func

from ..db import get_session
from ..models import Client, Order, Item

router = APIRouter()


@router.get("")
@router.get("/")
def list_clients(limit: int = Query(default=100, ge=1, le=1000)):
	with get_session() as session:
		rows = session.exec(select(Client).order_by(Client.id.desc()).limit(limit)).all()
		return {
			"clients": [
				{
					"id": c.id or 0,
					"name": c.name,
					"phone": c.phone,
					"address": c.address,
					"city": c.city,
					"created_at": c.created_at.isoformat(),
				}
				for c in rows
			]
		}


@router.get("/table")
def list_clients_table(request: Request):
	with get_session() as session:
		rows = session.exec(select(Client).order_by(Client.id.desc())).all()
		# order counts per client
		counts = session.exec(select(Order.client_id, func.count().label("cnt")).group_by(Order.client_id)).all()
		order_counts = {cid: cnt for cid, cnt in counts}
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"clients_table.html",
			{"request": request, "rows": rows, "order_counts": order_counts},
		)


@router.get("/{client_id}")
def client_detail(client_id: int, request: Request):
	with get_session() as session:
		client = session.get(Client, client_id)
		if not client:
			from fastapi import HTTPException
			raise HTTPException(status_code=404, detail="Client not found")
		orders = session.exec(select(Order).where(Order.client_id == client_id).order_by(Order.id.desc())).all()
		item_ids = [o.item_id for o in orders if o.item_id]
		items = session.exec(select(Item).where(Item.id.in_(item_ids))).all() if item_ids else []
		item_map = {it.id: it for it in items if it.id is not None}
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"client_detail.html",
			{"request": request, "client": client, "orders": orders, "item_map": item_map},
		)
