from fastapi import APIRouter, Request
from sqlmodel import select

from ..db import get_session
from ..models import Client, Item, Order, Payment, ImportRow, ImportRun

router = APIRouter()


@router.get("/dashboard")
def dashboard(request: Request):
	# pull small samples for quick display
	with get_session() as session:
		orders = session.exec(select(Order).order_by(Order.id.desc()).limit(20)).all()
		# fetch only the related clients/items for shown orders (no extra limits)
		client_ids = sorted({o.client_id for o in orders if o.client_id})
		item_ids = sorted({o.item_id for o in orders if o.item_id})
		clients = session.exec(select(Client).where(Client.id.in_(client_ids))).all() if client_ids else []
		items = session.exec(select(Item).where(Item.id.in_(item_ids))).all() if item_ids else []
		client_map = {c.id: c.name for c in clients if c.id is not None}
		item_map = {it.id: it.name for it in items if it.id is not None}
		payments = session.exec(select(Payment).order_by(Payment.id.desc()).limit(20)).all()

		# compute paid/unpaid flags for shown orders using net amounts
		order_ids = [o.id for o in orders if o.id]
		pays = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all() if order_ids else []
		paid_map: dict[int, float] = {}
		for p in pays:
			if p.order_id is None:
				continue
			paid_map[p.order_id] = paid_map.get(p.order_id, 0.0) + float(p.net_amount or 0.0)
		status_map: dict[int, str] = {}
		for o in orders:
			oid = o.id or 0
			total = float(o.total_amount or 0.0)
			net = paid_map.get(oid, 0.0)
			status_map[oid] = "paid" if (net > 0 and net >= total) else "unpaid"

		# Build source filename mappings using ImportRow -> ImportRun
		client_ids = [c.id for c in clients if c.id]
		order_ids = [o.id for o in orders if o.id]
		client_rows = session.exec(select(ImportRow).where(ImportRow.matched_client_id.in_(client_ids))).all() if client_ids else []
		order_rows = session.exec(select(ImportRow).where(ImportRow.matched_order_id.in_(order_ids))).all() if order_ids else []
		run_ids = {r.import_run_id for r in client_rows + order_rows}
		runs = session.exec(select(ImportRun).where(ImportRun.id.in_(run_ids))).all() if run_ids else []
		run_id_to_filename = {r.id: r.filename for r in runs if r.id is not None}

		client_sources: dict[int, str] = {}
		for r in client_rows:
			if r.matched_client_id:
				client_sources[r.matched_client_id] = run_id_to_filename.get(r.import_run_id, "")

		order_sources: dict[int, str] = {}
		for r in order_rows:
			if r.matched_order_id:
				order_sources[r.matched_order_id] = run_id_to_filename.get(r.import_run_id, "")

		item_sources: dict[int, str] = {}
		for o in orders:
			if o.item_id and o.id and o.id in order_sources:
				item_sources[o.item_id] = order_sources[o.id]

		templates = request.app.state.templates
		return templates.TemplateResponse(
			"dashboard.html",
			{
				"request": request,
				"clients": clients,
				"items": items,
				"orders": orders,
				"payments": payments,
				"client_map": client_map,
				"item_map": item_map,
				"client_sources": client_sources,
				"order_sources": order_sources,
				"item_sources": item_sources,
				"status_map": status_map,
			},
		)
