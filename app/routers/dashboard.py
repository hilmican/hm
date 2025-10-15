from fastapi import APIRouter, Request
from sqlmodel import select

from ..db import get_session
from ..models import Client, Item, Order, Payment, ImportRow, ImportRun

router = APIRouter()


@router.get("/dashboard")
def dashboard(request: Request):
	# pull small samples for quick display
	with get_session() as session:
		clients = session.exec(select(Client).order_by(Client.id.desc()).limit(20)).all()
		items = session.exec(select(Item).order_by(Item.id.desc()).limit(20)).all()
		orders = session.exec(select(Order).order_by(Order.id.desc()).limit(20)).all()
		payments = session.exec(select(Payment).order_by(Payment.id.desc()).limit(20)).all()

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
				"client_sources": client_sources,
				"order_sources": order_sources,
				"item_sources": item_sources,
			},
		)
