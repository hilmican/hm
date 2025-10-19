from fastapi import APIRouter, Request, HTTPException
from sqlmodel import select

from ..db import get_session
from ..models import Client, Item, Order, Payment, ImportRow, ImportRun

router = APIRouter()


@router.get("/dashboard")
def dashboard(request: Request):
	# require login
	uid = request.session.get("uid")
	if not uid:
		templates = request.app.state.templates
		return templates.TemplateResponse("login.html", {"request": request, "error": None})
	# pull small samples for quick display
	
	with get_session() as session:
		# aggregates for header
		all_orders = session.exec(select(Order)).all()
		total_sales = sum(float(o.total_amount or 0.0) for o in all_orders)
		all_payments = session.exec(select(Payment)).all()
		total_collected = sum(float(p.net_amount or 0.0) for p in all_payments)
		total_fees = 0.0
		for p in all_payments:
			total_fees += float((p.fee_komisyon or 0.0) + (p.fee_hizmet or 0.0) + (p.fee_kargo or 0.0) + (p.fee_iade or 0.0) + (p.fee_erken_odeme or 0.0))
		total_to_collect = max(0.0, total_sales - total_collected)

		orders = session.exec(select(Order).order_by(Order.id.desc()).limit(20)).all()
		# fetch only the related clients/items for shown orders (no extra limits)
		client_ids = sorted({o.client_id for o in orders if o.client_id})
		item_ids = sorted({o.item_id for o in orders if o.item_id})
		clients = session.exec(select(Client).where(Client.id.in_(client_ids))).all() if client_ids else []
		items = session.exec(select(Item).where(Item.id.in_(item_ids))).all() if item_ids else []
		client_map = {c.id: c.name for c in clients if c.id is not None}
		item_map = {it.id: it.name for it in items if it.id is not None}
		payments = session.exec(select(Payment).order_by(Payment.id.desc()).limit(20)).all()

		# Build order maps from the loaded orders; extend with any orders referenced by payments
		order_map: dict[int, str] = {o.id: o.tracking_no for o in orders if o.id is not None}
		order_total_map: dict[int, float] = {o.id: float(o.total_amount or 0.0) for o in orders if o.id is not None}
		p_client_ids = sorted({p.client_id for p in payments if p.client_id and p.client_id not in client_map})
		p_order_ids = sorted({p.order_id for p in payments if p.order_id and p.order_id not in order_map})
		if p_client_ids:
			extra_clients = session.exec(select(Client).where(Client.id.in_(p_client_ids))).all()
			for ec in extra_clients:
				if ec.id is not None:
					client_map[ec.id] = ec.name
		if p_order_ids:
			extra_orders = session.exec(select(Order).where(Order.id.in_(p_order_ids))).all()
			for eo in extra_orders:
				if eo.id is not None:
					order_map[eo.id] = eo.tracking_no
					order_total_map[eo.id] = float(eo.total_amount or 0.0)

		# compute paid/unpaid flags for shown orders
		order_ids = [o.id for o in orders if o.id]
		pays = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all() if order_ids else []
		paid_map: dict[int, float] = {}
		for p in pays:
			if p.order_id is None:
				continue
			# Prefer gross amount; if missing, reconstruct gross from net + fees
			gross_from_amount = float(p.amount or 0.0)
			gross_from_components = float((p.net_amount or 0.0) + (p.fee_komisyon or 0.0) + (p.fee_hizmet or 0.0) + (p.fee_kargo or 0.0) + (p.fee_iade or 0.0) + (p.fee_erken_odeme or 0.0))
			effective_gross = gross_from_amount if gross_from_amount > 0 else gross_from_components
			paid_map[p.order_id] = paid_map.get(p.order_id, 0.0) + effective_gross
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
				"total_sales": total_sales,
				"total_collected": total_collected,
				"total_to_collect": total_to_collect,
				"total_fees": total_fees,
				"clients": clients,
				"items": items,
				"orders": orders,
				"payments": payments,
				"client_map": client_map,
				"item_map": item_map,
				"order_map": order_map,
				"order_total_map": order_total_map,
				"client_sources": client_sources,
				"order_sources": order_sources,
				"item_sources": item_sources,
				"status_map": status_map,
			},
		)
