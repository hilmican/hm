import datetime as dt
from typing import Optional

from fastapi import APIRouter, Query, Request
from sqlmodel import select
from sqlalchemy import or_, and_

from ..db import get_session
from ..models import Order, Payment, Item, Client, ImportRun, StockMovement
from ..services.shipping import compute_shipping_fee


router = APIRouter()


def _parse_date_or_default(value: Optional[str], fallback: dt.date) -> dt.date:

	try:
		if value:
			return dt.date.fromisoformat(value)
	except Exception:
		pass
	return fallback


@router.get("/daily")
def daily_report(
	request: Request,
	start: Optional[str] = Query(default=None),
	end: Optional[str] = Query(default=None),
	date_field: str = Query(default="shipment", regex="^(shipment|data|both)$"),
):
	# default to last 7 days inclusive
	today = dt.date.today()
	default_start = today - dt.timedelta(days=6)
	start_date = _parse_date_or_default(start, default_start)
	end_date = _parse_date_or_default(end, today)
	if end_date < start_date:
		start_date, end_date = end_date, start_date

	with get_session() as session:
		# Select orders based on date filter mode
		if date_field == "both":
			orders = session.exec(
				select(Order)
				.where(
					or_(
						and_(Order.shipment_date.is_not(None), Order.shipment_date >= start_date, Order.shipment_date <= end_date),
						and_(Order.data_date.is_not(None), Order.data_date >= start_date, Order.data_date <= end_date),
					)
				)
				.order_by(Order.id.desc())
			).all()
		else:
			# If chosen date is missing, fall back to the other date (covers zero-price or incomplete rows)
			date_col = Order.shipment_date if date_field == "shipment" else Order.data_date
			alt_date_col = Order.data_date if date_field == "shipment" else Order.shipment_date
			orders = session.exec(
				select(Order)
				.where(
					or_(
						and_(date_col.is_not(None), date_col >= start_date, date_col <= end_date),
						and_(date_col.is_(None), alt_date_col.is_not(None), alt_date_col >= start_date, alt_date_col <= end_date),
					)
				)
				.order_by(Order.id.desc())
			).all()

		order_count = len(orders)
		total_quantity = sum(int(o.quantity or 0) for o in orders)
		total_sales = sum(float(o.total_amount or 0.0) for o in orders)

		# Prefetch items for cost estimation and names
		item_ids = sorted({o.item_id for o in orders if o.item_id})
		items = session.exec(select(Item).where(Item.id.in_(item_ids))).all() if item_ids else []
		item_map = {it.id: it for it in items if it.id is not None}

		# Prefetch clients for top clients section
		client_ids = sorted({o.client_id for o in orders if o.client_id})
		clients = session.exec(select(Client).where(Client.id.in_(client_ids))).all() if client_ids else []
		client_map = {c.id: c for c in clients if c.id is not None}

		# Compute total cost (use order.total_cost if available; otherwise estimate)
		total_cost = 0.0
		for o in orders:
			if o.total_cost is not None:
				total_cost += float(o.total_cost or 0.0)
			else:
				if o.item_id and o.item_id in item_map:
					base_cost = float(item_map[o.item_id].cost or 0.0)
					qty = int(o.quantity or 0)
					total_cost += base_cost * qty

		gross_profit = total_sales - total_cost
		gross_margin = (gross_profit / total_sales) if total_sales > 0 else 0.0
		aov = (total_sales / order_count) if order_count > 0 else 0.0
		asp = (total_sales / total_quantity) if total_quantity > 0 else 0.0

		# Payments in the period by payment.date
		payments = session.exec(
			select(Payment)
			.where(Payment.date.is_not(None))
			.where(Payment.date >= start_date)
			.where(Payment.date <= end_date)
			.order_by(Payment.id.desc())
		).all()
		gross_collected = sum(float(p.amount or 0.0) for p in payments)
		net_collected = sum(float(p.net_amount or 0.0) for p in payments)
		fee_kom = sum(float(p.fee_komisyon or 0.0) for p in payments)
		fee_hiz = sum(float(p.fee_hizmet or 0.0) for p in payments)
		# Shipping fee for KPIs: prefer stored per-order shipping fee; fallback to computed by toplam
		order_shipping_map = {}
		for o in orders:
			order_shipping_map[o.id or 0] = float((o.shipping_fee if o.shipping_fee is not None else compute_shipping_fee(float(o.total_amount or 0.0))) or 0.0)
		fee_kar = sum(order_shipping_map.get(o.id or 0, 0.0) for o in orders)
		fee_iad = sum(float(p.fee_iade or 0.0) for p in payments)
		fee_eok = sum(float(p.fee_erken_odeme or 0.0) for p in payments)
		total_fees = fee_kom + fee_hiz + fee_kar + fee_iad + fee_eok
		net_profit = gross_profit - total_fees
		collection_ratio = (gross_collected / total_sales) if total_sales > 0 else 0.0

		# Outstanding for period: payments linked to these orders (regardless of payment date)
		order_ids = [o.id for o in orders if o.id]
		linked_payments = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all() if order_ids else []
		linked_paid = sum(float(p.amount or 0.0) for p in linked_payments)
		outstanding = total_sales - linked_paid

		# Orders by channel (source)
		by_channel: dict[str, dict[str, float]] = {}
		for o in orders:
			src = o.source or "?"
			if src not in by_channel:
				by_channel[src] = {"count": 0.0, "sales": 0.0}
			by_channel[src]["count"] += 1.0
			by_channel[src]["sales"] += float(o.total_amount or 0.0)

		# Top items by revenue and quantity
		item_stats: dict[int, dict[str, float]] = {}
		for o in orders:
			if not o.item_id:
				continue
			st = item_stats.setdefault(o.item_id, {"revenue": 0.0, "quantity": 0.0})
			st["revenue"] += float(o.total_amount or 0.0)
			st["quantity"] += float(o.quantity or 0)
		top_items_by_revenue = sorted(
			[
				{
					"item_id": iid,
					"sku": (item_map.get(iid).sku if iid in item_map else None),
					"name": (item_map.get(iid).name if iid in item_map else None),
					"revenue": vals["revenue"],
					"quantity": vals["quantity"],
				}
				for iid, vals in item_stats.items()
			],
			key=lambda x: x["revenue"],
			reverse=True,
		)[:10]
		top_items_by_quantity = sorted(
			[
				{
					"item_id": iid,
					"sku": (item_map.get(iid).sku if iid in item_map else None),
					"name": (item_map.get(iid).name if iid in item_map else None),
					"revenue": vals["revenue"],
					"quantity": vals["quantity"],
				}
				for iid, vals in item_stats.items()
			],
			key=lambda x: x["quantity"],
			reverse=True,
		)[:10]

		# Top clients by revenue
		client_stats: dict[int, float] = {}
		for o in orders:
			if not o.client_id:
				continue
			client_stats[o.client_id] = client_stats.get(o.client_id, 0.0) + float(o.total_amount or 0.0)
		top_clients = sorted(
			[
				{
					"client_id": cid,
					"name": (client_map.get(cid).name if cid in client_map else None),
					"phone": (client_map.get(cid).phone if cid in client_map else None),
					"revenue": rev,
				}
				for cid, rev in client_stats.items()
			],
			key=lambda x: x["revenue"],
			reverse=True,
		)[:10]

		# Import activity in period
		imports = session.exec(
			select(ImportRun)
			.where(ImportRun.data_date.is_not(None))
			.where(ImportRun.data_date >= start_date)
			.where(ImportRun.data_date <= end_date)
			.order_by(ImportRun.started_at.desc())
		).all()
		imports_total = len(imports)
		imports_row_count = sum(int(r.row_count or 0) for r in imports)
		imports_created = {
			"clients": sum(int(r.created_clients or 0) for r in imports),
			"items": sum(int(r.created_items or 0) for r in imports),
			"orders": sum(int(r.created_orders or 0) for r in imports),
			"payments": sum(int(r.created_payments or 0) for r in imports),
		}
		recent_imports = imports[:10]

		# Stock movements in period (use datetime boundaries)
		start_dt = dt.datetime.combine(start_date, dt.time.min)
		end_dt = dt.datetime.combine(end_date, dt.time.max)
		movs = session.exec(
			select(StockMovement)
			.where(StockMovement.created_at >= start_dt)
			.where(StockMovement.created_at <= end_dt)
			.order_by(StockMovement.id.desc())
		).all()
		qty_in = sum(int(m.quantity or 0) for m in movs if (m.direction or "").lower() == "in")
		qty_out = sum(int(m.quantity or 0) for m in movs if (m.direction or "").lower() == "out")

		templates = request.app.state.templates
		return templates.TemplateResponse(
			"reports_daily.html",
			{
				"request": request,
				"start": start_date,
				"end": end_date,
				"date_field": date_field,
				# headline KPIs
				"order_count": order_count,
				"total_quantity": total_quantity,
				"total_sales": total_sales,
				"total_cost": total_cost,
				"gross_profit": gross_profit,
				"gross_margin": gross_margin,
				"net_profit": net_profit,
				"aov": aov,
				"asp": asp,
				# payments summary
				"gross_collected": gross_collected,
				"net_collected": net_collected,
				"total_fees": total_fees,
				"fee_breakdown": {
					"komisyon": fee_kom,
					"hizmet": fee_hiz,
					"kargo": fee_kar,
					"iade": fee_iad,
					"erken_odeme": fee_eok,
				},
				"collection_ratio": collection_ratio,
				"outstanding": outstanding,
				# breakdowns
				"by_channel": by_channel,
				"top_items_by_revenue": top_items_by_revenue,
				"top_items_by_quantity": top_items_by_quantity,
				"top_clients": top_clients,
				# import and stock
				"imports_total": imports_total,
				"imports_row_count": imports_row_count,
				"imports_created": imports_created,
				"recent_imports": recent_imports,
				"qty_in": qty_in,
				"qty_out": qty_out,
				# filtered orders and maps for listing
				"orders": orders,
				"client_map": client_map,
				"item_map": item_map,
			},
		)


