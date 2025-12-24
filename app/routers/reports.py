import datetime as dt
from typing import Optional

from fastapi import APIRouter, Query, Request
from sqlmodel import select
from sqlalchemy import or_, and_, text, func
import os

from ..db import get_session
from ..models import Order, Payment, Item, Client, ImportRun, StockMovement, OrderItem, Cost, Account, Income
from ..services.shipping import compute_shipping_fee
from ..services.inventory import get_stock_map
from ..services.cache import cached_json
from ..services.finance import get_account_balances, detect_payment_leaks


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
	# default window starts from 2025-10-01 until today
	today = dt.date.today()
	default_start = dt.date(2025, 10, 1)
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
					Order.merged_into_order_id.is_(None),  # Exclude merged orders
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
					Order.merged_into_order_id.is_(None),  # Exclude merged orders
					or_(
						and_(date_col.is_not(None), date_col >= start_date, date_col <= end_date),
						and_(date_col.is_(None), alt_date_col.is_not(None), alt_date_col >= start_date, alt_date_col <= end_date),
					)
				)
				.order_by(Order.id.desc())
			).all()

		# Filter out merged orders and count only primary orders
		orders = [o for o in orders if o.merged_into_order_id is None]
		order_count = len(orders)
		total_quantity = sum(int(o.quantity or 0) for o in orders)
		total_sales = sum(float(o.total_amount or 0.0) for o in orders)

		# Prefetch items for name display (Order.item_id) and also prefetch OrderItems for cost fallback
		item_ids = sorted({o.item_id for o in orders if o.item_id})
		items = session.exec(select(Item).where(Item.id.in_(item_ids))).all() if item_ids else []
		item_map = {it.id: it for it in items if it.id is not None}
		order_ids = [o.id for o in orders if o.id]
		order_items = session.exec(select(OrderItem).where(OrderItem.order_id.in_(order_ids))).all() if order_ids else []
		# Build order -> list of (item_id, qty) and collect all item ids for cost lookup
		order_item_map: dict[int, list[tuple[int, int]]] = {}
		all_oi_item_ids: set[int] = set()
		for oi in order_items:
			if oi.order_id is None or oi.item_id is None:
				continue
			order_item_map.setdefault(int(oi.order_id), []).append((int(oi.item_id), int(oi.quantity or 0)))
			all_oi_item_ids.add(int(oi.item_id))
		# Prefetch costs for all items appearing in order items
		cost_items = session.exec(select(Item).where(Item.id.in_(sorted(all_oi_item_ids)))).all() if all_oi_item_ids else []
		cost_map: dict[int, float] = {it.id: float(it.cost or 0.0) for it in cost_items if it.id is not None}

		# Prefetch clients for top clients section
		client_ids = sorted({o.client_id for o in orders if o.client_id})
		clients = session.exec(select(Client).where(Client.id.in_(client_ids))).all() if client_ids else []
		client_map = {c.id: c for c in clients if c.id is not None}

		# Compute total cost (prefer stored per-order total_cost; otherwise sum over OrderItems * item.cost)
		total_cost = 0.0
		for o in orders:
			# For refunded/switch/stitched or negative totals, treat product cost as zero for reporting
			status_lc = str(o.status or "").lower()
			if (status_lc in ("refunded", "switched", "stitched")) or (float(o.total_amount or 0.0) < 0.0):
				continue
			if o.total_cost is not None:
				total_cost += float(o.total_cost or 0.0)
			else:
				acc = 0.0
				if o.id and (o.id in order_item_map):
					for (iid, qty) in order_item_map.get(o.id, []):
						acc += float(cost_map.get(iid, 0.0)) * int(qty or 0)
				elif o.item_id and (o.item_id in item_map):
					# fallback to single-item orders if present
					acc = float(item_map[o.item_id].cost or 0.0) * int(o.quantity or 0)
				total_cost += acc

		# Add overhead/operational costs from Cost table within the period
		# Exclude payments to suppliers and MERTER MAL ALIM (type_id=9) costs
		try:
			period_costs_query = (
				select(func.sum(Cost.amount))
				.where(Cost.date.is_not(None))
				.where(Cost.date >= start_date)
				.where(Cost.date <= end_date)
				.where(or_(Cost.is_payment_to_supplier == False, Cost.is_payment_to_supplier.is_(None)))
				.where(or_(Cost.type_id != 9, Cost.type_id.is_(None)))
			)
			period_costs_result = session.exec(period_costs_query).first()
			period_costs = float(period_costs_result or 0.0)
		except Exception:
			period_costs = 0.0
		total_cost += period_costs

		gross_profit = total_sales - total_cost
		gross_margin = (gross_profit / total_sales) if total_sales > 0 else 0.0
		aov = (total_sales / order_count) if order_count > 0 else 0.0
		asp = (total_sales / total_quantity) if total_quantity > 0 else 0.0
		# net_margin is computed later after fees are loaded

		# Payments in the period by payment.date (use SQL sums with short cache)
		ttl = int(os.getenv("CACHE_TTL_REPORTS", "60"))
		cache_key = f"rep:daily:pay:{start_date.isoformat()}:{end_date.isoformat()}"
		pay_sums = cached_json(
			cache_key,
			ttl,
			lambda: {
				"gross": float((session.exec(text("SELECT SUM(amount) FROM payment WHERE date IS NOT NULL AND date >= :s AND date <= :e").bindparams(s=start_date, e=end_date)).first() or [0])[0] or 0),
				"net": float((session.exec(text("SELECT SUM(net_amount) FROM payment WHERE date IS NOT NULL AND date >= :s AND date <= :e").bindparams(s=start_date, e=end_date)).first() or [0])[0] or 0),
				"kom": float((session.exec(text("SELECT SUM(COALESCE(fee_komisyon,0)) FROM payment WHERE date IS NOT NULL AND date >= :s AND date <= :e").bindparams(s=start_date, e=end_date)).first() or [0])[0] or 0),
				"hiz": float((session.exec(text("SELECT SUM(COALESCE(fee_hizmet,0)) FROM payment WHERE date IS NOT NULL AND date >= :s AND date <= :e").bindparams(s=start_date, e=end_date)).first() or [0])[0] or 0),
				"iad": float((session.exec(text("SELECT SUM(COALESCE(fee_iade,0)) FROM payment WHERE date IS NOT NULL AND date >= :s AND date <= :e").bindparams(s=start_date, e=end_date)).first() or [0])[0] or 0),
				"eok": float((session.exec(text("SELECT SUM(COALESCE(fee_erken_odeme,0)) FROM payment WHERE date IS NOT NULL AND date >= :s AND date <= :e").bindparams(s=start_date, e=end_date)).first() or [0])[0] or 0),
			},
		)
		gross_collected = float(pay_sums.get("gross", 0.0))
		net_collected = float(pay_sums.get("net", 0.0))
		fee_kom = float(pay_sums.get("kom", 0.0))
		fee_hiz = float(pay_sums.get("hiz", 0.0))
		# Shipping fee for KPIs: prefer stored per-order shipping fee; fallback to computed by toplam
		order_shipping_map = {}
		for o in orders:
			# Compute pre-tax fee, then add 20% tax
			pre_tax = float((o.shipping_fee if o.shipping_fee is not None else compute_shipping_fee(float(o.total_amount or 0.0))) or 0.0)
			with_tax = round(pre_tax * 1.20, 2)
			order_shipping_map[o.id or 0] = with_tax
		fee_kar = sum(order_shipping_map.get(o.id or 0, 0.0) for o in orders)
		fee_iad = float(pay_sums.get("iad", 0.0))
		fee_eok = float(pay_sums.get("eok", 0.0))
		total_fees = fee_kom + fee_hiz + fee_kar + fee_iad + fee_eok
		net_profit = gross_profit - total_fees
		# Treat IBAN-marked orders as collected/completed for ratios/outstanding
		iban_collected = sum(float(o.total_amount or 0.0) for o in orders if bool(getattr(o, "paid_by_bank_transfer", False)))
		collection_ratio = ((gross_collected + iban_collected) / total_sales) if total_sales > 0 else 0.0
		net_margin = (net_profit / total_sales) if total_sales > 0 else 0.0

		# Outstanding for period: payments linked to these orders (regardless of payment date)
		order_ids = [o.id for o in orders if o.id]
		linked_payments = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all() if order_ids else []
		linked_paid = sum(float(p.amount or 0.0) for p in linked_payments)
		# Consider IBAN-marked orders fully paid
		outstanding = total_sales - (linked_paid + iban_collected)

		# Orders by channel (source)
		by_channel: dict[str, dict[str, float]] = {}
		for o in orders:
			src = o.source or "?"
			if src not in by_channel:
				by_channel[src] = {"count": 0.0, "sales": 0.0}
			by_channel[src]["count"] += 1.0
			by_channel[src]["sales"] += float(o.total_amount or 0.0)

		# Refund (iade) metrics: count, totals, shipping specifically for refunded orders in period
		refunded_orders = [o for o in orders if (str(o.status or "").lower() == "refunded")]
		refund_count = len(refunded_orders)
		refund_total_amount = sum(float(o.total_amount or 0.0) for o in refunded_orders)
		refund_shipping_total = sum(order_shipping_map.get(o.id or 0, 0.0) for o in refunded_orders)

		# Switch (değişim) metrics: count and totals for switched orders in period
		switched_orders = [o for o in orders if (str(o.status or "").lower() == "switched")]
		switch_count = len(switched_orders)
		switch_total_amount = sum(float(o.total_amount or 0.0) for o in switched_orders)

		# Partial payment metrics: count and totals for partial payment orders in period
		partial_paid_orders = [o for o in orders if (str(o.status or "").lower() == "partial_paid") or (bool(o.is_partial_payment))]
		partial_paid_count = len(partial_paid_orders)
		partial_paid_total_amount = sum(float(o.total_amount or 0.0) for o in partial_paid_orders)

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

		# Current inventory snapshot (on-hand quantities and value)
		stock_map = get_stock_map(session)
		inv_item_ids = [iid for iid, qty in stock_map.items() if int(qty or 0) > 0]
		inv_items = session.exec(select(Item).where(Item.id.in_(inv_item_ids))).all() if inv_item_ids else []
		inventory_value = 0.0
		for it in inv_items:
			if it.id is None:
				continue
			qty = int(stock_map.get(int(it.id), 0) or 0)
			cost = float(it.cost or 0.0)
			if qty > 0 and cost > 0:
				inventory_value += qty * cost
		inventory_item_count = len(inv_item_ids)

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
				"inventory_value": inventory_value,
				"inventory_item_count": inventory_item_count,
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
				"net_margin": net_margin,
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
				# refund (iade) KPIs
				"refund_count": refund_count,
				"refund_total_amount": refund_total_amount,
				"refund_shipping_total": refund_shipping_total,
				# switch (değişim) KPIs
				"switch_count": switch_count,
				"switch_total_amount": switch_total_amount,
				# partial payment KPIs
				"partial_paid_count": partial_paid_count,
				"partial_paid_total_amount": partial_paid_total_amount,
				# shipment costs (kargo) as separate KPI
				"total_shipment_costs": fee_kar,
			},
		)


@router.post("/recalculate-costs")
def recalculate_costs(
	start: Optional[str] = Query(default=None),
	end: Optional[str] = Query(default=None),
	date_field: str = Query(default="shipment", regex="^(shipment|data|both)$"),
):
	"""Backfill per-order total_cost using FIFO method based on purchase costs for the selected period."""
	# default to last 7 days inclusive
	today = dt.date.today()
	default_start = today - dt.timedelta(days=6)
	start_date = _parse_date_or_default(start, default_start)
	end_date = _parse_date_or_default(end, today)
	if end_date < start_date:
		start_date, end_date = end_date, start_date
	updated = 0
	with get_session() as session:
		from ..services.inventory import calculate_order_cost_fifo
		# Select orders as in daily_report
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
		# Recompute per order using FIFO
		for o in orders:
			if not o.id:
				continue
			new_cost = calculate_order_cost_fifo(session, o.id)
			# Update only if changed or previously null
			if o.total_cost is None or float(o.total_cost or 0.0) != new_cost:
				o.total_cost = new_cost
				updated += 1
	return {"status": "ok", "updated_orders": updated}


@router.get("/finance")
def finance_report(
	request: Request, 
	start: Optional[str] = Query(default=None), 
	end: Optional[str] = Query(default=None),
	leaks_page: int = Query(default=1, ge=1, alias="leaks_page"),
	leaks_per_page: int = Query(default=50, ge=10, le=200, alias="leaks_per_page")
):
	"""Financial overview: account balances, income vs expenses, unpaid orders (cash leaks)."""
	today = dt.date.today()
	default_start = today - dt.timedelta(days=29)
	start_date = _parse_date_or_default(start, default_start)
	end_date = _parse_date_or_default(end, today)
	if end_date < start_date:
		start_date, end_date = end_date, start_date
	
	with get_session() as session:
		# Get all accounts with balances
		accounts = session.exec(select(Account).where(Account.is_active == True).order_by(Account.name.asc())).all()
		balances = get_account_balances(session)
		
		# Get income entries in period
		incomes = session.exec(
			select(Income)
			.where(Income.date.is_not(None))
			.where(Income.date >= start_date)
			.where(Income.date <= end_date)
			.order_by(Income.date.desc())
		).all()
		total_income = sum(float(inc.amount) for inc in incomes)
		
		# Get expenses in period
		expenses = session.exec(
			select(Cost)
			.where(Cost.date.is_not(None))
			.where(Cost.date >= start_date)
			.where(Cost.date <= end_date)
			.order_by(Cost.date.desc())
		).all()
		total_expenses = sum(float(exp.amount) for exp in expenses)
		
		# Income by account
		income_by_account = {}
		for inc in incomes:
			acc_id = inc.account_id
			if acc_id not in income_by_account:
				income_by_account[acc_id] = 0.0
			income_by_account[acc_id] += float(inc.amount)
		
		# Expenses by account
		expense_by_account = {}
		for exp in expenses:
			if exp.account_id:
				acc_id = exp.account_id
				if acc_id not in expense_by_account:
					expense_by_account[acc_id] = 0.0
				expense_by_account[acc_id] += float(exp.amount)
		
		# Detect payment leaks with pagination
		all_leaks = detect_payment_leaks(session, min_days_old=7)
		total_leaks = len(all_leaks)
		
		# Apply pagination
		start_idx = (leaks_page - 1) * leaks_per_page
		end_idx = start_idx + leaks_per_page
		leaks = all_leaks[start_idx:end_idx]
		total_pages = (total_leaks + leaks_per_page - 1) // leaks_per_page if total_leaks > 0 else 1
		
		# Recent transactions (last 20) - optimize to avoid N+1 queries
		# Batch load all account IDs first
		account_ids = set()
		for inc in incomes[:20]:
			if inc.account_id:
				account_ids.add(inc.account_id)
		for exp in expenses[:20]:
			if exp.account_id:
				account_ids.add(exp.account_id)
		
		# Load all accounts in one query
		account_map = {}
		if account_ids:
			accounts_batch = session.exec(select(Account).where(Account.id.in_(account_ids))).all()
			account_map = {acc.id: acc.name for acc in accounts_batch if acc.id is not None}
		
		recent_transactions = []
		for inc in incomes[:20]:
			recent_transactions.append({
				"type": "income",
				"date": inc.date,
				"amount": inc.amount,
				"account": account_map.get(inc.account_id) if inc.account_id else f"Account {inc.account_id}",
				"description": f"{inc.source} - {inc.reference or ''}",
			})
		for exp in expenses[:20]:
			recent_transactions.append({
				"type": "expense",
				"date": exp.date,
				"amount": -exp.amount,
				"account": account_map.get(exp.account_id) if exp.account_id else "No account",
				"description": exp.details or "",
			})
		recent_transactions.sort(key=lambda x: x["date"] or dt.date.min, reverse=True)
		recent_transactions = recent_transactions[:20]
		
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"reports_finance.html",
			{
				"request": request,
				"start": start_date,
				"end": end_date,
				"accounts": accounts,
				"balances": balances,
				"total_income": total_income,
				"total_expenses": total_expenses,
				"income_by_account": income_by_account,
				"expense_by_account": expense_by_account,
				"leaks": leaks,
				"total_leaks": total_leaks,
				"leaks_page": leaks_page,
				"leaks_per_page": leaks_per_page,
				"total_pages": total_pages,
				"recent_transactions": recent_transactions,
			},
		)
