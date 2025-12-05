from typing import Optional, List, Dict, Any
import datetime as dt

from fastapi import APIRouter, Request, Query, Form, Body
from fastapi.responses import RedirectResponse, JSONResponse
from sqlmodel import select

from ..db import get_session
from ..models import Income, Account, Order, OrderPayment, Client
from ..services.finance import get_unpaid_orders, calculate_expected_payment, mark_orders_collected

router = APIRouter()


def _parse_date_or_default(value: Optional[str], fallback: dt.date) -> dt.date:
	try:
		if value:
			return dt.date.fromisoformat(value)
	except Exception:
		pass
	return fallback


@router.get("")
@router.get("/")
def list_income(limit: int = Query(default=100, ge=1, le=1000)):
	"""List income entries."""
	with get_session() as session:
		rows = session.exec(select(Income).order_by(Income.date.desc(), Income.id.desc()).limit(limit)).all()
		account_ids = {inc.account_id for inc in rows}
		accounts = session.exec(select(Account).where(Account.id.in_(account_ids))).all() if account_ids else []
		account_map = {acc.id: acc.name for acc in accounts if acc.id is not None}
		
		return {
			"income": [
				{
					"id": inc.id,
					"account_id": inc.account_id,
					"account_name": account_map.get(inc.account_id, ""),
					"amount": inc.amount,
					"date": inc.date.isoformat() if inc.date else None,
					"source": inc.source,
					"reference": inc.reference,
					"notes": inc.notes,
				}
				for inc in rows
			]
		}


@router.get("/table")
def income_table(
	request: Request,
	start: Optional[str] = Query(default=None),
	end: Optional[str] = Query(default=None),
):
	"""HTML table view of income entries."""
	today = dt.date.today()
	default_start = today - dt.timedelta(days=29)
	start_date = _parse_date_or_default(start, default_start)
	end_date = _parse_date_or_default(end, today)
	
	with get_session() as session:
		rows = session.exec(
			select(Income)
			.where(Income.date.is_not(None))
			.where(Income.date >= start_date)
			.where(Income.date <= end_date)
			.order_by(Income.date.desc(), Income.id.desc())
		).all()
		
		# Get all accounts for dropdown
		all_accounts = session.exec(select(Account).where(Account.is_active == True).order_by(Account.name.asc())).all()
		
		account_ids = {inc.account_id for inc in rows}
		accounts = session.exec(select(Account).where(Account.id.in_(account_ids))).all() if account_ids else []
		account_map = {acc.id: acc.name for acc in accounts if acc.id is not None}
		
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"income.html",
			{
				"request": request,
				"start": start_date,
				"end": end_date,
				"income_entries": rows,
				"accounts": all_accounts,
				"account_map": account_map,
				"today": today,
			},
		)


@router.post("/add")
def add_income(
	account_id: int = Form(...),
	amount: float = Form(...),
	date: Optional[str] = Form(default=None),
	source: str = Form(...),
	reference: Optional[str] = Form(default=None),
	notes: Optional[str] = Form(default=None),
	start: Optional[str] = Form(default=None),
	end: Optional[str] = Form(default=None),
):
	"""Create a new income entry."""
	try:
		when = dt.date.fromisoformat(date) if date else dt.date.today()
	except Exception:
		when = dt.date.today()
	
	with get_session() as session:
		try:
			income = Income(
				account_id=int(account_id),
				amount=float(amount),
				date=when,
				source=source.strip(),
				reference=reference.strip() if reference else None,
				notes=notes.strip() if notes else None,
			)
			session.add(income)
			session.commit()
		except Exception:
			session.rollback()
			pass
	
	url = "/income/table"
	if start or end:
		params = []
		if start:
			params.append(f"start={start}")
		if end:
			params.append(f"end={end}")
		url = f"{url}?{'&'.join(params)}"
	return RedirectResponse(url=url, status_code=303)


@router.get("/orders-to-collect")
def orders_to_collect(
	request: Request,
	q: Optional[str] = Query(default=None, description="Search query"),
	start: Optional[str] = Query(default=None),
	end: Optional[str] = Query(default=None),
	limit: int = Query(default=500, ge=1, le=5000),
):
	"""Get unpaid orders with search/filter capabilities."""
	start_date = _parse_date_or_default(start, None)
	end_date = _parse_date_or_default(end, None)
	
	with get_session() as session:
		unpaid = get_unpaid_orders(session, start_date, end_date)
		
		# Filter by search query if provided
		if q:
			q_lower = q.lower()
			filtered = []
			for order in unpaid:
				if order.id is None:
					continue
				# Search in tracking_no
				if order.tracking_no and q_lower in order.tracking_no.lower():
					filtered.append(order)
					continue
				# Search in client name
				client = session.exec(select(Client).where(Client.id == order.client_id)).first()
				if client and client.name and q_lower in client.name.lower():
					filtered.append(order)
					continue
			unpaid = filtered
		
		# Limit results
		unpaid = unpaid[:limit]
		
		# Get client info and calculate expected payments
		client_ids = {o.client_id for o in unpaid}
		clients = session.exec(select(Client).where(Client.id.in_(client_ids))).all() if client_ids else []
		client_map = {c.id: c.name for c in clients if c.id is not None}
		
		orders_data = []
		total_expected = 0.0
		for order in unpaid:
			if order.id is None:
				continue
			expected = calculate_expected_payment(session, order.id)
			total_expected += expected
			orders_data.append({
				"id": order.id,
				"tracking_no": order.tracking_no,
				"client_name": client_map.get(order.client_id, f"Client {order.client_id}"),
				"total_amount": order.total_amount,
				"expected_payment": expected,
				"shipment_date": order.shipment_date.isoformat() if order.shipment_date else None,
				"data_date": order.data_date.isoformat() if order.data_date else None,
			})
		
		return {
			"orders": orders_data,
			"total_expected": total_expected,
			"count": len(orders_data),
		}


@router.post("/bulk-collect")
def bulk_collect_orders(body: Dict[str, Any] = Body(...)):
	"""Mark multiple orders as collected and create income entry.
	
	Expected body:
	{
		"account_id": int,
		"amount": float,
		"date": "YYYY-MM-DD",
		"source": str,
		"reference": Optional[str],
		"notes": Optional[str],
		"order_ids": [int, ...],
		"collected_amounts": Optional[Dict[int, float]]  # order_id -> actual amount
	}
	"""
	try:
		account_id = int(body.get("account_id"))
		amount = float(body.get("amount"))
		date_str = body.get("date")
		source = body.get("source", "shipment_firm").strip()
		reference = body.get("reference")
		notes = body.get("notes")
		order_ids = [int(x) for x in body.get("order_ids", [])]
		collected_amounts = body.get("collected_amounts")
		
		if date_str:
			when = dt.date.fromisoformat(date_str)
		else:
			when = dt.date.today()
		
		if not order_ids:
			return JSONResponse({"error": "No orders selected"}, status_code=400)
		
		with get_session() as session:
			# Create income entry
			income = Income(
				account_id=account_id,
				amount=amount,
				date=when,
				source=source,
				reference=reference.strip() if reference else None,
				notes=notes.strip() if notes else None,
			)
			session.add(income)
			session.flush()
			
			if income.id is None:
				return JSONResponse({"error": "Failed to create income entry"}, status_code=500)
			
			# Mark orders as collected
			collected_dict = None
			if collected_amounts:
				collected_dict = {int(k): float(v) for k, v in collected_amounts.items()}
			
			count = mark_orders_collected(session, order_ids, income.id, collected_dict)
			session.commit()
			
			return {
				"success": True,
				"income_id": income.id,
				"orders_collected": count,
			}
	except Exception as e:
		return JSONResponse({"error": str(e)}, status_code=500)

