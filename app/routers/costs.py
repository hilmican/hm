from typing import Optional, List, Dict, Any
import datetime as dt
import json

from fastapi import APIRouter, Request, Query, Form, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from sqlmodel import select, delete

from ..db import get_session
from ..models import Cost, CostType, Account, Supplier, Product, CostHistoryLog


router = APIRouter()


def _parse_date_or_default(value: Optional[str], fallback: dt.date) -> dt.date:
	try:
		if value:
			return dt.date.fromisoformat(value)
	except Exception:
		pass
	return fallback


def _log_cost_change(
	session,
	cost_id: int,
	action: str,
	old_data: Optional[Dict] = None,
	new_data: Optional[Dict] = None,
	user_id: Optional[int] = None
):
	"""Log cost entry changes to history."""
	log_entry = CostHistoryLog(
		cost_id=cost_id,
		action=action,
		old_data_json=json.dumps(old_data) if old_data else None,
		new_data_json=json.dumps(new_data) if new_data else None,
		user_id=user_id
	)
	session.add(log_entry)


def _cost_to_dict(cost: Cost) -> Dict:
	"""Convert Cost object to dictionary for logging."""
	return {
		"id": cost.id,
		"type_id": cost.type_id,
		"account_id": cost.account_id,
		"supplier_id": cost.supplier_id,
		"product_id": cost.product_id,
		"quantity": cost.quantity,
		"is_payment_to_supplier": cost.is_payment_to_supplier,
		"amount": cost.amount,
		"date": cost.date.isoformat() if cost.date else None,
		"details": cost.details,
	}


@router.get("")
@router.get("/")
def costs_page(
	request: Request,
	start: Optional[str] = Query(default=None),
	end: Optional[str] = Query(default=None),
):
	# default: last 30 days inclusive
	today = dt.date.today()
	default_start = today - dt.timedelta(days=29)
	start_date = _parse_date_or_default(start, default_start)
	end_date = _parse_date_or_default(end, today)
	if end_date < start_date:
		start_date, end_date = end_date, start_date
	with get_session() as session:
		types = session.exec(select(CostType).order_by(CostType.name.asc())).all()
		accounts = session.exec(select(Account).where(Account.is_active == True).order_by(Account.name.asc())).all()
		suppliers = session.exec(select(Supplier).order_by(Supplier.name.asc())).all()
		products = session.exec(select(Product).order_by(Product.name.asc())).all()
		# Get "Genel Giderler" supplier as default
		general_supplier = session.exec(select(Supplier).where(Supplier.name == "Genel Giderler")).first()
		
		rows = session.exec(
			select(Cost)
			.where(Cost.date.is_not(None))
			.where(Cost.date >= start_date)
			.where(Cost.date <= end_date)
			.order_by(Cost.date.desc(), Cost.id.desc())
		).all()
		type_map = {t.id: t.name for t in types if t.id is not None}
		account_map = {a.id: a.name for a in accounts if a.id is not None}
		supplier_map = {s.id: s.name for s in suppliers if s.id is not None}
		product_map = {p.id: p.name for p in products if p.id is not None}

		# Calculate total amount (exclude payments to suppliers and MERTER MAL ALIM)
		total_amount = sum(
			float(r.amount or 0) for r in rows 
			if not (r.is_payment_to_supplier or (r.type_id == 9))
		)

		templates = request.app.state.templates
		return templates.TemplateResponse(
			"costs.html",
			{
				"request": request,
				"start": start_date,
				"end": end_date,
				"types": types,
				"accounts": accounts,
				"suppliers": suppliers,
				"products": products,
				"general_supplier": general_supplier,
				"rows": rows,
				"type_map": type_map,
				"account_map": account_map,
				"supplier_map": supplier_map,
				"product_map": product_map,
				"today": today,
				"total_amount": total_amount,
			},
		)


@router.post("/add-type")
def add_cost_type(name: str = Form(...), start: Optional[str] = Form(default=None), end: Optional[str] = Form(default=None)):
	val = (name or "").strip()
	if not val:
		# Redirect without creating empty names
		url = "/costs"
		if start or end:
			params = []
			if start:
				params.append(f"start={start}")
			if end:
				params.append(f"end={end}")
			url = f"/costs?{'&'.join(params)}"
		return RedirectResponse(url=url, status_code=303)
	with get_session() as session:
		try:
			# Try to find existing first (case-insensitive best-effort)
			existing = session.exec(select(CostType).where(CostType.name == val)).first()
			if not existing:
				session.add(CostType(name=val))
				session.commit()
		except Exception:
			# ignore uniqueness errors; proceed
			pass
	url = "/costs"
	if start or end:
		params = []
		if start:
			params.append(f"start={start}")
		if end:
			params.append(f"end={end}")
		url = f"/costs?{'&'.join(params)}"
	return RedirectResponse(url=url, status_code=303)


@router.post("/add")
def add_cost(
	type_id: int = Form(...),
	amount: float = Form(...),
	date: Optional[str] = Form(default=None),
	details: Optional[str] = Form(default=None),
	account_id: Optional[int] = Form(default=None),
	supplier_id: Optional[int] = Form(default=None),
	product_id: Optional[int] = Form(default=None),
	quantity: Optional[int] = Form(default=None),
	is_payment_to_supplier: bool = Form(default=False),
	start: Optional[str] = Form(default=None),
	end: Optional[str] = Form(default=None),
):
	try:
		when = dt.date.fromisoformat(date) if date else dt.date.today()
	except Exception:
		when = dt.date.today()
	
	# Validate MERTER MAL ALIM (type_id=9) requirements
	# Note: product_id and quantity are optional to allow recording bulk purchases
	# without specifying individual products. Stock tracking is handled separately.
	if type_id == 9:
		if not supplier_id:
			# Redirect with error - supplier required
			url = "/costs"
			if start or end:
				params = []
				if start:
					params.append(f"start={start}")
				if end:
					params.append(f"end={end}")
				url = f"/costs?{'&'.join(params)}"
			return RedirectResponse(url=url, status_code=303)
	
	with get_session() as session:
		try:
			c = Cost(
				type_id=int(type_id),
				amount=float(amount),
				date=when,
				details=(details or "").strip() or None,
				account_id=int(account_id) if account_id else None,
				supplier_id=int(supplier_id) if supplier_id else None,
				product_id=int(product_id) if product_id else None,
				quantity=int(quantity) if quantity else None,
				is_payment_to_supplier=is_payment_to_supplier,
			)
			session.add(c)
			session.flush()
			
			# Log creation
			if c.id:
				user_id = None  # TODO: Get from session if auth is implemented
				_log_cost_change(
					session,
					c.id,
					"create",
					old_data=None,
					new_data=_cost_to_dict(c),
					user_id=user_id
				)
			
			session.commit()
		except Exception:
			# best-effort insert; ignore on error
			session.rollback()
			pass
	url = "/costs"
	if start or end:
		params = []
		if start:
			params.append(f"start={start}")
		if end:
			params.append(f"end={end}")
		url = f"/costs?{'&'.join(params)}"
	return RedirectResponse(url=url, status_code=303)


@router.post("/delete")
def delete_costs(
	cost_ids: List[int] = Form(..., alias="cost_ids"),
	start: Optional[str] = Form(default=None),
	end: Optional[str] = Form(default=None),
):
	with get_session() as session:
		if cost_ids:
			# Get costs before deleting for logging
			costs_to_delete = session.exec(
				select(Cost).where(Cost.id.in_(cost_ids))
			).all()
			
			# Delete existing history log records manually if FK constraint is not CASCADE yet
			# This is a workaround until the migration updates the constraint to CASCADE
			for cost in costs_to_delete:
				if cost.id:
					history_logs = session.exec(
						select(CostHistoryLog).where(CostHistoryLog.cost_id == cost.id)
					).all()
					for log in history_logs:
						session.delete(log)
			
			session.flush()  # Flush to ensure history logs are deleted before deleting costs
			
			# Delete the selected costs
			session.exec(delete(Cost).where(Cost.id.in_(cost_ids)))
			session.commit()

	url = "/costs"
	if start or end:
		params = []
		if start:
			params.append(f"start={start}")
		if end:
			params.append(f"end={end}")
		url = f"/costs?{'&'.join(params)}"
	return RedirectResponse(url=url, status_code=303)


@router.get("/suppliers")
def list_suppliers_api():
	"""API endpoint to get supplier list."""
	with get_session() as session:
		suppliers = session.exec(select(Supplier).order_by(Supplier.name.asc())).all()
		return {
			"suppliers": [
				{
					"id": s.id,
					"name": s.name,
					"phone": s.phone,
					"address": s.address,
					"tax_id": s.tax_id,
				}
				for s in suppliers
			]
		}


@router.post("/suppliers/add")
def add_supplier(
	name: str = Form(...),
	phone: Optional[str] = Form(default=None),
	address: Optional[str] = Form(default=None),
	tax_id: Optional[str] = Form(default=None),
):
	"""Add a new supplier."""
	val = (name or "").strip()
	if not val:
		return RedirectResponse(url="/costs", status_code=303)
	
	with get_session() as session:
		try:
			# Check if supplier with same name exists
			existing = session.exec(select(Supplier).where(Supplier.name == val)).first()
			if not existing:
				s = Supplier(
					name=val,
					phone=(phone or "").strip() or None,
					address=(address or "").strip() or None,
					tax_id=(tax_id or "").strip() or None,
				)
				session.add(s)
				session.commit()
		except Exception:
			# ignore errors; proceed
			pass
	
	return RedirectResponse(url="/costs", status_code=303)


@router.get("/{cost_id}")
def get_cost(cost_id: int):
	"""Get a single cost entry by ID."""
	with get_session() as session:
		cost = session.exec(select(Cost).where(Cost.id == cost_id)).first()
		if not cost:
			raise HTTPException(status_code=404, detail="Cost entry not found")
		
		cost_type = session.exec(select(CostType).where(CostType.id == cost.type_id)).first()
		account = session.exec(select(Account).where(Account.id == cost.account_id)).first() if cost.account_id else None
		supplier = session.exec(select(Supplier).where(Supplier.id == cost.supplier_id)).first() if cost.supplier_id else None
		product = session.exec(select(Product).where(Product.id == cost.product_id)).first() if cost.product_id else None
		
		return {
			"id": cost.id,
			"type_id": cost.type_id,
			"type_name": cost_type.name if cost_type else "",
			"account_id": cost.account_id,
			"account_name": account.name if account else "",
			"supplier_id": cost.supplier_id,
			"supplier_name": supplier.name if supplier else "",
			"product_id": cost.product_id,
			"product_name": product.name if product else "",
			"quantity": cost.quantity,
			"is_payment_to_supplier": cost.is_payment_to_supplier,
			"amount": cost.amount,
			"date": cost.date.isoformat() if cost.date else None,
			"details": cost.details,
		}


@router.post("/update")
def update_cost(
	request: Request,
	cost_id: int = Form(...),
	type_id: Optional[int] = Form(default=None),
	amount: Optional[float] = Form(default=None),
	date: Optional[str] = Form(default=None),
	details: Optional[str] = Form(default=None),
	account_id: Optional[str] = Form(default=None),
	supplier_id: Optional[str] = Form(default=None),
	product_id: Optional[str] = Form(default=None),
	quantity: Optional[str] = Form(default=None),
	is_payment_to_supplier: Optional[bool] = Form(default=None),
	start: Optional[str] = Form(default=None),
	end: Optional[str] = Form(default=None),
):
	"""Update an existing cost entry."""
	with get_session() as session:
		cost = session.exec(select(Cost).where(Cost.id == cost_id)).first()
		if not cost:
			raise HTTPException(status_code=404, detail="Cost entry not found")
		
		# Store old data for logging
		old_data = _cost_to_dict(cost)
		
		# Helper function to parse optional int from string
		def parse_optional_int(value: Optional[str]) -> Optional[int]:
			if value is None or value == "":
				return None
			try:
				return int(value)
			except (ValueError, TypeError):
				return None
		
		# Update fields if provided
		if type_id is not None:
			cost.type_id = int(type_id)
		if amount is not None:
			cost.amount = float(amount)
		if date is not None and date != "":
			try:
				cost.date = dt.date.fromisoformat(date)
			except Exception:
				pass
		if details is not None:
			cost.details = details.strip() if details else None
		# Handle optional int fields - convert empty strings to None
		if account_id is not None:
			cost.account_id = parse_optional_int(account_id)
		if supplier_id is not None:
			cost.supplier_id = parse_optional_int(supplier_id)
		if product_id is not None:
			cost.product_id = parse_optional_int(product_id)
		if quantity is not None:
			cost.quantity = parse_optional_int(quantity)
		if is_payment_to_supplier is not None:
			cost.is_payment_to_supplier = is_payment_to_supplier
		
		# Log update
		user_id = None  # TODO: Get from session if auth is implemented
		_log_cost_change(
			session,
			cost_id,
			"update",
			old_data=old_data,
			new_data=_cost_to_dict(cost),
			user_id=user_id
		)
		
		session.add(cost)
		session.commit()
	
	url = "/costs"
	if start or end:
		params = []
		if start:
			params.append(f"start={start}")
		if end:
			params.append(f"end={end}")
		url = f"{url}?{'&'.join(params)}"
	return RedirectResponse(url=url, status_code=303)


@router.delete("/{cost_id}")
def delete_cost(
	cost_id: int,
	request: Request,
	start: Optional[str] = Query(default=None),
	end: Optional[str] = Query(default=None),
):
	"""Delete a single cost entry."""
	with get_session() as session:
		cost = session.exec(select(Cost).where(Cost.id == cost_id)).first()
		if not cost:
			raise HTTPException(status_code=404, detail="Cost entry not found")
		
		# Store old data for logging
		old_data = _cost_to_dict(cost)
		
		# Delete existing history log records manually if FK constraint is not CASCADE yet
		# This is a workaround until the migration updates the constraint to CASCADE
		history_logs = session.exec(
			select(CostHistoryLog).where(CostHistoryLog.cost_id == cost_id)
		).all()
		for log in history_logs:
			session.delete(log)
		session.flush()  # Flush to ensure history logs are deleted before deleting cost
		
		# Delete cost entry
		session.delete(cost)
		session.commit()
	
	url = "/costs"
	if start or end:
		params = []
		if start:
			params.append(f"start={start}")
		if end:
			params.append(f"end={end}")
		url = f"{url}?{'&'.join(params)}"
	return RedirectResponse(url=url, status_code=303)


@router.get("/{cost_id}/history")
def get_cost_history(cost_id: int):
	"""Get history log for a cost entry."""
	with get_session() as session:
		# Verify cost exists (or was deleted)
		cost = session.exec(select(Cost).where(Cost.id == cost_id)).first()
		
		logs = session.exec(
			select(CostHistoryLog)
			.where(CostHistoryLog.cost_id == cost_id)
			.order_by(CostHistoryLog.created_at.desc())
		).all()
		
		return {
			"cost_id": cost_id,
			"cost_exists": cost is not None,
			"history": [
				{
					"id": log.id,
					"action": log.action,
					"old_data": json.loads(log.old_data_json) if log.old_data_json else None,
					"new_data": json.loads(log.new_data_json) if log.new_data_json else None,
					"user_id": log.user_id,
					"created_at": log.created_at.isoformat() if log.created_at else None,
				}
				for log in logs
			]
		}


