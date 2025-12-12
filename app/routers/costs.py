from typing import Optional, List
import datetime as dt

from fastapi import APIRouter, Request, Query, Form
from fastapi.responses import RedirectResponse
from sqlmodel import select, delete

from ..db import get_session
from ..models import Cost, CostType, Account, Supplier, Product


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
		if not product_id:
			# Redirect with error - product required
			url = "/costs"
			if start or end:
				params = []
				if start:
					params.append(f"start={start}")
				if end:
					params.append(f"end={end}")
				url = f"/costs?{'&'.join(params)}"
			return RedirectResponse(url=url, status_code=303)
		if not quantity or quantity <= 0:
			# Redirect with error - quantity required
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
			session.commit()
		except Exception:
			# best-effort insert; ignore on error
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


