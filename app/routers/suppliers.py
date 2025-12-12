from typing import Optional
import datetime as dt

from fastapi import APIRouter, Request, Query, Form
from fastapi.responses import RedirectResponse
from sqlmodel import select
from sqlalchemy import func, and_

from ..db import get_session
from ..models import Supplier, Cost, Product, CostType, Account

router = APIRouter()


@router.get("")
@router.get("/")
def suppliers_page(request: Request):
	"""Supplier list page with debt/payment summary."""
	with get_session() as session:
		suppliers = session.exec(select(Supplier).order_by(Supplier.name.asc())).all()
		
		# Calculate debt and payment totals for each supplier
		supplier_data = []
		for supplier in suppliers:
			if supplier.id is None:
				continue
			
			# Get all costs for this supplier
			costs = session.exec(
				select(Cost)
				.where(Cost.supplier_id == supplier.id)
			).all()
			
			# Calculate debts (MERTER MAL ALIM costs, not payments)
			total_debt = sum(
				float(c.amount or 0) for c in costs
				if not c.is_payment_to_supplier and c.type_id == 9
			)
			
			# Calculate payments
			total_payment = sum(
				float(c.amount or 0) for c in costs
				if c.is_payment_to_supplier
			)
			
			# Remaining debt
			remaining_debt = total_debt - total_payment
			
			supplier_data.append({
				"supplier": supplier,
				"total_debt": total_debt,
				"total_payment": total_payment,
				"remaining_debt": remaining_debt,
			})
		
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"suppliers.html",
			{
				"request": request,
				"supplier_data": supplier_data,
			},
		)


@router.get("/{supplier_id}")
def supplier_detail(
	request: Request,
	supplier_id: int,
):
	"""Supplier detail page with debts, payments, and purchase history."""
	with get_session() as session:
		supplier = session.exec(select(Supplier).where(Supplier.id == supplier_id)).first()
		if not supplier:
			return RedirectResponse(url="/suppliers", status_code=303)
		
		# Get all costs for this supplier
		costs = session.exec(
			select(Cost)
			.where(Cost.supplier_id == supplier_id)
			.order_by(Cost.date.desc(), Cost.id.desc())
		).all()
		
		# Separate debts and payments
		debts = [c for c in costs if not c.is_payment_to_supplier and c.type_id == 9]
		payments = [c for c in costs if c.is_payment_to_supplier]
		
		# Get product and type maps
		products = session.exec(select(Product)).all()
		product_map = {p.id: p for p in products if p.id is not None}
		
		types = session.exec(select(CostType)).all()
		type_map = {t.id: t.name for t in types if t.id is not None}
		
		# Get accounts for payment form
		accounts = session.exec(select(Account).where(Account.is_active == True).order_by(Account.name.asc())).all()
		
		# Calculate totals
		total_debt = sum(float(c.amount or 0) for c in debts)
		total_payment = sum(float(c.amount or 0) for c in payments)
		remaining_debt = total_debt - total_payment
		
		# Get purchase history (MERTER MAL ALIM with product info)
		purchase_history = []
		for cost in debts:
			if cost.product_id and cost.product_id in product_map:
				purchase_history.append({
					"date": cost.date,
					"product": product_map[cost.product_id],
					"quantity": cost.quantity or 0,
					"unit_price": (cost.amount or 0) / (cost.quantity or 1) if cost.quantity else cost.amount or 0,
					"total": cost.amount or 0,
					"details": cost.details,
				})
		
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"supplier_detail.html",
			{
				"request": request,
				"supplier": supplier,
				"debts": debts,
				"payments": payments,
				"purchase_history": purchase_history,
				"product_map": product_map,
				"type_map": type_map,
				"accounts": accounts,
				"total_debt": total_debt,
				"total_payment": total_payment,
				"remaining_debt": remaining_debt,
			},
		)


@router.post("/add")
def add_supplier(
	name: str = Form(...),
	phone: Optional[str] = Form(default=None),
	address: Optional[str] = Form(default=None),
	tax_id: Optional[str] = Form(default=None),
):
	"""Add a new supplier."""
	val = (name or "").strip()
	if not val:
		return RedirectResponse(url="/suppliers", status_code=303)
	
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
	
	return RedirectResponse(url="/suppliers", status_code=303)


@router.post("/{supplier_id}/add-payment")
def add_payment_to_supplier(
	supplier_id: int,
	amount: float = Form(...),
	date: Optional[str] = Form(default=None),
	account_id: Optional[int] = Form(default=None),
	details: Optional[str] = Form(default=None),
):
	"""Add a payment to supplier (creates a cost with is_payment_to_supplier=True)."""
	try:
		when = dt.date.fromisoformat(date) if date else dt.date.today()
	except Exception:
		when = dt.date.today()
	
	with get_session() as session:
		# Verify supplier exists
		supplier = session.exec(select(Supplier).where(Supplier.id == supplier_id)).first()
		if not supplier:
			return RedirectResponse(url="/suppliers", status_code=303)
		
		try:
			# Create a cost entry as payment
			# We need a type_id - use a default or find/create one
			# For now, we'll use type_id=1 as default (should be handled better in production)
			c = Cost(
				type_id=1,  # Default type - should be improved
				amount=float(amount),
				date=when,
				details=(details or "").strip() or None,
				account_id=int(account_id) if account_id else None,
				supplier_id=supplier_id,
				is_payment_to_supplier=True,
			)
			session.add(c)
			session.commit()
		except Exception:
			# best-effort insert; ignore on error
			pass
	
	return RedirectResponse(url=f"/suppliers/{supplier_id}", status_code=303)

