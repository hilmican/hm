from typing import Optional
import datetime as dt

from fastapi import APIRouter, Request, Query, Form
from fastapi.responses import RedirectResponse
from sqlmodel import select
from sqlalchemy import func, and_

from ..db import get_session
from ..models import Supplier, Cost, Product, CostType, Account, SupplierPaymentAllocation

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
			
			# Get all costs for this supplier (exclude soft-deleted)
			costs = session.exec(
				select(Cost)
				.where(Cost.supplier_id == supplier.id)
				.where(Cost.deleted_at.is_(None))
			).all()
			
			# Get debts (MERTER MAL ALIM costs, not payments)
			debts = [c for c in costs if not c.is_payment_to_supplier and c.type_id == 9]
			total_debt = sum(float(c.amount or 0) for c in debts)
			
			# Get payment allocations
			debt_ids = [d.id for d in debts if d.id is not None]
			total_closed = 0.0
			if debt_ids:
				allocs = session.exec(
					select(SupplierPaymentAllocation)
					.where(SupplierPaymentAllocation.debt_cost_id.in_(debt_ids))
				).all()
				total_closed = sum(float(a.amount or 0) for a in allocs)
			
			# Calculate payments
			total_payment = sum(
				float(c.amount or 0) for c in costs
				if c.is_payment_to_supplier
			)
			
			# Remaining debt (based on closed amounts, not just payments)
			remaining_debt = total_debt - total_closed
			
			supplier_data.append({
				"supplier": supplier,
				"total_debt": total_debt,
				"total_closed": total_closed,
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
		
		# Get all costs for this supplier (exclude soft-deleted)
		costs = session.exec(
			select(Cost)
			.where(Cost.supplier_id == supplier_id)
			.where(Cost.deleted_at.is_(None))
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
		account_map = {a.id: a.name for a in accounts if a.id is not None}
		
		# Get payment allocations to calculate closed amounts per debt
		all_debt_ids = [d.id for d in debts if d.id is not None]
		allocations = {}
		payment_allocations = {}  # Track how much each payment has allocated
		if all_debt_ids:
			allocs = session.exec(
				select(SupplierPaymentAllocation)
				.where(SupplierPaymentAllocation.debt_cost_id.in_(all_debt_ids))
			).all()
			for alloc in allocs:
				if alloc.debt_cost_id not in allocations:
					allocations[alloc.debt_cost_id] = 0.0
				allocations[alloc.debt_cost_id] += float(alloc.amount or 0)
				
				# Track allocation per payment
				if alloc.payment_cost_id not in payment_allocations:
					payment_allocations[alloc.payment_cost_id] = 0.0
				payment_allocations[alloc.payment_cost_id] += float(alloc.amount or 0)
		
		# Calculate remaining debt per entry and totals
		debt_details = []
		for debt in debts:
			closed_amount = allocations.get(debt.id, 0.0)
			debt_amount = float(debt.amount or 0)
			remaining = debt_amount - closed_amount
			debt_details.append({
				"debt": debt,
				"closed_amount": closed_amount,
				"remaining": remaining,
			})
		
		# Calculate totals
		total_debt = sum(float(c.amount or 0) for c in debts)
		total_closed = sum(d["closed_amount"] for d in debt_details)
		total_payment = sum(float(c.amount or 0) for c in payments)
		remaining_debt = total_debt - total_closed
		
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
		
		# Calculate allocated amount for each payment
		payment_details = []
		for payment in payments:
			allocated = payment_allocations.get(payment.id, 0.0) if payment.id else 0.0
			payment_amount = float(payment.amount or 0)
			remaining = payment_amount - allocated
			payment_details.append({
				"payment": payment,
				"allocated": allocated,
				"remaining": remaining,
			})
		
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"supplier_detail.html",
			{
				"request": request,
				"supplier": supplier,
				"debts": debts,
				"debt_details": debt_details,
				"payments": payments,
				"payment_details": payment_details,
				"purchase_history": purchase_history,
				"product_map": product_map,
				"type_map": type_map,
				"account_map": account_map,
				"accounts": accounts,
				"total_debt": total_debt,
				"total_closed": total_closed,
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
	debt_allocations: Optional[str] = Form(default=None),  # JSON string: {debt_id: amount}
):
	"""Add a payment to supplier and allocate it to specific debts."""
	try:
		when = dt.date.fromisoformat(date) if date else dt.date.today()
	except Exception:
		when = dt.date.today()
	
	import json
	
	with get_session() as session:
		# Verify supplier exists
		supplier = session.exec(select(Supplier).where(Supplier.id == supplier_id)).first()
		if not supplier:
			return RedirectResponse(url="/suppliers", status_code=303)
		
		try:
			# Create a cost entry as payment
			# Use MERTER MAL ALIM type (type_id=9) for supplier payments since they're payments against MERTER MAL ALIM debts
			# This ensures they're properly categorized and excluded from cost calculations
			type_id = 9  # MERTER MAL ALIM type
			
			payment_cost = Cost(
				type_id=type_id,
				amount=float(amount),
				date=when,
				details=(details or "").strip() or None,
				account_id=int(account_id) if account_id else None,
				supplier_id=supplier_id,
				is_payment_to_supplier=True,
			)
			session.add(payment_cost)
			session.flush()  # Get the ID
			
			# Parse debt allocations if provided
			if debt_allocations:
				try:
					allocations = json.loads(debt_allocations)
					payment_amount = float(amount)
					remaining_payment = payment_amount
					
					# Get all debts for this supplier (exclude soft-deleted)
					debts = session.exec(
						select(Cost)
						.where(Cost.supplier_id == supplier_id)
						.where(Cost.is_payment_to_supplier == False)
						.where(Cost.type_id == 9)
						.where(Cost.deleted_at.is_(None))
						.order_by(Cost.date.asc(), Cost.id.asc())  # FIFO: oldest first
					).all()
					
					# Get existing allocations to calculate remaining debt per entry
					debt_ids = [d.id for d in debts if d.id is not None]
					existing_allocations = {}
					if debt_ids:
						existing = session.exec(
							select(SupplierPaymentAllocation)
							.where(SupplierPaymentAllocation.debt_cost_id.in_(debt_ids))
						).all()
						for alloc in existing:
							if alloc.debt_cost_id not in existing_allocations:
								existing_allocations[alloc.debt_cost_id] = 0.0
							existing_allocations[alloc.debt_cost_id] += float(alloc.amount or 0)
					
					# Calculate total allocation amount first to validate
					total_allocation = 0.0
					if allocations:
						for debt in debts:
							if debt.id is None:
								continue
							if str(debt.id) in allocations:
								alloc_val = float(allocations[str(debt.id)])
								total_allocation += alloc_val
					
					# Validate total allocation doesn't exceed payment (with small epsilon for floating point)
					epsilon = 0.01
					if total_allocation > payment_amount + epsilon:
						# Skip allocations if total exceeds payment
						pass
					else:
						# Allocate payment to debts
						for debt in debts:
							if remaining_payment <= 0:
								break
							if debt.id is None:
								continue
							
							debt_amount = float(debt.amount or 0)
							already_closed = existing_allocations.get(debt.id, 0.0)
							remaining_debt = debt_amount - already_closed
							
							if remaining_debt <= 0:
								continue
							
							# Check if this debt is in the allocations dict
							alloc_amount = 0.0
							if allocations and str(debt.id) in allocations:
								alloc_amount = float(allocations[str(debt.id)])
							elif not allocations:
								# Auto-allocate: use remaining payment up to remaining debt
								alloc_amount = min(remaining_payment, remaining_debt)
							
							# Use epsilon for floating point comparison
							if alloc_amount > 0 and alloc_amount <= remaining_payment + epsilon:
								# Don't allocate more than remaining debt
								alloc_amount = min(alloc_amount, remaining_debt)
								
								allocation = SupplierPaymentAllocation(
									payment_cost_id=payment_cost.id,  # type: ignore
									debt_cost_id=debt.id,
									amount=alloc_amount,
								)
								session.add(allocation)
								remaining_payment -= alloc_amount
					
					# If there's remaining payment and no specific allocations, auto-allocate to oldest debts
					if remaining_payment > 0 and not allocations:
						for debt in debts:
							if remaining_payment <= 0:
								break
							if debt.id is None:
								continue
							
							debt_amount = float(debt.amount or 0)
							already_closed = existing_allocations.get(debt.id, 0.0)
							remaining_debt = debt_amount - already_closed
							
							if remaining_debt > 0:
								alloc_amount = min(remaining_payment, remaining_debt)
								allocation = SupplierPaymentAllocation(
									payment_cost_id=payment_cost.id,  # type: ignore
									debt_cost_id=debt.id,
									amount=alloc_amount,
								)
								session.add(allocation)
								remaining_payment -= alloc_amount
					
				except Exception as e:
					# If allocation fails, still save the payment
					pass
			
			session.commit()
		except Exception:
			# best-effort insert; ignore on error
			session.rollback()
			pass
	
	return RedirectResponse(url=f"/suppliers/{supplier_id}", status_code=303)


@router.post("/{supplier_id}/allocate-payment/{payment_id}")
def allocate_existing_payment_to_debts(
	supplier_id: int,
	payment_id: int,
	debt_allocations: Optional[str] = Form(default=None),  # JSON string: {debt_id: amount}
):
	"""Allocate an existing payment to specific debts."""
	import json
	
	with get_session() as session:
		# Verify supplier exists
		supplier = session.exec(select(Supplier).where(Supplier.id == supplier_id)).first()
		if not supplier:
			return RedirectResponse(url="/suppliers", status_code=303)
		
		# Verify payment exists and belongs to this supplier (exclude soft-deleted)
		payment = session.exec(
			select(Cost)
			.where(Cost.id == payment_id)
			.where(Cost.supplier_id == supplier_id)
			.where(Cost.is_payment_to_supplier == True)
			.where(Cost.deleted_at.is_(None))
		).first()
		if not payment:
			return RedirectResponse(url=f"/suppliers/{supplier_id}", status_code=303)
		
		payment_amount = float(payment.amount or 0)
		
		# Get existing allocations for this payment
		existing_allocs = session.exec(
			select(SupplierPaymentAllocation)
			.where(SupplierPaymentAllocation.payment_cost_id == payment_id)
		).all()
		existing_total = sum(float(a.amount or 0) for a in existing_allocs)
		remaining_payment = payment_amount - existing_total
		
		# Parse debt allocations if provided
		if debt_allocations:
			try:
				allocations = json.loads(debt_allocations)
				
				# Get all debts for this supplier (exclude soft-deleted)
				debts = session.exec(
					select(Cost)
					.where(Cost.supplier_id == supplier_id)
					.where(Cost.is_payment_to_supplier == False)
					.where(Cost.type_id == 9)
					.where(Cost.deleted_at.is_(None))
					.order_by(Cost.date.asc(), Cost.id.asc())  # FIFO: oldest first
				).all()
				
				# Get existing allocations to calculate remaining debt per entry
				debt_ids = [d.id for d in debts if d.id is not None]
				existing_debt_allocations = {}
				if debt_ids:
					existing = session.exec(
						select(SupplierPaymentAllocation)
						.where(SupplierPaymentAllocation.debt_cost_id.in_(debt_ids))
					).all()
					for alloc in existing:
						if alloc.debt_cost_id not in existing_debt_allocations:
							existing_debt_allocations[alloc.debt_cost_id] = 0.0
						existing_debt_allocations[alloc.debt_cost_id] += float(alloc.amount or 0)
				
				# Calculate total allocation amount first to validate
				total_allocation = 0.0
				if allocations:
					for debt in debts:
						if debt.id is None:
							continue
						if str(debt.id) in allocations:
							alloc_val = float(allocations[str(debt.id)])
							total_allocation += alloc_val
				
				# Validate total allocation doesn't exceed remaining payment (with small epsilon for floating point)
				epsilon = 0.01
				if total_allocation <= remaining_payment + epsilon:
					# Allocate payment to debts
					for debt in debts:
						if remaining_payment <= 0:
							break
						if debt.id is None:
							continue
						
						debt_amount = float(debt.amount or 0)
						already_closed = existing_debt_allocations.get(debt.id, 0.0)
						remaining_debt = debt_amount - already_closed
						
						if remaining_debt <= 0:
							continue
						
						# Check if this debt is in the allocations dict
						alloc_amount = 0.0
						if allocations and str(debt.id) in allocations:
							alloc_amount = float(allocations[str(debt.id)])
						
						# Use epsilon for floating point comparison
						if alloc_amount > 0 and alloc_amount <= remaining_payment + epsilon:
							# Don't allocate more than remaining debt
							alloc_amount = min(alloc_amount, remaining_debt)
							
							allocation = SupplierPaymentAllocation(
								payment_cost_id=payment_id,
								debt_cost_id=debt.id,
								amount=alloc_amount,
							)
							session.add(allocation)
							remaining_payment -= alloc_amount
				
			except Exception as e:
				# If allocation fails, still proceed
				pass
		
		session.commit()
	
	return RedirectResponse(url=f"/suppliers/{supplier_id}", status_code=303)

