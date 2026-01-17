from typing import Optional
import uuid
import datetime as dt

from fastapi import APIRouter, Request, Query, Form, HTTPException
from fastapi.responses import RedirectResponse
from sqlmodel import select

from ..db import get_session
from ..models import Account, Income, Cost, CostType, Supplier
from ..services.finance import calculate_account_balance, get_account_balances

router = APIRouter()


@router.get("")
@router.get("/")
def list_accounts(limit: int = Query(default=1000, ge=1, le=10000)):
	"""List all accounts with current balances."""
	with get_session() as session:
		accounts = session.exec(select(Account).order_by(Account.name.asc()).limit(limit)).all()
		balances = get_account_balances(session)
		
		return {
			"accounts": [
				{
					"id": acc.id,
					"name": acc.name,
					"type": acc.type,
					"iban": acc.iban,
					"initial_balance": acc.initial_balance,
					"current_balance": balances.get(acc.id, 0.0),
					"is_active": acc.is_active,
					"notes": acc.notes,
				}
				for acc in accounts
				if acc.id is not None
			]
		}


@router.get("/table")
def accounts_table(request: Request):
	"""HTML table view of accounts."""
	with get_session() as session:
		accounts = session.exec(select(Account).order_by(Account.name.asc())).all()
		balances = get_account_balances(session)
		
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"accounts.html",
			{
				"request": request,
				"accounts": accounts,
				"balances": balances,
				"today": dt.date.today(),
			},
		)


@router.post("/add")
def add_account(
	name: str = Form(...),
	type: str = Form(...),
	iban: Optional[str] = Form(default=None),
	initial_balance: float = Form(default=0.0),
	notes: Optional[str] = Form(default=None),
):
	"""Create a new account."""
	with get_session() as session:
		try:
			account = Account(
				name=name.strip(),
				type=type.strip(),
				iban=iban.strip() if iban else None,
				initial_balance=float(initial_balance),
				notes=notes.strip() if notes else None,
			)
			session.add(account)
			session.commit()
		except Exception as e:
			session.rollback()
			# Best effort - redirect anyway
			pass
	
	return RedirectResponse(url="/accounts/table", status_code=303)


@router.post("/{account_id}/update")
def update_account(
	account_id: int,
	name: Optional[str] = Form(default=None),
	type: Optional[str] = Form(default=None),
	iban: Optional[str] = Form(default=None),
	initial_balance: Optional[float] = Form(default=None),
	notes: Optional[str] = Form(default=None),
	is_active: Optional[bool] = Form(default=None),
):
	"""Update an existing account."""
	with get_session() as session:
		account = session.exec(select(Account).where(Account.id == account_id)).first()
		if not account:
			return RedirectResponse(url="/accounts/table", status_code=303)
		
		if name is not None:
			account.name = name.strip()
		if type is not None:
			account.type = type.strip()
		if iban is not None:
			account.iban = iban.strip() if iban else None
		if initial_balance is not None:
			account.initial_balance = float(initial_balance)
		if notes is not None:
			account.notes = notes.strip() if notes else None
		if is_active is not None:
			account.is_active = is_active
		
		account.updated_at = dt.datetime.utcnow()
		session.add(account)
		session.commit()
	
	return RedirectResponse(url="/accounts/table", status_code=303)


@router.get("/{account_id}/transactions")
def account_transactions(request: Request, account_id: int, start: Optional[str] = Query(default=None), end: Optional[str] = Query(default=None)):
	"""View all transactions (income and expenses) for an account."""
	def _parse_date(value: Optional[str]) -> Optional[dt.date]:
		if not value:
			return None
		try:
			return dt.date.fromisoformat(value)
		except:
			return None
	
	start_date = _parse_date(start)
	end_date = _parse_date(end)
	
	with get_session() as session:
		account = session.exec(select(Account).where(Account.id == account_id)).first()
		if not account:
			return RedirectResponse(url="/accounts/table", status_code=303)
		
		# Get income entries (exclude soft-deleted)
		income_q = select(Income).where(Income.account_id == account_id).where(Income.deleted_at.is_(None))
		if start_date:
			income_q = income_q.where(Income.date >= start_date)
		if end_date:
			income_q = income_q.where(Income.date <= end_date)
		incomes = session.exec(income_q.order_by(Income.date.desc(), Income.id.desc())).all()
		
		# Get expenses (exclude soft-deleted)
		expense_q = select(Cost).where(Cost.account_id == account_id).where(Cost.deleted_at.is_(None))
		if start_date:
			expense_q = expense_q.where(Cost.date >= start_date)
		if end_date:
			expense_q = expense_q.where(Cost.date <= end_date)
		expenses = session.exec(expense_q.order_by(Cost.date.desc(), Cost.id.desc())).all()
		
		# Prefetch cost type and supplier names for display
		type_ids = sorted({exp.type_id for exp in expenses if exp.type_id})
		supplier_ids = sorted({exp.supplier_id for exp in expenses if exp.supplier_id})
		type_map = (
			{t.id: t.name for t in session.exec(select(CostType).where(CostType.id.in_(type_ids))).all() if t.id is not None}
			if type_ids else {}
		)
		supplier_map = (
			{s.id: s.name for s in session.exec(select(Supplier).where(Supplier.id.in_(supplier_ids))).all() if s.id is not None}
			if supplier_ids else {}
		)
		
		# Combine and sort by date
		transactions = []
		for inc in incomes:
			transactions.append({
				"type": "income",
				"id": inc.id,
				"date": inc.date,
				"amount": inc.amount,
				"description": f"{inc.source} - {inc.reference or ''}",
				"notes": inc.notes,
				"cost_type_name": None,
				"supplier_name": None,
			})
		for exp in expenses:
			transactions.append({
				"type": "expense",
				"id": exp.id,
				"date": exp.date,
				"amount": -exp.amount,  # Negative for expenses
				"description": exp.details or "",
				"notes": None,
				"cost_type_name": type_map.get(exp.type_id),
				"supplier_name": supplier_map.get(exp.supplier_id),
			})
		
		# Sort by date descending
		transactions.sort(key=lambda x: x["date"] or dt.date.min, reverse=True)
		
		# Calculate running balance
		balance = float(account.initial_balance or 0.0)
		for txn in reversed(transactions):  # Process oldest first
			balance += txn["amount"]
			txn["running_balance"] = balance
		
		current_balance = calculate_account_balance(session, account_id)
		
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"account_transactions.html",
			{
				"request": request,
				"account": account,
				"transactions": transactions,
				"current_balance": current_balance,
				"start": start_date,
				"end": end_date,
			},
		)


def _get_or_create_transfer_cost_type(session) -> int:
	"""Ensure a CostType named 'Virman' exists and return its id."""
	ct = session.exec(select(CostType).where(CostType.name == "Virman")).first()
	if ct and ct.id:
		return ct.id
	new_ct = CostType(name="Virman")
	session.add(new_ct)
	session.flush()
	if not new_ct.id:
		raise HTTPException(status_code=500, detail="Virman maliyet türü oluşturulamadı")
	return new_ct.id


@router.post("/transfer")
def transfer_between_accounts(
	source_account_id: int = Form(...),
	dest_account_id: int = Form(...),
	amount: float = Form(...),
	date: Optional[str] = Form(default=None),
	notes: Optional[str] = Form(default=None),
):
	"""Create a single-step internal transfer (cost + income)."""
	if source_account_id == dest_account_id:
		raise HTTPException(status_code=400, detail="Kaynak ve hedef hesap aynı olamaz")
	try:
		when = dt.date.fromisoformat(date) if date else dt.date.today()
	except Exception:
		when = dt.date.today()
	if amount <= 0:
		raise HTTPException(status_code=400, detail="Tutar 0'dan büyük olmalı")

	with get_session() as session:
		source = session.exec(select(Account).where(Account.id == source_account_id)).first()
		dest = session.exec(select(Account).where(Account.id == dest_account_id)).first()
		if not source or not dest:
			raise HTTPException(status_code=404, detail="Hesap bulunamadı")
		virman_type_id = _get_or_create_transfer_cost_type(session)

		detail_text = notes.strip() if notes else f"Virman {source.name} -> {dest.name}"
		ref = f"transfer:{uuid.uuid4()}"

		out_cost = Cost(
			type_id=virman_type_id,
			account_id=source_account_id,
			amount=float(amount),
			date=when,
			details=f"{detail_text} [{ref}]",
			is_payment_to_supplier=False,
		)
		in_income = Income(
			account_id=dest_account_id,
			amount=float(amount),
			date=when,
			source="internal_transfer",
			reference=ref,
			notes=detail_text,
		)
		session.add(out_cost)
		session.add(in_income)
		session.commit()

	return RedirectResponse(url="/accounts/table", status_code=303)


@router.get("/{account_id}/balance")
def get_account_balance(account_id: int):
	"""Get current balance for an account."""
	with get_session() as session:
		balance = calculate_account_balance(session, account_id)
		return {"account_id": account_id, "balance": balance}

