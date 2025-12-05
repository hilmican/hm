from typing import Optional
import datetime as dt

from fastapi import APIRouter, Request, Query, Form
from fastapi.responses import RedirectResponse
from sqlmodel import select

from ..db import get_session
from ..models import Cost, CostType, Account


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
		rows = session.exec(
			select(Cost)
			.where(Cost.date.is_not(None))
			.where(Cost.date >= start_date)
			.where(Cost.date <= end_date)
			.order_by(Cost.date.desc(), Cost.id.desc())
		).all()
		type_map = {t.id: t.name for t in types if t.id is not None}
		account_map = {a.id: a.name for a in accounts if a.id is not None}
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"costs.html",
			{
				"request": request,
				"start": start_date,
				"end": end_date,
				"types": types,
				"accounts": accounts,
				"rows": rows,
				"type_map": type_map,
				"account_map": account_map,
				"today": today,
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
	start: Optional[str] = Form(default=None),
	end: Optional[str] = Form(default=None),
):
	try:
		when = dt.date.fromisoformat(date) if date else dt.date.today()
	except Exception:
		when = dt.date.today()
	with get_session() as session:
		try:
			c = Cost(
				type_id=int(type_id),
				amount=float(amount),
				date=when,
				details=(details or "").strip() or None,
				account_id=int(account_id) if account_id else None,
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


