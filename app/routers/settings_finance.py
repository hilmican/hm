from typing import Optional, Dict

from fastapi import APIRouter, Request, Form
from fastapi.responses import RedirectResponse
from sqlmodel import select

from ..db import get_session
from ..models import SystemSetting, Account


router = APIRouter(prefix="/settings", tags=["settings"])


def _get_settings(session) -> Dict[str, str]:
	rows = session.exec(select(SystemSetting)).all()
	return {r.key: r.value for r in rows}


def _set_setting(session, key: str, value: str, description: Optional[str] = None) -> None:
	row = session.exec(select(SystemSetting).where(SystemSetting.key == key)).first()
	if row:
		row.value = value
		row.description = description or row.description
	else:
		row = SystemSetting(key=key, value=value, description=description)
		session.add(row)
	session.flush()


@router.get("/finance")
def finance_settings_page(request: Request):
	with get_session() as session:
		accounts = session.exec(select(Account).where(Account.is_active == True).order_by(Account.name.asc())).all()
		settings = _get_settings(session)
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"settings_finance.html",
			{
				"request": request,
				"accounts": accounts,
				"settings": settings,
			},
		)


@router.post("/finance")
def finance_settings_save(
	default_cash_account_id: Optional[int] = Form(default=None),
	default_bank_account_id: Optional[int] = Form(default=None),
):
	with get_session() as session:
		if default_cash_account_id:
			_set_setting(session, "pos_income_cash_account_id", str(default_cash_account_id), "POS cash income account")
		if default_bank_account_id:
			_set_setting(session, "pos_income_bank_account_id", str(default_bank_account_id), "POS bank/IBAN income account")
	return RedirectResponse(url="/settings/finance", status_code=303)

