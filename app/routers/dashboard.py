from fastapi import APIRouter, Request
from sqlmodel import select

from ..db import get_session
from ..models import Client, Item, Order, Payment

router = APIRouter()


@router.get("/dashboard")
def dashboard(request: Request):
	# pull small samples for quick display
	with get_session() as session:
		clients = session.exec(select(Client).order_by(Client.id.desc()).limit(20)).all()
		items = session.exec(select(Item).order_by(Item.id.desc()).limit(20)).all()
		orders = session.exec(select(Order).order_by(Order.id.desc()).limit(20)).all()
		payments = session.exec(select(Payment).order_by(Payment.id.desc()).limit(20)).all()

		templates = request.app.state.templates
		return templates.TemplateResponse(
			"dashboard.html",
			{
				"request": request,
				"clients": clients,
				"items": items,
				"orders": orders,
				"payments": payments,
			},
		)
