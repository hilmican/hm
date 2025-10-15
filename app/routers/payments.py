from fastapi import APIRouter, Query
from sqlmodel import select

from ..db import get_session
from ..models import Payment

router = APIRouter()


@router.get("")
@router.get("/")
def list_payments(limit: int = Query(default=100, ge=1, le=1000)):
	with get_session() as session:
		rows = session.exec(select(Payment).order_by(Payment.id.desc()).limit(limit)).all()
		return {
			"payments": [
				{
					"id": p.id or 0,
					"client_id": p.client_id,
					"order_id": p.order_id,
					"amount": p.amount,
					"date": p.date.isoformat() if p.date else None,
					"method": p.method,
				}
				for p in rows
			]
		}
