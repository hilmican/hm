from fastapi import APIRouter, Query
from sqlmodel import select

from ..db import get_session
from ..models import Order

router = APIRouter()


@router.get("")
@router.get("/")
def list_orders(limit: int = Query(default=100, ge=1, le=1000)):
	with get_session() as session:
		rows = session.exec(select(Order).order_by(Order.id.desc()).limit(limit)).all()
		return {
			"orders": [
				{
					"id": o.id or 0,
					"tracking_no": o.tracking_no,
					"client_id": o.client_id,
					"item_id": o.item_id,
					"quantity": o.quantity,
					"total_amount": o.total_amount,
					"shipment_date": o.shipment_date.isoformat() if o.shipment_date else None,
					"source": o.source,
				}
				for o in rows
			]
		}
