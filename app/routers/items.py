from fastapi import APIRouter, Query
from sqlmodel import select

from ..db import get_session
from ..models import Item

router = APIRouter()


@router.get("")
@router.get("/")
def list_items(limit: int = Query(default=100, ge=1, le=1000)):
	with get_session() as session:
		rows = session.exec(select(Item).order_by(Item.id.desc()).limit(limit)).all()
		return {
			"items": [
				{
					"id": it.id or 0,
					"sku": it.sku,
					"name": it.name,
					"unit": it.unit,
				}
				for it in rows
			]
		}
