from fastapi import APIRouter, Query, Request
from sqlmodel import select

from ..db import get_session
from ..models import Item
from ..services.inventory import get_stock_map
from fastapi import HTTPException

router = APIRouter()


@router.get("")
@router.get("/")
def list_items(limit: int = Query(default=100, ge=1, le=1000)):
	with get_session() as session:
		rows = session.exec(select(Item).order_by(Item.id.desc()).limit(limit)).all()
		stock_map = get_stock_map(session)
		return {
			"items": [
				{
					"id": it.id or 0,
					"sku": it.sku,
					"name": it.name,
					"unit": it.unit,
					"product_id": it.product_id,
					"size": it.size,
					"color": it.color,
					"pack_type": it.pack_type,
					"price": it.price,
					"cost": it.cost,
					"on_hand": stock_map.get(it.id or 0, 0),
				}
				for it in rows
			]
		}


@router.get("/table")
def list_items_table(request: Request, limit: int = Query(default=1000000, ge=1, le=1000000)):
	with get_session() as session:
		rows = session.exec(select(Item).order_by(Item.id.desc()).limit(limit)).all()
		stock_map = get_stock_map(session)
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"items_table.html",
			{"request": request, "rows": rows, "limit": limit, "stock_map": stock_map},
		)


@router.get("/variants")
def list_variants(limit: int = Query(default=1000, ge=1, le=10000)):
	with get_session() as session:
		rows = session.exec(select(Item).order_by(Item.id.desc()).limit(limit)).all()
		stock_map = get_stock_map(session)
		return {
			"variants": [
				{
					"id": it.id or 0,
					"sku": it.sku,
					"name": it.name,
					"product_id": it.product_id,
					"size": it.size,
					"color": it.color,
					"pack_type": it.pack_type,
					"price": it.price,
					"on_hand": stock_map.get(it.id or 0, 0),
				}
				for it in rows
			]
		}


@router.put("/{item_id}/price")
def update_price(item_id: int, price: float):
	with get_session() as session:
		it = session.exec(select(Item).where(Item.id == item_id)).first()
		if not it:
			raise HTTPException(status_code=404, detail="Item not found")
		it.price = price
		return {"status": "ok", "item_id": it.id, "price": it.price}
