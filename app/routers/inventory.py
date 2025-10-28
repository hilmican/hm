from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from sqlmodel import select

from ..db import get_session
from ..models import Item, Product, StockMovement
from ..services.inventory import get_stock_map, recalc_orders_from_mappings, adjust_stock


router = APIRouter(prefix="/inventory", tags=["inventory"])


@router.get("/attributes")
def product_attributes(product_id: int = Query(...)):
	with get_session() as session:
		# distinct sizes for the given product (exclude nulls/empty)
		size_rows = session.exec(
			select(Item.size).where(
				Item.product_id == product_id,
				Item.size != None,
				(Item.status.is_(None)) | (Item.status != "inactive"),
			).distinct()
		).all()
		color_rows = session.exec(
			select(Item.color).where(
				Item.product_id == product_id,
				Item.color != None,
				(Item.status.is_(None)) | (Item.status != "inactive"),
			).distinct()
		).all()
		# SQLModel may return scalars for single-column selects; avoid indexing into strings
		def _extract(rows):
			vals = []
			for r in rows:
				v = r
				if isinstance(r, (list, tuple)):
					v = r[0]
				if v:
					vals.append(v)
			return sorted(set(vals))
		sizes = _extract(size_rows)
		colors = _extract(color_rows)
		return {"sizes": sizes, "colors": colors}

@router.get("/stock")
def list_stock(product_id: Optional[int] = Query(default=None), size: Optional[str] = Query(default=None), color: Optional[str] = Query(default=None), limit: int = Query(default=1000, ge=1, le=10000)):
	with get_session() as session:
		q = select(Item).where((Item.status.is_(None)) | (Item.status != "inactive")).order_by(Item.id.desc())
		if product_id:
			q = q.where(Item.product_id == product_id)
		if size:
			q = q.where(Item.size == size)
		if color:
			q = q.where(Item.color == color)
		rows = session.exec(q.limit(limit)).all()
		stock_map = get_stock_map(session)
		return {
			"items": [
				{
					"id": it.id or 0,
					"sku": it.sku,
					"name": it.name,
					"product_id": it.product_id,
					"size": it.size,
					"color": it.color,
					"price": it.price,
					"cost": it.cost,
					"on_hand": stock_map.get(it.id or 0, 0),
				}
				for it in rows
			]
		}


@router.get("/table")
def stock_table(request: Request, product_id: Optional[int] = Query(default=None), size: Optional[str] = Query(default=None), color: Optional[str] = Query(default=None), limit: int = Query(default=10000, ge=1, le=100000)):
	with get_session() as session:
		q = select(Item).where((Item.status.is_(None)) | (Item.status != "inactive")).order_by(Item.id.desc())
		if product_id:
			q = q.where(Item.product_id == product_id)
		if size:
			q = q.where(Item.size == size)
		if color:
			q = q.where(Item.color == color)
		rows = session.exec(q.limit(limit)).all()
		stock_map = get_stock_map(session)
		# Build product_id -> name map for display
		pids = sorted({it.product_id for it in rows if it.product_id})
		pmap = {}
		if pids:
			prows = session.exec(select(Product).where(Product.id.in_(pids))).all()
			for p in prows:
				if p.id is not None:
					pmap[p.id] = p.name
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"inventory_table.html",
			{"request": request, "rows": rows, "stock_map": stock_map, "product_map": pmap, "limit": limit},
		)


@router.post("/movements")
def create_movement(body: Dict[str, Any]):
    item_id = body.get("item_id")
    delta = body.get("delta")
    direction = body.get("direction")
    quantity = body.get("quantity")
    if item_id is None:
        raise HTTPException(status_code=400, detail="item_id required")
    # Support either {delta} (may be negative) or {direction, quantity>0}
    if delta is not None:
        try:
            d = int(delta)
        except Exception:
            raise HTTPException(status_code=400, detail="delta must be integer")
        if d == 0:
            return {"status": "noop"}
        with get_session() as session:
            it = session.exec(select(Item).where(Item.id == item_id)).first()
            if not it:
                raise HTTPException(status_code=404, detail="Item not found")
            adjust_stock(session, item_id=item_id, delta=d, related_order_id=None)
            return {"status": "ok"}
    # Fallback to direction/quantity path
    if direction not in ("in", "out") or not isinstance(quantity, int) or quantity <= 0:
        raise HTTPException(status_code=400, detail="Provide delta or direction in|out and quantity>0")
    with get_session() as session:
        it = session.exec(select(Item).where(Item.id == item_id)).first()
        if not it:
            raise HTTPException(status_code=404, detail="Item not found")
        mv = StockMovement(item_id=item_id, direction=direction, quantity=quantity)
        session.add(mv)
        return {"status": "ok", "movement_id": mv.id or 0}


@router.get("/movements")
def list_movements(item_id: Optional[int] = Query(default=None), limit: int = Query(default=200, ge=1, le=5000)):
    with get_session() as session:
        q = select(StockMovement).order_by(StockMovement.id.desc())
        if item_id:
            q = q.where(StockMovement.item_id == item_id)
        rows = session.exec(q.limit(limit)).all()
        return {
            "movements": [
                {
                    "id": m.id or 0,
                    "item_id": m.item_id,
                    "direction": m.direction,
                    "quantity": m.quantity,
                    "related_order_id": m.related_order_id,
                    "created_at": m.created_at.isoformat() if m.created_at else None,
                }
                for m in rows
            ]
        }


@router.patch("/movements/{movement_id}")
def update_movement(movement_id: int, body: Dict[str, Any]):
    """Update a manual movement (related_order_id must be null)."""
    with get_session() as session:
        mv = session.exec(select(StockMovement).where(StockMovement.id == movement_id)).first()
        if not mv:
            raise HTTPException(status_code=404, detail="Movement not found")
        if mv.related_order_id:
            raise HTTPException(status_code=400, detail="Cannot edit order-linked movement; use recalc instead")
        # allow direction and/or quantity update
        new_dir = body.get("direction")
        new_qty = body.get("quantity")
        if new_dir is not None:
            if new_dir not in ("in", "out"):
                raise HTTPException(status_code=400, detail="direction must be 'in' or 'out'")
            mv.direction = new_dir  # type: ignore
        if new_qty is not None:
            try:
                q = int(new_qty)
            except Exception:
                raise HTTPException(status_code=400, detail="quantity must be integer > 0")
            if q <= 0:
                raise HTTPException(status_code=400, detail="quantity must be > 0")
            mv.quantity = q  # type: ignore
        return {"status": "ok"}


@router.delete("/movements/{movement_id}")
def delete_movement(movement_id: int):
    with get_session() as session:
        mv = session.exec(select(StockMovement).where(StockMovement.id == movement_id)).first()
        if not mv:
            raise HTTPException(status_code=404, detail="Movement not found")
        if mv.related_order_id:
            raise HTTPException(status_code=400, detail="Cannot delete order-linked movement; use recalc instead")
        session.delete(mv)
        return {"status": "ok"}


@router.post("/series")
def series_add(body: Dict[str, Any]):
	product_id = body.get("product_id")
	sizes: List[str] = body.get("sizes") or []
	colors: List[str] = body.get("colors") or [None]  # type: ignore
	quantity_per_variant: int = body.get("quantity_per_variant") or 0
	price = body.get("price")
	cost = body.get("cost")
	if not product_id or quantity_per_variant <= 0 or not sizes:
		raise HTTPException(status_code=400, detail="product_id, sizes[], quantity_per_variant>0 required")
	with get_session() as session:
		prod = session.exec(select(Product).where(Product.id == product_id)).first()
		if not prod:
			raise HTTPException(status_code=404, detail="Product not found")
		from ..services.mapping import find_or_create_variant
		created: List[int] = []
		for sz in sizes:
			for col in colors:
				it = find_or_create_variant(session, product=prod, size=sz, color=col)
				if price is not None:
					it.price = price
				if cost is not None:
					it.cost = cost
				mv = StockMovement(item_id=it.id, direction="in", quantity=quantity_per_variant)
				session.add(mv)
				created.append(it.id or 0)
		return {"status": "ok", "created_item_ids": created}


@router.post("/recalc")
def recalc_inventory(body: Dict[str, Any], request: Request):
    if not request.session.get("uid"):
        raise HTTPException(status_code=401, detail="Unauthorized")
    product_id = body.get("product_id")
    since_raw = body.get("since")
    dry_run = bool(body.get("dry_run", False))
    since = None
    if since_raw:
        try:
            import datetime as _dt
            since = _dt.date.fromisoformat(str(since_raw))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid since; expected YYYY-MM-DD")
    # take DB snapshot before mutating
    if not dry_run:
        try:
            from . import importer as _importer_router
            if hasattr(_importer_router, "_backup_db_snapshot"):
                _importer_router._backup_db_snapshot(tag="recalc-stock")
        except Exception:
            pass
    with get_session() as session:
        summary = recalc_orders_from_mappings(session, product_id=product_id, since=since, dry_run=dry_run)
        return {"status": "ok", **summary, "dry_run": dry_run}

