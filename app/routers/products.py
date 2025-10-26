from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Form, Request
from sqlmodel import select

from ..db import get_session
from ..models import Product, Item
from ..utils.slugify import slugify


router = APIRouter(prefix="/products", tags=["products"])


@router.get("")
@router.get("/")
def list_products(limit: int = Query(default=500, ge=1, le=5000)):
	with get_session() as session:
		rows = session.exec(select(Product).order_by(Product.id.desc()).limit(limit)).all()
		return {
			"products": [
				{
					"id": p.id or 0,
					"name": p.name,
					"slug": p.slug,
					"default_unit": p.default_unit,
					"default_color": p.default_color,
					"default_price": p.default_price,
				}
				for p in rows
			]
		}


@router.post("")
def create_product(
    name: str = Form(...),
    default_unit: str = Form("adet"),
    default_price: float | None = Form(None),
    default_color: str | None = Form(None),
):
	if not name:
		raise HTTPException(status_code=400, detail="name required")
	with get_session() as session:
		slug = slugify(name)
		existing = session.exec(select(Product).where(Product.slug == slug)).first()
		if existing:
			raise HTTPException(status_code=409, detail="Product already exists")
		p = Product(name=name, slug=slug, default_unit=default_unit, default_price=default_price, default_color=default_color)
		session.add(p)
		session.flush()
		return {"id": p.id, "name": p.name, "slug": p.slug, "default_unit": p.default_unit, "default_price": p.default_price, "default_color": p.default_color}


@router.get("/table")
def products_table(request: Request, limit: int = Query(default=10000, ge=1, le=100000)):
    with get_session() as session:
        rows = session.exec(select(Product).order_by(Product.id.desc()).limit(limit)).all()
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "products_table.html",
            {"request": request, "rows": rows, "limit": limit},
        )


@router.put("/{product_id}")
def update_product(product_id: int, body: dict):
    allowed = {"name", "default_unit", "default_price", "default_color"}
    with get_session() as session:
        p = session.exec(select(Product).where(Product.id == product_id)).first()
        if not p:
            raise HTTPException(status_code=404, detail="Product not found")
        # name -> slug update with uniqueness check
        new_name = body.get("name")
        if new_name and new_name != p.name:
            new_slug = slugify(new_name)
            existing = session.exec(select(Product).where(Product.slug == new_slug, Product.id != product_id)).first()
            if existing:
                raise HTTPException(status_code=409, detail="Another product with same name/slug exists")
            p.name = new_name
            p.slug = new_slug
        if "default_unit" in body:
            p.default_unit = body.get("default_unit") or p.default_unit
        if "default_price" in body:
            try:
                val = body.get("default_price")
                p.default_price = float(val) if val is not None else None
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid default_price")
        if "default_color" in body:
            p.default_color = body.get("default_color") or None
        return {"status": "ok", "id": p.id}


@router.delete("/{product_id}")
def delete_product(product_id: int):
    with get_session() as session:
        p = session.exec(select(Product).where(Product.id == product_id)).first()
        if not p:
            raise HTTPException(status_code=404, detail="Product not found")
        # block delete if referenced by any items
        ref = session.exec(select(Item).where(Item.product_id == product_id).limit(1)).first()
        if ref:
            raise HTTPException(status_code=400, detail="Product has items; cannot delete")
        session.delete(p)
        return {"status": "ok"}

