from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Form
from sqlmodel import select

from ..db import get_session
from ..models import Product
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
):
	if not name:
		raise HTTPException(status_code=400, detail="name required")
	with get_session() as session:
		slug = slugify(name)
		existing = session.exec(select(Product).where(Product.slug == slug)).first()
		if existing:
			raise HTTPException(status_code=409, detail="Product already exists")
		p = Product(name=name, slug=slug, default_unit=default_unit, default_price=default_price)
		session.add(p)
		session.flush()
		return {"id": p.id, "name": p.name, "slug": p.slug}


