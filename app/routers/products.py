from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Form, Request
from fastapi.encoders import jsonable_encoder
from sqlmodel import select

from ..db import get_session
from ..models import Product, Item, ProductUpsell, ProductSizeChart, SizeChart, SupplierProductPrice, Supplier
from ..utils.slugify import slugify


router = APIRouter(prefix="/products", tags=["products"])


@router.get("")
@router.get("/")
def list_products(limit: int = Query(default=500, ge=1, le=5000)):
	with get_session() as session:
		rows = session.exec(select(Product).order_by(Product.id.desc()).limit(limit)).all()
		assignments = session.exec(select(ProductSizeChart)).all()
		assignment_map = {a.product_id: a.size_chart_id for a in assignments if a.product_id and a.size_chart_id}
		return {
			"products": [
				{
					"id": p.id or 0,
					"name": p.name,
					"slug": p.slug,
					"default_unit": p.default_unit,
					"default_color": p.default_color,
					"default_price": p.default_price,
					"ai_variant_exclusions": p.ai_variant_exclusions,
					"size_chart_id": assignment_map.get(p.id or 0),
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
    ai_variant_exclusions: str | None = Form(None),
	size_chart_id: int | None = Form(None),
):
	if not name:
		raise HTTPException(status_code=400, detail="name required")
	with get_session() as session:
		slug = slugify(name)
		existing = session.exec(select(Product).where(Product.slug == slug)).first()
		if existing:
			raise HTTPException(status_code=409, detail="Product already exists")
		p = Product(
			name=name,
			slug=slug,
			default_unit=default_unit,
			default_price=default_price,
			default_color=default_color,
			ai_variant_exclusions=ai_variant_exclusions,
		)
		session.add(p)
		if size_chart_id:
			sc = session.exec(select(SizeChart).where(SizeChart.id == size_chart_id)).first()
			if not sc:
				raise HTTPException(status_code=404, detail="Size chart not found")
			session.flush()
			link = ProductSizeChart(product_id=p.id, size_chart_id=size_chart_id)
			session.add(link)
		session.flush()
		return {
			"id": p.id,
			"name": p.name,
			"slug": p.slug,
			"default_unit": p.default_unit,
			"default_price": p.default_price,
			"default_color": p.default_color,
			"ai_variant_exclusions": p.ai_variant_exclusions,
			"size_chart_id": size_chart_id,
		}


@router.get("/table")
def products_table(request: Request, limit: int = Query(default=10000, ge=1, le=100000)):
    with get_session() as session:
        rows = session.exec(select(Product).order_by(Product.id.desc()).limit(limit)).all()
        suppliers = session.exec(select(Supplier).order_by(Supplier.name.asc())).all()
        suppliers_json = [
            {"id": s.id, "name": s.name}
            for s in suppliers
            if s.id is not None
        ]
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "products_table.html",
            {"request": request, "rows": rows, "suppliers": suppliers_json, "limit": limit},
        )


@router.get("/{product_id}/supplier-prices")
def list_supplier_prices(product_id: int):
	with get_session() as session:
		prod = session.exec(select(Product).where(Product.id == product_id)).first()
		if not prod:
			raise HTTPException(status_code=404, detail="Product not found")
		rows = session.exec(
			select(SupplierProductPrice, Supplier)
			.join(Supplier, SupplierProductPrice.supplier_id == Supplier.id, isouter=True)
			.where(SupplierProductPrice.product_id == product_id)
			.order_by(Supplier.name.asc(), SupplierProductPrice.id.asc())
		).all()
		return {
			"product_id": product_id,
			"supplier_prices": [
				{
					"id": spp.id,
					"supplier_id": spp.supplier_id,
					"supplier_name": sup.name if sup else None,
					"item_id": spp.item_id,
					"price": spp.price,
					"cost": spp.cost,
				}
				for spp, sup in rows
			],
		}


@router.post("/{product_id}/supplier-prices")
def create_supplier_price(product_id: int, body: dict):
	supplier_id = body.get("supplier_id")
	price = body.get("price")
	cost = body.get("cost")
	item_id = body.get("item_id")
	if not supplier_id:
		raise HTTPException(status_code=400, detail="supplier_id required")
	with get_session() as session:
		prod = session.exec(select(Product).where(Product.id == product_id)).first()
		if not prod:
			raise HTTPException(status_code=404, detail="Product not found")
		supp = session.exec(select(Supplier).where(Supplier.id == supplier_id)).first()
		if not supp:
			raise HTTPException(status_code=404, detail="Supplier not found")
		# Optional: validate item belongs to product
		if item_id is not None:
			it = session.exec(select(Item).where(Item.id == item_id)).first()
			if not it or int(it.product_id or 0) != int(product_id):
				raise HTTPException(status_code=400, detail="item_id does not belong to product")
		spp = SupplierProductPrice(
			supplier_id=supplier_id,
			product_id=product_id,
			item_id=item_id,
			price=float(price) if price is not None else None,
			cost=float(cost) if cost is not None else None,
		)
		session.add(spp)
		session.flush()
		return {"status": "ok", "id": spp.id}


@router.put("/supplier-prices/{spp_id}")
def update_supplier_price(spp_id: int, body: dict):
	with get_session() as session:
		spp = session.exec(select(SupplierProductPrice).where(SupplierProductPrice.id == spp_id)).first()
		if not spp:
			raise HTTPException(status_code=404, detail="Supplier price not found")
		if "price" in body:
			val = body.get("price")
			spp.price = float(val) if val is not None else None
		if "cost" in body:
			val = body.get("cost")
			spp.cost = float(val) if val is not None else None
		if "supplier_id" in body:
			sid = body.get("supplier_id")
			if sid:
				supp = session.exec(select(Supplier).where(Supplier.id == sid)).first()
				if not supp:
					raise HTTPException(status_code=404, detail="Supplier not found")
				spp.supplier_id = sid
		if "item_id" in body:
			item_id = body.get("item_id")
			if item_id is None:
				spp.item_id = None
			else:
				it = session.exec(select(Item).where(Item.id == item_id)).first()
				if not it:
					raise HTTPException(status_code=404, detail="Item not found")
				spp.item_id = item_id
		return {"status": "ok"}


@router.delete("/supplier-prices/{spp_id}")
def delete_supplier_price(spp_id: int):
	with get_session() as session:
		spp = session.exec(select(SupplierProductPrice).where(SupplierProductPrice.id == spp_id)).first()
		if not spp:
			raise HTTPException(status_code=404, detail="Supplier price not found")
		session.delete(spp)
		return {"status": "ok"}

@router.get("/upsells")
def products_upsells_page(request: Request):
	with get_session() as session:
		products = session.exec(select(Product).order_by(Product.name.asc())).all()
	# Ensure plain JSON-serializable payload for the template (avoid SQLModel instances)
	products_json = jsonable_encoder(
		[
			{"id": p.id, "name": p.name, "slug": p.slug, "default_price": p.default_price}
			for p in products
		]
	)
	templates = request.app.state.templates
	return templates.TemplateResponse(
		"product_upsells.html",
		{"request": request, "products": products_json},
	)


@router.get("/{product_id}/upsells")
def list_product_upsells(product_id: int):
	with get_session() as session:
		product = session.exec(select(Product).where(Product.id == product_id)).first()
		if not product:
			raise HTTPException(status_code=404, detail="Product not found")
		rows = (
			session.exec(
				select(ProductUpsell, Product)
				.join(Product, ProductUpsell.upsell_product_id == Product.id)
				.where(ProductUpsell.product_id == product_id)
				.order_by(ProductUpsell.position.asc(), ProductUpsell.id.asc())
			).all()
			or []
		)
		return {
			"product_id": product_id,
			"upsells": [
				{
					"id": pu.id,
					"product_id": pu.product_id,
					"upsell_product_id": pu.upsell_product_id,
					"upsell_product_name": upsell_prod.name if upsell_prod else None,
					"copy": pu.copy_text,
					"position": pu.position,
					"is_active": pu.is_active,
				}
				for pu, upsell_prod in rows
			],
		}


@router.post("/{product_id}/upsells")
def create_product_upsell(product_id: int, body: dict):
	upsell_product_id = body.get("upsell_product_id")
	if not upsell_product_id:
		raise HTTPException(status_code=400, detail="upsell_product_id required")
	copy = (body.get("copy") or "").strip() or None
	position_raw = body.get("position")
	try:
		position = int(position_raw) if position_raw is not None else None
	except Exception:
		raise HTTPException(status_code=400, detail="Invalid position")

	def _to_bool(val) -> bool:
		if isinstance(val, bool):
			return val
		if isinstance(val, str):
			return val.strip().lower() in {"1", "true", "yes", "on"}
		return bool(val)

	is_active = _to_bool(body.get("is_active", True))

	with get_session() as session:
		product = session.exec(select(Product).where(Product.id == product_id)).first()
		if not product:
			raise HTTPException(status_code=404, detail="Product not found")
		upsell_product = session.exec(select(Product).where(Product.id == upsell_product_id)).first()
		if not upsell_product:
			raise HTTPException(status_code=404, detail="Upsell product not found")
		existing = session.exec(
			select(ProductUpsell).where(
				ProductUpsell.product_id == product_id,
				ProductUpsell.upsell_product_id == upsell_product_id,
			)
		).first()
		if existing:
			raise HTTPException(status_code=409, detail="Upsell already exists for this product")

		if position is None:
			current_max = session.exec(
				select(ProductUpsell.position)
				.where(ProductUpsell.product_id == product_id)
				.order_by(ProductUpsell.position.desc())
				.limit(1)
			).first()
			position = (current_max or 0) + 1

		pu = ProductUpsell(
			product_id=product_id,
			upsell_product_id=upsell_product_id,
			copy_text=copy,
			position=position,
			is_active=is_active,
		)
		session.add(pu)
		session.flush()
		return {
			"id": pu.id,
			"product_id": pu.product_id,
			"upsell_product_id": pu.upsell_product_id,
			"copy": pu.copy_text,
			"position": pu.position,
			"is_active": pu.is_active,
		}


@router.put("/upsells/{upsell_id}")
def update_product_upsell(upsell_id: int, body: dict):
	with get_session() as session:
		pu = session.exec(select(ProductUpsell).where(ProductUpsell.id == upsell_id)).first()
		if not pu:
			raise HTTPException(status_code=404, detail="Upsell not found")
		if "copy" in body:
			val = (body.get("copy") or "").strip()
			pu.copy_text = val or None
		if "position" in body:
			try:
				pu.position = int(body.get("position"))
			except Exception:
				raise HTTPException(status_code=400, detail="Invalid position")
		if "is_active" in body:
			val = body.get("is_active")
			if isinstance(val, bool):
				pu.is_active = val
			elif isinstance(val, str):
				pu.is_active = val.strip().lower() in {"1", "true", "yes", "on"}
			else:
				pu.is_active = bool(val)
		return {"status": "ok", "id": pu.id}


@router.delete("/upsells/{upsell_id}")
def delete_product_upsell(upsell_id: int):
	with get_session() as session:
		pu = session.exec(select(ProductUpsell).where(ProductUpsell.id == upsell_id)).first()
		if not pu:
			raise HTTPException(status_code=404, detail="Upsell not found")
		session.delete(pu)
		return {"status": "ok"}


@router.put("/{product_id}")
def update_product(product_id: int, body: dict):
    allowed = {"name", "default_unit", "default_price", "default_color", "ai_variant_exclusions", "size_chart_id"}
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
        if "ai_variant_exclusions" in body:
            val = body.get("ai_variant_exclusions")
            p.ai_variant_exclusions = val if isinstance(val, str) and val.strip() else None
        if "size_chart_id" in body:
            sc_id = body.get("size_chart_id")
            if sc_id in (None, ""):
                existing = session.exec(
                    select(ProductSizeChart).where(ProductSizeChart.product_id == product_id)
                ).first()
                if existing:
                    session.delete(existing)
            else:
                try:
                    sc_int = int(sc_id)
                except Exception:
                    raise HTTPException(status_code=400, detail="Invalid size_chart_id")
                sc = session.exec(select(SizeChart).where(SizeChart.id == sc_int)).first()
                if not sc:
                    raise HTTPException(status_code=404, detail="Size chart not found")
                existing = session.exec(
                    select(ProductSizeChart).where(ProductSizeChart.product_id == product_id)
                ).first()
                if existing:
                    existing.size_chart_id = sc_int
                else:
                    session.add(ProductSizeChart(product_id=product_id, size_chart_id=sc_int))
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

