from fastapi import APIRouter, HTTPException, Query, Request, Body
from sqlmodel import select
from sqlalchemy import or_, and_
import datetime as dt
from typing import Optional, List, Dict, Any

from ..db import get_session
from ..models import Client, Item, Order, OrderItem, Payment, Product, SystemSetting, Income
from ..services.inventory import adjust_stock, get_stock_map
from ..utils.normalize import normalize_phone, client_unique_key


router = APIRouter(prefix="/magaza-satis", tags=["magaza-satis"])


def _serialize_client(c: Client) -> Dict[str, Any]:
	return {
		"id": c.id,
		"name": c.name,
		"phone": c.phone,
		"city": c.city,
		"address": c.address,
	}


def _serialize_item(it: Item, on_hand: Optional[int] = None, product_price: Optional[float] = None) -> Dict[str, Any]:
	price = it.price
	if price is None and product_price is not None:
		price = product_price
	return {
		"id": it.id,
		"sku": it.sku,
		"name": it.name,
		"size": it.size,
		"color": it.color,
		"price": price,
		"on_hand": on_hand,
	}


def _ensure_client(session, *, client_id: Optional[int], name: Optional[str], phone: Optional[str], city: Optional[str], address: Optional[str]) -> Client:
	"""Find existing client by id or phone; create if missing."""
	phone_norm = normalize_phone(phone)
	client: Optional[Client] = None

	if client_id:
		client = session.get(Client, client_id)
		if not client:
			raise HTTPException(status_code=404, detail="Client not found")
	elif phone_norm:
		client = session.exec(select(Client).where(Client.phone == phone_norm)).first()

	if client:
		# Update basic fields if provided
		if name:
			client.name = name
		if phone_norm:
			client.phone = phone_norm
		if city:
			client.city = city
		if address:
			client.address = address
		return client

	# Create new client
	if not name or not phone_norm:
		raise HTTPException(status_code=400, detail="Name and phone are required to create client")

	# Build a unique_key with small collision handling
	base_key = client_unique_key(name, phone_norm)
	unique_key = base_key or None
	if unique_key:
		suffix = 1
		while session.exec(select(Client).where(Client.unique_key == unique_key)).first():
			unique_key = f"{base_key}_{suffix}"
			suffix += 1

	client = Client(
		name=name,
		phone=phone_norm,
		city=city,
		address=address,
		unique_key=unique_key,
	)
	session.add(client)
	session.flush()
	return client


@router.get("", include_in_schema=False)
@router.get("/", include_in_schema=False)
def page(request: Request):
	with get_session() as session:
		q = (
			select(Item)
			.where((Item.status.is_(None)) | (Item.status != "inactive"))
			.order_by(Item.id.desc())
			.limit(15)
		)
		items = session.exec(q).all()
		prod_ids = [it.product_id for it in items if it.product_id]
		products = session.exec(select(Product).where(Product.id.in_(prod_ids))).all() if prod_ids else []
		prod_price_map = {p.id: p.default_price for p in products if p.id is not None}
		stock_map = get_stock_map(session)
		items_payload = [_serialize_item(it, stock_map.get(it.id or 0), prod_price_map.get(it.product_id)) for it in items]
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"magaza_satis.html",
			{
				"request": request,
				"items": items_payload,
			},
		)


@router.get("/api/client-lookup")
def client_lookup(phone: str = Query(..., description="Telefon numarası (her türlü karakter, otomatik temizlenir)")):
	phone_norm = normalize_phone(phone)
	if not phone_norm:
		raise HTTPException(status_code=400, detail="Phone is required")
	with get_session() as session:
		client = session.exec(select(Client).where(Client.phone == phone_norm).order_by(Client.id.desc())).first()
		if not client:
			return {"found": False}
		return {"found": True, "client": _serialize_client(client)}


@router.post("/api/client")
def create_or_update_client(
	payload: dict = Body(..., description="name, phone, city, address, optional client_id")
):
	client_id = payload.get("client_id")
	name = (payload.get("name") or "").strip()
	phone = payload.get("phone")
	city = (payload.get("city") or "").strip() or None
	address = (payload.get("address") or "").strip() or None

	with get_session() as session:
		client = _ensure_client(
			session,
			client_id=client_id,
			name=name,
			phone=phone,
			city=city,
			address=address,
		)
		return {"client": _serialize_client(client)}


@router.get("/api/items")
def search_items(q: Optional[str] = Query(default=None, description="SKU/isim/renk/boyut araması"), limit: int = Query(default=20, ge=1, le=100)):
	with get_session() as session:
		query = (
			select(Item)
			.where((Item.status.is_(None)) | (Item.status != "inactive"))
			.order_by(Item.id.desc())
			.limit(limit)
		)
		if q:
			terms = [t for t in str(q).strip().split() if t]
			if terms:
				clauses = []
				for t in terms:
					pat = f"%{t}%"
					clauses.append(
						or_(
							Item.sku.ilike(pat),
							Item.name.ilike(pat),
							Item.color.ilike(pat),
							Item.size.ilike(pat),
						)
					)
				query = query.where(and_(*clauses))
		items = session.exec(query).all()
		prod_ids = [it.product_id for it in items if it.product_id]
		products = session.exec(select(Product).where(Product.id.in_(prod_ids))).all() if prod_ids else []
		prod_price_map = {p.id: p.default_price for p in products if p.id is not None}
		stock_map = get_stock_map(session)
		return {
			"items": [
				_serialize_item(it, stock_map.get(it.id or 0), prod_price_map.get(it.product_id))
				for it in items
			]
		}


@router.post("/api/checkout")
def checkout(payload: dict = Body(...)):
	cart: List[dict] = payload.get("cart") or []
	if not cart:
		raise HTTPException(status_code=400, detail="Cart is empty")

	payment_method = (payload.get("payment_method") or "").lower()
	if payment_method not in ("cash", "bank_transfer"):
		raise HTTPException(status_code=400, detail="payment_method must be cash or bank_transfer")

	discount = float(payload.get("discount") or 0.0)
	if discount < 0:
		discount = 0.0
	commission = float(payload.get("commission") or 0.0)
	if commission < 0:
		commission = 0.0

	notes = (payload.get("notes") or "").strip() or None
	skip_stock = bool(payload.get("skip_stock"))
	with get_session() as session:
		# load finance settings once
		settings_rows = session.exec(select(SystemSetting)).all()
		settings_map = {s.key: s.value for s in settings_rows}

		client = _ensure_client(
			session,
			client_id=payload.get("client_id"),
			name=(payload.get("client") or {}).get("name") or payload.get("name"),
			phone=(payload.get("client") or {}).get("phone") or payload.get("phone"),
			city=(payload.get("client") or {}).get("city"),
			address=(payload.get("client") or {}).get("address"),
		)

		item_ids = [int(it.get("item_id")) for it in cart if it.get("item_id")]
		if not item_ids:
			raise HTTPException(status_code=400, detail="Cart is empty")

		items = session.exec(select(Item).where(Item.id.in_(item_ids))).all()
		item_map = {it.id: it for it in items if it.id is not None}

		lines: List[dict] = []
		for entry in cart:
			iid = int(entry.get("item_id"))
			if iid not in item_map:
				raise HTTPException(status_code=404, detail=f"Item not found: {iid}")
			qty = int(entry.get("quantity") or 1)
			if qty <= 0:
				raise HTTPException(status_code=400, detail="Quantity must be >0")
			unit_price = float(entry.get("unit_price") or 0.0)
			lines.append(
				{
					"item_id": iid,
					"quantity": qty,
					"unit_price": unit_price,
				}
			)

		subtotal = sum(l["unit_price"] * l["quantity"] for l in lines)
		total_amount = max(subtotal - discount + commission, 0.0)

		order = Order(
			client_id=client.id,  # type: ignore[arg-type]
			item_id=lines[0]["item_id"],
			quantity=sum(l["quantity"] for l in lines),
			unit_price=lines[0]["unit_price"],
			total_amount=total_amount,
			source="bizim",
			channel="magaza",
			status="paid",
			paid_by_bank_transfer=(payment_method == "bank_transfer"),
			payment_date=dt.date.today(),
			data_date=dt.date.today(),
			notes=notes,
			shipping_fee=0.0,
			shipping_company=None,
		)
		session.add(order)
		session.flush()

		for l in lines:
			oi = OrderItem(
				order_id=order.id,  # type: ignore[arg-type]
				item_id=l["item_id"],
				quantity=l["quantity"],
			)
			session.add(oi)
			if not skip_stock:
				adjust_stock(
					session,
					item_id=l["item_id"],
					delta=-l["quantity"],
					related_order_id=order.id,
					reason="magaza_satis",
				)

		payment = Payment(
			client_id=client.id,  # type: ignore[arg-type]
			order_id=order.id,  # type: ignore[arg-type]
			amount=total_amount,
			payment_date=dt.date.today(),
			method="bank_transfer" if payment_method == "bank_transfer" else "cash",
			net_amount=total_amount,
		)
		session.add(payment)

		# Optional: create income entry mapped to configured account
		try:
			if payment_method == "cash":
				acc_id = int(settings_map.get("pos_income_cash_account_id", "0"))
				src = "pos_cash_magaza"
			else:
				acc_id = int(settings_map.get("pos_income_bank_account_id", "0"))
				src = "pos_bank_magaza"
			if acc_id > 0:
				income = Income(
					account_id=acc_id,
					amount=total_amount,
					date=dt.date.today(),
					source=src,
					reference=f"POS order {order.id}",
					notes=notes,
				)
				session.add(income)
		except Exception:
			# Fail-safe: do not block order creation
			pass

		return {
			"status": "ok",
			"order_id": order.id,
			"client_id": client.id,
			"total_amount": total_amount,
		}

