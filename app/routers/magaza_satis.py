from fastapi import APIRouter, HTTPException, Query, Request, Body
from sqlmodel import select
from sqlalchemy import or_, and_
import datetime as dt
from typing import Optional, List, Dict, Any, Tuple
import json
import os

from ..db import get_session
from ..models import Client, Item, Order, OrderItem, Payment, PaymentHistoryLog, Product, SystemSetting, Income, StockUnit
from ..services.inventory import adjust_stock, get_stock_map
from ..services.stock_units import get_units_for_movement
from ..services.finance import ensure_iban_income
from ..services.mobile_qr import parse_kargo_qr, parse_stock_qr, merge_kargo_fields
from ..utils.normalize import normalize_phone, client_unique_key


router = APIRouter(prefix="/magaza-satis", tags=["magaza-satis"])


def _require_mobile_api_key(request: Request) -> None:
	key = (os.getenv("HMA_MOBILE_API_KEY") or "").strip()
	if not key:
		return  # disabled until configured
	header = (request.headers.get("x-mobile-api-key") or request.headers.get("X-Mobile-API-Key") or "").strip()
	if header != key:
		raise HTTPException(status_code=401, detail="Invalid or missing X-Mobile-API-Key")


def _log_payment(session, payment_id: int, action: str, old=None, new=None):
	try:
		session.add(
			PaymentHistoryLog(
				payment_id=payment_id,
				action=action,
				old_data_json=json.dumps(old) if old else None,
				new_data_json=json.dumps(new) if new else None,
			)
		)
	except Exception:
		pass


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


def _ensure_client_for_kargo(
	session,
	*,
	name: str,
	phone: Optional[str],
	city: Optional[str],
	address: Optional[str],
	tracking_no: str,
) -> Client:
	"""Client for kargo QR flow: allow missing phone; tie to tracking_no via unique_key."""
	phone_norm = normalize_phone(phone) if phone else None
	track = (tracking_no or "").strip()
	uk_track = f"kargo_{track}" if track else None

	if phone_norm:
		client = session.exec(select(Client).where(Client.phone == phone_norm).order_by(Client.id.desc())).first()
		if client:
			if name:
				client.name = name
			if city:
				client.city = city
			if address:
				client.address = address
			return client

	if uk_track:
		client = session.exec(select(Client).where(Client.unique_key == uk_track)).first()
		if client:
			if name:
				client.name = name
			if phone_norm:
				client.phone = phone_norm
			if city:
				client.city = city
			if address:
				client.address = address
			return client

	display_name = (name or "").strip() or f"Kargo {track or 'müşteri'}"

	if phone_norm:
		base_key = client_unique_key(display_name, phone_norm)
		unique_key = base_key or None
		if unique_key:
			suffix = 1
			while session.exec(select(Client).where(Client.unique_key == unique_key)).first():
				unique_key = f"{base_key}_{suffix}"
				suffix += 1
		client = Client(
			name=display_name,
			phone=phone_norm,
			city=city,
			address=address,
			unique_key=unique_key,
		)
	else:
		client = Client(
			name=display_name,
			phone=None,
			city=city,
			address=address,
			unique_key=uk_track,
		)
	session.add(client)
	session.flush()
	return client


def _resolve_item_from_stock_qr(
	session, qr_content: str, item_id: Optional[int] = None
) -> Tuple[Optional[Item], Optional[int]]:
	"""(Item, stock_unit_id veya None). hma:unit ile ikinci bileşen dolu."""
	if item_id is not None:
		return session.exec(select(Item).where(Item.id == item_id)).first(), None
	parsed = parse_stock_qr(qr_content or "")
	if not parsed:
		return None, None
	if parsed.get("stock_unit_id") is not None:
		uid = int(parsed["stock_unit_id"])
		u = session.get(StockUnit, uid)
		if not u:
			return None, None
		it = session.exec(select(Item).where(Item.id == u.item_id)).first()
		return it, uid
	if parsed.get("item_id"):
		return session.exec(select(Item).where(Item.id == int(parsed["item_id"]))).first(), None
	if parsed.get("sku"):
		return session.exec(select(Item).where(Item.sku == str(parsed["sku"]))).first(), None
	return None, None


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
		def _parse_int_setting(key: str) -> int:
			val = settings_map.get(key)
			if val is None:
				return 0
			try:
				return int(str(val).strip() or 0)
			except Exception:
				return 0

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
		session.flush()
		_log_payment(session, payment.id or 0, "create", None, {
			"amount": total_amount,
			"method": payment.method,
			"payment_date": payment.payment_date.isoformat() if payment.payment_date else None,
		})

		# Optional: create income entry mapped to configured account
		try:
			if payment_method == "cash":
				acc_id = _parse_int_setting("pos_income_cash_account_id")
				src = "pos_cash_magaza"
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
			else:
				# IBAN -> Garanti bank income
				acc_id = ensure_iban_income(session, order, float(total_amount))
				if not acc_id:
					acc_id = _parse_int_setting("pos_income_bank_account_id")
					if acc_id > 0:
						income = Income(
							account_id=acc_id,
							amount=total_amount,
							date=dt.date.today(),
							source="pos_bank_magaza",
							reference=f"POS order {order.id}",
							notes=notes,
						)
						session.add(income)
		except Exception as e:
			# Fail-safe: do not block order creation
			try:
				print(f"[magaza_satis] income_create_failed order={order.id} err={e}")
			except Exception:
				pass

		return {
			"status": "ok",
			"order_id": order.id,
			"client_id": client.id,
			"total_amount": total_amount,
		}


@router.post("/api/order-from-kargo-qr")
def order_from_kargo_qr(request: Request, payload: dict = Body(...)):
	"""
	Start or resume a draft order from a carrier label QR (or explicit fields).
	Auth: X-Mobile-API-Key when HMA_MOBILE_API_KEY is set.
	"""
	_require_mobile_api_key(request)
	qr_content = payload.get("qr_content")
	fields = payload.get("fields") or {}
	if isinstance(fields, dict) and fields:
		parsed = parse_kargo_qr(str(qr_content or ""))
		merged = merge_kargo_fields(parsed, fields)
	else:
		merged = parse_kargo_qr(str(qr_content or ""))

	tracking = (merged.get("tracking_no") or "").strip()
	if not tracking:
		raise HTTPException(status_code=400, detail="tracking_no could not be determined from QR; send fields.tracking_no")

	name = (merged.get("name") or "").strip()
	phone = merged.get("phone")
	city = merged.get("city")
	address = merged.get("address")

	with get_session() as session:
		# Resume draft kargo_qr order
		draft = session.exec(
			select(Order)
			.where(Order.tracking_no == tracking, Order.channel == "kargo_qr", Order.status == "draft")
			.order_by(Order.id.desc())
		).first()
		if draft and draft.id:
			client = session.get(Client, draft.client_id)
			line_rows = session.exec(select(OrderItem).where(OrderItem.order_id == draft.id)).all()
			order_item_count = sum(int(x.quantity or 0) for x in line_rows)
			return {
				"status": "ok",
				"order_id": draft.id,
				"client_id": draft.client_id,
				"tracking_no": tracking,
				"client": _serialize_client(client) if client else None,
				"resumed": True,
				"order_item_count": order_item_count,
			}

		# Block if paid order already has this tracking (avoid duplicate sales)
		paid_conflict = session.exec(
			select(Order)
			.where(Order.tracking_no == tracking, Order.status == "paid")
			.order_by(Order.id.desc())
		).first()
		if paid_conflict:
			raise HTTPException(
				status_code=409,
				detail=f"An order with this tracking_no is already completed (order_id={paid_conflict.id})",
			)

		client = _ensure_client_for_kargo(
			session,
			name=name,
			phone=phone,
			city=city,
			address=address,
			tracking_no=tracking,
		)

		order = Order(
			client_id=client.id,  # type: ignore[arg-type]
			item_id=None,
			quantity=0,
			unit_price=0.0,
			total_amount=0.0,
			source="bizim",
			channel="kargo_qr",
			status="draft",
			tracking_no=tracking,
			shipping_company="surat",
			data_date=dt.date.today(),
			notes=(payload.get("notes") or "").strip() or None,
		)
		session.add(order)
		session.flush()

		return {
			"status": "ok",
			"order_id": order.id,
			"client_id": client.id,
			"tracking_no": tracking,
			"client": _serialize_client(client),
			"resumed": False,
			"order_item_count": 0,
		}


@router.post("/api/order-add-item")
def order_add_item(request: Request, payload: dict = Body(...)):
	"""Add a line to a draft kargo_qr order and decrement stock."""
	_require_mobile_api_key(request)
	order_id = payload.get("order_id")
	if not order_id:
		raise HTTPException(status_code=400, detail="order_id required")
	try:
		order_id = int(order_id)
	except Exception:
		raise HTTPException(status_code=400, detail="order_id must be integer")

	qr_content = payload.get("qr_content") or ""
	item_id_raw = payload.get("item_id")
	try:
		item_id_opt = int(item_id_raw) if item_id_raw is not None else None
	except Exception:
		item_id_opt = None

	qty = int(payload.get("quantity") or 1)
	if qty <= 0:
		raise HTTPException(status_code=400, detail="quantity must be > 0")

	with get_session() as session:
		order = session.get(Order, order_id)
		if not order:
			raise HTTPException(status_code=404, detail="Order not found")
		if (order.channel or "") != "kargo_qr" or (order.status or "") != "draft":
			raise HTTPException(status_code=400, detail="Order must be draft kargo_qr")

		item, consume_unit_id = _resolve_item_from_stock_qr(session, str(qr_content), item_id_opt)
		if not item or not item.id:
			raise HTTPException(status_code=404, detail="Item not found from QR")

		if consume_unit_id is not None:
			if qty != 1:
				raise HTTPException(status_code=400, detail="hma:unit QR ile quantity 1 olmalı")
			u = session.get(StockUnit, consume_unit_id)
			if not u or int(u.item_id) != int(item.id):
				raise HTTPException(status_code=400, detail="stock_unit item uyumsuz")
			if (u.status or "") != "in_stock":
				raise HTTPException(status_code=400, detail=f"Parça stokta değil (status={u.status})")
		else:
			stock_map = get_stock_map(session)
			on_hand = int(stock_map.get(item.id, 0))
			if on_hand < qty:
				raise HTTPException(status_code=400, detail=f"Insufficient stock for item {item.id} (on_hand={on_hand})")

		existing = session.exec(
			select(OrderItem).where(OrderItem.order_id == order_id, OrderItem.item_id == item.id)
		).first()
		if existing:
			existing.quantity = int(existing.quantity or 0) + qty
			session.add(existing)
		else:
			session.add(
				OrderItem(
					order_id=order_id,
					item_id=item.id,  # type: ignore[arg-type]
					quantity=qty,
				)
			)

		try:
			adjust_stock(
				session,
				item_id=item.id,  # type: ignore[arg-type]
				delta=-qty,
				related_order_id=order_id,
				reason="kargo_qr_scan",
				consume_unit_ids=[consume_unit_id] if consume_unit_id is not None else None,
			)
		except ValueError as e:
			msg = str(e)
			if "insufficient_stock_units" in msg:
				raise HTTPException(
					status_code=409,
					detail=msg + " — backfill çalıştırın veya HMA_STOCK_UNIT_TRACKING=0 yapın.",
				) from e
			raise HTTPException(status_code=400, detail=msg) from e

		# Refresh order header counts (legacy columns)
		lines = session.exec(select(OrderItem).where(OrderItem.order_id == order_id)).all()
		total_q = sum(int(x.quantity or 0) for x in lines)
		if lines:
			first = lines[0]
			order.item_id = first.item_id
			order.quantity = total_q
		session.add(order)

		stock_after = get_stock_map(session)
		return {
			"status": "ok",
			"order_id": order_id,
			"item_id": item.id,
			"quantity_added": qty,
			"on_hand_after": stock_after.get(item.id or 0, 0),
			"order_item_count": total_q,
		}


@router.post("/api/order-complete")
def order_complete(request: Request, payload: dict = Body(...)):
	"""Finalize draft kargo_qr order: set total, create payment (and income if configured)."""
	_require_mobile_api_key(request)
	order_id = payload.get("order_id")
	if not order_id:
		raise HTTPException(status_code=400, detail="order_id required")
	try:
		order_id = int(order_id)
	except Exception:
		raise HTTPException(status_code=400, detail="order_id must be integer")

	payment_method = (payload.get("payment_method") or "").lower()
	if payment_method not in ("cash", "bank_transfer"):
		raise HTTPException(status_code=400, detail="payment_method must be cash or bank_transfer")

	total_amount = float(payload.get("total_amount") or 0.0)
	if total_amount < 0:
		raise HTTPException(status_code=400, detail="total_amount invalid")

	notes = (payload.get("notes") or "").strip() or None

	with get_session() as session:
		order = session.get(Order, order_id)
		if not order:
			raise HTTPException(status_code=404, detail="Order not found")
		if (order.channel or "") != "kargo_qr" or (order.status or "") != "draft":
			raise HTTPException(status_code=400, detail="Order must be draft kargo_qr")

		lines = session.exec(select(OrderItem).where(OrderItem.order_id == order_id)).all()
		if not lines:
			raise HTTPException(status_code=400, detail="Order has no lines; scan stock QR first")

		total_q = sum(int(x.quantity or 0) for x in lines)
		order.total_amount = total_amount
		order.quantity = total_q
		order.item_id = lines[0].item_id
		if total_q > 0:
			order.unit_price = round(total_amount / total_q, 2)
		else:
			order.unit_price = 0.0
		order.status = "paid"
		order.paid_by_bank_transfer = payment_method == "bank_transfer"
		order.payment_date = dt.date.today()
		order.notes = notes or order.notes
		session.add(order)

		payment = Payment(
			client_id=order.client_id,  # type: ignore[arg-type]
			order_id=order.id,  # type: ignore[arg-type]
			amount=total_amount,
			payment_date=dt.date.today(),
			method="bank_transfer" if payment_method == "bank_transfer" else "cash",
			net_amount=total_amount,
			reference=order.tracking_no,
		)
		session.add(payment)
		session.flush()
		_log_payment(session, payment.id or 0, "create", None, {
			"amount": total_amount,
			"method": payment.method,
			"payment_date": payment.payment_date.isoformat() if payment.payment_date else None,
		})

		settings_rows = session.exec(select(SystemSetting)).all()
		settings_map = {s.key: s.value for s in settings_rows}

		def _parse_int_setting(key: str) -> int:
			val = settings_map.get(key)
			if val is None:
				return 0
			try:
				return int(str(val).strip() or 0)
			except Exception:
				return 0

		try:
			if payment_method == "cash":
				acc_id = _parse_int_setting("pos_income_cash_account_id")
				if acc_id > 0:
					income = Income(
						account_id=acc_id,
						amount=total_amount,
						date=dt.date.today(),
						source="pos_cash_kargo_qr",
						reference=f"POS order {order.id}",
						notes=notes,
					)
					session.add(income)
			else:
				acc_id = ensure_iban_income(session, order, float(total_amount))
				if not acc_id:
					acc_id = _parse_int_setting("pos_income_bank_account_id")
					if acc_id > 0:
						income = Income(
							account_id=acc_id,
							amount=total_amount,
							date=dt.date.today(),
							source="pos_bank_kargo_qr",
							reference=f"POS order {order.id}",
							notes=notes,
						)
						session.add(income)
		except Exception as e:
			try:
				print(f"[magaza_satis] kargo_qr income_create_failed order={order.id} err={e}")
			except Exception:
				pass

		return {
			"status": "ok",
			"order_id": order.id,
			"payment_id": payment.id,
			"total_amount": total_amount,
		}


@router.post("/api/series-print-and-stock")
def series_print_and_stock(request: Request, payload: dict = Body(...)):
	"""Add stock for all sizes of one product color; return QR payloads for labels.
	If dry_run=true: validate only; no StockMovement, no new Item rows, no price/cost updates.
	"""
	_require_mobile_api_key(request)
	product_id = payload.get("product_id")
	color = (payload.get("color") or "").strip() or None
	quantity_per_variant = int(payload.get("quantity_per_variant") or 0)
	unit_cost = payload.get("unit_cost")
	supplier_id = payload.get("supplier_id")
	price = payload.get("price")
	dry_run = bool(payload.get("dry_run"))

	if not product_id or quantity_per_variant <= 0:
		raise HTTPException(status_code=400, detail="product_id and quantity_per_variant>0 required")

	try:
		uc = float(unit_cost) if unit_cost is not None else None
	except Exception:
		uc = None
	if uc is None or uc <= 0:
		raise HTTPException(status_code=400, detail="unit_cost > 0 required for inbound stock")

	with get_session() as session:
		prod = session.exec(select(Product).where(Product.id == product_id)).first()
		if not prod:
			raise HTTPException(status_code=404, detail="Product not found")

		size_rows = session.exec(
			select(Item.size).where(
				Item.product_id == product_id,
				Item.size != None,
				(Item.status.is_(None)) | (Item.status != "inactive"),
			).distinct()
		).all()
		sizes: List[str] = []
		for r in size_rows:
			v = r[0] if isinstance(r, (list, tuple)) else r
			if v:
				sizes.append(str(v))
		if not sizes:
			raise HTTPException(
				status_code=400,
				detail="No sizes found for product; create variants in HMA first or add size chart items",
			)
		sizes = sorted(set(sizes))

		from ..services.mapping import (
			build_variant_sku,
			find_or_create_variant,
			find_variant_if_exists,
		)

		colors = [color] if color else [None]  # type: ignore[list-item]
		qr_payloads: List[Dict[str, Any]] = []
		item_ids: List[Any] = []
		stock_units_all: List[Dict[str, Any]] = []

		for sz in sizes:
			for col in colors:
				if dry_run:
					it = find_variant_if_exists(session, product=prod, size=sz, color=col)
					preview_sku = build_variant_sku(prod, sz, col)
					if it:
						iid = int(it.id or 0)
						item_ids.append(iid)
						qr_payloads.append(
							{
								"item_id": iid,
								"sku": it.sku,
								"size": it.size,
								"color": it.color,
								"qr_data": f"hma:item:{iid}",
								"would_create_variant": False,
							}
						)
					else:
						item_ids.append(None)
						qr_payloads.append(
							{
								"item_id": None,
								"sku": preview_sku,
								"size": sz,
								"color": col,
								"qr_data": f"hma:item:DRY_RUN:{preview_sku}",
								"would_create_variant": True,
							}
						)
					continue

				it = find_or_create_variant(session, product=prod, size=sz, color=col)
				if price is not None:
					try:
						it.price = float(price)
					except Exception:
						pass
				if payload.get("cost") is not None:
					try:
						it.cost = float(payload.get("cost"))
					except Exception:
						pass
				iid = int(it.id or 0)
				item_ids.append(iid)
				qr_payloads.append(
					{
						"item_id": iid,
						"sku": it.sku,
						"size": it.size,
						"color": it.color,
						"qr_data": f"hma:item:{iid}",
					}
				)
				try:
					mv = adjust_stock(
						session,
						item_id=iid,
						delta=quantity_per_variant,
						unit_cost=uc,
						supplier_id=int(supplier_id) if supplier_id is not None else None,
						reason="series_print_mobile",
					)
				except ValueError as e:
					raise HTTPException(status_code=400, detail=str(e)) from e
				if mv and mv.id:
					for su in get_units_for_movement(session, int(mv.id)):
						if su.id is None:
							continue
						stock_units_all.append(
							{
								"stock_unit_id": int(su.id),
								"item_id": iid,
								"sku": it.sku,
								"size": it.size,
								"color": it.color,
								"qr_data": f"hma:unit:{su.id}",
							}
						)

		out: Dict[str, Any] = {
			"status": "ok",
			"product_id": int(product_id),
			"color": color,
			"quantity_per_variant": quantity_per_variant,
			"item_ids": item_ids,
			"qr_payloads": qr_payloads,
			"stock_units": stock_units_all,
		}
		if dry_run:
			out["dry_run"] = True
			out["message"] = "dry_run: veritabanında stok veya yeni varyant oluşturulmadı."
		return out


@router.post("/api/reconcile-incomes")
def reconcile_incomes():
	"""Backfill Income rows for POS (magaza) orders that have payments but missing income.
	- Uses finance settings to choose accounts.
	- Idempotent by reference: creates Income with reference 'POS order {order_id}' if not exists.
	"""
	with get_session() as session:
		settings = {s.key: s.value for s in session.exec(select(SystemSetting)).all()}
		def _parse_int_setting(key: str) -> int:
			try:
				return int(str(settings.get(key, "")).strip() or 0)
			except Exception:
				return 0
		cash_acc = _parse_int_setting("pos_income_cash_account_id")
		bank_acc = _parse_int_setting("pos_income_bank_account_id")
		if cash_acc <= 0 and bank_acc <= 0:
			raise HTTPException(status_code=400, detail="No POS income accounts configured")

		orders = session.exec(
			select(Order)
			.where(Order.channel == "magaza")
			.order_by(Order.id.desc())
		).all()
		created = 0
		for o in orders:
			if not o.id:
				continue
			# check existing income by reference
			ref = f"POS order {o.id}"
			exists = session.exec(select(Income).where(Income.reference == ref)).first()
			if exists:
				continue
			# find payment(s) for this order
			pays = session.exec(select(Payment).where(Payment.order_id == o.id)).all()
			if not pays:
				continue
			amount = sum(float(p.amount or 0.0) for p in pays)
			if amount <= 0:
				continue
			method = (pays[0].method or "").lower()
			if method == "cash":
				acc_id = cash_acc
				src = "pos_cash_magaza"
			else:
				acc_id = bank_acc
				src = "pos_bank_magaza"
			if acc_id <= 0:
				continue
			inc = Income(
				account_id=acc_id,
				amount=amount,
				date=o.payment_date or dt.date.today(),
				source=src,
				reference=ref,
				notes=o.notes,
			)
			session.add(inc)
			created += 1
		return {"status": "ok", "created": created}

