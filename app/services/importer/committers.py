from __future__ import annotations

from typing import Tuple, Optional, List
import datetime as dt
import json

from sqlmodel import select
from sqlalchemy import func

from ...models import Client, Order, Payment, PaymentHistoryLog, Item, Product, StockMovement, OrderItem
from ...utils.normalize import client_unique_key, client_name_key, normalize_key
from ...utils.slugify import slugify
from ..matching import (
    find_order_by_tracking,
    find_order_by_client_and_date,
)
from ..matching import find_recent_placeholder_kargo_for_client
from ..mapping import resolve_mapping, find_or_create_variant
from ..inventory import adjust_stock
from ..shipping import compute_shipping_fee
from ..finance import ensure_iban_income


def _normalize_shipping_company(val: Optional[str]) -> str:
	try:
		s = (val or "").strip().lower()
		if not s:
			return "surat"
		if s.startswith("sur"):
			return "surat"
		if s.startswith("mng"):
			return "mng"
		if s.startswith("dhl"):
			return "dhl"
		if s.startswith("ptt"):
			return "ptt"
		# default fallback
		return "surat"
	except Exception:
		return "surat"


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


def _maybe_rehome_payment(
	session,
	*,
	client_id: Optional[int],
	target_order_id: Optional[int],
	amount: Optional[float],
	date_hint: Optional[dt.date],
) -> None:
	"""
	Move a payment that sits on a refunded/placeholder order (or is dangling)
	to the current Bizim order when it is the best candidate.

	Heuristics (conservative):
	- Same client
	- Payment amount matches within tolerance
	- Source order (if any) is refunded/iade/cancelled/placeholder/negative
	- Target order currently has no payments
	- Date proximity within 14 days when available
	"""
	if not (client_id and target_order_id and amount and amount > 0):
		return

	# If target already has a payment, do nothing
	existing_target = session.exec(
		select(Payment).where(Payment.order_id == target_order_id)
	).first()
	if existing_target:
		return

	tol = max(5.0, 0.02 * float(amount))
	window_days = 14
	best: Optional[tuple[tuple[float, float, float], Payment, Optional[Order]]] = None

	candidates = session.exec(
		select(Payment).where(
			Payment.client_id == client_id,
			Payment.order_id != target_order_id,
		)
	).all()

	for p in candidates:
		try:
			if abs(float(p.amount or 0.0) - float(amount)) > tol:
				continue
		except Exception:
			continue

		src_order = session.get(Order, p.order_id) if p.order_id else None
		src_status = (src_order.status or "").lower() if src_order else ""

		# Only move from "safe" sources to avoid stealing from healthy orders
		can_move = (
			(src_order is None)
			or (src_status in {"refunded", "iade", "iade_bekliyor", "cancelled", "switched", "stitched"})
			or ((src_order.source or "") == "kargo" and (src_order.status or "") == "placeholder")
			or (src_order.total_amount is not None and float(src_order.total_amount) < 0)
		)
		if not can_move:
			continue

		pd = p.payment_date or p.date
		if date_hint and pd:
			if abs((pd - date_hint).days) > window_days:
				continue

		score = (
			-abs(float(p.amount or 0.0) - float(amount)),  # tighter amount first
			0 if not (date_hint and pd) else -abs((pd - date_hint).days),  # closer date
			-(p.id or 0),  # deterministic tie-break
		)
		if best is None or score > best[0]:
			best = (score, p, src_order)

	if best:
		_, payment, src_order = best
		payment.order_id = target_order_id
		# keep reference/payment_date as-is; client_id already matches
		try:
			if src_order and src_order.status == "placeholder":
				src_order.status = "merged"
		except Exception:
			pass


def _maybe_rehome_payment_cross_client_by_name(
	session,
	*,
	target_order_id: Optional[int],
	target_client_name: str | None,
	amount: Optional[float],
	date_hint,
	tol_ratio: float = 0.02,
	window_days: int = 14,
) -> None:
	"""
	Move a payment sitting on placeholder/kargo orders of a *different* client,
	when name matches (case-insensitive exact) and amount/dates align.

	Use when kargo Excel has no phone (new client created) but Bizim Excel has phone.
	"""
	if not (target_order_id and target_client_name and amount and amount > 0):
		return
	from datetime import timedelta
	name_norm = (target_client_name or "").strip().lower()
	if not name_norm:
		return
	tol = max(5.0, tol_ratio * float(amount))
	with session.no_autoflush:
		from ...models import Order as _Order, Payment as _Payment, Client as _Client
		orders = session.exec(
			select(_Order, _Client)
			.join(_Client, _Order.client_id == _Client.id)
			.where(
				_Order.id != target_order_id,
				(_Order.status.in_(["placeholder", "merged"]) | (_Order.source == "kargo")),
			)
		).all()
		best = None
		for o, c in orders:
			try:
				if (c.name or "").strip().lower() != name_norm:
					continue
				pays = session.exec(select(_Payment).where(_Payment.order_id == o.id)).all()
				for p in pays:
					if abs(float(p.amount or 0.0) - float(amount)) > tol:
						continue
					if date_hint and p.date:
						if abs((p.date - date_hint).days) > window_days:
							continue
					score = (
						-abs(float(p.amount or 0.0) - float(amount)),
						0 if not (date_hint and p.date) else -abs((p.date - date_hint).days),
						-(p.id or 0),
					)
					if best is None or score > best[0]:
						best = (score, p, o)
			except Exception:
				continue
		if best:
			_, pay, src_order = best
			pay.order_id = target_order_id
			try:
				# if target has no tracking, adopt from source
				target = session.get(_Order, target_order_id)
				if target and (not target.tracking_no) and src_order.tracking_no:
					target.tracking_no = src_order.tracking_no
				if src_order and src_order.status == "placeholder":
					src_order.status = "merged"
					src_order.total_amount = 0.0
			except Exception:
				pass


def _maybe_mark_order_paid(session, order: Optional[Order]) -> None:
	"""
	If total_amount is positive and collected payments cover it, mark as paid.
	Skip refunded/iade/cancelled/switched/stitched orders and non-positive totals.
	"""
	if order is None or order.id is None or order.total_amount is None:
		return
	status = (order.status or "").lower()
	if status in {"refunded", "iade", "cancelled", "switched", "stitched"}:
		return
	if float(order.total_amount or 0.0) <= 0:
		return

	pays = session.exec(select(Payment).where(Payment.order_id == order.id)).all()
	paid = sum(float(p.amount or 0.0) for p in pays)
	if paid >= float(order.total_amount or 0.0):
		order.status = "paid"


def process_kargo_row(session, run, rec) -> Tuple[str, Optional[str], Optional[int], Optional[int]]:
    """Process a single Kargo record. Returns (status, message, matched_client_id, matched_order_id).

    This function mutates the session (creating/updating Orders/Clients/Payments) and updates run counters.
    """
    status = "created"
    message = None
    matched_client_id = None
    matched_order_id = None

    # never treat kargo item_name as an item; push into notes if exists
    if rec.get("item_name"):
        itm = str(rec.get("item_name") or "").strip()
        if itm:
            rec["notes"] = (f"{rec.get('notes')} | {itm}" if rec.get("notes") else itm)
        rec.pop("item_name", None)

    # Try direct match by tracking
    order = find_order_by_tracking(session, rec.get("tracking_no"))
    if order:
        matched_order_id = order.id
        matched_client_id = order.client_id
        # enrich order if missing data (do not overwrite existing non-null values)
        derived_total = rec.get("total_amount")
        if derived_total is None and (rec.get("payment_amount") or 0.0) > 0:
            derived_total = float(rec.get("payment_amount") or 0.0)
        if derived_total and not order.total_amount:
            order.total_amount = derived_total
        if rec.get("unit_price") and not order.unit_price and (order.quantity or 1):
            order.unit_price = rec.get("unit_price")
        if rec.get("shipment_date") and not order.shipment_date:
            order.shipment_date = rec.get("shipment_date")
        if rec.get("delivery_date") and not getattr(order, "delivery_date", None):
            order.delivery_date = rec.get("delivery_date")
        # DO NOT update data_date from shipment_date - preserve original data_date (from bizim excel import)
        # data_date represents when the order data was imported, not the shipment date
        if rec.get("alici_kodu"):
            cur = order.notes or None
            ak = f"AliciKodu:{rec.get('alici_kodu')}"
            if not cur or (ak not in cur):
                order.notes = f"{cur} | {ak}" if cur else ak
        if rec.get("notes"):
            cur = order.notes or None
            note_val = str(rec.get("notes") or "").strip()
            if note_val and (not cur or (note_val not in cur)):
                order.notes = f"{cur} | {note_val}" if cur else note_val
    else:
        # resolve client by unique key (name + optional surname + phone)
        new_uq = client_unique_key(rec.get("name"), rec.get("phone"))
        client = None
        if new_uq:
            client = session.exec(select(Client).where(Client.unique_key == new_uq)).first()

        # When phone is missing and strict match fails, fall back to name-only matching
        if (client is None) and (not rec.get("phone")):
            name_raw = (rec.get("name") or "").strip()
            if name_raw:
                # Try unique_key prefix match using normalized name key
                try:
                    nkey = client_name_key(name_raw)
                    if nkey:
                        cands = session.exec(select(Client).where(Client.unique_key.like(f"{nkey}%"))).all()
                        if len(cands) == 1:
                            client = cands[0]
                except Exception:
                    pass
                # If ambiguous, try exact case-insensitive name equality
                if client is None:
                    try:
                        c2 = session.exec(select(Client).where(func.lower(Client.name) == func.lower(name_raw))).all()
                        if len(c2) == 1:
                            client = c2[0]
                    except Exception:
                        pass
                # Last resort: exact normalized name match (single hit) to avoid creating wrong client
                if client is None:
                    try:
                        nk = normalize_key(name_raw)
                        if nk:
                            all_clients = session.exec(select(Client)).all()
                            cands = [c for c in all_clients if normalize_key(c.name) == nk]
                            if len(cands) == 1:
                                client = cands[0]
                    except Exception:
                        pass

        # Allow creating client even if phone is missing (kargo excel often lacks phone)
        if not client:
            client = Client(
                name=rec.get("name") or "",
                phone=rec.get("phone"),
                address=rec.get("address"),
                city=rec.get("city"),
                unique_key=new_uq or None,
                status="missing-bizim",
            )
            session.add(client)
            session.flush()
            run.created_clients += 1
        else:
            # Avoid downgrading an existing client's unique_key to a name-only key from kargo rows
            if new_uq and client.unique_key != new_uq and rec.get("phone"):
                client.unique_key = new_uq
            updated = False
            for f in ("phone", "address", "city"):
                val = rec.get(f)
                if val and not getattr(client, f):
                    setattr(client, f, val)
                    updated = True
            if updated:
                run.updated_clients += 1

        # try to find an existing bizim order by client/date
        # Prefer orders with matching payment_amount if available
        payment_amount = rec.get("payment_amount")
        order = find_order_by_client_and_date(session, client.id, rec.get("shipment_date"), preferred_amount=payment_amount)
        if order:
            matched_order_id = order.id
            matched_client_id = client.id
            if rec.get("total_amount") and not order.total_amount:
                order.total_amount = rec.get("total_amount")
            if rec.get("shipment_date") and not order.shipment_date:
                order.shipment_date = rec.get("shipment_date")
            if rec.get("delivery_date") and not getattr(order, "delivery_date", None):
                order.delivery_date = rec.get("delivery_date")
            # update shipping company if missing
            if not order.shipping_company:
                order.shipping_company = _normalize_shipping_company(rec.get("shipping_company"))
            # backfill shipping_fee if missing and total_amount is present
            if (order.shipping_fee is None) and order.total_amount is not None:
                company_code = order.shipping_company or _normalize_shipping_company(rec.get("shipping_company"))
                order.shipping_fee = compute_shipping_fee(float(order.total_amount or 0.0), company_code=company_code, paid_by_bank_transfer=bool(order.paid_by_bank_transfer))
            # DO NOT update data_date - preserve the oldest date (original bizim order date)
            # data_date should remain as the original order creation date
            if rec.get("alici_kodu"):
                cur = order.notes or None
                ak = f"AliciKodu:{rec.get('alici_kodu')}"
                order.notes = f"{cur} | {ak}" if cur else ak
            if rec.get("notes"):
                cur = order.notes or None
                order.notes = f"{cur} | {rec.get('notes')}" if cur else rec.get("notes")
        else:
            # derive amount/unit_price when only payment is present (common when payment Excel arrives before bizim)
            derived_total = rec.get("total_amount")
            if derived_total is None and (rec.get("payment_amount") or 0.0) > 0:
                derived_total = float(rec.get("payment_amount") or 0.0)
            qty_val = rec.get("quantity") or 1
            derived_unit_price = rec.get("unit_price")
            if derived_unit_price is None and derived_total is not None and qty_val:
                try:
                    derived_unit_price = round(float(derived_total) / float(qty_val), 2)
                except Exception:
                    derived_unit_price = None
            # create placeholder kargo order
            order_notes = rec.get("notes") or None
            if rec.get("alici_kodu"):
                order_notes = f"{order_notes} | AliciKodu:{rec.get('alici_kodu')}" if order_notes else f"AliciKodu:{rec.get('alici_kodu')}"
            company_code = _normalize_shipping_company(rec.get("shipping_company"))
            shipping_fee = None
            if derived_total is not None:
                shipping_fee = compute_shipping_fee(float(derived_total or 0.0), company_code=company_code, paid_by_bank_transfer=bool(rec.get("paid_by_bank_transfer")))
            order = Order(
                tracking_no=rec.get("tracking_no"),
                client_id=client.id,  # type: ignore
                item_id=None,
                quantity=rec.get("quantity") or 1,
                unit_price=derived_unit_price,
                total_amount=derived_total,
                shipping_company=company_code,
                shipping_fee=shipping_fee,
                shipment_date=rec.get("shipment_date"),  # kargo tarihi from Excel row
                delivery_date=rec.get("delivery_date"),
                data_date=run.data_date,  # data tarihi from filename (when kargo Excel was imported)
                source="kargo",
                notes=order_notes,
                status="placeholder",
            )
            session.add(order)
            session.flush()
            run.created_orders += 1
            matched_order_id = order.id
            matched_client_id = client.id

    # payments: idempotent per (order_id, date)
    amt_raw = rec.get("payment_amount")
    # For kargo: payment_date comes from Excel filename (when payment was received)
    # Legacy 'date' field uses delivery_date/shipment_date for compatibility
    pdate_legacy = rec.get("delivery_date") or rec.get("shipment_date") or run.data_date
    # Extract payment date from filename (for kargo, this is when payment Excel was created/received)
    payment_date_from_filename = None
    if run.source == "kargo" and run.filename:
        # Extract date from filename (first 10 chars if ISO format: YYYY-MM-DD)
        import re
        import datetime as _dt
        match = re.match(r'^(\d{4}-\d{2}-\d{2})', run.filename)
        if match:
            try:
                payment_date_from_filename = _dt.date.fromisoformat(match.group(1))
            except:
                pass
    
    # Detect iade bekliyor sinyali: "Müşteri Tahsil etti ya da Evrak İade edildi."
    try:
        if order is not None:
            IADE_HINT = "müşteri tahsil etti ya da evrak iade edildi"
            note_source = " ".join([
                str(rec.get("notes") or ""),
                str(getattr(order, "notes", "") or ""),
            ]).lower()
            current_status = (order.status or "").lower()
            final_states = {"refunded", "iade", "cancelled", "switched", "stitched"}
            if IADE_HINT in note_source and current_status not in final_states:
                order.status = "iade_bekliyor"
    except Exception:
        pass

    if order and run.source == "returns":
        # If returns import carries a negative amount, ensure order status reflects refund
        if rec.get("amount") is not None:
            try:
                amt_val = float(rec.get("amount") or 0.0)
                if amt_val < 0:
                    # Always force to refunded for negative return rows (completed return)
                    order.status = "refunded"
            except Exception:
                pass
        if rec.get("date") and not getattr(order, "return_or_switch_date", None):
            order.return_or_switch_date = rec.get("date")
        # For returns source, skip payment creation (no positive tahsilat expected)
        return status, message, matched_client_id, matched_order_id

    if (amt_raw or 0.0) > 0 and pdate_legacy is not None and order is not None:
        amt = float(amt_raw or 0.0)

        # Duplicate guard across orders by (reference, amount)
        existing_ref = None
        if rec.get("tracking_no"):
            existing_ref = session.exec(
                select(Payment).where(
                    Payment.reference == rec.get("tracking_no"),
                    Payment.amount == amt,
                )
            ).first()

        existing = session.exec(select(Payment).where(Payment.order_id == order.id, Payment.date == pdate_legacy)).first()
        if existing_ref and not existing:
            existing = existing_ref
            # If the ref belongs to another order, prefer reusing that instead of creating a duplicate
            if existing_ref.order_id and existing_ref.order_id != order.id:
                matched_order_id = existing_ref.order_id
                matched_client_id = existing_ref.client_id
                order = session.get(Order, existing_ref.order_id) or order
                status = "updated"
                message = message or "payment reference already exists; reused"

        fee_kom = rec.get("fee_komisyon") or 0.0
        fee_hiz = rec.get("fee_hizmet") or 0.0
        # Ignore any fee_kargo coming from Excel and compute deterministically
        # based on TahsilatTutari (payment amount)
        fee_iad = rec.get("fee_iade") or 0.0
        fee_eok = rec.get("fee_erken_odeme") or 0.0
        fee_kar = compute_shipping_fee(amt)
        net = round((amt or 0.0) - sum([fee_kom, fee_hiz, fee_kar, fee_iad, fee_eok]), 2)
        if not existing:
            pmt = Payment(
                client_id=order.client_id,
                order_id=order.id,
                amount=amt,
                date=pdate_legacy,  # Legacy field
                payment_date=payment_date_from_filename,  # Actual payment date from filename
                method=rec.get("payment_method") or "kargo",
                reference=rec.get("tracking_no"),
                fee_komisyon=fee_kom,
                fee_hizmet=fee_hiz,
                fee_kargo=fee_kar,
                fee_iade=fee_iad,
                fee_erken_odeme=fee_eok,
                net_amount=net,
            )
            session.add(pmt)
            session.flush()
            _log_payment(session, pmt.id or 0, "create", None, {
                "amount": amt,
                "method": pmt.method,
                "payment_date": pmt.payment_date.isoformat() if pmt.payment_date else None,
                "reference": pmt.reference,
            })
            run.created_payments += 1
        else:
            old_snap = {
                "amount": existing.amount,
                "method": existing.method,
                "payment_date": (existing.payment_date or existing.date).isoformat() if (existing.payment_date or existing.date) else None,
                "reference": existing.reference,
            }
            if amt > float(existing.amount or 0.0):
                existing.amount = amt
                existing.method = rec.get("payment_method") or existing.method
                existing.reference = rec.get("tracking_no") or existing.reference
                existing.fee_komisyon = fee_kom
                existing.fee_hizmet = fee_hiz
                existing.fee_kargo = fee_kar
                existing.fee_iade = fee_iad
                existing.fee_erken_odeme = fee_eok
                existing.net_amount = net
                _log_payment(session, existing.id or 0, "update", old_snap, {
                    "amount": existing.amount,
                    "method": existing.method,
                    "payment_date": (existing.payment_date or existing.date).isoformat() if (existing.payment_date or existing.date) else None,
                    "reference": existing.reference,
                })
        # After creating/updating payment, auto-mark paid if fully covered
        try:
            _maybe_mark_order_paid(session, order)
        except Exception:
            pass
        try:
            # If order total is missing, backfill from payment amount (common when payment Excel arrives first)
            if (order.total_amount is None) or (float(order.total_amount or 0.0) == 0.0 and amt > 0):
                order.total_amount = amt
                if (order.quantity or 0) > 0 and not order.unit_price:
                    try:
                        order.unit_price = round(float(order.total_amount) / float(order.quantity), 2)
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            if bool(getattr(order, "paid_by_bank_transfer", False)):
                ensure_iban_income(session, order, float(order.total_amount or amt or 0.0))
        except Exception:
            pass
    return status, message, matched_client_id, matched_order_id


def process_bizim_row(session, run, rec) -> Tuple[str, Optional[str], Optional[int], Optional[int]]:
    """Process a single Bizim record. Returns (status, message, matched_client_id, matched_order_id)."""
    try:
        status = "created"
        message = None
        matched_client_id: Optional[int] = None
        matched_order_id: Optional[int] = None

        # resolve or create client
        new_uq = client_unique_key(rec.get("name"), rec.get("phone"))
        client = None
        if new_uq:
            client = session.exec(select(Client).where(Client.unique_key == new_uq)).first()
        if not client:
            client = Client(
                name=(rec.get("name") or ""),
                phone=rec.get("phone"),
                address=rec.get("address"),
                city=rec.get("city"),
                unique_key=new_uq or None,
            )
            session.add(client)
            session.flush()
            run.created_clients += 1
            client.status = client.status or "missing-kargo"
        else:
            if new_uq and client.unique_key != new_uq:
                client.unique_key = new_uq
            updated = False
            for f in ("phone", "address", "city"):
                val = rec.get(f)
                if val:
                    setattr(client, f, val)
                    updated = True
            if updated:
                run.updated_clients += 1
        matched_client_id = client.id

        # item mapping via package rule for the entire base name
        item_name_raw = rec.get("item_name") or "Genel Ürün"
        # prefer pre-parsed base from router to avoid height/weight/notes noise
        base_name = str(rec.get("item_name_base") or item_name_raw).strip()
        outputs, _matched_rule = resolve_mapping(session, base_name)
        try:
            if idx := rec.get("__row_index__") is not None:  # optional if set by caller
                pass
            print(f"[MAP DEBUG] base='{base_name}' -> outputs={len(outputs)}")
        except Exception:
            pass
        created_items: List[tuple[Item, int]] = []
        if outputs:
            for out in outputs:
                it: Optional[Item] = None
                if out.item_id:
                    it = session.exec(select(Item).where(Item.id == out.item_id)).first()
                    if it and (it.status or "") == "inactive":
                        # Skip tombstoned variants
                        it = None
                else:
                    prod: Optional[Product] = None
                    if out.product_id:
                        prod = session.exec(select(Product).where(Product.id == out.product_id)).first()
                    if prod is None:
                        pslug = slugify(base_name)
                        prod = session.exec(select(Product).where(Product.slug == pslug)).first()
                        if not prod:
                            prod = Product(name=base_name, slug=pslug)
                            session.add(prod)
                            session.flush()
                    it = find_or_create_variant(
                        session,
                        product=prod,  # type: ignore
                        size=out.size,
                        color=out.color,
                    )
                    if it and (it.status or "") == "inactive":
                        # Do not resurrect deleted variants
                        it = None
                if it:
                    # Optionally update price from mapping output; do not use for accounting
                    if out.unit_price is not None:
                        it.price = out.unit_price
                    created_items.append((it, int(out.quantity or 1)))
        else:
            # fallback generic item; mark as unmatched later
            sku = slugify(base_name)
            item = session.exec(select(Item).where(Item.sku == sku)).first()
            if item and (item.status or "") == "inactive":
                # Do not use or recreate tombstoned generic item
                item = None
            if not item:
                item = Item(sku=sku, name=base_name)
                session.add(item)
                session.flush()
                run.created_items += 1
            created_items.append((item, 1))

        # merge/create order
        existing_order = None
        tracking_no = rec.get("tracking_no")
        if tracking_no:
            tracking_match = find_order_by_tracking(session, tracking_no)
            if tracking_match and (((tracking_match.source or "").lower() == "kargo") or ((tracking_match.status or "").lower() == "placeholder")):
                existing_order = tracking_match
        date_hint = rec.get("shipment_date") or run.data_date
        if date_hint and not existing_order:
            existing_order = find_order_by_client_and_date(session, client.id, date_hint)

        # Note: order-level idempotency checks are applied later once chosen_item_id is known
        chosen_item_id = created_items[0][0].id if created_items else None
        can_merge_into_existing = False
        if existing_order:
            # Only merge a Bizim row into an existing order if that order is a kargo placeholder
            can_merge_into_existing = ((existing_order.source or "") == "kargo") or ((existing_order.status or "") == "placeholder")

        if existing_order and can_merge_into_existing:
            if (existing_order.source or "") == "kargo":
                existing_order.item_id = chosen_item_id  # type: ignore
            existing_order.quantity = rec.get("quantity") or existing_order.quantity or 1
            existing_order.unit_price = rec.get("unit_price") or existing_order.unit_price
            existing_order.total_amount = rec.get("total_amount") or existing_order.total_amount
            existing_order.shipment_date = rec.get("shipment_date") or existing_order.shipment_date
            existing_order.data_date = existing_order.data_date or run.data_date
            existing_order.source = "bizim"
            # Preserve refund/return statuses; only mark merged if previously placeholder/blank
            current_status = (existing_order.status or "").lower()
            if current_status in ("", "placeholder", "merged"):
                existing_order.status = "merged"
            # ensure original name is preserved in notes
            if item_name_raw:
                cur = existing_order.notes or None
                if cur:
                    if item_name_raw not in cur:
                        existing_order.notes = f"{cur} | {item_name_raw}"
                else:
                    existing_order.notes = item_name_raw
            matched_order_id = existing_order.id
        else:
            # If we couldn't merge into an existing kargo placeholder, try the most recent placeholder; otherwise create a NEW Bizim order
            placeholder = find_recent_placeholder_kargo_for_client(session, client.id)
            if placeholder and (((placeholder.source or "") == "kargo") or ((placeholder.status or "") == "placeholder")):
                if (placeholder.source or "") == "kargo":
                    placeholder.item_id = chosen_item_id  # type: ignore
                placeholder.quantity = rec.get("quantity") or placeholder.quantity or 1
                placeholder.unit_price = rec.get("unit_price") or placeholder.unit_price
                placeholder.total_amount = rec.get("total_amount") or placeholder.total_amount
                placeholder.shipment_date = rec.get("shipment_date") or placeholder.shipment_date
                placeholder.data_date = placeholder.data_date or run.data_date
                placeholder.source = "bizim"
                placeholder.status = "merged"
                # ensure original name preserved
                if item_name_raw:
                    cur = placeholder.notes or None
                    if cur:
                        if item_name_raw not in cur:
                            placeholder.notes = f"{cur} | {item_name_raw}"
                    else:
                        placeholder.notes = item_name_raw
                matched_order_id = placeholder.id
            else:
                # Final guard: if a Bizim order already exists for this client/date/item, skip creating new
                try:
                    if date_hint and matched_client_id is not None and chosen_item_id is not None:
                        from sqlmodel import select as _select
                        from ...models import Order as _Order
                        dup = session.exec(
                            _select(_Order)
                            .where(
                                _Order.client_id == matched_client_id,
                                (_Order.shipment_date == date_hint) | (_Order.data_date == date_hint),
                                _Order.source == "bizim",
                                _Order.item_id == chosen_item_id,
                            )
                            .order_by(_Order.id.desc())
                        ).first()
                        if dup:
                            status = "skipped"
                            message = "duplicate bizim row (order exists)"
                            matched_order_id = dup.id
                            return status, message, matched_client_id, matched_order_id
                except Exception:
                    pass
                order = Order(
                    tracking_no=rec.get("tracking_no"),
                    client_id=client.id,  # type: ignore
                    item_id=chosen_item_id,  # type: ignore
                    quantity=rec.get("quantity") or 1,
                    unit_price=rec.get("unit_price"),
                    total_amount=rec.get("total_amount"),
                shipment_date=rec.get("shipment_date"),
                data_date=rec.get("shipment_date") or run.data_date,  # Use shipment_date (actual order date) as oldest date
                source="bizim",
                    notes=(rec.get("notes") or None),
                )
                # append original string to order notes
                if item_name_raw:
                    if order.notes:
                        if item_name_raw not in order.notes:
                            order.notes = f"{order.notes} | {item_name_raw}"
                    else:
                        order.notes = item_name_raw
                session.add(order)
                session.flush()
                run.created_orders += 1
                matched_order_id = order.id

        # create order items and stock movements based on mapping outputs
        try:
            qty_base = int(rec.get("quantity") or 1)
            for it, out_qty_each in created_items:
                total_qty = qty_base * int(out_qty_each or 1)
                if total_qty <= 0:
                    continue
                # create order item
                if matched_order_id is not None and it.id is not None:
                    oi = OrderItem(order_id=matched_order_id, item_id=it.id, quantity=total_qty)
                    session.add(oi)
                # stock movement: decrement canonical item only
                if it.id is not None:
                    adjust_stock(session, item_id=it.id, delta=-total_qty, related_order_id=matched_order_id)
            # after creating order items, compute total_cost on the order using FIFO
            if matched_order_id is not None:
                from ...services.inventory import calculate_order_cost_fifo
                from sqlmodel import select as _select
                from ...models import Order as _Order
                order_obj = session.exec(_select(_Order).where(_Order.id == matched_order_id)).first()
                if order_obj:
                    order_obj.total_cost = calculate_order_cost_fifo(session, matched_order_id)
        except Exception:
            pass

        # If the Bizim order was created/merged successfully, try to move any misplaced
        # payments that might be stuck on refunded/placeholder orders for the same client.
        try:
            if matched_order_id and matched_client_id and rec.get("total_amount"):
                _maybe_rehome_payment(
                    session,
                    client_id=matched_client_id,
                    target_order_id=matched_order_id,
                    amount=float(rec.get("total_amount") or 0.0),
                    date_hint=rec.get("shipment_date") or run.data_date,
                )
                # Cross-client rescue by name when kargo has no phone and created a different client
                _maybe_rehome_payment_cross_client_by_name(
                    session,
                    target_order_id=matched_order_id,
                    target_client_name=client.name,
                    amount=float(rec.get("total_amount") or 0.0),
                    date_hint=rec.get("shipment_date") or run.data_date,
                )
        except Exception:
            pass

        # mark unmatched if there was no usable mapping output
        if not outputs or not created_items:
            if not outputs:
                status = "unmatched"
                message = f"No mapping rule for '{base_name}'"
            else:
                status = "skipped"
                message = f"Variant is deleted/inactive for '{base_name}'"
    except Exception as e:
        status = "error"
        message = str(e)
        print(f"[BIZIM ROW ERROR] {rec.get('name', 'unknown')}: {e}")
        import traceback
        traceback.print_exc()
    return status, message, matched_client_id, matched_order_id


