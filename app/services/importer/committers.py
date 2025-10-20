from __future__ import annotations

from typing import Tuple, Optional, List

from sqlmodel import select

from ...models import Client, Order, Payment, Item, Product, StockMovement
from ...utils.normalize import client_unique_key, legacy_client_unique_key
from ...utils.slugify import slugify
from ..matching import (
    find_order_by_tracking,
    find_order_by_client_and_date,
)
from ..matching import find_recent_placeholder_kargo_for_client
from ..mapping import resolve_mapping, find_or_create_variant


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
        # enrich order if missing data
        if rec.get("total_amount") and not order.total_amount:
            order.total_amount = rec.get("total_amount")
        if rec.get("shipment_date") and not order.shipment_date:
            order.shipment_date = rec.get("shipment_date")
        if rec.get("shipment_date") and not order.data_date:
            order.data_date = rec.get("shipment_date")
        if rec.get("alici_kodu"):
            cur = order.notes or None
            ak = f"AliciKodu:{rec.get('alici_kodu')}"
            order.notes = f"{cur} | {ak}" if cur else ak
        if rec.get("notes"):
            cur = order.notes or None
            order.notes = f"{cur} | {rec.get('notes')}" if cur else rec.get("notes")
    else:
        # resolve client by unique key
        new_uq = client_unique_key(rec.get("name"), rec.get("phone"))
        old_uq = legacy_client_unique_key(rec.get("name"), rec.get("phone"))
        client = None
        if new_uq:
            client = session.exec(select(Client).where(Client.unique_key == new_uq)).first()
        if not client and old_uq:
            client = session.exec(select(Client).where(Client.unique_key == old_uq)).first()
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
            if new_uq and client.unique_key != new_uq:
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
        order = find_order_by_client_and_date(session, client.id, rec.get("shipment_date"))
        if order:
            matched_order_id = order.id
            matched_client_id = client.id
            if rec.get("total_amount") and not order.total_amount:
                order.total_amount = rec.get("total_amount")
            if rec.get("shipment_date") and not order.shipment_date:
                order.shipment_date = rec.get("shipment_date")
            if rec.get("shipment_date") and not order.data_date:
                order.data_date = rec.get("shipment_date")
            if rec.get("alici_kodu"):
                cur = order.notes or None
                ak = f"AliciKodu:{rec.get('alici_kodu')}"
                order.notes = f"{cur} | {ak}" if cur else ak
            if rec.get("notes"):
                cur = order.notes or None
                order.notes = f"{cur} | {rec.get('notes')}" if cur else rec.get("notes")
        else:
            # create placeholder kargo order
            order_notes = rec.get("notes") or None
            if rec.get("alici_kodu"):
                order_notes = f"{order_notes} | AliciKodu:{rec.get('alici_kodu')}" if order_notes else f"AliciKodu:{rec.get('alici_kodu')}"
            order = Order(
                tracking_no=rec.get("tracking_no"),
                client_id=client.id,  # type: ignore
                item_id=None,
                quantity=rec.get("quantity") or 1,
                unit_price=rec.get("unit_price"),
                total_amount=rec.get("total_amount"),
                shipment_date=rec.get("shipment_date"),
                data_date=rec.get("shipment_date") or run.data_date,
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
    pdate = rec.get("delivery_date") or rec.get("shipment_date") or run.data_date
    if (amt_raw or 0.0) > 0 and pdate is not None and order is not None:
        existing = session.exec(select(Payment).where(Payment.order_id == order.id, Payment.date == pdate)).first()
        fee_kom = rec.get("fee_komisyon") or 0.0
        fee_hiz = rec.get("fee_hizmet") or 0.0
        fee_kar = rec.get("fee_kargo") or 0.0
        fee_iad = rec.get("fee_iade") or 0.0
        fee_eok = rec.get("fee_erken_odeme") or 0.0
        amt = float(amt_raw or 0.0)
        net = (amt or 0.0) - sum([fee_kom, fee_hiz, fee_kar, fee_iad, fee_eok])
        if not existing:
            pmt = Payment(
                client_id=order.client_id,
                order_id=order.id,
                amount=amt,
                date=pdate,
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
            run.created_payments += 1
        else:
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
    return status, message, matched_client_id, matched_order_id


def process_bizim_row(session, run, rec) -> Tuple[str, Optional[str], Optional[int], Optional[int]]:
    """Process a single Bizim record. Returns (status, message, matched_client_id, matched_order_id)."""
    status = "created"
    message = None
    matched_client_id: Optional[int] = None
    matched_order_id: Optional[int] = None

    # resolve or create client
    new_uq = client_unique_key(rec.get("name"), rec.get("phone"))
    old_uq = legacy_client_unique_key(rec.get("name"), rec.get("phone"))
    client = None
    if new_uq:
        client = session.exec(select(Client).where(Client.unique_key == new_uq)).first()
    if not client and old_uq:
        client = session.exec(select(Client).where(Client.unique_key == old_uq)).first()
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

    # item mapping
    item_name_raw = rec.get("item_name") or "Genel Ürün"
    base_name = str(item_name_raw).strip()
    outputs, _matched_rule = resolve_mapping(session, base_name)
    created_items: List[tuple[Item, int]] = []
    if outputs:
        for out in outputs:
            it: Optional[Item] = None
            if out.item_id:
                it = session.exec(select(Item).where(Item.id == out.item_id)).first()
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
                    pack_type=out.pack_type,
                    pair_multiplier=out.pair_multiplier or 1,
                )
            if it:
                created_items.append((it, int(out.quantity or 1)))
    else:
        # fallback generic item
        sku = slugify(base_name)
        item = session.exec(select(Item).where(Item.sku == sku)).first()
        if not item:
            item = Item(sku=sku, name=base_name)
            session.add(item)
            session.flush()
            run.created_items += 1
        created_items.append((item, 1))

    # merge/create order
    existing_order = None
    date_hint = rec.get("shipment_date") or run.data_date
    if date_hint:
        existing_order = find_order_by_client_and_date(session, client.id, date_hint)
    if existing_order:
        chosen_item_id = created_items[0][0].id if created_items else None
        if (existing_order.source or "") == "kargo":
            existing_order.item_id = chosen_item_id  # type: ignore
        existing_order.quantity = rec.get("quantity") or existing_order.quantity or 1
        existing_order.unit_price = rec.get("unit_price") or existing_order.unit_price
        existing_order.total_amount = rec.get("total_amount") or existing_order.total_amount
        existing_order.shipment_date = rec.get("shipment_date") or existing_order.shipment_date
        existing_order.data_date = existing_order.data_date or run.data_date
        existing_order.source = "bizim"
        existing_order.status = "merged"
        matched_order_id = existing_order.id
    else:
        # fallback to recent kargo placeholder
        existing_order = find_recent_placeholder_kargo_for_client(session, client.id)
        chosen_item_id = created_items[0][0].id if created_items else None
        if existing_order:
            if (existing_order.source or "") == "kargo":
                existing_order.item_id = chosen_item_id  # type: ignore
            existing_order.quantity = rec.get("quantity") or existing_order.quantity or 1
            existing_order.unit_price = rec.get("unit_price") or existing_order.unit_price
            existing_order.total_amount = rec.get("total_amount") or existing_order.total_amount
            existing_order.shipment_date = rec.get("shipment_date") or existing_order.shipment_date
            existing_order.data_date = existing_order.data_date or run.data_date
            existing_order.source = "bizim"
            existing_order.status = "merged"
            matched_order_id = existing_order.id
        else:
            order = Order(
                tracking_no=rec.get("tracking_no"),
                client_id=client.id,  # type: ignore
                item_id=chosen_item_id,  # type: ignore
                quantity=rec.get("quantity") or 1,
                unit_price=rec.get("unit_price"),
                total_amount=rec.get("total_amount"),
                shipment_date=rec.get("shipment_date"),
                data_date=run.data_date,
                source="bizim",
                notes=rec.get("notes") or None,
            )
            session.add(order)
            session.flush()
            run.created_orders += 1
            matched_order_id = order.id

    # stock movements for mapped variants
    try:
        qty_base = int(rec.get("quantity") or 1)
        for it, out_qty_each in created_items:
            multiplier = int(it.pair_multiplier or 1)
            total_qty = qty_base * int(out_qty_each or 1) * multiplier
            if total_qty > 0:
                mv = StockMovement(item_id=it.id, direction="out", quantity=total_qty, related_order_id=matched_order_id)
                session.add(mv)
    except Exception:
        pass

    return status, message, matched_client_id, matched_order_id


