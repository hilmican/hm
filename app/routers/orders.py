from fastapi import APIRouter, Query, Request, HTTPException, Body
from sqlmodel import select
from sqlalchemy import or_, and_, not_
import datetime as dt
from typing import Optional
from collections import defaultdict

from ..db import get_session
from ..models import Order, Payment, OrderItem, Item, Client, OrderEditLog, ShippingCompanyRate
from ..services.inventory import get_or_create_item as _get_or_create_item
from ..services.inventory import adjust_stock
from ..services.shipping import compute_shipping_fee
from fastapi.responses import StreamingResponse
import io
try:
    import openpyxl
except Exception:
    openpyxl = None  # type: ignore

router = APIRouter()


@router.get("")
@router.get("/")
def list_orders(limit: int = Query(default=100, ge=1, le=1000)):
	with get_session() as session:
		rows = session.exec(select(Order).order_by(Order.id.desc()).limit(limit)).all()
		return {
			"orders": [
				{
					"id": o.id or 0,
					"tracking_no": o.tracking_no,
					"client_id": o.client_id,
					"item_id": o.item_id,
					"quantity": o.quantity,
					"total_amount": o.total_amount,
					"total_cost": o.total_cost,
					"shipment_date": o.shipment_date.isoformat() if o.shipment_date else None,
                    "data_date": o.data_date.isoformat() if o.data_date else None,
					"source": o.source,
				}
				for o in rows
			]
		}


@router.get("/table")
def list_orders_table(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    date_field: str = Query(default="shipment", regex="^(shipment|data|both)$"),
    source: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    preset: Optional[str] = Query(default=None),
    ig_linked: Optional[str] = Query(default=None, regex="^(linked|unlinked)?$"),
    repeat_customer: Optional[str] = Query(default=None, regex="^(multi)?$"),
):
    def _parse_date_or_default(value: Optional[str], fallback: dt.date) -> dt.date:
        try:
            if value:
                return dt.date.fromisoformat(value)
        except Exception:
            pass
        return fallback

    with get_session() as session:
        # default window starts from 2025-10-01 until today
        today = dt.date.today()
        default_start = dt.date(2025, 10, 1)
        start_date = _parse_date_or_default(start, default_start)
        end_date = _parse_date_or_default(end, today)
        if end_date < start_date:
            start_date, end_date = end_date, start_date

        # Preset-driven date adjustment
        if preset == "overdue_unpaid_7":
            # Show all orders strictly before 7 days ago
            end_date = today - dt.timedelta(days=7)
            # Fixed start anchor per request
            start_date = dt.date(2025, 1, 1)
        elif preset == "all":
            # Show the full history by widening the range
            start_date = dt.date(2000, 1, 1)
            end_date = today

        # Build base query with date filter logic similar to reports
        if date_field == "both":
            q = (
                select(Order)
                .where(
                    or_(
                        and_(Order.shipment_date.is_not(None), Order.shipment_date >= start_date, Order.shipment_date <= end_date),
                        and_(Order.data_date.is_not(None), Order.data_date >= start_date, Order.data_date <= end_date),
                    )
                )
                .order_by(Order.id.desc())
            )
        else:
            date_col = Order.shipment_date if date_field == "shipment" else Order.data_date
            alt_date_col = Order.data_date if date_field == "shipment" else Order.shipment_date
            q = (
                select(Order)
                .where(
                    or_(
                        and_(date_col.is_not(None), date_col >= start_date, date_col <= end_date),
                        and_(date_col.is_(None), alt_date_col.is_not(None), alt_date_col >= start_date, alt_date_col <= end_date),
                    )
                )
                .order_by(Order.id.desc())
            )

        # Optional source filter (bizim|kargo) — ignored for quicksearch presets
        if (preset not in ("overdue_unpaid_7", "all")) and source in ("bizim", "kargo"):
            q = q.where(Order.source == source)

        # Optional IG linked filter on the SQL side when possible
        if ig_linked == "linked":
            from sqlalchemy import not_
            q = q.where(Order.ig_conversation_id.is_not(None))
        elif ig_linked == "unlinked":
            q = q.where(Order.ig_conversation_id.is_(None))

        rows = session.exec(q).all()
        
        # Filter out merged orders from main display (or show them with reduced opacity)
        # For now, we'll show them but mark them differently in the template

        # Payments and status map for paid/unpaid classification and refund/stitch display
        order_ids = [o.id for o in rows if o.id]
        pays = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all() if order_ids else []
        paid_map: dict[int, float] = {}
        for p in pays:
            if p.order_id is None:
                continue
            paid_map[p.order_id] = paid_map.get(p.order_id, 0.0) + float(p.amount or 0.0)
        
        # For partial payment groups, sum payments across all orders in the group
        # Load partial payment groups
        partial_groups: dict[int, list[int]] = defaultdict(list)
        for o in rows:
            if o.partial_payment_group_id and o.id:
                partial_groups[o.partial_payment_group_id].append(o.id)
        
        # Sum payments for partial payment groups
        for group_id, group_order_ids in partial_groups.items():
            if group_id in group_order_ids:
                # Primary order exists in group
                group_paid = sum(paid_map.get(oid, 0.0) for oid in group_order_ids)
                paid_map[group_id] = group_paid
        
        status_map: dict[int, str] = {}
        for o in rows:
            oid = o.id or 0
            total = float(o.total_amount or 0.0)
            
            # For partial payment groups, use group total payments
            if o.partial_payment_group_id and o.partial_payment_group_id == oid:
                # This is the primary order in a partial payment group
                paid = paid_map.get(oid, 0.0)
            else:
                paid = paid_map.get(oid, 0.0)
            
            # refunded/switched/cancelled take precedence for row classes
            if (o.status or "") in ("refunded", "switched", "stitched", "cancelled"):
                status_map[oid] = str(o.status)
            elif (o.status or "") == "paid":
                status_map[oid] = "paid"
            elif (o.status or "") == "partial_paid":
                status_map[oid] = "partial_paid"
            else:
                # Consider IBAN (bank transfer) orders as completed/paid
                if bool(o.paid_by_bank_transfer):
                    status_map[oid] = "paid"
                else:
                    status_map[oid] = "paid" if (paid > 0 and paid >= total) else "unpaid"

        # Optional status filter — ignored for quicksearch presets
        if (preset not in ("overdue_unpaid_7", "all")) and status in ("paid", "unpaid", "refunded", "switched", "partial_paid", "cancelled"):
            rows = [o for o in rows if status_map.get(o.id or 0) == status]

        # Preset filters
        if preset == "overdue_unpaid_7":
            cutoff = today - dt.timedelta(days=7)
            def _is_overdue_unpaid(o: Order) -> bool:
                if (o.status or "") in ("refunded", "switched", "stitched", "cancelled"):
                    return False
                # IBAN-marked orders are considered completed, not overdue/unpaid
                if bool(o.paid_by_bank_transfer):
                    return False
                base_date = o.shipment_date or o.data_date
                if (base_date is None) or (base_date > cutoff):
                    return False
                paid = paid_map.get(o.id or 0, 0.0)
                return paid <= 0.0
            rows = [o for o in rows if _is_overdue_unpaid(o)]

        # Optional repeat-customer filter (show only clients with 2+ orders in the current range)
        if repeat_customer == "multi":
            client_order_counts: dict[int, int] = defaultdict(int)
            for o in rows:
                if o.client_id is None:
                    continue
                client_order_counts[int(o.client_id)] += 1
            rows = [o for o in rows if (o.client_id is not None and client_order_counts.get(int(o.client_id), 0) >= 2)]

        # build simple maps for names based on filtered rows
        client_ids = sorted({o.client_id for o in rows if o.client_id})
        item_ids = sorted({o.item_id for o in rows if o.item_id})
        from ..models import Client, Item  # local import to avoid circulars
        clients = session.exec(select(Client).where(Client.id.in_(client_ids))).all() if client_ids else []
        items = session.exec(select(Item).where(Item.id.in_(item_ids))).all() if item_ids else []
        client_map = {c.id: c.name for c in clients if c.id is not None}
        item_map = {it.id: it.name for it in items if it.id is not None}

        # shipping_map based on Order.total_amount for display
        shipping_map: dict[int, float] = {}
        for o in rows:
            oid = o.id or 0
            # Compute full shipping including 20% tax for display
            pre_tax = None
            if o.shipping_fee is not None:
                pre_tax = float(o.shipping_fee or 0.0)
            else:
                amt = float(o.total_amount or 0.0)
                company_code = o.shipping_company or None
                pre_tax = compute_shipping_fee(
                    amt,
                    company_code=company_code,
                    paid_by_bank_transfer=bool(o.paid_by_bank_transfer),
                    session=session
                )
            shipping_map[oid] = round(float(pre_tax or 0.0) * 1.20, 2)

        # Use stored total_cost; if missing, compute via batch-loaded OrderItems and Items
        from sqlmodel import select as _select
        cost_map: dict[int, float] = {}
        for o in rows:
            if o.total_cost is not None:
                cost_map[o.id or 0] = float(o.total_cost or 0.0)
        missing_ids = [o.id for o in rows if (o.id and (o.id not in cost_map))]
        if missing_ids:
            oitems = session.exec(_select(OrderItem).where(OrderItem.order_id.in_(missing_ids))).all()
            order_item_map: dict[int, list[tuple[int, int]]] = {}
            item_ids_needed: set[int] = set()
            for oi in oitems:
                if oi.order_id is None or oi.item_id is None:
                    continue
                order_item_map.setdefault(int(oi.order_id), []).append((int(oi.item_id), int(oi.quantity or 0)))
                item_ids_needed.add(int(oi.item_id))
            item_cost_map: dict[int, float] = {}
            if item_ids_needed:
                cost_items = session.exec(_select(Item).where(Item.id.in_(sorted(item_ids_needed)))).all()
                for it in cost_items:
                    if it.id is not None:
                        item_cost_map[int(it.id)] = float(it.cost or 0.0)
            for oid in missing_ids:
                acc = 0.0
                for (iid, qty) in order_item_map.get(int(oid), []):
                    acc += float(item_cost_map.get(int(iid), 0.0)) * int(qty or 0)
                cost_map[int(oid)] = round(acc, 2)

        # For refunded/switched/cancelled orders, force cost to zero for display/aggregates
        for o in rows:
            if (o.status or "") in ("refunded", "switched", "stitched", "cancelled"):
                if o.id is not None:
                    cost_map[int(o.id)] = 0.0

        # Filter for high cost ratio (>= 70% of Toplam)
        if preset == "high_cost_70":
            def _high_cost(o: Order) -> bool:
                total = float(o.total_amount or 0.0)
                if total <= 0.0:
                    return False
                cost = float(cost_map.get(o.id or 0, 0.0))
                return cost >= 0.7 * total
            rows = [o for o in rows if _high_cost(o)]

        # Load active shipping companies for dropdown
        shipping_companies = session.exec(
            select(ShippingCompanyRate)
            .where(ShippingCompanyRate.is_active == True)
            .order_by(ShippingCompanyRate.company_code)
        ).all()
        
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "orders_table.html",
            {
                "request": request,
                "rows": rows,
                "client_map": client_map,
                "item_map": item_map,
                "status_map": status_map,
                "shipping_map": shipping_map,
                "cost_map": cost_map,
                "shipping_companies": shipping_companies,
                # totals over filtered rows
                "sum_qty": sum(int(o.quantity or 0) for o in rows),
                "sum_total": sum(float(o.total_amount or 0.0) for o in rows),
                "sum_cost": sum(float(cost_map.get(o.id or 0, 0.0)) for o in rows),
                "sum_shipping": sum(float(shipping_map.get(o.id or 0, 0.0)) for o in rows),
                # current filters
                "start": start_date,
                "end": end_date,
                "date_field": date_field,
                "source": source,
                "status": status,
                "ig_linked": ig_linked,
                "repeat_customer": repeat_customer,
                # current preset
                "preset": preset,
            },
        )


@router.post("/recalc-financials")
def recalc_financials():
    with get_session() as session:
        from sqlmodel import select as _select
        rows = session.exec(select(Order)).all()
        updated = 0
        for o in rows:
            # shipping from toplam; zero totals => base only
            amt = float(o.total_amount or 0.0)
            company_code = o.shipping_company or None
            if bool(o.paid_by_bank_transfer):
                # IBAN ödemelerinde sadece base fee (kargo firmasına göre)
                o.shipping_fee = compute_shipping_fee(
                    amt,
                    company_code=company_code,
                    paid_by_bank_transfer=True,
                    session=session
                )
            else:
                o.shipping_fee = compute_shipping_fee(
                    amt,
                    company_code=company_code,
                    paid_by_bank_transfer=False,
                    session=session
                )
            # cost from order items using FIFO (zero if refunded/switched or negative totals)
            if (o.status or "") in ("refunded", "switched", "stitched", "cancelled") or (float(o.total_amount or 0.0) < 0.0):
                o.total_cost = 0.0
            else:
                from ..services.inventory import calculate_order_cost_fifo
                o.total_cost = calculate_order_cost_fifo(session, o.id or 0)
            updated += 1
        return {"status": "ok", "orders_updated": updated}


@router.post("/recalc-costs")
def recalc_costs():
    with get_session() as session:
        rows = session.exec(select(Order)).all()
        from ..services.inventory import calculate_order_cost_fifo
        updated = 0
        for o in rows:
            if not o.id:
                continue
            if (o.status or "") in ("refunded", "switched", "stitched") or (float(o.total_amount or 0.0) < 0.0):
                if o.total_cost != 0.0:
                    o.total_cost = 0.0
                    updated += 1
                continue
            new_cost = calculate_order_cost_fifo(session, o.id)
            if o.total_cost != new_cost:
                o.total_cost = new_cost
                updated += 1
        return {"status": "ok", "orders_updated": updated}


@router.post("/{order_id}/refund")
def refund_order(order_id: int):
    with get_session() as session:
        o = session.exec(select(Order).where(Order.id == order_id)).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")
        # idempotent: if already refunded/switched/cancelled, do nothing
        if (o.status or "") in ("refunded", "switched", "stitched", "cancelled"):
            return {"status": "ok", "message": "already_processed"}
        # add stock back for all order items
        oitems = session.exec(select(OrderItem).where(OrderItem.order_id == order_id)).all()
        for oi in oitems:
            if oi.item_id is None:
                continue
            qty = int(oi.quantity or 0)
            if qty > 0:
                adjust_stock(session, item_id=int(oi.item_id), delta=qty, related_order_id=order_id)
        o.status = "refunded"
        o.return_or_switch_date = dt.date.today()
        # cost should be zero for refunds
        try:
            o.total_cost = 0.0
        except Exception:
            pass
        return {"status": "ok"}


@router.post("/{order_id}/cancel")
def cancel_order(order_id: int, body: dict = Body(default={})):
    """Cancel an order. Optionally restore inventory and zero out financials."""
    with get_session() as session:
        o = session.exec(select(Order).where(Order.id == order_id)).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")
        # Allow re-processing cancelled orders to fix financials
        # Only skip if already refunded/switched/stitched (those are final states)
        if (o.status or "") in ("refunded", "switched", "stitched"):
            return {"status": "ok", "message": "already_processed"}

        # Check if we should restore inventory (default: True)
        restore_inventory = body.get("restore_inventory", True)
        if isinstance(restore_inventory, str):
            restore_inventory = restore_inventory.lower() in ("true", "1", "yes", "on")

        # Restore inventory if requested
        if restore_inventory:
            oitems = session.exec(select(OrderItem).where(OrderItem.order_id == order_id)).all()
            for oi in oitems:
                if oi.item_id is None:
                    continue
                qty = int(oi.quantity or 0)
                if qty > 0:
                    adjust_stock(
                        session,
                        item_id=int(oi.item_id),
                        delta=qty,
                        related_order_id=order_id,
                        reason=f"order-cancel:{order_id}",
                    )

        # Mark as cancelled and zero out financial impact
        o.status = "cancelled"
        o.return_or_switch_date = dt.date.today()
        try:
            # These will make reports treat the order as "no sale"
            o.total_amount = 0.0
            o.total_cost = 0.0
            o.shipping_fee = 0.0
        except Exception:
            pass

        # Log the cancellation
        try:
            from ..models import OrderEditLog

            session.add(
                OrderEditLog(
                    order_id=order_id,
                    action="cancel",
                    changes_json=str({"restore_inventory": restore_inventory}),
                )
            )
        except Exception:
            pass

        return {"status": "ok", "restore_inventory": restore_inventory}


@router.get("/export")
def export_orders(
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    date_field: str = Query(default="shipment", regex="^(shipment|data|both)$"),
    source: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    preset: Optional[str] = Query(default=None),
    repeat_customer: Optional[str] = Query(default=None, regex="^(multi)?$"),
):
    if openpyxl is None:
        raise HTTPException(status_code=500, detail="openpyxl not available for export")
    def _parse_date_or_default(value: Optional[str], fallback: dt.date) -> dt.date:
        try:
            if value:
                return dt.date.fromisoformat(value)
        except Exception:
            pass
        return fallback
    with get_session() as session:
        today = dt.date.today()
        default_start = today - dt.timedelta(days=6)
        start_date = _parse_date_or_default(start, default_start)
        end_date = _parse_date_or_default(end, today)
        if end_date < start_date:
            start_date, end_date = end_date, start_date
        if date_field == "both":
            q = (
                select(Order)
                .where(
                    or_(
                        and_(Order.shipment_date.is_not(None), Order.shipment_date >= start_date, Order.shipment_date <= end_date),
                        and_(Order.data_date.is_not(None), Order.data_date >= start_date, Order.data_date <= end_date),
                    )
                )
                .order_by(Order.id.desc())
            )
        else:
            date_col = Order.shipment_date if date_field == "shipment" else Order.data_date
            alt_date_col = Order.data_date if date_field == "shipment" else Order.shipment_date
            q = (
                select(Order)
                .where(
                    or_(
                        and_(date_col.is_not(None), date_col >= start_date, date_col <= end_date),
                        and_(date_col.is_(None), alt_date_col.is_not(None), alt_date_col >= start_date, alt_date_col <= end_date),
                    )
                )
                .order_by(Order.id.desc())
            )
        if (preset not in ("overdue_unpaid_7", "all")) and source in ("bizim", "kargo"):
            q = q.where(Order.source == source)
        rows = session.exec(q).all()
        # payments map
        order_ids = [o.id for o in rows if o.id]
        pays = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all() if order_ids else []
        paid_map: dict[int, float] = {}
        for p in pays:
            if p.order_id is None:
                continue
            paid_map[p.order_id] = paid_map.get(p.order_id, 0.0) + float(p.amount or 0.0)
        status_map: dict[int, str] = {}
        for o in rows:
            oid = o.id or 0
            total = float(o.total_amount or 0.0)
            paid = paid_map.get(oid, 0.0)
            if (o.status or "") in ("refunded", "switched", "stitched", "cancelled"):
                status_map[oid] = str(o.status)
            else:
                # Treat IBAN (bank transfer) as paid/completed
                if bool(o.paid_by_bank_transfer):
                    status_map[oid] = "paid"
                else:
                    status_map[oid] = "paid" if (paid > 0 and paid >= total) else "unpaid"
        if (preset not in ("overdue_unpaid_7", "all")) and status in ("paid", "unpaid", "refunded", "switched", "cancelled"):
            rows = [o for o in rows if status_map.get(o.id or 0) == status]
        if preset == "overdue_unpaid_7":
            cutoff = today - dt.timedelta(days=7)
            def _is_overdue_unpaid(o: Order) -> bool:
                if (o.status or "") in ("refunded", "switched", "stitched", "cancelled"):
                    return False
                base_date = o.shipment_date or o.data_date
                if (base_date is None) or (base_date > cutoff):
                    return False
                paid = paid_map.get(o.id or 0, 0.0)
                return paid <= 0.0
            rows = [o for o in rows if _is_overdue_unpaid(o)]
        # Optional repeat-customer filter for export (2+ orders in current range)
        if repeat_customer == "multi":
            client_order_counts: dict[int, int] = defaultdict(int)
            for o in rows:
                if o.client_id is None:
                    continue
                client_order_counts[int(o.client_id)] += 1
            rows = [o for o in rows if (o.client_id is not None and client_order_counts.get(int(o.client_id), 0) >= 2)]
        # Build cost_map similar to table for high_cost filtering
        from sqlmodel import select as _select
        cost_map: dict[int, float] = {}
        for o in rows:
            if o.total_cost is not None:
                cost_map[o.id or 0] = float(o.total_cost or 0.0)
        missing_ids = [o.id for o in rows if (o.id and (o.id not in cost_map))]
        if missing_ids:
            oitems = session.exec(_select(OrderItem).where(OrderItem.order_id.in_(missing_ids))).all()
            order_item_map: dict[int, list[tuple[int, int]]] = {}
            item_ids_needed: set[int] = set()
            for oi in oitems:
                if oi.order_id is None or oi.item_id is None:
                    continue
                order_item_map.setdefault(int(oi.order_id), []).append((int(oi.item_id), int(oi.quantity or 0)))
                item_ids_needed.add(int(oi.item_id))
            item_cost_map: dict[int, float] = {}
            if item_ids_needed:
                cost_items = session.exec(_select(Item).where(Item.id.in_(sorted(item_ids_needed)))).all()
                for it in cost_items:
                    if it.id is not None:
                        item_cost_map[int(it.id)] = float(it.cost or 0.0)
            for oid in missing_ids:
                acc = 0.0
                for (iid, qty) in order_item_map.get(int(oid), []):
                    acc += float(item_cost_map.get(int(iid), 0.0)) * int(qty or 0)
                cost_map[int(oid)] = round(acc, 2)
        # Force zero cost for refunded/switched/cancelled orders
        for o in rows:
            if (o.status or "") in ("refunded", "switched", "stitched", "cancelled"):
                if o.id is not None:
                    cost_map[int(o.id)] = 0.0
        # Apply high_cost_70 preset if requested
        if preset == "high_cost_70":
            def _high_cost(o: Order) -> bool:
                total = float(o.total_amount or 0.0)
                if total <= 0.0:
                    return False
                cost = float(cost_map.get(o.id or 0, 0.0))
                return cost >= 0.7 * total
            rows = [o for o in rows if _high_cost(o)]
        # client names
        client_ids = sorted({o.client_id for o in rows if o.client_id})
        clients = session.exec(select(Item).where(Item.id == 0)).all()  # no-op to keep types
        from ..models import Client as _Client
        clients = session.exec(select(_Client).where(_Client.id.in_(client_ids))).all() if client_ids else []
        client_map = {c.id: c.name for c in clients if c.id is not None}
        # workbook
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Orders"
        ws.append(["ID", "Takip", "Musteri", "Adet", "Toplam", "Kargo", "KargoTarihi", "DataTarihi", "I/D Tarihi", "Durum", "Kanal"]) 
        for o in rows:
            cname = client_map.get(o.client_id) if o.client_id else None
            ws.append([
                o.id,
                o.tracking_no,
                cname,
                o.quantity,
                o.total_amount,
                o.shipping_fee,
                o.shipment_date,
                o.data_date,
                getattr(o, "return_or_switch_date", None),
                status_map.get(o.id or 0),
                o.source,
            ])
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"orders_export_{ts}.xlsx"
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )


@router.post("/{order_id}/switch")
def switch_order(order_id: int):
    with get_session() as session:
        o = session.exec(select(Order).where(Order.id == order_id)).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")
        # idempotent: if already refunded/switched/cancelled, do nothing
        if (o.status or "") in ("refunded", "switched", "stitched", "cancelled"):
            return {"status": "ok", "message": "already_processed"}
        # add stock back for all order items
        oitems = session.exec(select(OrderItem).where(OrderItem.order_id == order_id)).all()
        for oi in oitems:
            if oi.item_id is None:
                continue
            qty = int(oi.quantity or 0)
            if qty > 0:
                adjust_stock(session, item_id=int(oi.item_id), delta=qty, related_order_id=order_id)
        o.status = "switched"
        o.return_or_switch_date = dt.date.today()
        # cost should be zero for switches
        try:
            o.total_cost = 0.0
        except Exception:
            pass
        return {"status": "ok"}


# Backward-compatible alias: /stitch maps to switch
@router.post("/{order_id}/stitch")
def stitch_order(order_id: int):
    return switch_order(order_id)


@router.post("/{order_id}/update-total")
def update_total(order_id: int, body: dict):
    try:
        new_total_raw = body.get("total")
        new_total = float(new_total_raw)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid total")
    # Optional IBAN flag
    iban_flag_raw = body.get("paid_by_bank_transfer", None)
    try:
        iban_flag = False if iban_flag_raw is None else (str(iban_flag_raw).lower() in ("1", "true", "on", "yes"))
    except Exception:
        iban_flag = False
    # Optional shipping company
    shipping_company_raw = body.get("shipping_company", None)
    shipping_company = None
    if shipping_company_raw is not None:
        shipping_company = str(shipping_company_raw).strip() if shipping_company_raw else None
        if shipping_company == "":
            shipping_company = None
    # Optional payment status
    payment_status_raw = body.get("payment_status", None)
    payment_status = None
    if payment_status_raw is not None:
        payment_status = str(payment_status_raw).strip() if payment_status_raw else None
        if payment_status == "":
            payment_status = None
    # Optional payment date
    payment_date_raw = body.get("payment_date", None)
    payment_date = None
    if payment_date_raw is not None:
        try:
            payment_date_str = str(payment_date_raw).strip()
            if payment_date_str:
                payment_date = dt.date.fromisoformat(payment_date_str)
        except Exception:
            pass
    with get_session() as session:
        o = session.exec(select(Order).where(Order.id == order_id)).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")
        prev_total = float(o.total_amount or 0.0)
        new_total_rounded = round(new_total, 2)
        o.total_amount = new_total_rounded
        # Update IBAN flag if provided; None keeps existing value
        changes = {"total_amount": [prev_total, new_total_rounded]}
        try:
            if iban_flag_raw is not None:
                prev_iban = bool(o.paid_by_bank_transfer or False)
                if bool(iban_flag) != prev_iban:
                    o.paid_by_bank_transfer = bool(iban_flag)
                    changes["paid_by_bank_transfer"] = [prev_iban, bool(iban_flag)]
        except Exception:
            pass
        # Update shipping company if provided
        try:
            if shipping_company_raw is not None:
                prev_company = o.shipping_company
                if shipping_company != prev_company:
                    o.shipping_company = shipping_company
                    changes["shipping_company"] = [prev_company, shipping_company]
        except Exception:
            pass
        # Update payment status if provided
        try:
            if payment_status_raw is not None:
                prev_status = o.status
                if payment_status != prev_status:
                    o.status = payment_status
                    changes["status"] = [prev_status, payment_status]
        except Exception:
            pass
        # Update payment date if provided
        try:
            if payment_date_raw is not None:
                prev_payment_date = o.payment_date
                if payment_date != prev_payment_date:
                    o.payment_date = payment_date
                    changes["payment_date"] = [prev_payment_date.isoformat() if prev_payment_date else None, payment_date.isoformat() if payment_date else None]
        except Exception:
            pass
        
        # Manage Payment records based on payment_status
        try:
            # Get existing payment records for this order
            existing_payments = session.exec(
                select(Payment).where(Payment.order_id == order_id)
            ).all()
            
            if payment_status_raw is not None:
                if payment_status == "paid":
                    # Create or update Payment record
                    # Use provided payment_date or Order.payment_date or today
                    effective_payment_date = payment_date or o.payment_date or dt.date.today()
                    
                    if existing_payments:
                        # Update existing payment (use the first one if multiple)
                        payment = existing_payments[0]
                        prev_amount = payment.amount
                        prev_date = payment.payment_date or payment.date
                        payment.amount = new_total_rounded
                        payment.payment_date = effective_payment_date
                        payment.date = effective_payment_date  # Update legacy field too
                        payment.net_amount = new_total_rounded  # Simple: net = amount (no fees)
                        session.add(payment)
                        changes["payment"] = [f"updated: {prev_amount} -> {new_total_rounded}, {prev_date} -> {effective_payment_date}"]
                        
                        # Delete other payment records if there are multiple
                        if len(existing_payments) > 1:
                            for extra_payment in existing_payments[1:]:
                                session.delete(extra_payment)
                            changes["payment"] = changes.get("payment", "") + f", deleted {len(existing_payments)-1} duplicate(s)"
                    else:
                        # Create new payment record
                        if o.client_id:
                            payment = Payment(
                                client_id=o.client_id,
                                order_id=order_id,
                                amount=new_total_rounded,
                                payment_date=effective_payment_date,
                                date=effective_payment_date,  # Legacy field
                                net_amount=new_total_rounded,  # Simple: net = amount (no fees)
                                method=None,  # Can be set later if needed
                            )
                            session.add(payment)
                            changes["payment"] = [f"created: {new_total_rounded} on {effective_payment_date}"]
                
                elif payment_status == "unpaid":
                    # Delete all payment records for this order
                    if existing_payments:
                        for payment in existing_payments:
                            session.delete(payment)
                        changes["payment"] = [f"deleted {len(existing_payments)} payment record(s)"]
                
                # For other statuses (partial_paid, etc.), keep existing payments as-is
                # Payment records remain unchanged for partial_paid, cancelled, refunded, etc.
            
            # If payment_date was updated and we have existing payments (and status is not unpaid), update their dates too
            # Note: If payment_status was "paid", we already updated the payment date above, so this handles other cases
            if payment_date_raw is not None and payment_status != "unpaid":
                # Re-fetch payments in case they were just created/updated above
                current_payments = session.exec(
                    select(Payment).where(Payment.order_id == order_id)
                ).all()
                if current_payments:
                    for payment in current_payments:
                        if payment.payment_date != payment_date:
                            payment.payment_date = payment_date
                            payment.date = payment_date  # Update legacy field too
                            session.add(payment)
                            if "payment" not in changes:
                                changes["payment"] = []
                            if isinstance(changes["payment"], list) and len(changes["payment"]) > 0 and isinstance(changes["payment"][0], str):
                                if "updated date" not in changes["payment"][0]:
                                    changes["payment"][0] = changes["payment"][0] + f", updated date to {payment_date}"
                            else:
                                changes["payment"] = [f"updated payment date to {payment_date}"]
        except Exception as e:
            # Log error but don't fail the request
            changes["payment_error"] = [str(e)]
            pass
        # Recompute shipping_fee pre-tax based on flag, company, and new total
        try:
            company_code = o.shipping_company or None
            o.shipping_fee = compute_shipping_fee(
                new_total_rounded,
                company_code=company_code,
                paid_by_bank_transfer=bool(o.paid_by_bank_transfer),
                session=session
            )
        except Exception:
            pass
        # Ensure product cost is zero when refunded/switched/stitched or negative totals
        try:
            status_lc = str(o.status or "").lower()
            if status_lc in ("refunded", "switched", "stitched", "cancelled") or (float(o.total_amount or 0.0) < 0.0):
                o.total_cost = 0.0
        except Exception:
            pass
        try:
            # log
            session.add(OrderEditLog(order_id=order_id, action="update_total", changes_json=str(changes)))
        except Exception:
            pass
        return {"status": "ok"}


@router.get("/{order_id}/edit")
def edit_order_page(order_id: int, request: Request, base: Optional[str] = Query(default=None)):
    with get_session() as session:
        o = session.exec(select(Order).where(Order.id == order_id)).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")
        # small reference lists for comboboxes
        clients = session.exec(select(Client).order_by(Client.id.desc()).limit(500)).all()
        items = session.exec(select(Item).order_by(Item.id.desc()).limit(500)).all()
        # load existing order items and a larger item pool for comboboxes
        from sqlmodel import select as _select
        oitems = session.exec(_select(OrderItem).where(OrderItem.order_id == order_id)).all()
        order_items: list[dict] = []
        for oi in oitems:
            it = session.exec(_select(Item).where(Item.id == oi.item_id)).first() if oi.item_id is not None else None
            order_items.append({"item": it, "quantity": int(oi.quantity or 0)})
        items_all = session.exec(select(Item).order_by(Item.id.desc()).limit(5000)).all()
        logs = session.exec(select(OrderEditLog).where(OrderEditLog.order_id == order_id).order_by(OrderEditLog.id.desc()).limit(100)).all()
        
        # Load merged orders (orders merged into this one, or this order merged into another)
        merged_orders = []
        if o.partial_payment_group_id and o.partial_payment_group_id == o.id:
            # This is the primary order, find all orders merged into it
            merged_orders = session.exec(select(Order).where(Order.merged_into_order_id == order_id)).all()
        
        # current display strings
        client_disp = None
        if o.client_id:
            c = session.exec(select(Client).where(Client.id == o.client_id)).first()
            if c:
                client_disp = f"{c.id} | {c.name} | {c.phone or ''}"
        item_disp = None
        if o.item_id:
            it = session.exec(select(Item).where(Item.id == o.item_id)).first()
            if it:
                item_disp = f"{it.id} | {it.sku} | {it.name}"
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "order_edit.html",
            {
                "request": request,
                "order": o,
                "clients": clients,
                "items": items,
                "order_items": order_items,
                "items_all": items_all,
                "logs": logs,
                "client_disp": client_disp,
                "item_disp": item_disp,
                "merged_orders": merged_orders,
            },
        )


@router.post("/{order_id}/edit")
async def edit_order_apply(order_id: int, request: Request):
    form = await request.form()
    def _get(name: str) -> str:
        return str(form.get(name) or "").strip()
    # parse combobox inputs: expect leading numeric id
    def _parse_id(val: str) -> Optional[int]:
        s = (val or "").strip()
        if not s:
            return None
        # take only the first token before '|' to avoid concatenating phone numbers
        first = s.split("|", 1)[0].strip()
        # extract leading digits only
        try:
            import re as _re
            m = _re.match(r"^(\d+)", first)
            if not m:
                return None
            return int(m.group(1))
        except Exception:
            return None
    client_val = _get("client")
    item_val = _get("item")
    new_client_id = _parse_id(client_val)
    new_item_id = _parse_id(item_val)
    new_tracking = _get("tracking_no") or None
    new_quantity = _get("quantity")
    new_total = _get("total_amount")
    new_ship = _get("shipment_date")
    new_data = _get("data_date")
    new_status = _get("status") or None
    new_notes = _get("notes") or None
    new_source = _get("source") or None
    new_paid_by_bank_transfer = (_get("paid_by_bank_transfer").lower() in ("1","true","on","yes"))

    with get_session() as session:
        o = session.exec(select(Order).where(Order.id == order_id)).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")
        changes = {}
        # Multi-item editor support: parse arrays item_ids[] and qtys[]
        def _getlist(key: str) -> list[str]:
            try:
                return list(form.getlist(key))  # type: ignore[attr-defined]
            except Exception:
                vals = []
                try:
                    for k, v in form.multi_items():  # type: ignore[attr-defined]
                        if k == key:
                            vals.append(str(v))
                except Exception:
                    pass
                return vals
        items_changed = False
        item_ids_arr = _getlist("item_ids[]")
        qtys_arr = _getlist("qtys[]")
        new_items_map: dict[int, int] = {}
        # keep raw mapping to help resolve by SKU if id not found
        raw_by_parsed: dict[int, list[tuple[str, int]]] = {}
        if item_ids_arr and qtys_arr and (len(item_ids_arr) == len(qtys_arr)):
            for s_iid, s_qty in zip(item_ids_arr, qtys_arr):
                iid = _parse_id(str(s_iid))
                try:
                    qv = int(str(s_qty).replace(",", "."))
                except Exception:
                    qv = 0
                if iid is None or qv <= 0:
                    continue
                new_items_map[iid] = new_items_map.get(iid, 0) + qv
                raw_by_parsed.setdefault(iid, []).append((str(s_iid), int(qv)))
        if new_items_map:
            from sqlmodel import select as _select
            cur_items = session.exec(_select(OrderItem).where(OrderItem.order_id == order_id)).all()
            old_items_map: dict[int, int] = {}
            for oi in cur_items:
                if oi.item_id is None:
                    continue
                old_items_map[int(oi.item_id)] = old_items_map.get(int(oi.item_id), 0) + int(oi.quantity or 0)
            if new_items_map != old_items_map:
                items_changed = True
                changes["items"] = [old_items_map, new_items_map]
                # validate all incoming item ids exist before mutating state
                try:
                    from ..models import Item as _Item
                    existing = session.exec(_select(_Item).where(_Item.id.in_(list(new_items_map.keys())))).all()
                    existing_ids = {int(it.id) for it in existing if it.id is not None}
                    missing_ids = [int(i) for i in new_items_map.keys() if int(i) not in existing_ids]
                    # Attempt to resolve missing ids by SKU from "id | sku | name" display strings
                    if missing_ids:
                        resolved: dict[int, int] = {}
                        for mid in list(missing_ids):
                            raws = raw_by_parsed.get(int(mid), [])
                            # take first raw to resolve; quantities are aggregated below
                            sku_candidate = None
                            for raw_val, _q in raws:
                                parts = [p.strip() for p in str(raw_val).split("|")]
                                if len(parts) >= 2:
                                    sku_candidate = parts[1]
                                    break
                            if sku_candidate:
                                it2 = session.exec(_select(_Item).where(_Item.sku == sku_candidate)).first()
                                if it2 and it2.id is not None:
                                    resolved[int(mid)] = int(it2.id)
                        # apply resolutions
                        for old_id, new_id in resolved.items():
                            qty_sum = new_items_map.pop(int(old_id), 0)
                            if qty_sum:
                                new_items_map[int(new_id)] = new_items_map.get(int(new_id), 0) + int(qty_sum)
                        # recompute missing after resolution
                        existing = session.exec(_select(_Item).where(_Item.id.in_(list(new_items_map.keys())))).all()
                        existing_ids = {int(it.id) for it in existing if it.id is not None}
                        missing_ids = [int(i) for i in new_items_map.keys() if int(i) not in existing_ids]
                    if missing_ids:
                        # include raw hints if available
                        missing_hints = []
                        for mid in missing_ids:
                            raws = raw_by_parsed.get(int(mid), [])
                            if raws:
                                missing_hints.append(f"{mid} ({raws[0][0]})")
                            else:
                                missing_hints.append(str(mid))
                        raise HTTPException(status_code=400, detail=f"Unknown item_id(s): {missing_hints}")
                except HTTPException:
                    # bubble up validation error
                    raise
                except Exception:
                    # fall back to runtime FK error if validation unexpectedly fails
                    pass
                # remove existing order items
                for oi in cur_items:
                    session.delete(oi)
                # delete only 'out' movements for this order
                from ..models import StockMovement as _SM
                mvs = session.exec(_select(_SM).where(_SM.related_order_id == order_id)).all()
                for mv in mvs:
                    if (mv.direction or "out") == "out":
                        session.delete(mv)
                # rebuild from new map
                total_qty_sum = 0
                rep_item_id = None
                for iid in new_items_map.keys():
                    rep_item_id = rep_item_id or int(iid)
                for iid, qv in new_items_map.items():
                    total_qty_sum += int(qv)
                    session.add(OrderItem(order_id=order_id, item_id=int(iid), quantity=int(qv)))
                    if (o.status or "") not in ("refunded", "switched", "stitched", "cancelled"):
                        adjust_stock(session, item_id=int(iid), delta=-int(qv), related_order_id=order_id, reason=f"order-edit:{order_id}")
                    else:
                        adjust_stock(session, item_id=int(iid), delta=int(qv), related_order_id=order_id, reason=f"order-edit:{order_id}:status={o.status}")
                if rep_item_id is not None and rep_item_id != (o.item_id or None):
                    changes["item_id"] = [o.item_id, rep_item_id]
                    o.item_id = rep_item_id
                if total_qty_sum != int(o.quantity or 0):
                    changes["quantity"] = [o.quantity, total_qty_sum]
                    o.quantity = total_qty_sum
                # recompute total_cost using FIFO (zero if refunded/switched)
                try:
                    from ..services.inventory import calculate_order_cost_fifo
                    if (o.status or "") in ("refunded", "switched", "stitched"):
                        o.total_cost = 0.0
                    else:
                        o.total_cost = calculate_order_cost_fifo(session, order_id)
                except Exception:
                    pass
        # compare and set fields
        if new_client_id and new_client_id != o.client_id:
            # validate client exists
            cexists = session.exec(select(Client).where(Client.id == new_client_id)).first()
            if not cexists:
                raise HTTPException(status_code=400, detail="Client not found")
            changes["client_id"] = [o.client_id, new_client_id]
            o.client_id = new_client_id  # type: ignore
        if (not items_changed) and new_item_id and new_item_id != (o.item_id or None):
            # validate target item exists to avoid FK violation on autoflush.
            # If missing, attempt to resolve by SKU from combobox value "id | sku | name".
            it_exists = session.exec(select(Item).where(Item.id == new_item_id)).first()
            if not it_exists:
                # try resolving by SKU
                parts = [p.strip() for p in (item_val or "").split("|")]
                sku = parts[1] if len(parts) >= 2 else None
                if sku:
                    it2 = session.exec(select(Item).where(Item.sku == sku)).first()
                    if it2 and it2.id is not None:
                        new_item_id = int(it2.id)
                    else:
                        raise HTTPException(status_code=400, detail="Item not found")
                else:
                    raise HTTPException(status_code=400, detail="Item not found")
            changes["item_id"] = [o.item_id, new_item_id]
            o.item_id = new_item_id
        if new_tracking != (o.tracking_no or None):
            changes["tracking_no"] = [o.tracking_no, new_tracking]
            o.tracking_no = new_tracking
        try:
            if (not items_changed) and new_quantity != "":
                qv = int(new_quantity)
                if qv != int(o.quantity or 0):
                    changes["quantity"] = [o.quantity, qv]
                    o.quantity = qv
        except Exception:
            pass
        try:
            if new_total != "":
                tv = round(float(new_total.replace(",", ".")), 2)
                if tv != float(o.total_amount or 0.0):
                    changes["total_amount"] = [o.total_amount, tv]
                    o.total_amount = tv
        except Exception:
            pass
        import datetime as _dt
        def _parse_date(s: Optional[str]):
            if not s:
                return None
            try:
                return _dt.date.fromisoformat(str(s))
            except Exception:
                return None
        sd = _parse_date(new_ship)
        if sd != (o.shipment_date or None):
            changes["shipment_date"] = [o.shipment_date, sd]
            o.shipment_date = sd
        dd = _parse_date(new_data)
        if dd != (o.data_date or None):
            changes["data_date"] = [o.data_date, dd]
            o.data_date = dd
        if new_source and new_source != (o.source or None):
            changes["source"] = [o.source, new_source]
            o.source = new_source
        if new_notes != (o.notes or None):
            changes["notes"] = [o.notes, new_notes]
            o.notes = new_notes
        # paid_by_bank_transfer flag
        prev_iban = bool(o.paid_by_bank_transfer or False)
        if new_paid_by_bank_transfer != prev_iban:
            changes["paid_by_bank_transfer"] = [prev_iban, new_paid_by_bank_transfer]
            o.paid_by_bank_transfer = new_paid_by_bank_transfer

        prev_status = o.status or None
        if new_status != prev_status:
            changes["status"] = [prev_status, new_status]
            o.status = new_status

        # apply inventory adjustments when item/quantity/status changed
        inv_touch = items_changed or any(k in changes for k in ("item_id", "quantity", "status"))
        if inv_touch and (not items_changed):
            from sqlmodel import select as _select
            # remove existing out movements and order items, then rebuild from current item_id/quantity
            # when transitioning to refunded/switched -> add 'in' movements
            oitems = session.exec(_select(OrderItem).where(OrderItem.order_id == order_id)).all()
            for oi in oitems:
                session.delete(oi)
            # delete only 'out' movements; preserve 'in' history
            from ..models import StockMovement as _SM
            mvs = session.exec(_select(_SM).where(_SM.related_order_id == order_id)).all()
            for mv in mvs:
                if (mv.direction or "out") == "out":
                    session.delete(mv)
            # rebuild order items/out movements if order is not refunded/switched
            if (o.status or "") not in ("refunded", "switched", "stitched", "cancelled"):
                if o.item_id and int(o.quantity or 0) > 0:
                    session.add(OrderItem(order_id=order_id, item_id=int(o.item_id), quantity=int(o.quantity or 0)))
                    adjust_stock(session, item_id=int(o.item_id), delta=-int(o.quantity or 0), related_order_id=order_id, reason=f"order-edit:{order_id}")
            else:
                # ensure restock 'in' movements exist
                if o.item_id and int(o.quantity or 0) > 0:
                    adjust_stock(session, item_id=int(o.item_id), delta=int(o.quantity or 0), related_order_id=order_id, reason=f"order-edit:{order_id}:status={o.status}")
                if not o.return_or_switch_date:
                    o.return_or_switch_date = _dt.date.today()

        # write log
        try:
            editor = request.session.get("uid") if hasattr(request, "session") else None
            session.add(OrderEditLog(order_id=order_id, editor_user_id=editor, action="edit", changes_json=str(changes)))
        except Exception:
            pass

        # Update shipping fee based on IBAN flag and current total
        try:
            if bool(o.paid_by_bank_transfer):
                # IBAN: only base fee (pre-tax)
                o.shipping_fee = 89.0
            else:
                amt = float(o.total_amount or 0.0)
                o.shipping_fee = compute_shipping_fee(amt)
        except Exception:
            pass
        # Final guard: zero product cost if refunded/switched/stitched or negative totals
        try:
            status_lc = str(o.status or "").lower()
            if status_lc in ("refunded", "switched", "stitched", "cancelled") or (float(o.total_amount or 0.0) < 0.0):
                o.total_cost = 0.0
        except Exception:
            pass

        return {"status": "ok", "changed": changes}


@router.post("/{order_id}/apply-mapping")
async def apply_mapping(order_id: int, request: Request):
    form = await request.form()
    def _get(name: str) -> str:
        return str(form.get(name) or "").strip()
    try:
        out_id = int(_get("output_id"))
    except Exception:
        raise HTTPException(status_code=400, detail="output_id required")
    apply_price = _get("apply_price") in ("1", "true", "on", "yes")
    mode = _get("mode") or "apply"
    qty_override = _get("quantity")
    with get_session() as session:
        from ..models import ItemMappingOutput as _Out, StockMovement as _SM
        o = session.exec(select(Order).where(Order.id == order_id)).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")
        out = session.exec(select(_Out).where(_Out.id == out_id)).first()
        if not out:
            raise HTTPException(status_code=404, detail="Mapping output not found")
        # materialize item
        target_item_id = None
        if out.item_id:
            target_item_id = int(out.item_id)
        else:
            if not out.product_id:
                raise HTTPException(status_code=400, detail="Output has no product reference")
            # ensure variant exists
            item = _get_or_create_item(session, product_id=int(out.product_id), size=out.size, color=out.color)
            target_item_id = int(item.id or 0)
        if target_item_id <= 0:
            raise HTTPException(status_code=400, detail="Unable to resolve item for mapping")
        # If mode=append, just return item info for client to add a row
        if mode == "append":
            q_each = int(out.quantity or 1)
            try:
                if qty_override != "":
                    q_each = int(qty_override)
            except Exception:
                pass
            return {"status": "ok", "item_id": target_item_id, "quantity": q_each}
        changes = {}
        if target_item_id != (o.item_id or 0):
            changes["item_id"] = [o.item_id, target_item_id]
            o.item_id = target_item_id
        # quantity
        if qty_override != "":
            try:
                qv = int(qty_override)
            except Exception:
                qv = int(o.quantity or 0)
        else:
            qv = int(out.quantity or (o.quantity or 1))
        if qv != int(o.quantity or 0):
            changes["quantity"] = [o.quantity, qv]
            o.quantity = qv
        # price
        if apply_price and (out.unit_price is not None):
            # Keep manual total_amount per requirement; do not auto-update
            pass
        # rebuild movements/items
        oitems = session.exec(select(OrderItem).where(OrderItem.order_id == order_id)).all()
        for oi in oitems:
            session.delete(oi)
        mvs = session.exec(select(_SM).where(_SM.related_order_id == order_id)).all()
        for mv in mvs:
            if (mv.direction or "out") == "out":
                session.delete(mv)
        if (o.status or "") not in ("refunded", "switched", "stitched"):
            if o.item_id and int(o.quantity or 0) > 0:
                session.add(OrderItem(order_id=order_id, item_id=int(o.item_id), quantity=int(o.quantity or 0)))
                adjust_stock(session, item_id=int(o.item_id), delta=-int(o.quantity or 0), related_order_id=order_id, reason=f"order-edit:{order_id}")
        else:
            if o.item_id and int(o.quantity or 0) > 0:
                adjust_stock(session, item_id=int(o.item_id), delta=int(o.quantity or 0), related_order_id=order_id, reason=f"order-edit:{order_id}:status={o.status}")
            import datetime as _dt
            if not o.return_or_switch_date:
                o.return_or_switch_date = _dt.date.today()
        try:
            editor = request.session.get("uid") if hasattr(request, "session") else None
            session.add(OrderEditLog(order_id=order_id, editor_user_id=editor, action="apply_mapping", changes_json=str(changes)))
        except Exception:
            pass
        return {"status": "ok", "changed": changes}


@router.get("/duplicates")
def find_duplicates(request: Request, start: Optional[str] = Query(default=None), end: Optional[str] = Query(default=None)):
    import datetime as _dt
    def _parse_date_or_default(value: Optional[str], fallback: _dt.date) -> _dt.date:
        try:
            if value:
                return _dt.date.fromisoformat(value)
        except Exception:
            pass
        return fallback
    with get_session() as session:
        today = _dt.date.today()
        default_start = today - _dt.timedelta(days=30)
        start_date = _parse_date_or_default(start, default_start)
        end_date = _parse_date_or_default(end, today)
        date_col = Order.shipment_date
        from sqlalchemy import func
        # group by (client_id, total_amount, shipment_date) within window; count>1
        groups = session.exec(
            select(Order.client_id, Order.total_amount, date_col, func.count(Order.id)).where(
                date_col.is_not(None), date_col >= start_date, date_col <= end_date
            ).group_by(Order.client_id, Order.total_amount, date_col).having(func.count(Order.id) > 1)
        ).all()
        # fetch orders for those groups
        dupes: list[dict] = []
        for cid, tot, d, cnt in groups:
            rows = session.exec(select(Order).where(Order.client_id == cid, Order.total_amount == tot, date_col == d).order_by(Order.id.desc())).all()
            if len(rows) > 1:
                dupes.append({
                    "client_id": cid,
                    "total": float(tot or 0.0),
                    "date": d,
                    "orders": [{"id": r.id, "tracking_no": r.tracking_no, "status": r.status} for r in rows],
                })
        templates = request.app.state.templates
        return templates.TemplateResponse("orders_duplicates.html", {"request": request, "groups": dupes, "start": start_date, "end": end_date})


@router.get("/partial-payments")
def find_partial_payments(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
):
    """Find potential partial payment orders (same customer phone, same product, different order IDs)."""
    from ..utils.normalize import normalize_phone
    
    def _parse_date_or_default(value: Optional[str], fallback: dt.date) -> dt.date:
        try:
            if value:
                return dt.date.fromisoformat(value)
        except Exception:
            pass
        return fallback

    with get_session() as session:
        today = dt.date.today()
        default_start = dt.date(2025, 10, 1)
        start_date = _parse_date_or_default(start, default_start)
        end_date = _parse_date_or_default(end, today)
        if end_date < start_date:
            start_date, end_date = end_date, start_date

        # Find orders that are not already merged and not refunded/switched/stitched
        base_query = (
            select(Order)
            .where(
                Order.merged_into_order_id.is_(None),
                or_(
                    and_(Order.shipment_date.is_not(None), Order.shipment_date >= start_date, Order.shipment_date <= end_date),
                    and_(Order.data_date.is_not(None), Order.data_date >= start_date, Order.data_date <= end_date),
                ),
                or_(Order.status.is_(None), not_(Order.status.in_(["refunded", "switched", "stitched", "cancelled"]))),
            )
        )
        orders_filtered = session.exec(base_query).all()

        # Group by (normalized phone, item_id)
        groups: dict[tuple[str, Optional[int]], list[Order]] = defaultdict(list)
        
        # Load clients for phone lookup
        client_ids = {o.client_id for o in orders_filtered if o.client_id}
        clients = session.exec(select(Client).where(Client.id.in_(list(client_ids)))).all() if client_ids else []
        client_map = {c.id: c for c in clients if c.id is not None}
        
        for order in orders_filtered:
            if not order.client_id or order.client_id not in client_map:
                continue
            client = client_map[order.client_id]
            phone_norm = normalize_phone(client.phone) if client.phone else ""
            if not phone_norm:
                continue
            # Group by phone and item_id
            key = (phone_norm, order.item_id)
            groups[key].append(order)

        # Filter groups: only keep groups with 2+ orders
        candidate_groups = []
        
        # Load payments for all orders
        order_ids = [o.id for o in orders_filtered if o.id]
        payments = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all() if order_ids else []
        payment_map: dict[int, list[Payment]] = defaultdict(list)
        for p in payments:
            if p.order_id:
                payment_map[p.order_id].append(p)

        # Load items for display
        item_ids = {o.item_id for o in orders_filtered if o.item_id}
        items = session.exec(select(Item).where(Item.id.in_(list(item_ids)))).all() if item_ids else []
        item_map = {it.id: it for it in items if it.id is not None}

        for (phone_norm, item_id), order_list in groups.items():
            if len(order_list) < 2:
                continue
            
            # Calculate totals and payments for this group
            group_data = []
            total_payments = 0.0
            max_order_total = 0.0
            
            for order in order_list:
                order_payments = payment_map.get(order.id or 0, [])
                paid_amount = sum(float(p.amount or 0.0) for p in order_payments)
                total_payments += paid_amount
                order_total = float(order.total_amount or 0.0)
                max_order_total = max(max_order_total, order_total)
                
                item = item_map.get(order.item_id) if order.item_id else None
                client = client_map.get(order.client_id) if order.client_id else None
                
                group_data.append({
                    "order": order,
                    "paid_amount": paid_amount,
                    "order_total": order_total,
                    "item": item,
                    "client": client,
                })
            
            # Candidate if: sum of payments < max single order total (indicating partial payment)
            # Or if orders have different totals but same customer/product
            if total_payments < max_order_total or len(set(o.total_amount for o in order_list if o.total_amount)) > 1:
                # Identify primary order (bizim if exists, else first)
                bizim_orders = [o for o in order_list if (o.source or "") == "bizim"]
                primary_order = bizim_orders[0] if bizim_orders else order_list[0]
                
                candidate_groups.append({
                    "phone": phone_norm,
                    "item_id": item_id,
                    "item": item_map.get(item_id) if item_id else None,
                    "client": client_map.get(order_list[0].client_id) if order_list[0].client_id else None,
                    "orders": group_data,
                    "total_payments": total_payments,
                    "max_order_total": max_order_total,
                    "primary_order_id": primary_order.id,
                })

        templates = request.app.state.templates
        return templates.TemplateResponse(
            "orders_partial_payments.html",
            {
                "request": request,
                "groups": candidate_groups,
                "start": start_date,
                "end": end_date,
            },
        )


@router.post("/merge-partial-payments")
async def merge_partial_payments(request: Request):
    """Merge selected orders into a primary order for partial payments."""
    form = await request.form()
    
    # Get order IDs from form
    order_ids_raw = form.getlist("order_ids")
    try:
        order_ids = [int(oid) for oid in order_ids_raw if oid]
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid order IDs")
    
    if len(order_ids) < 2:
        raise HTTPException(status_code=400, detail="At least 2 orders required for merging")
    
    with get_session() as session:
        # Load all orders
        orders = session.exec(select(Order).where(Order.id.in_(order_ids))).all()
        if len(orders) != len(order_ids):
            raise HTTPException(status_code=404, detail="Some orders not found")
        
        # Check if any are already merged
        already_merged = [o for o in orders if o.merged_into_order_id is not None]
        if already_merged:
            raise HTTPException(status_code=400, detail=f"Some orders are already merged: {[o.id for o in already_merged]}")
        
        # Check if any are refunded/switched/stitched/cancelled
        invalid_status = [o for o in orders if (o.status or "") in ("refunded", "switched", "stitched", "cancelled")]
        if invalid_status:
            raise HTTPException(status_code=400, detail=f"Some orders have invalid status: {[o.id for o in invalid_status]}")
        
        # Identify primary order: bizim if exists, else first kargo (by ID, oldest)
        bizim_orders = [o for o in orders if (o.source or "") == "bizim"]
        if bizim_orders:
            # Use first bizim order (by ID, oldest)
            primary_order = min(bizim_orders, key=lambda o: o.id or 0)
        else:
            # Use first kargo order (by ID, oldest)
            primary_order = min(orders, key=lambda o: o.id or 0)
        
        secondary_orders = [o for o in orders if o.id != primary_order.id]
        
        # Get primary order ID
        primary_id = primary_order.id
        if not primary_id:
            raise HTTPException(status_code=400, detail="Primary order has no ID")
        
        # Load payments for all orders
        all_payments = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all()
        payment_map: dict[int, list[Payment]] = defaultdict(list)
        for p in all_payments:
            if p.order_id:
                payment_map[p.order_id].append(p)
        
        # Merge each secondary order into primary
        changes_log = []
        for secondary in secondary_orders:
            sec_id = secondary.id
            if not sec_id:
                continue
            
            # Set merged_into_order_id
            secondary.merged_into_order_id = primary_id
            secondary.is_partial_payment = True
            
            # Set partial_payment_group_id on both
            primary_order.partial_payment_group_id = primary_id
            primary_order.is_partial_payment = True
            secondary.partial_payment_group_id = primary_id
            
            # Transfer payments from secondary to primary
            sec_payments = payment_map.get(sec_id, [])
            for payment in sec_payments:
                payment.order_id = primary_id
                changes_log.append(f"Payment {payment.id} moved from order {sec_id} to {primary_id}")
            
            # Combine notes
            if secondary.notes:
                if primary_order.notes:
                    primary_order.notes = f"{primary_order.notes} | [Birleştirildi #{sec_id}]: {secondary.notes}"
                else:
                    primary_order.notes = f"[Birleştirildi #{sec_id}]: {secondary.notes}"
            
            # Keep earliest dates
            if secondary.shipment_date and (not primary_order.shipment_date or secondary.shipment_date < primary_order.shipment_date):
                primary_order.shipment_date = secondary.shipment_date
            if secondary.data_date and (not primary_order.data_date or secondary.data_date < primary_order.data_date):
                primary_order.data_date = secondary.data_date
            
            changes_log.append(f"Order {sec_id} merged into {primary_id}")
        
        # Calculate total payments for primary order (including transferred)
        all_primary_payments = session.exec(select(Payment).where(Payment.order_id == primary_id)).all()
        total_paid = sum(float(p.amount or 0.0) for p in all_primary_payments)
        primary_total = float(primary_order.total_amount or 0.0)
        
        # Update status to "partial_paid" if payments < total
        if total_paid > 0 and total_paid < primary_total:
            primary_order.status = "partial_paid"
        elif total_paid >= primary_total and primary_total > 0:
            # Fully paid, but keep status as is or set to None
            if (primary_order.status or "") == "partial_paid":
                primary_order.status = None
        
        # Log merge action
        try:
            editor = request.session.get("uid") if hasattr(request, "session") else None
            session.add(OrderEditLog(
                order_id=primary_id,
                editor_user_id=editor,
                action="merge_partial_payments",
                changes_json=str({
                    "merged_orders": [o.id for o in secondary_orders],
                    "total_paid": total_paid,
                    "primary_total": primary_total,
                    "changes": changes_log,
                })
            ))
        except Exception:
            pass
        
        return {
            "status": "ok",
            "primary_order_id": primary_id,
            "merged_orders": [o.id for o in secondary_orders],
            "total_paid": total_paid,
            "primary_total": primary_total,
        }


@router.post("/merge-partial-payments")
async def merge_partial_payments(request: Request):
    """Merge selected orders into a primary order for partial payments."""
    form = await request.form()
    
    # Get order IDs from form
    order_ids_raw = form.getlist("order_ids")
    try:
        order_ids = [int(oid) for oid in order_ids_raw if oid]
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid order IDs")
    
    if len(order_ids) < 2:
        raise HTTPException(status_code=400, detail="At least 2 orders required for merging")
    
    with get_session() as session:
        # Load all orders
        orders = session.exec(select(Order).where(Order.id.in_(order_ids))).all()
        if len(orders) != len(order_ids):
            raise HTTPException(status_code=404, detail="Some orders not found")
        
        # Check if any are already merged
        already_merged = [o for o in orders if o.merged_into_order_id is not None]
        if already_merged:
            raise HTTPException(status_code=400, detail=f"Some orders are already merged: {[o.id for o in already_merged]}")
        
        # Check if any are refunded/switched/stitched/cancelled
        invalid_status = [o for o in orders if (o.status or "") in ("refunded", "switched", "stitched", "cancelled")]
        if invalid_status:
            raise HTTPException(status_code=400, detail=f"Some orders have invalid status: {[o.id for o in invalid_status]}")
        
        # Identify primary order: bizim if exists, else first kargo (by ID, oldest)
        bizim_orders = [o for o in orders if (o.source or "") == "bizim"]
        if bizim_orders:
            # Use first bizim order (by ID, oldest)
            primary_order = min(bizim_orders, key=lambda o: o.id or 0)
        else:
            # Use first kargo order (by ID, oldest)
            primary_order = min(orders, key=lambda o: o.id or 0)
        
        secondary_orders = [o for o in orders if o.id != primary_order.id]
        
        # Get primary order ID
        primary_id = primary_order.id
        if not primary_id:
            raise HTTPException(status_code=400, detail="Primary order has no ID")
        
        # Load payments for all orders
        all_payments = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all()
        payment_map: dict[int, list[Payment]] = defaultdict(list)
        for p in all_payments:
            if p.order_id:
                payment_map[p.order_id].append(p)
        
        # Merge each secondary order into primary
        changes_log = []
        for secondary in secondary_orders:
            sec_id = secondary.id
            if not sec_id:
                continue
            
            # Set merged_into_order_id
            secondary.merged_into_order_id = primary_id
            secondary.is_partial_payment = True
            
            # Set partial_payment_group_id on both
            primary_order.partial_payment_group_id = primary_id
            primary_order.is_partial_payment = True
            secondary.partial_payment_group_id = primary_id
            
            # Transfer payments from secondary to primary
            sec_payments = payment_map.get(sec_id, [])
            for payment in sec_payments:
                payment.order_id = primary_id
                changes_log.append(f"Payment {payment.id} moved from order {sec_id} to {primary_id}")
            
            # Combine notes
            if secondary.notes:
                if primary_order.notes:
                    primary_order.notes = f"{primary_order.notes} | [Birleştirildi #{sec_id}]: {secondary.notes}"
                else:
                    primary_order.notes = f"[Birleştirildi #{sec_id}]: {secondary.notes}"
            
            # Keep earliest dates
            if secondary.shipment_date and (not primary_order.shipment_date or secondary.shipment_date < primary_order.shipment_date):
                primary_order.shipment_date = secondary.shipment_date
            if secondary.data_date and (not primary_order.data_date or secondary.data_date < primary_order.data_date):
                primary_order.data_date = secondary.data_date
            
            changes_log.append(f"Order {sec_id} merged into {primary_id}")
        
        # Calculate total payments for primary order (including transferred)
        all_primary_payments = session.exec(select(Payment).where(Payment.order_id == primary_id)).all()
        total_paid = sum(float(p.amount or 0.0) for p in all_primary_payments)
        primary_total = float(primary_order.total_amount or 0.0)
        
        # Update status to "partial_paid" if payments < total
        if total_paid > 0 and total_paid < primary_total:
            primary_order.status = "partial_paid"
        elif total_paid >= primary_total and primary_total > 0:
            # Fully paid, but keep status as is or set to None
            if (primary_order.status or "") == "partial_paid":
                primary_order.status = None
        
        # Log merge action
        try:
            editor = request.session.get("uid") if hasattr(request, "session") else None
            session.add(OrderEditLog(
                order_id=primary_id,
                editor_user_id=editor,
                action="merge_partial_payments",
                changes_json=str({
                    "merged_orders": [o.id for o in secondary_orders],
                    "total_paid": total_paid,
                    "primary_total": primary_total,
                    "changes": changes_log,
                })
            ))
        except Exception:
            pass
        
        return {
            "status": "ok",
            "primary_order_id": primary_id,
            "merged_orders": [o.id for o in secondary_orders],
            "total_paid": total_paid,
            "primary_total": primary_total,
        }
