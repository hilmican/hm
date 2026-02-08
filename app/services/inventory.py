from __future__ import annotations

from typing import Dict, Iterable, Optional, List, Tuple, Set
import ast
import datetime as dt

from sqlmodel import Session, select
from sqlalchemy import and_
from sqlalchemy import func, case, delete
from sqlalchemy.exc import IntegrityError

from ..models import Item, StockMovement, Product, ImportRow, ImportRun, Order, OrderItem
from .mapping import find_or_create_variant, resolve_mapping
from sqlmodel import select as _select


def compute_on_hand_for_items(session: Session, item_ids: Iterable[int]) -> Dict[int, int]:
	ids = [i for i in item_ids if i is not None]
	if not ids:
		return {}
	# Use a single SQL aggregation instead of Python-side accumulation
	qty_expr = func.sum(case((StockMovement.direction == "in", StockMovement.quantity), else_=-StockMovement.quantity))
	rows = session.exec(
		select(StockMovement.item_id, qty_expr).where(StockMovement.item_id.in_(ids)).group_by(StockMovement.item_id)
	).all()
	return {int(item_id): int(total or 0) for item_id, total in rows if item_id is not None}


def get_stock_map(session: Session) -> Dict[int, int]:
	ids = [it for it in session.exec(select(Item.id)).all() if it is not None]
	return compute_on_hand_for_items(session, [i for i in ids if i is not None])



def compute_all_time_sold_map(session: Session) -> Dict[int, int]:
	"""Return total sold quantities per item across all time.

	Sums quantities from OrderItem. Also includes fallback counts from legacy
	Order.item_id rows for orders that have no OrderItem records at all.
	"""
	# Base: aggregate from OrderItem
	rows_oi: List[Tuple[Optional[int], Optional[int]]] = session.exec(
		select(OrderItem.item_id, func.sum(OrderItem.quantity))
		.group_by(OrderItem.item_id)
	).all()
	sold_map: Dict[int, int] = {int(iid): int(qty or 0) for iid, qty in rows_oi if iid is not None}

	# Determine orders that already have any OrderItem rows
	orders_with_items: List[Optional[int]] = session.exec(
		select(OrderItem.order_id).group_by(OrderItem.order_id)
	).all()
	orders_with_items_set: Set[int] = {int(oid) for oid in orders_with_items if oid is not None}

	# Fallback: aggregate from Order for orders without OrderItem rows
	if orders_with_items_set:
		rows_ord = session.exec(
			select(Order.item_id, func.sum(Order.quantity))
			.where((Order.item_id != None) & (~Order.id.in_(orders_with_items_set)))
			.group_by(Order.item_id)
		).all()
	else:
		rows_ord = session.exec(
			select(Order.item_id, func.sum(Order.quantity))
			.where(Order.item_id != None)
			.group_by(Order.item_id)
		).all()
	for iid, qty in rows_ord:
		if iid is None:
			continue
		sold_map[int(iid)] = sold_map.get(int(iid), 0) + int(qty or 0)

	return sold_map


def get_or_create_item(session: Session, *, product_id: int, size: Optional[str] = None, color: Optional[str] = None) -> Item:
    """Return a canonical variant Item by product + attributes; create if missing.

    This delegates to the same SKU construction logic used by mapping.find_or_create_variant
    to ensure SKU/name consistency across the app.
    """
    prod = session.exec(select(Product).where(Product.id == product_id)).first()
    if prod is None:
        raise ValueError(f"Product not found: {product_id}")
    return find_or_create_variant(
        session,
        product=prod,  # type: ignore
        size=size,
        color=color,
    )


def _fifo_cost_for_item(session: Session, *, item_id: int, target_order_id: int) -> float:
	"""Compute FIFO cost for a given item, allocating only the out-movement linked to target_order_id.

	We simulate stock consumption across all movements in chronological order, so each purchase batch
	is consumed once globally. When we encounter the out movement tied to target_order_id, we allocate
	cost from the current FIFO queue. If inventory is insufficient, we fall back to item.cost.
	"""
	movs = session.exec(
		_select(StockMovement)
		.where(StockMovement.item_id == item_id)
		.order_by(StockMovement.created_at.asc(), StockMovement.id.asc())
	).all()
	if not movs:
		return 0.0
	fifo: list[tuple[int, float]] = []  # (remaining_qty, unit_cost)
	cost_acc = 0.0
	# Preload fallback cost
	item = session.exec(_select(Item).where(Item.id == item_id)).first()
	product_cost = None
	if item and item.product_id:
		prod = session.exec(_select(Product).where(Product.id == item.product_id)).first()
		if prod and getattr(prod, "default_cost", None) is not None:
			try:
				product_cost = float(prod.default_cost) if prod.default_cost is not None else None
			except Exception:
				product_cost = None
	fallback_cost = product_cost if product_cost is not None else 0.0

	for mv in movs:
		if mv.direction == "in":
			cost_in = mv.unit_cost
			if cost_in is None:
				# if purchase cost missing, fall back to product/item default cost to avoid zero-cost batches
				cost_in = fallback_cost
			fifo.append([int(mv.quantity or 0), float(cost_in or 0.0)])  # type: ignore[list-item]
			continue

		need = int(mv.quantity or 0)
		if need <= 0:
			continue

		def consume(amount: int, price: float, is_target: bool):
			nonlocal cost_acc
			if is_target:
				cost_acc += amount * price

		is_target_out = (mv.related_order_id == target_order_id)
		while need > 0 and fifo:
			batch_qty, batch_cost = fifo[0]
			take = min(batch_qty, need)
			consume(take, batch_cost, is_target_out)
			batch_qty -= take
			need -= take
			if batch_qty == 0:
				fifo.pop(0)
			else:
				fifo[0][0] = batch_qty  # type: ignore[index]

		if need > 0:
			# Negative inventory; allocate remainder at fallback cost for target order only
			consume(need, fallback_cost, is_target_out)

	return round(cost_acc, 2)


def calculate_order_cost_fifo(session: Session, order_id: int) -> float:
	"""Calculate order total cost using FIFO with global movement consumption.

	- Uses StockMovement chronology (created_at, id) to ensure each purchase batch is consumed once.
	- If inventory deficit occurs, falls back to Item.cost for the remaining units of the target order.
	- Status handling: refunded/switched/stitched/cancelled => cost 0; negative totals => 0.
	"""
	order = session.exec(_select(Order).where(Order.id == order_id)).first()
	if not order:
		return 0.0

	status_lc = str(order.status or "").lower()
	if status_lc in ("refunded", "switched", "stitched", "cancelled"):
		return 0.0
	if float(order.total_amount or 0.0) < 0.0:
		return 0.0

	order_items = session.exec(_select(OrderItem).where(OrderItem.order_id == order_id)).all()

	# Single-item fallback if no OrderItem rows
	if not order_items:
		if order.item_id and order.quantity:
			return _fifo_cost_for_item(session, item_id=int(order.item_id), target_order_id=order_id)
		return 0.0

	total_cost = 0.0
	for oi in order_items:
		if not oi.item_id:
			continue
		total_cost += _fifo_cost_for_item(session, item_id=int(oi.item_id), target_order_id=order_id)

	return round(total_cost, 2)


def adjust_stock(
	session: Session,
	*,
	item_id: int,
	delta: int,
	related_order_id: Optional[int] = None,
	reason: Optional[str] = None,
	unit_cost: Optional[float] = None,
	supplier_id: Optional[int] = None,
) -> None:
    """Record a stock movement for the given item.

    Positive delta => direction "in"; Negative delta => direction "out".
    unit_cost: Purchase cost per unit (only used for "in" movements when buying from producer)
    """
    direction = "in" if int(delta) >= 0 else "out"
    qty = abs(int(delta))
    if qty <= 0:
        return
    if direction == "in":
        try:
            if unit_cost is None or float(unit_cost) <= 0:
                raise ValueError("unit_cost must be provided and > 0 for inbound stock")
        except Exception:
            raise
    mv = StockMovement(
        item_id=item_id,
        direction=direction,
        quantity=qty,
        related_order_id=related_order_id,
        reason=reason,
		unit_cost=unit_cost if direction == "in" else None,
		supplier_id=supplier_id if direction == "in" else None,
    )
    session.add(mv)


def _parse_mapped_json(s: Optional[str]) -> Dict:
    if not s:
        return {}
    try:
        obj = ast.literal_eval(str(s))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def recalc_orders_from_mappings(session: Session, *, product_id: Optional[int] = None, since: Optional[dt.date] = None, dry_run: bool = False) -> Dict[str, int]:
    """Rebuild OrderItem rows and related stock-out movements from current mapping rules.

    - Uses ImportRow records from source='bizim' runs and their stored mapped_json to re-resolve
      mapping outputs with current rules, grouped per matched_order_id.
    - Only replaces "out" StockMovements tied to the order; "in" movements are preserved.
    - Optionally filter by product_id (only orders that touch that product) and by a starting date.
    """
    # Preload runs to filter source == 'bizim'
    runs = session.exec(_select(ImportRun)).all()
    run_source_map: Dict[int, str] = {}
    run_date_map: Dict[int, Optional[dt.date]] = {}
    for r in runs:
        if r.id is not None:
            run_source_map[r.id] = str(r.source or "")
            run_date_map[r.id] = r.data_date

    rows = session.exec(_select(ImportRow).where(ImportRow.matched_order_id != None)).all()
    by_order: Dict[int, List[ImportRow]] = {}
    for ir in rows:
        rid = ir.import_run_id
        if rid is None:
            continue
        if run_source_map.get(rid) != "bizim":
            continue
        # optional since filter using ImportRun.data_date when present
        if since is not None:
            rdate = run_date_map.get(rid)
            if (rdate is None) or (rdate < since):
                continue
        if ir.matched_order_id is None:
            continue
        oid = int(ir.matched_order_id)
        by_order.setdefault(oid, []).append(ir)

    orders_processed = 0
    orders_updated = 0
    items_rewritten = 0
    outs_recreated = 0

    def _resolve_items_for_row(rec: Dict) -> List[Tuple[Item, int]]:
        base = str(rec.get("item_name_base") or rec.get("item_name") or "Genel Ürün").strip()
        outs, _rule = resolve_mapping(session, base)
        items_out: List[Tuple[Item, int]] = []
        if outs:
            for out in outs:
                it: Optional[Item] = None
                if out.item_id:
                    it = session.exec(_select(Item).where(Item.id == out.item_id)).first()
                    if it and (it.status or "") == "inactive":
                        it = None
                else:
                    prod: Optional[Product] = None
                    if out.product_id:
                        prod = session.exec(_select(Product).where(Product.id == out.product_id)).first()
                    if prod is None:
                        from ..utils.slugify import slugify as _slugify
                        pslug = _slugify(base)
                        prod = session.exec(_select(Product).where(Product.slug == pslug)).first()
                        if prod is None:
                            prod = Product(name=base, slug=pslug)
                            session.add(prod)
                            try:
                                session.flush()
                            except IntegrityError:
                                # concurrent create; rollback and re-fetch
                                session.rollback()
                                prod = session.exec(_select(Product).where(Product.slug == pslug)).first()
                    it = find_or_create_variant(session, product=prod, size=out.size, color=out.color)  # type: ignore
                    if it and (it.status or "") == "inactive":
                        it = None
                if it is None:
                    continue
                qty_base = int(rec.get("quantity") or 1)
                total_qty = qty_base * int(out.quantity or 1)
                if total_qty <= 0:
                    continue
                items_out.append((it, total_qty))
        else:
            from ..utils.slugify import slugify as _slugify
            sku = _slugify(base)
            it = session.exec(_select(Item).where(Item.sku == sku)).first()
            if it and (it.status or "") == "inactive":
                it = None
            if not it:
                it = Item(sku=sku, name=base)
                session.add(it)
                try:
                    session.flush()
                except IntegrityError:
                    # concurrent create; rollback and re-fetch
                    session.rollback()
                    it = session.exec(_select(Item).where(Item.sku == sku)).first()
            qty_base = int(rec.get("quantity") or 1)
            if qty_base > 0 and it:
                items_out.append((it, qty_base))
        return items_out

    for oid, rows_for_order in by_order.items():
        orders_processed += 1
        order = session.exec(_select(Order).where(Order.id == oid)).first()
        if not order:
            continue
        if since is not None:
            o_date = order.data_date or order.shipment_date
            if o_date and o_date < since:
                continue

        agg: Dict[int, int] = {}
        item_product_map: Dict[int, Optional[int]] = {}
        for ir in rows_for_order:
            rec = _parse_mapped_json(ir.mapped_json)
            if not rec:
                continue
            for it, tq in _resolve_items_for_row(rec):
                if (it.id is None):
                    continue
                iid = int(it.id)
                agg[iid] = agg.get(iid, 0) + int(tq)
                if iid not in item_product_map:
                    item_product_map[iid] = it.product_id

        if not agg:
            continue

        if product_id is not None:
            touches_product = any((pid == product_id) for pid in item_product_map.values())
            if not touches_product:
                continue

        existing_items = session.exec(_select(OrderItem).where(OrderItem.order_id == oid)).all()
        existing_map: Dict[int, int] = {}
        for oi in existing_items:
            existing_map[int(oi.item_id)] = existing_map.get(int(oi.item_id), 0) + int(oi.quantity or 0)

        changed = (existing_map != agg)
        if dry_run and not changed:
            continue

        if dry_run:
            orders_updated += 1
            items_rewritten += sum(agg.values())
            outs_recreated += sum(agg.values())
            continue

        # Delete existing "out" stock movements for this order in bulk
        session.exec(
            delete(StockMovement).where(
                (StockMovement.related_order_id == oid) & (StockMovement.direction == "out")
            )
        )
        # Delete existing OrderItem rows for this order in bulk
        session.exec(
            delete(OrderItem).where(OrderItem.order_id == oid)
        )

        for iid, qty in agg.items():
            session.add(OrderItem(order_id=oid, item_id=iid, quantity=int(qty)))
            adjust_stock(session, item_id=iid, delta=-int(qty), related_order_id=oid)
            items_rewritten += int(qty)
            outs_recreated += int(qty)

        try:
            rep_iid = next(iter(agg.keys()))
            order.item_id = int(rep_iid)
        except Exception:
            pass

        try:
            if (order.status or "") in ("refunded", "switched", "stitched"):
                order.total_cost = 0.0
            else:
                # Use FIFO method to calculate cost based on purchase prices
                order.total_cost = calculate_order_cost_fifo(session, oid)
        except Exception:
            pass

        orders_updated += 1

    return {
        "orders_processed": orders_processed,
        "orders_updated": orders_updated,
        "items_rewritten": items_rewritten,
        "outs_recreated": outs_recreated,
    }
