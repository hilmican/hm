from __future__ import annotations

from typing import Dict, Iterable, Optional, List, Tuple
import ast
import datetime as dt

from sqlmodel import Session, select
from sqlalchemy import func, case

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


def adjust_stock(session: Session, *, item_id: int, delta: int, related_order_id: Optional[int] = None) -> None:
    """Record a stock movement for the given item.

    Positive delta => direction "in"; Negative delta => direction "out".
    """
    direction = "in" if int(delta) >= 0 else "out"
    qty = abs(int(delta))
    if qty <= 0:
        return
    mv = StockMovement(item_id=item_id, direction=direction, quantity=qty, related_order_id=related_order_id)
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
                            session.flush()
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
                session.flush()
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

        for oi in existing_items:
            session.delete(oi)
        mvs = session.exec(_select(StockMovement).where(
            StockMovement.related_order_id == oid
        )).all()
        for mv in mvs:
            if (mv.direction or "out") == "out" and (mv.related_order_id == oid):
                session.delete(mv)

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
            oitems = session.exec(_select(OrderItem).where(OrderItem.order_id == oid)).all()
            total_cost = 0.0
            for oi in oitems:
                it = session.exec(_select(Item).where(Item.id == oi.item_id)).first()
                total_cost += float(oi.quantity or 0) * float((it.cost or 0.0) if it else 0.0)
            order.total_cost = round(total_cost, 2)
        except Exception:
            pass

        orders_updated += 1

    return {
        "orders_processed": orders_processed,
        "orders_updated": orders_updated,
        "items_rewritten": items_rewritten,
        "outs_recreated": outs_recreated,
    }
