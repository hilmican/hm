from __future__ import annotations

from typing import Dict, Iterable, Optional

from sqlmodel import Session, select

from ..models import Item, StockMovement, Product
from .mapping import find_or_create_variant


def compute_on_hand_for_items(session: Session, item_ids: Iterable[int]) -> Dict[int, int]:
	ids = [i for i in item_ids if i is not None]
	if not ids:
		return {}
	rows = session.exec(select(StockMovement).where(StockMovement.item_id.in_(ids))).all()
	acc: Dict[int, int] = {}
	for mv in rows:
		if mv.item_id is None:
			continue
		cur = acc.get(mv.item_id, 0)
		if (mv.direction or "out") == "in":
			cur += int(mv.quantity or 0)
		else:
			cur -= int(mv.quantity or 0)
		acc[mv.item_id] = cur
	return acc


def get_stock_map(session: Session) -> Dict[int, int]:
	ids = [it for it in session.exec(select(Item.id)).all() if it is not None]
	return compute_on_hand_for_items(session, [i for i in ids if i is not None])



def get_or_create_item(session: Session, *, product_id: int, size: Optional[str] = None, color: Optional[str] = None, pack_type: Optional[str] = None, pair_multiplier: Optional[int] = None) -> Item:
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
        pack_type=pack_type,
        pair_multiplier=pair_multiplier or 1,
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

