from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Tuple

from sqlmodel import select

from app.db import get_session
from app.models import Item, StockMovement, OrderItem, Order


def merge_items() -> None:
    """Merge Items that only differ by legacy pack fields into a canonical (product_id,size,color).

    Strategy:
      - Group items by (product_id, size, color) ignoring pack.
      - Choose canonical as the lowest id with non-null product_id.
      - Repoint StockMovement.item_id and OrderItem.item_id to canonical.
      - If an Order directly references a merged Item via order.item_id, repoint.
      - Mark non-canonical items inactive.
    """
    with get_session() as session:
        rows: List[Item] = session.exec(select(Item).order_by(Item.id.asc())).all()
        groups: Dict[Tuple[int|None, str|None, str|None], List[Item]] = defaultdict(list)
        for it in rows:
            groups[(it.product_id, it.size, it.color)].append(it)

        for key, items in groups.items():
            if len(items) <= 1:
                continue
            # pick canonical: prefer one with product_id set and smallest id
            items_sorted = sorted(items, key=lambda x: (0 if x.product_id else 1, x.id or 10**9))
            canonical = items_sorted[0]
            others = [i for i in items_sorted[1:] if (i.id != canonical.id)]
            if not others:
                continue

            canon_id = canonical.id
            if canon_id is None:
                continue

            other_ids = [i.id for i in others if i.id is not None]
            if not other_ids:
                continue

            # repoint stock movements
            mvs = session.exec(select(StockMovement).where(StockMovement.item_id.in_(other_ids))).all()
            for mv in mvs:
                mv.item_id = canon_id

            # repoint order items
            oitems = session.exec(select(OrderItem).where(OrderItem.item_id.in_(other_ids))).all()
            for oi in oitems:
                oi.item_id = canon_id

            # repoint orders directly referencing item_id
            orders = session.exec(select(Order).where(Order.item_id.in_(other_ids))).all()
            for o in orders:
                o.item_id = canon_id

            # mark others inactive
            for it in others:
                it.status = "inactive"

            print(f"Merged group {key}: canonical {canon_id}, merged {other_ids}")


if __name__ == "__main__":
    merge_items()


