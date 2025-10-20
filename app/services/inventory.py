from __future__ import annotations

from typing import Dict, Iterable, Optional

from sqlmodel import Session, select

from ..models import Item, StockMovement


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
	ids = [it.id for it in session.exec(select(Item.id)).all() if it is not None]
	return compute_on_hand_for_items(session, [i for i in ids if i is not None])


