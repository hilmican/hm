"""Parça bazlı stok (StockUnit). HMA_STOCK_UNIT_TRACKING=1 ile StockMovement ile birlikte güncellenir."""
from __future__ import annotations

import datetime as dt
import os
from typing import List, Optional, Sequence

from sqlmodel import Session, select

from ..models import StockMovement, StockUnit


def stock_unit_tracking_enabled() -> bool:
    v = (os.getenv("HMA_STOCK_UNIT_TRACKING") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def create_units_for_inbound(
    session: Session,
    *,
    item_id: int,
    quantity: int,
    inbound_movement_id: int,
    source: str = "live",
) -> List[StockUnit]:
    """Yeni giriş hareketine bağlı N adet stok parçası."""
    out: List[StockUnit] = []
    for _ in range(int(quantity)):
        u = StockUnit(
            item_id=item_id,
            status="in_stock",
            inbound_movement_id=inbound_movement_id,
            source=source,
        )
        session.add(u)
        out.append(u)
    session.flush()
    return out


def allocate_units_fifo(
    session: Session,
    *,
    item_id: int,
    quantity: int,
    outbound_movement_id: int,
    order_id: Optional[int] = None,
) -> List[StockUnit]:
    rows = session.exec(
        select(StockUnit)
        .where(StockUnit.item_id == item_id, StockUnit.status == "in_stock")
        .order_by(StockUnit.created_at.asc(), StockUnit.id.asc())
        .limit(int(quantity))
    ).all()
    if len(rows) < int(quantity):
        raise ValueError(
            f"insufficient_stock_units: item_id={item_id} need={quantity} have_in_stock={len(rows)}; "
            "run scripts/backfill_stock_units.py or set HMA_STOCK_UNIT_TRACKING=0"
        )
    now = dt.datetime.utcnow()
    for u in rows:
        u.status = "sold"
        u.outbound_movement_id = outbound_movement_id
        u.order_id = order_id
        u.updated_at = now
        session.add(u)
    session.flush()
    return list(rows)


def consume_specific_units(
    session: Session,
    *,
    unit_ids: Sequence[int],
    outbound_movement_id: int,
    order_id: Optional[int] = None,
) -> List[StockUnit]:
    """Çıkışta belirli parça id''leri (QR hma:unit:)."""
    ids = [int(x) for x in unit_ids]
    if not ids:
        return []
    rows = session.exec(select(StockUnit).where(StockUnit.id.in_(ids))).all()  # type: ignore[arg-type]
    by_id = {int(r.id): r for r in rows if r.id is not None}
    missing = [i for i in ids if i not in by_id]
    if missing:
        raise ValueError(f"stock_unit_not_found: {missing}")
    now = dt.datetime.utcnow()
    out: List[StockUnit] = []
    for i in ids:
        u = by_id[i]
        if (u.status or "") != "in_stock":
            raise ValueError(f"stock_unit_not_in_stock: id={u.id} status={u.status}")
        u.status = "sold"
        u.outbound_movement_id = outbound_movement_id
        u.order_id = order_id
        u.updated_at = now
        session.add(u)
        out.append(u)
    session.flush()
    return out


def sync_units_after_movement(
    session: Session,
    mv: StockMovement,
    *,
    consume_unit_ids: Optional[Sequence[int]] = None,
) -> None:
    if not stock_unit_tracking_enabled():
        return
    if mv.id is None:
        return
    direction = (mv.direction or "").lower()
    qty = int(mv.quantity or 0)
    if qty <= 0:
        return
    if direction == "in":
        create_units_for_inbound(
            session,
            item_id=int(mv.item_id),
            quantity=qty,
            inbound_movement_id=int(mv.id),
            source="live",
        )
        return
    if direction == "out":
        if consume_unit_ids is not None:
            cids = [int(x) for x in consume_unit_ids]
            if len(cids) != qty:
                raise ValueError(
                    f"consume_unit_ids length {len(cids)} must match movement_qty={qty}"
                )
            consume_specific_units(
                session,
                unit_ids=cids,
                outbound_movement_id=int(mv.id),
                order_id=order_id_from_mv(mv),
            )
        else:
            allocate_units_fifo(
                session,
                item_id=int(mv.item_id),
                quantity=qty,
                outbound_movement_id=int(mv.id),
                order_id=order_id_from_mv(mv),
            )


def order_id_from_mv(mv: StockMovement) -> Optional[int]:
    rid = mv.related_order_id
    return int(rid) if rid is not None else None


def count_in_stock_units(session: Session, item_id: int) -> int:
    rows = session.exec(
        select(StockUnit.id).where(StockUnit.item_id == item_id, StockUnit.status == "in_stock")
    ).all()
    return len(rows)


def get_units_for_movement(session: Session, inbound_movement_id: int) -> List[StockUnit]:
    return list(
        session.exec(
            select(StockUnit).where(StockUnit.inbound_movement_id == inbound_movement_id)
        ).all()
    )
