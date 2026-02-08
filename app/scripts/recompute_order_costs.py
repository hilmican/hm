import argparse
import datetime as dt
from sqlmodel import Session, select

from ..db import engine
from ..models import Order
from ..services.inventory import calculate_order_cost_fifo

CANCEL_STATUSES = {"refunded", "switched", "stitched", "cancelled"}


def _parse_date(val: str | None):
    if not val:
        return None
    return dt.date.fromisoformat(val)


def recompute(start: dt.date | None, end: dt.date | None, batch: int = 200) -> tuple[int, int]:
    """Recompute FIFO cost for orders and persist to total_cost.

    Returns (updated_count, error_count).
    """
    updated = 0
    errors = 0
    with Session(engine) as session:
        q = select(Order)
        if start:
            q = q.where(Order.data_date >= start)
        if end:
            q = q.where(Order.data_date <= end)
        rows = session.exec(q).all()
        for idx, o in enumerate(rows, start=1):
            try:
                if (o.status or "") in CANCEL_STATUSES:
                    cost = 0.0
                else:
                    cost = float(calculate_order_cost_fifo(session, int(o.id)))
                o.total_cost = cost
                session.add(o)
                updated += 1
            except Exception:
                errors += 1
            if idx % batch == 0:
                session.commit()
        session.commit()
    return updated, errors


def main():
    parser = argparse.ArgumentParser(description="Recompute and persist order costs (FIFO).")
    parser.add_argument("--start", help="Start data_date (YYYY-MM-DD)", default=None)
    parser.add_argument("--end", help="End data_date (YYYY-MM-DD)", default=None)
    parser.add_argument("--batch", type=int, default=200, help="Commit batch size")
    args = parser.parse_args()
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    updated, errors = recompute(start, end, batch=args.batch)
    print(f"Recompute finished. Updated={updated} Errors={errors} Range start={start} end={end}")


if __name__ == "__main__":
    main()
