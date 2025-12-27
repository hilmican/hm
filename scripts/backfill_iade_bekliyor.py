"""
Backfill script: Mark orders as `iade_bekliyor` if their notes contain the
shipment note "Müşteri Tahsil etti ya da Evrak İade edildi."

Usage:
    python scripts/backfill_iade_bekliyor.py
"""
from __future__ import annotations

import datetime as dt
from sqlmodel import select

from app.db import get_session
from app.models import Order


IADE_HINT = "müşteri tahsil etti ya da evrak iade edildi"
FINAL_STATUSES = {"refunded", "switched", "stitched", "cancelled", "iade"}


def main() -> None:
    with get_session() as session:
        q = select(Order).where(Order.notes.is_not(None))
        rows = session.exec(q).all()
        updated = 0
        for o in rows:
            try:
                if not o.notes:
                    continue
                note_lc = str(o.notes).lower()
                if IADE_HINT not in note_lc:
                    continue
                st = str(o.status or "").lower()
                if st in FINAL_STATUSES:
                    continue
                if st == "iade_bekliyor":
                    continue
                o.status = "iade_bekliyor"
                # keep a small audit trail in notes
                marker = f"[auto_iade_bekliyor {dt.date.today().isoformat()}]"
                if marker not in o.notes:
                    o.notes = f"{o.notes} | {marker}"
                updated += 1
            except Exception:
                continue
        print(f"Updated {updated} orders to iade_bekliyor")


if __name__ == "__main__":
    main()

