#!/usr/bin/env python3
"""
Mevcut stok hareketlerinden türetilen on_hand kadar sentetik StockUnit (source=backfill) ekler.

  --dry-run   Sadece rapor, yazmaz.
  --reconcile Hareket özeti ile parça sayısını karşılaştırır (backfill sonrası).

Önkoşul: stock_unit tablosu oluşmuş olmalı (uygulama boot veya db.py DDL).
"""
from __future__ import annotations

import argparse
import os
import sys

# noqa: E402 — path sonra import
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from sqlmodel import select  # noqa: E402

from app.db import get_session  # noqa: E402
from app.models import Item, StockUnit  # noqa: E402
from app.services.inventory import compute_on_hand_for_items  # noqa: E402
from app.services.stock_units import count_in_stock_units  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="StockUnit backfill from movement on_hand")
    parser.add_argument("--dry-run", action="store_true", help="Do not insert rows")
    parser.add_argument("--reconcile", action="store_true", help="Only compare counts after backfill")
    args = parser.parse_args()

    if not os.getenv("DATABASE_URL") and not os.getenv("MYSQL_URL"):
        print("DATABASE_URL or MYSQL_URL required", file=sys.stderr)
        return 1

    with get_session() as session:
        items = session.exec(select(Item)).all()
        item_ids = [int(it.id) for it in items if it.id is not None]

        to_add: list[tuple[int, int]] = []
        mismatches: list[tuple[int, int, int, int]] = []

        # Reconcile: negatif on_hand (hareket gecikmesi / eski düzeltmeler) bilgi; gerçek uyumsuzluk on_hand >= 0 iken sayı farkı
        reconcile_critical: list[tuple[int, int, int, int]] = []
        reconcile_negative_oh: list[tuple[int, int, int, int]] = []

        for iid in sorted(set(item_ids)):
            oh = compute_on_hand_for_items(session, [iid]).get(iid, 0)
            su = count_in_stock_units(session, iid)
            if args.reconcile:
                if oh < 0:
                    if oh != su:
                        reconcile_negative_oh.append((iid, oh, su, oh - su))
                    continue
                if oh != su:
                    reconcile_critical.append((iid, oh, su, oh - su))
                continue
            need = int(oh) - int(su)
            if need > 0:
                to_add.append((iid, need))
            elif need < 0:
                mismatches.append((iid, oh, su, need))

        if args.reconcile:
            if reconcile_negative_oh:
                print(
                    f"reconcile: items_negative_on_hand={len(reconcile_negative_oh)} "
                    "(hareket özeti; parça backfill bu kalemlerde uygulanmaz — hata sayılmaz)"
                )
                for row in reconcile_negative_oh[:50]:
                    print(f"  item_id={row[0]} on_hand={row[1]} in_stock_units={row[2]} diff={row[3]}")
                if len(reconcile_negative_oh) > 50:
                    print(f"  ... and {len(reconcile_negative_oh) - 50} more")
            print(
                f"reconcile: items_critical_mismatch={len(reconcile_critical)} "
                "(on_hand>=0 iken in_stock parça sayısı farklı — düzeltme gerekir)"
            )
            for row in reconcile_critical[:200]:
                print(f"  item_id={row[0]} on_hand={row[1]} in_stock_units={row[2]} diff={row[3]}")
            if len(reconcile_critical) > 200:
                print(f"  ... and {len(reconcile_critical) - 200} more")
            return 0 if not reconcile_critical else 126

        print(f"candidates: {len(to_add)} items need synthetic units")
        total_ins = sum(n for _, n in to_add)
        print(f"rows_to_insert: {total_ins}")
        if mismatches:
            print(f"warnings (more units than on_hand): {len(mismatches)} items — manual review")
            for row in mismatches[:50]:
                print(f"  item_id={row[0]} on_hand={row[1]} in_stock_units={row[2]}")

        if args.dry_run:
            return 0

        inserted = 0
        for iid, need in to_add:
            for _ in range(need):
                session.add(
                    StockUnit(
                        item_id=iid,
                        status="in_stock",
                        source="backfill",
                        inbound_movement_id=None,
                    )
                )
                inserted += 1
        print(f"inserted: {inserted} (commit on context exit)")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
