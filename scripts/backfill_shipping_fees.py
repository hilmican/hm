#!/usr/bin/env python3
from __future__ import annotations

import argparse

from sqlmodel import Session, select

from app.db import engine
from app.models import Payment
from app.services.shipping import compute_shipping_fee


def backfill(dry_run: bool = False, limit: int | None = None) -> tuple[int, int]:
	"""Recalculate fee_kargo and net_amount for historical payments.

	Returns (updated_count, scanned_count).
	"""
	updated = 0
	scanned = 0
	with Session(engine) as s:
		query = select(Payment)
		rows = s.exec(query).all()
		if limit is not None:
			rows = rows[: int(limit)]
		for p in rows:
			scanned += 1
			amt = float(p.amount or 0.0)
			fee_kom = float(p.fee_komisyon or 0.0)
			fee_hiz = float(p.fee_hizmet or 0.0)
			fee_iad = float(p.fee_iade or 0.0)
			fee_eok = float(p.fee_erken_odeme or 0.0)
			fee_kar = compute_shipping_fee(amt)
			net = round(amt - sum([fee_kom, fee_hiz, fee_kar, fee_iad, fee_eok]), 2)
			if (float(p.fee_kargo or 0.0) != fee_kar) or (float(p.net_amount or 0.0) != net):
				updated += 1
				if not dry_run:
					p.fee_kargo = fee_kar
					p.net_amount = net
		if not dry_run and updated:
			s.commit()
	return updated, scanned


def main():
	parser = argparse.ArgumentParser(description="Backfill shipping fees on Payment rows")
	parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing to DB")
	parser.add_argument("--limit", type=int, default=None, help="Process at most this many rows")
	args = parser.parse_args()
	updated, scanned = backfill(dry_run=bool(args.dry_run), limit=args.limit)
	mode = "DRY-RUN" if args.dry_run else "APPLIED"
	print(f"[{mode}] Updated {updated} / {scanned} payments")


if __name__ == "__main__":
	main()


