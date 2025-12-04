#!/usr/bin/env python3
"""
Simple fix: Update data_date to shipment_date for bizim orders where shipment_date is older.

This preserves the original bizim order date (shipment_date) as data_date,
which is the oldest date and should not be overwritten by kargo updates.
"""

import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except:
    PROJECT_ROOT = Path("/app")
sys.path.insert(0, str(PROJECT_ROOT))

from app.db import get_session
from app.models import Order
from sqlmodel import select
from sqlalchemy import text


def fix_data_dates_simple(dry_run: bool = True):
    """Fix data_date by using shipment_date if it's older."""
    updated_count = 0
    skipped_count = 0
    
    with get_session() as session:
        # Get all bizim orders where shipment_date exists and is older than data_date
        if session.get_bind().dialect.name == "mysql":
            query = text("""
                SELECT id, data_date, shipment_date
                FROM `order`
                WHERE source = 'bizim'
                  AND shipment_date IS NOT NULL
                  AND (data_date IS NULL OR shipment_date < data_date)
                  AND merged_into_order_id IS NULL
                ORDER BY id
            """)
        else:
            query = text("""
                SELECT id, data_date, shipment_date
                FROM "order"
                WHERE source = "bizim"
                  AND shipment_date IS NOT NULL
                  AND (data_date IS NULL OR shipment_date < data_date)
                  AND merged_into_order_id IS NULL
                ORDER BY id
            """)
        
        rows = session.exec(query).all()
        print(f"Found {len(rows)} bizim orders where shipment_date < data_date or data_date is NULL")
        
        for row in rows:
            order_id, current_data_date, shipment_date = row
            order = session.exec(select(Order).where(Order.id == order_id)).first()
            
            if not order:
                skipped_count += 1
                continue
            
            if dry_run:
                print(f"  Would update order {order_id}: data_date {current_data_date} -> {shipment_date}")
            else:
                order.data_date = shipment_date
                print(f"  Updated order {order_id}: data_date {current_data_date} -> {shipment_date}")
            
            updated_count += 1
        
        if not dry_run:
            session.commit()
            print(f"\n✓ Committed changes to database")
        else:
            print(f"\n⚠ DRY RUN - No changes committed")
    
    print(f"\nSummary:")
    print(f"  Updated: {updated_count}")
    print(f"  Skipped: {skipped_count}")
    
    return updated_count, skipped_count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fix data_date for orders using shipment_date")
    parser.add_argument("--execute", action="store_true", help="Actually update the database")
    args = parser.parse_args()
    
    dry_run = not args.execute
    if dry_run:
        print("=" * 60)
        print("DRY RUN MODE - No changes will be made")
        print("Use --execute to actually update the database")
        print("=" * 60)
    
    fix_data_dates_simple(dry_run=dry_run)

