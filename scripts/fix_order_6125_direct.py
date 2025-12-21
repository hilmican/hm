#!/usr/bin/env python3
"""
Direct fix for order 6125 and similar orders.
Updates data_date for orders matched by kargo imports.
"""

import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except:
    PROJECT_ROOT = Path("/app")
sys.path.insert(0, str(PROJECT_ROOT))

from app.db import get_session
from app.models import Order, ImportRun, ImportRow
from sqlmodel import select
import re
from datetime import datetime


def fix_order_data_dates(dry_run=True):
    """Fix data_date for orders matched by kargo imports."""
    with get_session() as session:
        # Get all kargo ImportRuns
        kargo_runs = session.exec(
            select(ImportRun)
            .where(ImportRun.source == "kargo")
            .order_by(ImportRun.started_at.desc())
        ).all()
        
        print(f"Found {len(kargo_runs)} kargo import runs")
        
        orders_to_update = {}  # order_id -> (filename_date, run_id, reason)
        
        for run in kargo_runs:
            # Extract date from filename
            match = re.match(r'^(\d{4}-\d{2}-\d{2})', run.filename)
            if not match:
                continue
            try:
                filename_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
            except:
                continue
            
            # Get all ImportRows for this run that matched orders
            import_rows = session.exec(
                select(ImportRow)
                .where(
                    ImportRow.import_run_id == run.id,
                    ImportRow.matched_order_id.is_not(None)
                )
            ).all()
            
            for ir in import_rows:
                order_id = ir.matched_order_id
                if not order_id:
                    continue
                
                order = session.exec(select(Order).where(Order.id == order_id)).first()
                if not order:
                    continue
                
                # Check if we should update this order
                should_update = False
                reason = ""
                
                if order.data_date is None:
                    should_update = True
                    reason = "data_date is None"
                elif order.shipment_date and order.data_date == order.shipment_date:
                    should_update = True
                    reason = f"data_date equals shipment_date ({order.shipment_date})"
                elif filename_date != order.data_date:
                    # If filename date is different, consider updating (especially for bizim orders matched by kargo)
                    if order.source == "bizim":
                        should_update = True
                        reason = f"bizim order matched by kargo: update data_date to kargo import date"
                
                if should_update and filename_date != order.data_date:
                    # Store the update (later date wins if multiple kargo imports matched)
                    if order_id not in orders_to_update:
                        orders_to_update[order_id] = (filename_date, run.id, reason)
                    else:
                        # Use the later date (most recent kargo import)
                        existing_date, _, _ = orders_to_update[order_id]
                        if filename_date > existing_date:
                            orders_to_update[order_id] = (filename_date, run.id, reason)
        
        print(f"\nFound {len(orders_to_update)} orders to update")
        
        updated_count = 0
        for order_id, (filename_date, run_id, reason) in orders_to_update.items():
            order = session.exec(select(Order).where(Order.id == order_id)).first()
            if not order:
                continue
            
            if dry_run:
                print(f"Would update order {order_id}: data_date {order.data_date} -> {filename_date} ({reason})")
                print(f"  source={order.source}, shipment_date={order.shipment_date}")
            else:
                old_data_date = order.data_date
                order.data_date = filename_date
                print(f"Updated order {order_id}: data_date {old_data_date} -> {filename_date} ({reason})")
                updated_count += 1
        
        if not dry_run:
            session.commit()
            print(f"\n✓ Committed {updated_count} updates")
        else:
            print(f"\n⚠ DRY RUN - No changes committed")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    
    fix_order_data_dates(dry_run=not args.execute)

