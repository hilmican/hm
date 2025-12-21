#!/usr/bin/env python3
"""
Fix data_date for kargo orders and bizim orders matched by kargo imports.

For kargo Excel files, the date is encoded in the filename (when the payment Excel was received):
- "2025-11-28 KOodenenler.xlsx" -> 2025-11-28
- "2025-10-30 KOodenenler (1).xlsx" -> 2025-10-30

This script:
1. Finds all ImportRun records for kargo source
2. Extracts date from filename (first 10 chars: YYYY-MM-DD)
3. Updates order.data_date to use filename date (when kargo Excel was imported)
4. Updates both kargo orders AND bizim orders that were matched by kargo imports
5. Updates orders even if they would be skipped due to duplicates (to ensure correctness)

Note: This should be run after the code fix to correct historical data.
"""

import sys
import re
from pathlib import Path
from datetime import date, datetime

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except:
    PROJECT_ROOT = Path("/app")
sys.path.insert(0, str(PROJECT_ROOT))

from app.db import get_session
from app.models import ImportRun, ImportRow, Order
from sqlmodel import select


def extract_date_from_filename(filename: str) -> date | None:
    """Extract date from kargo Excel filename.
    
    Expects ISO date format (YYYY-MM-DD) at the start of filename:
    - "2025-11-28 KOodenenler.xlsx" -> 2025-11-28
    - "2025-10-30 KOodenenler (1).xlsx" -> 2025-10-30
    """
    # Extract first 10 characters (YYYY-MM-DD)
    match = re.match(r'^(\d{4}-\d{2}-\d{2})', filename)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y-%m-%d').date()
        except:
            pass
    
    return None


def fix_kargo_data_dates(dry_run: bool = True):
    """Fix data_date for kargo orders based on ImportRun filename dates."""
    updated_count = 0
    skipped_count = 0
    error_count = 0
    no_date_count = 0
    
    with get_session() as session:
        # Get all kargo import runs
        kargo_runs = session.exec(
            select(ImportRun)
            .where(ImportRun.source == "kargo")
            .order_by(ImportRun.started_at.desc())
        ).all()
        
        print(f"Found {len(kargo_runs)} kargo import runs")
        
        for run in kargo_runs:
            # Extract date from filename
            filename_date = extract_date_from_filename(run.filename)
            
            if not filename_date:
                # Count how many orders we're skipping for this run
                skipped_rows = session.exec(
                    select(ImportRow)
                    .where(
                        ImportRow.import_run_id == run.id,
                        ImportRow.matched_order_id.is_not(None)
                    )
                ).all()
                no_date_count += len(skipped_rows)
                if skipped_rows:
                    print(f"\nSkipping run {run.id}: {run.filename} (no date in filename)")
                continue
            
            print(f"\nProcessing run {run.id}: {run.filename}")
            print(f"  Extracted date from filename: {filename_date}")
            print(f"  Run data_date (old buggy value): {run.data_date}")
            
            # Get all import rows for this run that have matched orders
            import_rows = session.exec(
                select(ImportRow)
                .where(
                    ImportRow.import_run_id == run.id,
                    ImportRow.matched_order_id.is_not(None)
                )
            ).all()
            
            print(f"  Found {len(import_rows)} rows with matched orders")
            
            for ir in import_rows:
                try:
                    # Get the order
                    order = session.exec(
                        select(Order).where(Order.id == ir.matched_order_id)
                    ).first()
                    
                    if not order:
                        error_count += 1
                        continue
                    
                    # Process both kargo orders AND bizim orders that were matched by kargo imports
                    # (bizim orders can get payments added via kargo Excel imports)
                    if order.source not in ("kargo", "bizim"):
                        skipped_count += 1
                        continue
                    
                    # Check if data_date needs fixing
                    # For kargo orders: data_date should be from filename (when kargo Excel was imported)
                    # For bizim orders matched by kargo: if data_date equals shipment_date, update to filename date
                    
                    should_update = False
                    reason = ""
                    
                    if order.source == "kargo":
                        # For kargo orders, check if data_date was incorrectly set
                        if order.data_date is None:
                            should_update = True
                            reason = "data_date is None"
                        elif order.shipment_date and order.data_date == order.shipment_date:
                            # BUG: data_date equals shipment_date (the bug we're fixing)
                            should_update = True
                            reason = f"data_date equals shipment_date ({order.shipment_date})"
                        elif run.data_date and order.data_date == run.data_date:
                            # Also check if it equals the old buggy run.data_date (which was max(shipment_date))
                            should_update = True
                            reason = f"data_date equals old buggy run.data_date ({run.data_date})"
                    elif order.source == "bizim":
                        # For bizim orders matched by kargo: if data_date equals shipment_date, it's likely wrong
                        # Update to the kargo filename date (when payment Excel was imported)
                        if order.data_date is None:
                            should_update = True
                            reason = "data_date is None"
                        elif order.shipment_date and order.data_date == order.shipment_date:
                            # Likely bug: data_date equals shipment_date for bizim order matched by kargo
                            should_update = True
                            reason = f"bizim order: data_date equals shipment_date ({order.shipment_date}), update to kargo import date"
                        # Also update if data_date is older than filename_date (preserve the actual import date)
                        elif filename_date and order.data_date and filename_date < order.data_date:
                            should_update = True
                            reason = f"bizim order: kargo import date ({filename_date}) is older than current data_date ({order.data_date})"
                    
                    # Always update if we have a filename_date and it's different from current data_date
                    # (This ensures we fix the date even if it matches shipment_date)
                    if should_update and filename_date and filename_date != order.data_date:
                        if dry_run:
                            print(f"    Would update order {order.id}: data_date {order.data_date} -> {filename_date} ({reason})")
                            if order.shipment_date:
                                print(f"      shipment_date: {order.shipment_date}, data_date will be: {filename_date}")
                        else:
                            old_data_date = order.data_date
                            order.data_date = filename_date
                            print(f"    Updated order {order.id}: data_date {old_data_date} -> {filename_date} ({reason})")
                            if order.shipment_date:
                                print(f"      shipment_date: {order.shipment_date}, data_date now: {filename_date}")
                        updated_count += 1
                    else:
                        skipped_count += 1
                        
                except Exception as e:
                    error_count += 1
                    print(f"    ERROR processing row {ir.id}: {e}")
                    import traceback
                    traceback.print_exc()
        
        if not dry_run:
            session.commit()
            print(f"\n✓ Committed changes to database")
        else:
            print(f"\n⚠ DRY RUN - No changes committed")
    
    print(f"\nSummary:")
    print(f"  Updated: {updated_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Errors: {error_count}")
    print(f"  No date in filename: {no_date_count}")
    
    return updated_count, skipped_count, error_count, no_date_count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fix data_date for kargo orders from Excel filenames")
    parser.add_argument("--execute", action="store_true", help="Actually update the database (default is dry-run)")
    args = parser.parse_args()
    
    dry_run = not args.execute
    if dry_run:
        print("=" * 60)
        print("DRY RUN MODE - No changes will be made")
        print("Use --execute to actually update the database")
        print("=" * 60)
    
    fix_kargo_data_dates(dry_run=dry_run)

