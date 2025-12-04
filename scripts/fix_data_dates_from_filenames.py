#!/usr/bin/env python3
"""
Fix data_date for bizim orders by extracting date from Excel filenames.

For bizim Excel files, the date is encoded in the filename:
- "27 KASIM Hİ-MAN BİZİM EXCEL.xlsx" -> 2025-11-27
- "2025-11-28 KASIM.xlsx" -> 2025-11-28
- "1 ARALIK Hİ-MAN BİZİM +1.xlsx" -> 2025-12-01

This script:
1. Finds all ImportRun records for bizim source
2. Extracts date from filename
3. Updates order.data_date to use filename date (oldest date, not import date)
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
    """Extract date from bizim Excel filename.
    
    Only processes filenames that START with ISO date format (YYYY-MM-DD):
    - "2025-11-28 KASIM.xlsx" -> 2025-11-28
    - "2025-10-30-30 EKİM Hİ-MANN-1-2-3.xlsx" -> 2025-10-30
    
    Ignores filenames like "27 KASIM Hİ-MAN BİZİM EXCEL.xlsx" that don't start with ISO date.
    """
    # Remove .xlsx extension
    name = filename.replace('.xlsx', '').strip()
    
    # Only process if filename starts with ISO date format: YYYY-MM-DD
    iso_match = re.match(r'^(\d{4}-\d{2}-\d{2})', name)
    if iso_match:
        try:
            return datetime.strptime(iso_match.group(1), '%Y-%m-%d').date()
        except:
            pass
    
    # If filename doesn't start with ISO date, return None (ignore it)
    return None


def fix_data_dates_from_filenames(dry_run: bool = True):
    """Fix data_date for orders based on ImportRun filename dates."""
    updated_count = 0
    skipped_count = 0
    error_count = 0
    
    with get_session() as session:
        # Get all bizim import runs
        bizim_runs = session.exec(
            select(ImportRun)
            .where(ImportRun.source == "bizim")
            .order_by(ImportRun.started_at.desc())
        ).all()
        
        print(f"Found {len(bizim_runs)} bizim import runs")
        
        for run in bizim_runs:
            # Extract date from filename (only if it starts with ISO date format)
            filename_date = extract_date_from_filename(run.filename)
            
            if not filename_date:
                # Skip files that don't start with ISO date format
                skipped_count += len(session.exec(
                    select(ImportRow)
                    .where(
                        ImportRow.import_run_id == run.id,
                        ImportRow.matched_order_id.is_not(None)
                    )
                ).all())
                continue
            
            print(f"\nProcessing run {run.id}: {run.filename}")
            print(f"  Extracted date from filename: {filename_date}")
            print(f"  Run data_date: {run.data_date}")
            
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
                    
                    if order.source != "bizim":
                        skipped_count += 1
                        continue
                    
                    # Update if:
                    # 1. data_date is None
                    # 2. data_date equals run.data_date (import date, might be wrong) - use filename date
                    # 3. filename_date is OLDER than current data_date (preserve oldest date)
                    should_update = False
                    reason = ""
                    
                    if order.data_date is None:
                        should_update = True
                        reason = "data_date is None"
                    elif run.data_date and order.data_date == run.data_date:
                        # data_date was set to import date, use filename date instead
                        should_update = True
                        reason = f"data_date equals import date ({run.data_date})"
                    elif filename_date < order.data_date:
                        # Filename date is OLDER, use it (preserve oldest)
                        should_update = True
                        reason = f"filename date ({filename_date}) is older than current ({order.data_date})"
                    
                    if should_update and filename_date != order.data_date:
                        if dry_run:
                            print(f"    Would update order {order.id}: data_date {order.data_date} -> {filename_date} ({reason})")
                        else:
                            order.data_date = filename_date
                            print(f"    Updated order {order.id}: data_date {order.data_date} -> {filename_date} ({reason})")
                        updated_count += 1
                    else:
                        skipped_count += 1
                        
                except Exception as e:
                    error_count += 1
                    print(f"    ERROR processing row {ir.id}: {e}")
        
        if not dry_run:
            session.commit()
            print(f"\n✓ Committed changes to database")
        else:
            print(f"\n⚠ DRY RUN - No changes committed")
    
    print(f"\nSummary:")
    print(f"  Updated: {updated_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Errors: {error_count}")
    
    return updated_count, skipped_count, error_count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fix data_date for orders from Excel filenames")
    parser.add_argument("--execute", action="store_true", help="Actually update the database")
    args = parser.parse_args()
    
    dry_run = not args.execute
    if dry_run:
        print("=" * 60)
        print("DRY RUN MODE - No changes will be made")
        print("Use --execute to actually update the database")
        print("=" * 60)
    
    fix_data_dates_from_filenames(dry_run=dry_run)

