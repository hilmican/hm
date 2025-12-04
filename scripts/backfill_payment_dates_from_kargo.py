#!/usr/bin/env python3
"""
Backfill payment_date for payments created from kargo Excel files.

For kargo payments, payment_date should come from the Excel filename date
(when the payment Excel was received), not from delivery_date/shipment_date
(which is when cargo was received/shipped).

This script:
1. Finds all ImportRun records for kargo source
2. Extracts date from filename (must start with YYYY-MM-DD)
3. Finds all payments created from that import run
4. Updates payment.payment_date to the filename date
5. Reports payments that couldn't be fixed (missing Excel or no date in filename)
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
from app.models import ImportRun, ImportRow, Payment, Order
from sqlmodel import select
from sqlalchemy import text


def extract_date_from_filename(filename: str) -> date | None:
    """Extract date from kargo Excel filename.
    
    Only processes filenames that START with ISO date format (YYYY-MM-DD):
    - "2025-11-28 KOodenenler.xlsx" -> 2025-11-28
    
    Returns None if filename doesn't start with ISO date.
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
    
    return None


def backfill_payment_dates(dry_run: bool = True):
    """Backfill payment_date for kargo payments from Excel filenames."""
    updated_count = 0
    skipped_count = 0
    error_count = 0
    missing_excel = []
    no_date_in_filename = []
    
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
                # No date in filename - track for reporting
                no_date_in_filename.append(run.filename)
                # Count payments from this run
                payments_count = session.exec(
                    select(Payment)
                    .join(Order).where(Order.id == Payment.order_id)
                    .join(ImportRow).where(
                        ImportRow.import_run_id == run.id,
                        ImportRow.matched_order_id == Order.id
                    )
                ).all()
                skipped_count += len(payments_count)
                continue
            
            # Check if Excel file exists
            excel_file = PROJECT_ROOT / "kargocununexcelleri" / run.filename
            if not excel_file.exists():
                missing_excel.append(run.filename)
                # Still try to update payments from this run
            
            print(f"\nProcessing run {run.id}: {run.filename}")
            print(f"  Extracted date from filename: {filename_date}")
            
            # Find all payments created from this import run
            # Method 1: Payments linked to orders that were matched in this import run
            import_rows = session.exec(
                select(ImportRow)
                .where(
                    ImportRow.import_run_id == run.id,
                    ImportRow.matched_order_id.is_not(None)
                )
            ).all()
            
            # Get orders from these import rows
            order_ids = [ir.matched_order_id for ir in import_rows if ir.matched_order_id]
            
            # Method 2: Also find payments by reference (tracking_no) from this import run
            # Extract tracking numbers from import rows' mapped_json
            import json
            import ast
            tracking_nos = set()
            for ir in import_rows:
                try:
                    mapped = json.loads(ir.mapped_json or "{}") if ir.mapped_json else {}
                    tracking_no = mapped.get("tracking_no")
                    if tracking_no:
                        tracking_nos.add(str(tracking_no))
                except:
                    try:
                        mapped = ast.literal_eval(ir.mapped_json or "{}") if ir.mapped_json else {}
                        tracking_no = mapped.get("tracking_no")
                        if tracking_no:
                            tracking_nos.add(str(tracking_no))
                    except:
                        pass
            
            # Update payments using raw SQL
            # Method 1: Update by order_id (payments for orders matched in this run)
            order_ids_to_update = set(order_ids)
            rows_affected_1 = 0
            
            if order_ids_to_update:
                order_ids_list = list(order_ids_to_update)
                order_ids_tuple = tuple(order_ids_list)
                if len(order_ids_list) == 1:
                    order_ids_tuple = f"({order_ids_list[0]})"
                else:
                    order_ids_tuple = str(order_ids_tuple)
                
                result1 = session.execute(text(f"""
                    UPDATE payment 
                    SET payment_date = :payment_date
                    WHERE order_id IN {order_ids_tuple}
                      AND (payment_date IS NULL OR payment_date != :payment_date)
                """), {"payment_date": filename_date})
                rows_affected_1 = result1.rowcount
            
            # Method 2: Also update payments by reference/tracking_no (even if for different orders)
            rows_affected_2 = 0
            if tracking_nos:
                tracking_list = list(tracking_nos)
                placeholders = ",".join([f"'{t}'" for t in tracking_list])
                result2 = session.execute(text(f"""
                    UPDATE payment 
                    SET payment_date = :payment_date
                    WHERE reference IN ({placeholders})
                      AND (payment_date IS NULL OR payment_date != :payment_date)
                """), {"payment_date": filename_date})
                rows_affected_2 = result2.rowcount
            
            rows_affected = rows_affected_1 + rows_affected_2
            
            if rows_affected > 0:
                if dry_run:
                    print(f"    Would update {rows_affected} payments: payment_date -> {filename_date}")
                else:
                    print(f"    Updated {rows_affected} payments: payment_date -> {filename_date}")
                updated_count += rows_affected
            else:
                skipped_count += len(order_ids)
        
        if not dry_run:
            session.commit()
            print(f"\n✓ Committed changes to database")
        else:
            print(f"\n⚠ DRY RUN - No changes committed")
    
    print(f"\nSummary:")
    print(f"  Updated: {updated_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Errors: {error_count}")
    
    if missing_excel:
        print(f"\n⚠ Missing Excel files ({len(missing_excel)}):")
        for fn in missing_excel[:20]:  # Show first 20
            print(f"    - {fn}")
        if len(missing_excel) > 20:
            print(f"    ... and {len(missing_excel) - 20} more")
    
    if no_date_in_filename:
        print(f"\n⚠ Files without ISO date in filename ({len(no_date_in_filename)}):")
        for fn in no_date_in_filename[:20]:  # Show first 20
            print(f"    - {fn}")
        if len(no_date_in_filename) > 20:
            print(f"    ... and {len(no_date_in_filename) - 20} more")
    
    return updated_count, skipped_count, error_count, missing_excel, no_date_in_filename


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Backfill payment_date for kargo payments from Excel filenames")
    parser.add_argument("--execute", action="store_true", help="Actually update the database")
    args = parser.parse_args()
    
    dry_run = not args.execute
    if dry_run:
        print("=" * 60)
        print("DRY RUN MODE - No changes will be made")
        print("Use --execute to actually update the database")
        print("=" * 60)
    
    backfill_payment_dates(dry_run=dry_run)

