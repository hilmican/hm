#!/usr/bin/env python3
"""
Fix data_date for bizim orders by reading back from ImportRow records.

This script:
1. Finds all ImportRow records for bizim source that have matched_order_id
2. Parses mapped_json to get the original shipment_date from Excel
3. Updates order.data_date to use the original shipment_date (oldest date)
4. Only updates if data_date is currently set to run.data_date (import date) or is missing
"""

import sys
import json
from pathlib import Path
from datetime import date

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.db import get_session
from app.models import ImportRun, ImportRow, Order
from sqlmodel import select
from app.services.importer import read_bizim_file
from pathlib import Path
import ast


def parse_mapped_json(s: str) -> dict:
    """Parse mapped_json string (can be JSON or Python dict literal)."""
    if not s:
        return {}
    try:
        # Try JSON first
        return json.loads(s)
    except json.JSONDecodeError:
        try:
            # Try Python literal eval (ast.literal_eval)
            obj = ast.literal_eval(s)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}


def fix_data_dates(dry_run: bool = True):
    """Fix data_date for orders based on ImportRow records."""
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
            print(f"\nProcessing run {run.id}: {run.filename} (data_date: {run.data_date})")
            
            # Try to read the Excel file directly
            excel_file = PROJECT_ROOT / "bizimexcellerimiz" / run.filename
            excel_records = None
            if excel_file.exists():
                try:
                    excel_records = read_bizim_file(str(excel_file))
                    print(f"  Read {len(excel_records)} records from Excel file")
                except Exception as e:
                    print(f"  WARNING: Could not read Excel file: {e}")
            
            # Get all import rows for this run that have matched orders
            import_rows = session.exec(
                select(ImportRow)
                .where(
                    ImportRow.import_run_id == run.id,
                    ImportRow.matched_order_id.is_not(None)
                )
                .order_by(ImportRow.row_index)
            ).all()
            
            print(f"  Found {len(import_rows)} rows with matched orders")
            
            # Build a lookup map: (client_id, total_amount) -> shipment_date from Excel
            excel_lookup = {}
            if excel_records:
                for excel_rec in excel_records:
                    # Try to match by client and total_amount
                    client_name = excel_rec.get("name")
                    total_amt = excel_rec.get("total_amount")
                    shipment_dt = excel_rec.get("shipment_date")
                    if client_name and total_amt and shipment_dt:
                        # Normalize for lookup
                        from app.utils.normalize import client_unique_key
                        client_key = client_unique_key(client_name, excel_rec.get("phone"))
                        if client_key:
                            lookup_key = (client_key, float(total_amt))
                            if lookup_key not in excel_lookup:
                                excel_lookup[lookup_key] = shipment_dt
            
            for idx, ir in enumerate(import_rows):
                try:
                    # Get the order first
                    order = session.exec(
                        select(Order).where(Order.id == ir.matched_order_id)
                    ).first()
                    
                    if not order:
                        error_count += 1
                        print(f"    ERROR: Order {ir.matched_order_id} not found")
                        continue
                    
                    if order.source != "bizim":
                        skipped_count += 1
                        continue
                    
                    # Try to get shipment_date from Excel lookup, then mapped_json, then Excel by index
                    original_shipment_date = None
                    
                    # Method 1: Lookup by client + total_amount
                    if order.client_id and order.total_amount:
                        from app.models import Client
                        client = session.exec(select(Client).where(Client.id == order.client_id)).first()
                        if client and client.unique_key:
                            lookup_key = (client.unique_key, float(order.total_amount))
                            original_shipment_date = excel_lookup.get(lookup_key)
                    
                    # Method 2: Try Excel by row_index (less reliable due to skipped rows)
                    # But also try to find by matching client + amount in Excel
                    if not original_shipment_date and excel_records:
                        # First try by row_index
                        if ir.row_index < len(excel_records):
                            excel_rec = excel_records[ir.row_index]
                            original_shipment_date = excel_rec.get("shipment_date")
                        
                        # If still not found, search by client + amount
                        if not original_shipment_date and order.client_id and order.total_amount:
                            from app.models import Client
                            client = session.exec(select(Client).where(Client.id == order.client_id)).first()
                            if client:
                                for excel_rec in excel_records:
                                    excel_name = excel_rec.get("name", "")
                                    excel_amount = excel_rec.get("total_amount")
                                    excel_date = excel_rec.get("shipment_date")
                                    # Match by name (normalized) and amount
                                    if excel_date and excel_amount and order.total_amount:
                                        if abs(float(excel_amount) - float(order.total_amount)) < 0.01:
                                            # Check if names match (simple check)
                                            if client.name and excel_name:
                                                if client.name.lower().strip() == excel_name.lower().strip():
                                                    original_shipment_date = excel_date
                                                    break
                    
                    # Method 3: Fall back to mapped_json
                    if not original_shipment_date:
                        mapped = parse_mapped_json(ir.mapped_json or "")
                        original_shipment_date = mapped.get("shipment_date")
                    
                    if not original_shipment_date:
                        skipped_count += 1
                        continue
                    
                    # Convert original_shipment_date to date if it's a string
                    if isinstance(original_shipment_date, str):
                        try:
                            from datetime import datetime
                            original_shipment_date = datetime.strptime(original_shipment_date, "%Y-%m-%d").date()
                        except Exception:
                            skipped_count += 1
                            continue
                    
                    if not isinstance(original_shipment_date, date):
                        skipped_count += 1
                        continue
                    
                    # Check if we should update
                    # Update if:
                    # 1. data_date is None
                    # 2. data_date equals run.data_date (import date, not actual order date)
                    # 3. original_shipment_date is different and valid
                    should_update = False
                    reason = ""
                    
                    if order.data_date is None:
                        should_update = True
                        reason = "data_date is None"
                    elif run.data_date and order.data_date == run.data_date:
                        # data_date was set to import date, not actual order date
                        should_update = True
                        reason = f"data_date equals import date ({run.data_date})"
                    elif original_shipment_date and original_shipment_date != order.data_date:
                        # Original date is different, use it (preserve oldest date)
                        should_update = True
                        reason = f"original date ({original_shipment_date}) differs from current ({order.data_date})"
                    
                    if should_update and original_shipment_date and original_shipment_date != order.data_date:
                        if dry_run:
                            print(f"    Would update order {order.id}: data_date {order.data_date} -> {original_shipment_date} ({reason})")
                        else:
                            order.data_date = original_shipment_date
                            print(f"    Updated order {order.id}: data_date {order.data_date} -> {original_shipment_date} ({reason})")
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
    
    return updated_count, skipped_count, error_count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fix data_date for orders from ImportRow records")
    parser.add_argument("--execute", action="store_true", help="Actually update the database (default is dry-run)")
    args = parser.parse_args()
    
    dry_run = not args.execute
    if dry_run:
        print("=" * 60)
        print("DRY RUN MODE - No changes will be made")
        print("Use --execute to actually update the database")
        print("=" * 60)
    
    fix_data_dates(dry_run=dry_run)

