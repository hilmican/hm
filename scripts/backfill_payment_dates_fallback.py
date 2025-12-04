#!/usr/bin/env python3
"""
Backfill payment_date for payments in kargo imports without ISO date filenames.

This script handles payments that:
1. Have references that appear in kargo imports
2. But the kargo import filename doesn't start with ISO date (YYYY-MM-DD)
3. Uses run.data_date as a fallback (though less accurate than filename date)
"""

import sys
import re
import json
import ast
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except:
    PROJECT_ROOT = Path("/app")
sys.path.insert(0, str(PROJECT_ROOT))

from app.db import get_session
from app.models import ImportRun, ImportRow, Payment
from sqlmodel import select
from sqlalchemy import text


def parse_mapped_json(s):
    """Parse mapped_json safely."""
    if not s:
        return {}
    try:
        return json.loads(s)
    except:
        try:
            return ast.literal_eval(s)
        except:
            return {}


def backfill_payment_dates_fallback(dry_run: bool = True):
    """Backfill payment_date using run.data_date for files without ISO dates."""
    updated_count = 0
    skipped_count = 0
    
    with get_session() as session:
        # Get all kargo import runs without ISO date in filename
        kargo_runs = session.exec(
            select(ImportRun)
            .where(ImportRun.source == "kargo")
            .order_by(ImportRun.started_at.desc())
        ).all()
        
        print(f"Found {len(kargo_runs)} kargo import runs")
        
        for run in kargo_runs:
            # Check if filename starts with ISO date
            filename_date = None
            match = re.match(r'^(\d{4}-\d{2}-\d{2})', run.filename)
            if match:
                # Skip files with ISO dates (already handled by main script)
                continue
            
            # Use run.data_date as fallback
            if not run.data_date:
                continue
            
            fallback_date = run.data_date
            print(f"\nProcessing run {run.id}: {run.filename} (using data_date: {fallback_date})")
            
            # Get all import rows for this run
            import_rows = session.exec(
                select(ImportRow)
                .where(ImportRow.import_run_id == run.id)
            ).all()
            
            # Extract all references from mapped_json (search entire JSON, not just tracking_no)
            references = set()
            for ir in import_rows:
                mapped = parse_mapped_json(ir.mapped_json or "")
                # Check tracking_no field
                tracking_no = mapped.get("tracking_no")
                if tracking_no:
                    references.add(str(tracking_no))
                # Also search entire JSON string for any numeric references that look like tracking numbers
                if ir.mapped_json:
                    # Find all numeric strings that might be references (14+ digits)
                    numeric_refs = re.findall(r'\b\d{14,}\b', ir.mapped_json)
                    references.update(numeric_refs)
            
            if not references:
                continue
            
            print(f"  Found {len(references)} unique references in this run")
            
            # Update payments by reference
            ref_list = list(references)
            placeholders = ",".join([f"'{r}'" for r in ref_list])
            
            result = session.execute(text(f"""
                UPDATE payment 
                SET payment_date = :payment_date
                WHERE reference IN ({placeholders})
                  AND payment_date IS NULL
            """), {"payment_date": fallback_date})
            rows_affected = result.rowcount
            
            if rows_affected > 0:
                if dry_run:
                    print(f"    Would update {rows_affected} payments: payment_date -> {fallback_date} (from run.data_date)")
                else:
                    print(f"    Updated {rows_affected} payments: payment_date -> {fallback_date} (from run.data_date)")
                updated_count += rows_affected
            else:
                skipped_count += len(references)
        
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
    parser = argparse.ArgumentParser(description="Backfill payment_date using run.data_date for files without ISO dates")
    parser.add_argument("--execute", action="store_true", help="Actually update the database")
    args = parser.parse_args()
    
    dry_run = not args.execute
    if dry_run:
        print("=" * 60)
        print("DRY RUN MODE - No changes will be made")
        print("Use --execute to actually update the database")
        print("=" * 60)
    
    backfill_payment_dates_fallback(dry_run=dry_run)

