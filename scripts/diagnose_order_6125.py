#!/usr/bin/env python3
"""
Diagnose order #6125 to understand why data_date is still wrong.
"""

import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except:
    PROJECT_ROOT = Path("/app")
sys.path.insert(0, str(PROJECT_ROOT))

from app.db import get_session
from app.models import Order, ImportRun, ImportRow, Payment
from sqlmodel import select


def diagnose_order_6125():
    with get_session() as session:
        # Get order 6125
        order = session.exec(select(Order).where(Order.id == 6125)).first()
        
        if not order:
            print("Order #6125 not found!")
            return
        
        print(f"Order #6125:")
        print(f"  source: {order.source}")
        print(f"  shipment_date (kargo tarihi): {order.shipment_date}")
        print(f"  data_date (data tarihi): {order.data_date}")
        print(f"  tracking_no: {order.tracking_no}")
        print(f"  client_id: {order.client_id}")
        
        # Find ImportRow records that matched this order
        import_rows = session.exec(
            select(ImportRow)
            .where(ImportRow.matched_order_id == 6125)
            .order_by(ImportRow.id.desc())
        ).all()
        
        print(f"\nFound {len(import_rows)} ImportRow records for this order:")
        for ir in import_rows:
            run = session.exec(select(ImportRun).where(ImportRun.id == ir.import_run_id)).first()
            if run:
                print(f"\n  ImportRun #{run.id}:")
                print(f"    source: {run.source}")
                print(f"    filename: {run.filename}")
                print(f"    run.data_date: {run.data_date}")
                print(f"    started_at: {run.started_at}")
                print(f"    ImportRow status: {ir.status}")
                print(f"    ImportRow row_index: {ir.row_index}")
        
        # Check payments
        payments = session.exec(select(Payment).where(Payment.order_id == 6125)).all()
        print(f"\nFound {len(payments)} Payment records:")
        for p in payments:
            print(f"  Payment #{p.id}:")
            print(f"    amount: {p.amount}")
            print(f"    date (legacy): {p.date}")
            print(f"    payment_date (from filename): {p.payment_date}")
            print(f"    method: {p.method}")


if __name__ == "__main__":
    diagnose_order_6125()






