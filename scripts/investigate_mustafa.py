#!/usr/bin/env python3
"""Investigate why Mustafa Bahşi's row is marked as duplicate and payment not created."""

import sys
import os
import json
import datetime as dt
from pathlib import Path

# Add app directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from app.models import ImportRow, ImportRun, Order, Payment, Client
from app.utils.hashing import compute_row_hash
from sqlmodel import select

# The mapped_json from the report
mustafa_row_data = {
    'notes': 'Tesellüm Tahsil Edildi. | Tahsil Edildi.',
    'shipment_date': dt.date(2025, 10, 13),
    'tracking_no': '83541959608890',
    'alici_kodu': '1498980179',
    'name': 'Mustafa Bahşi',
    'payment_amount': 1300.0,
    'fee_kargo': 92.08,
    'payment_method': 'Nakit'
}

# Compute the row hash
row_hash = compute_row_hash(mustafa_row_data)
print(f"Row hash for Mustafa Bahşi's row: {row_hash}\n")

with get_session() as session:
    # Find all ImportRow records with this hash
    all_rows = session.exec(
        select(ImportRow)
        .where(ImportRow.row_hash == row_hash)
        .order_by(ImportRow.id.asc())
    ).all()
    
    print(f"Found {len(all_rows)} ImportRow records with this hash:\n")
    
    for idx, ir in enumerate(all_rows, 1):
        print(f"--- ImportRow #{idx} (ID: {ir.id}) ---")
        print(f"  Import Run ID: {ir.import_run_id}")
        print(f"  Row Index: {ir.row_index}")
        print(f"  Status: {ir.status}")
        print(f"  Message: {ir.message}")
        print(f"  Matched Client ID: {ir.matched_client_id}")
        print(f"  Matched Order ID: {ir.matched_order_id}")
        
        # Get the import run info
        run = session.exec(select(ImportRun).where(ImportRun.id == ir.import_run_id)).first()
        if run:
            print(f"  Run Source: {run.source}")
            print(f"  Run Filename: {run.filename}")
            print(f"  Run Date: {run.data_date}")
        
        print()
    
    # Check the first (oldest) ImportRow that matched an order
    first_with_order = None
    for ir in all_rows:
        if ir.matched_order_id:
            first_with_order = ir
            break
    
    if first_with_order:
        print(f"\n=== Investigating Order ID {first_with_order.matched_order_id} ===\n")
        
        order = session.exec(select(Order).where(Order.id == first_with_order.matched_order_id)).first()
        if order:
            print(f"Order Details:")
            print(f"  ID: {order.id}")
            print(f"  Client ID: {order.client_id}")
            print(f"  Tracking No: {order.tracking_no}")
            print(f"  Source: {order.source}")
            print(f"  Status: {order.status}")
            print(f"  Total Amount: {order.total_amount}")
            print(f"  Shipment Date: {order.shipment_date}")
            print(f"  Data Date: {order.data_date}")
            
            # Get client info
            if order.client_id:
                client = session.exec(select(Client).where(Client.id == order.client_id)).first()
                if client:
                    print(f"\nClient Details:")
                    print(f"  ID: {client.id}")
                    print(f"  Name: {client.name}")
                    print(f"  Phone: {client.phone}")
                    print(f"  Unique Key: {client.unique_key}")
            
            # Check payments for this order
            payments = session.exec(
                select(Payment).where(Payment.order_id == order.id)
            ).all()
            
            print(f"\nPayments for Order {order.id}:")
            if payments:
                for p in payments:
                    print(f"  Payment ID: {p.id}")
                    print(f"    Amount: {p.amount}")
                    print(f"    Date: {p.date}")
                    print(f"    Method: {p.method}")
                    print(f"    Net Amount: {p.net_amount}")
            else:
                print("  NO PAYMENTS FOUND!")
                print(f"  This is the problem! Order {order.id} has no payments despite payment_amount=1300.0 in the kargo row.")
        
        # Check if there are payments for other orders with same tracking_no
        if order and order.tracking_no:
            other_orders = session.exec(
                select(Order).where(Order.tracking_no == order.tracking_no)
            ).all()
            if len(other_orders) > 1:
                print(f"\n⚠️  Found {len(other_orders)} orders with tracking_no '{order.tracking_no}':")
                for o in other_orders:
                    p_count = len(session.exec(select(Payment).where(Payment.order_id == o.id)).all())
                    print(f"  Order {o.id} (source={o.source}, status={o.status}) - {p_count} payments")
    
    else:
        print("\n⚠️  No ImportRow with matched_order_id found!")
        print("This means the row was never successfully matched to an order.")

