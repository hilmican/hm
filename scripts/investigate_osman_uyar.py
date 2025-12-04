#!/usr/bin/env python3
"""Investigate Osman Uyar's skipped payments."""

import sys
import datetime as dt
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from app.models import Order, Payment, Client, ImportRow
from app.utils.hashing import compute_row_hash
from sqlmodel import select

with get_session() as session:
    # Find Osman Uyar
    client = session.exec(
        select(Client).where(Client.name.ilike("%Osman Uyar%"))
    ).first()
    
    if not client:
        print("Client not found!")
        exit(1)
    
    print(f"Client: {client.name} (ID: {client.id})\n")
    
    # Get all orders
    orders = session.exec(
        select(Order)
        .where(Order.client_id == client.id)
        .order_by(Order.id.desc())
    ).all()
    
    print(f"All Orders for Osman Uyar:\n")
    for o in orders:
        payments = session.exec(
            select(Payment).where(Payment.order_id == o.id)
        ).all()
        total_paid = sum(float(p.amount or 0.0) for p in payments)
        total = float(o.total_amount or 0.0)
        
        status = "✓ PAID" if total_paid >= total else "✗ UNPAID"
        print(f"Order {o.id}: {total:.2f} total, {total_paid:.2f} paid - {status}")
        print(f"  Data Date: {o.data_date}, Shipment Date: {o.shipment_date}")
        for p in payments:
            print(f"    Payment {p.id}: {p.amount:.2f} on {p.date} ({p.method})")
        print()
    
    # Check the two skipped rows
    row1_data = {
        'notes': 'Tesellüm Tahsil Edildi. | Tahsil Edildi.',
        'shipment_date': dt.date(2025, 11, 4),
        'tracking_no': '82565878724549',
        'alici_kodu': '1502692590',
        'name': 'Osman Uyar',
        'payment_amount': 1300.0,
        'fee_kargo': 92.08,
        'payment_method': 'Pos'
    }
    
    row2_data = {
        'notes': 'Tesellüm Tahsil Edildi. | Tahsil Edildi.',
        'shipment_date': dt.date(2025, 11, 3),
        'tracking_no': '89906978398327',
        'alici_kodu': '1502445266',
        'name': 'Osman Uyar',
        'payment_amount': 1400.0,
        'fee_kargo': 93.62,
        'payment_method': 'Pos'
    }
    
    hash1 = compute_row_hash(row1_data)
    hash2 = compute_row_hash(row2_data)
    
    print(f"\nChecking ImportRow records:\n")
    
    # Find ImportRow records for these hashes
    irs1 = session.exec(
        select(ImportRow).where(ImportRow.row_hash == hash1).order_by(ImportRow.id.asc())
    ).all()
    
    irs2 = session.exec(
        select(ImportRow).where(ImportRow.row_hash == hash2).order_by(ImportRow.id.asc())
    ).all()
    
    print(f"Row 1 (1300.0, date 2025-11-04, hash: {hash1[:16]}...):")
    for ir in irs1:
        print(f"  ImportRow {ir.id}: Run {ir.import_run_id}, Status: {ir.status}, Order: {ir.matched_order_id}, Message: {ir.message}")
    
    print(f"\nRow 2 (1400.0, date 2025-11-03, hash: {hash2[:16]}...):")
    for ir in irs2:
        print(f"  ImportRow {ir.id}: Run {ir.import_run_id}, Status: {ir.status}, Order: {ir.matched_order_id}, Message: {ir.message}")
    
    # Check Order 5026 specifically
    order_5026 = session.exec(select(Order).where(Order.id == 5026)).first()
    if order_5026:
        print(f"\nOrder 5026 details:")
        print(f"  Total: {order_5026.total_amount}")
        print(f"  Data Date: {order_5026.data_date}")
        print(f"  Shipment Date: {order_5026.shipment_date}")
        payments_5026 = session.exec(select(Payment).where(Payment.order_id == 5026)).all()
        print(f"  Payments: {len(payments_5026)}")
        for p in payments_5026:
            print(f"    Payment {p.id}: {p.amount:.2f} on {p.date}")

