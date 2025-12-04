#!/usr/bin/env python3
"""Check order 5209 payment status calculation."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from app.models import Order, Payment
from sqlmodel import select

with get_session() as session:
    order = session.exec(select(Order).where(Order.id == 5209)).first()
    if not order:
        print("Order 5209 not found!")
        exit(1)
    
    payments = session.exec(select(Payment).where(Payment.order_id == 5209)).all()
    
    total_paid = sum(float(p.amount or 0.0) for p in payments)
    total_amount = float(order.total_amount or 0.0)
    
    print(f"Order 5209:")
    print(f"  Total Amount: {total_amount}")
    print(f"  Total Paid: {total_paid}")
    print(f"  Paid >= Total: {total_paid >= total_amount}")
    print(f"  Status field: {order.status}")
    print(f"  paid_by_bank_transfer: {order.paid_by_bank_transfer}")
    print(f"\nPayments:")
    for p in payments:
        print(f"  - Payment {p.id}: {p.amount} on {p.date} ({p.method})")
    
    # Check if there are other orders for this client
    if order.client_id:
        other_orders = session.exec(
            select(Order).where(Order.client_id == order.client_id).order_by(Order.id.desc())
        ).all()
        print(f"\nAll orders for client {order.client_id}:")
        for o in other_orders[:10]:  # Show first 10
            o_payments = session.exec(select(Payment).where(Payment.order_id == o.id)).all()
            o_paid = sum(float(p.amount or 0.0) for p in o_payments)
            o_total = float(o.total_amount or 0.0)
            print(f"  Order {o.id}: total={o_total}, paid={o_paid}, status={o.status}, source={o.source}, tracking={o.tracking_no}")

