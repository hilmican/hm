#!/usr/bin/env python3
"""Fix Mustafa Bahşi's payment: move Payment 970 from Order 5209 to Order 5282."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from app.models import Payment, Order
from sqlmodel import select

with get_session() as session:
    # Get the payment
    payment = session.exec(select(Payment).where(Payment.id == 970)).first()
    if not payment:
        print("Payment 970 not found!")
        exit(1)
    
    print(f"Current Payment 970:")
    print(f"  Order ID: {payment.order_id}")
    print(f"  Amount: {payment.amount}")
    print(f"  Date: {payment.date}")
    
    # Verify orders
    order_5209 = session.exec(select(Order).where(Order.id == 5209)).first()
    order_5282 = session.exec(select(Order).where(Order.id == 5282)).first()
    
    if not order_5209 or not order_5282:
        print("One of the orders not found!")
        exit(1)
    
    print(f"\nOrder 5209: total={order_5209.total_amount}, client={order_5209.client_id}")
    print(f"Order 5282: total={order_5282.total_amount}, client={order_5282.client_id}, data_date={order_5282.data_date}")
    
    if payment.order_id != 5209:
        print(f"\n⚠️  Payment is already on Order {payment.order_id}, not 5209. Aborting.")
        exit(1)
    
    if order_5282.total_amount != payment.amount:
        print(f"\n⚠️  Order 5282 total ({order_5282.total_amount}) doesn't match payment amount ({payment.amount}). Continue anyway?")
        # For safety, we'll still do it but warn
    
    # Move the payment
    print(f"\nMoving Payment 970 from Order 5209 to Order 5282...")
    payment.order_id = 5282
    session.add(payment)
    session.commit()
    
    print("✅ Payment moved successfully!")
    
    # Verify
    payments_5209 = session.exec(select(Payment).where(Payment.order_id == 5209)).all()
    payments_5282 = session.exec(select(Payment).where(Payment.order_id == 5282)).all()
    
    print(f"\nOrder 5209 now has {len(payments_5209)} payments:")
    for p in payments_5209:
        print(f"  - {p.amount} on {p.date}")
    
    print(f"\nOrder 5282 now has {len(payments_5282)} payments:")
    for p in payments_5282:
        print(f"  - {p.amount} on {p.date}")

