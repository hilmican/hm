#!/usr/bin/env python3
"""Fix Osman Uyar's payments: move Payment 1091 to Order 5538 and Payment 1109 to Order 5516."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from app.models import Payment, Order
from sqlmodel import select

with get_session() as session:
    # Get the payments
    payment_1091 = session.exec(select(Payment).where(Payment.id == 1091)).first()
    payment_1109 = session.exec(select(Payment).where(Payment.id == 1109)).first()
    
    if not payment_1091 or not payment_1109:
        print("Payments not found!")
        exit(1)
    
    print(f"Current Payment 1091:")
    print(f"  Order ID: {payment_1091.order_id}")
    print(f"  Amount: {payment_1091.amount}")
    print(f"  Date: {payment_1091.date}")
    
    print(f"\nCurrent Payment 1109:")
    print(f"  Order ID: {payment_1109.order_id}")
    print(f"  Amount: {payment_1109.amount}")
    print(f"  Date: {payment_1109.date}")
    
    # Verify orders
    order_5026 = session.exec(select(Order).where(Order.id == 5026)).first()
    order_5538 = session.exec(select(Order).where(Order.id == 5538)).first()
    order_5516 = session.exec(select(Order).where(Order.id == 5516)).first()
    
    if not all([order_5026, order_5538, order_5516]):
        print("One of the orders not found!")
        exit(1)
    
    print(f"\nOrder 5026: total={order_5026.total_amount}, client={order_5026.client_id}")
    print(f"Order 5538: total={order_5538.total_amount}, client={order_5538.client_id}, data_date={order_5538.data_date}")
    print(f"Order 5516: total={order_5516.total_amount}, client={order_5516.client_id}, data_date={order_5516.data_date}")
    
    # Verify amounts match
    if order_5538.total_amount != payment_1091.amount:
        print(f"\n⚠️  Order 5538 total ({order_5538.total_amount}) doesn't match Payment 1091 amount ({payment_1091.amount})")
    
    if order_5516.total_amount != payment_1109.amount:
        print(f"\n⚠️  Order 5516 total ({order_5516.total_amount}) doesn't match Payment 1109 amount ({payment_1109.amount})")
    
    # Move Payment 1091 to Order 5538
    print(f"\nMoving Payment 1091 from Order {payment_1091.order_id} to Order 5538...")
    payment_1091.order_id = 5538
    session.add(payment_1091)
    
    # Move Payment 1109 to Order 5516
    print(f"Moving Payment 1109 from Order {payment_1109.order_id} to Order 5516...")
    payment_1109.order_id = 5516
    session.add(payment_1109)
    
    session.commit()
    
    print("✅ Payments moved successfully!")
    
    # Verify
    print(f"\nOrder 5026 now has:")
    payments_5026 = session.exec(select(Payment).where(Payment.order_id == 5026)).all()
    for p in payments_5026:
        print(f"  Payment {p.id}: {p.amount:.2f} on {p.date}")
    total_5026 = sum(float(p.amount or 0.0) for p in payments_5026)
    print(f"  Total Paid: {total_5026:.2f} (Order Total: {order_5026.total_amount:.2f})")
    
    print(f"\nOrder 5538 now has:")
    payments_5538 = session.exec(select(Payment).where(Payment.order_id == 5538)).all()
    for p in payments_5538:
        print(f"  Payment {p.id}: {p.amount:.2f} on {p.date}")
    total_5538 = sum(float(p.amount or 0.0) for p in payments_5538)
    print(f"  Total Paid: {total_5538:.2f} (Order Total: {order_5538.total_amount:.2f})")
    
    print(f"\nOrder 5516 now has:")
    payments_5516 = session.exec(select(Payment).where(Payment.order_id == 5516)).all()
    for p in payments_5516:
        print(f"  Payment {p.id}: {p.amount:.2f} on {p.date}")
    total_5516 = sum(float(p.amount or 0.0) for p in payments_5516)
    print(f"  Total Paid: {total_5516:.2f} (Order Total: {order_5516.total_amount:.2f})")

