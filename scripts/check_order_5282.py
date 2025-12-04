#!/usr/bin/env python3
"""Check order 5282 details."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from app.models import Order, Payment, Client
from sqlmodel import select

with get_session() as session:
    order = session.exec(select(Order).where(Order.id == 5282)).first()
    if not order:
        print("Order 5282 not found!")
        exit(1)
    
    print(f"Order 5282:")
    print(f"  Client ID: {order.client_id}")
    print(f"  Total Amount: {order.total_amount}")
    print(f"  Shipment Date: {order.shipment_date}")
    print(f"  Data Date: {order.data_date}")
    print(f"  Source: {order.source}")
    print(f"  Status: {order.status}")
    print(f"  Tracking No: {order.tracking_no}")
    print(f"  Created/Updated: {order.created_at if hasattr(order, 'created_at') else 'N/A'}")
    
    if order.client_id:
        client = session.exec(select(Client).where(Client.id == order.client_id)).first()
        if client:
            print(f"\nClient: {client.name} ({client.phone})")
    
    payments = session.exec(select(Payment).where(Payment.order_id == 5282)).all()
    print(f"\nPayments: {len(payments)}")
    for p in payments:
        print(f"  - {p.amount} on {p.date}")

