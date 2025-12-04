#!/usr/bin/env python3
"""Find orders with overpaid amounts that might have payments belonging to other orders."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from app.models import Order, Payment, Client
from sqlmodel import select
from collections import defaultdict

with get_session() as session:
    # Get all orders with payments
    orders = session.exec(select(Order)).all()
    
    # Build payment map
    payments_by_order = defaultdict(list)
    all_payments = session.exec(select(Payment)).all()
    for p in all_payments:
        if p.order_id:
            payments_by_order[p.order_id].append(p)
    
    # Find overpaid orders
    overpaid_orders = []
    for o in orders:
        if o.id is None or o.total_amount is None:
            continue
        
        total = float(o.total_amount or 0.0)
        payments = payments_by_order.get(o.id, [])
        paid = sum(float(p.amount or 0.0) for p in payments)
        
        # Skip refunded/switched orders
        if (o.status or "").lower() in ("refunded", "switched", "stitched"):
            continue
        
        # Skip IBAN orders
        if bool(getattr(o, "paid_by_bank_transfer", False)):
            continue
        
        # Find significantly overpaid orders (more than 50% overpaid)
        if paid > total * 1.5 and total > 0:
            overpaid_orders.append({
                "order_id": o.id,
                "client_id": o.client_id,
                "total": total,
                "paid": paid,
                "overpaid": paid - total,
                "payments": payments,
            })
    
    # Sort by overpaid amount
    overpaid_orders.sort(key=lambda x: x["overpaid"], reverse=True)
    
    print(f"Found {len(overpaid_orders)} significantly overpaid orders:\n")
    
    # Show top 20
    for item in overpaid_orders[:20]:
        client = session.exec(select(Client).where(Client.id == item["client_id"])).first() if item["client_id"] else None
        client_name = client.name if client else "Unknown"
        
        print(f"Order {item['order_id']}: {client_name}")
        print(f"  Total: {item['total']:.2f}, Paid: {item['paid']:.2f}, Overpaid: {item['overpaid']:.2f}")
        print(f"  Payments:")
        for p in item["payments"]:
            print(f"    Payment {p.id}: {p.amount:.2f} on {p.date} ({p.method})")
        
        # Check if there are other orders for this client with matching amounts
        if item["client_id"]:
            other_orders = session.exec(
                select(Order)
                .where(Order.client_id == item["client_id"])
                .where(Order.id != item["order_id"])
                .where(Order.total_amount.is_not(None))
            ).all()
            
            matching_orders = []
            for p in item["payments"]:
                for o2 in other_orders:
                    if abs(float(o2.total_amount or 0.0) - float(p.amount or 0.0)) < 0.01:
                        matching_orders.append((o2.id, o2.total_amount, p.id, p.amount))
            
            if matching_orders:
                print(f"  ⚠️  Potential matches:")
                for oid, ototal, pid, pamt in matching_orders:
                    print(f"    Payment {pid} ({pamt:.2f}) might belong to Order {oid} (total: {ototal:.2f})")
        print()

