#!/usr/bin/env python3
"""Fix all cancelled orders that have non-zero financial values.

This script sets total_amount, total_cost, and shipping_fee to 0.0
for all orders with status='cancelled' that still have non-zero values.
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import get_session
from app.models import Order
from sqlmodel import select

def main():
    with get_session() as session:
        # Find all cancelled orders with non-zero financials
        cancelled_orders = session.exec(
            select(Order).where(
                Order.status == "cancelled",
                # At least one financial field is non-zero
                (
                    (Order.total_amount.is_not(None)) & (Order.total_amount != 0.0)
                ) | (
                    (Order.total_cost.is_not(None)) & (Order.total_cost != 0.0)
                ) | (
                    (Order.shipping_fee.is_not(None)) & (Order.shipping_fee != 0.0)
                )
            )
        ).all()
        
        if not cancelled_orders:
            print("No cancelled orders with non-zero financials found.")
            return
        
        print(f"Found {len(cancelled_orders)} cancelled orders to fix:")
        
        fixed_count = 0
        for o in cancelled_orders:
            original_amount = float(o.total_amount or 0.0)
            original_cost = float(o.total_cost or 0.0)
            original_shipping = float(o.shipping_fee or 0.0)
            
            # Zero out financials
            o.total_amount = 0.0
            o.total_cost = 0.0
            o.shipping_fee = 0.0
            
            print(f"  Order {o.id}: amount={original_amount:.2f}, cost={original_cost:.2f}, shipping={original_shipping:.2f} -> all set to 0.0")
            fixed_count += 1
        
        print(f"\nFixed {fixed_count} cancelled orders.")
        print("Changes will be committed automatically by the session context manager.")

if __name__ == "__main__":
    main()

