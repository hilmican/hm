#!/usr/bin/env python3
"""
Add payment_date column to payment table if it doesn't exist.
"""

import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except:
    PROJECT_ROOT = Path("/app")
sys.path.insert(0, str(PROJECT_ROOT))

from app.db import get_session
from sqlalchemy import text


def add_payment_date_column():
    """Add payment_date column to payment table."""
    with get_session() as session:
        # Check if column exists
        result = session.exec(text("SHOW COLUMNS FROM payment LIKE 'payment_date'")).first()
        if result:
            print("✓ payment_date column already exists")
            return
        
        # Add the column
        try:
            session.exec(text("ALTER TABLE payment ADD COLUMN payment_date DATE NULL"))
            # Check if index exists before creating
            indexes = session.exec(text("SHOW INDEX FROM payment WHERE Key_name = 'idx_payment_payment_date'")).all()
            if not indexes:
                session.exec(text("CREATE INDEX idx_payment_payment_date ON payment(payment_date)"))
            
            session.commit()
            print("✓ Added payment_date column to payment table")
        except Exception as e:
            # Column might already exist, check
            if "Duplicate column" in str(e) or "already exists" in str(e).lower():
                print("✓ payment_date column already exists (or was just added)")
            else:
                raise


if __name__ == "__main__":
    add_payment_date_column()

