#!/usr/bin/env python3
"""
Create the raw_events table in MySQL database.
Run this if the table is missing.
"""

import os
import sys

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.db import get_session
from sqlalchemy import text

def create_raw_events_table():
    """Create the raw_events table if it doesn't exist."""
    with get_session() as session:
        try:
            # Check if table exists
            result = session.exec(text("SHOW TABLES LIKE 'raw_events'"))
            if result.first():
                print("✅ raw_events table already exists")
                return

            # Create the table
            session.exec(text("""
                CREATE TABLE raw_events (
                    id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    object VARCHAR(255) NOT NULL,
                    entry_id VARCHAR(255) NOT NULL,
                    payload LONGTEXT NOT NULL,
                    sig256 VARCHAR(255) NOT NULL,
                    uniq_hash VARCHAR(255) UNIQUE
                )
            """))
            session.commit()
            print("✅ raw_events table created successfully")

        except Exception as e:
            print(f"❌ Error creating raw_events table: {e}")
            session.rollback()

if __name__ == "__main__":
    create_raw_events_table()
