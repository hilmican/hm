#!/usr/bin/env python3
"""Migration script to unify ads and posts into a single links structure.

1. Add link_type column to ads table
2. Migrate posts to ads table
3. Add link_type column to ads_products table  
4. Migrate posts_products to ads_products
5. Update conversations.last_ad_id to last_link_type/last_link_id
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session, engine
from sqlalchemy import text as _text
import logging

log = logging.getLogger("migrate_unify_links")
logging.basicConfig(level=logging.INFO)

def migrate():
    """Run the migration."""
    log.info("Starting migration: unify ads and posts into links structure")
    
    with engine.begin() as conn:
        # Step 1: Add link_type column to ads table
        log.info("Step 1: Adding link_type column to ads table")
        try:
            rows = conn.exec_driver_sql(
                """
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ads' AND COLUMN_NAME = 'link_type'
                """
            ).fetchall()
            if not rows:
                conn.exec_driver_sql("ALTER TABLE ads ADD COLUMN link_type VARCHAR(16) NOT NULL DEFAULT 'ad'")
                conn.exec_driver_sql("CREATE INDEX idx_ads_link_type ON ads(link_type)")
                log.info("  ✓ Added link_type column to ads table")
            else:
                log.info("  ✓ link_type column already exists")
        except Exception as e:
            log.error(f"  ✗ Failed to add link_type to ads: {e}")
            raise
        
        # Step 2: Migrate posts to ads table
        log.info("Step 2: Migrating posts to ads table")
        try:
            # Check if posts table exists
            posts_exists = conn.exec_driver_sql(
                """
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'posts'
                LIMIT 1
                """
            ).fetchone()
            
            if posts_exists:
                # Migrate posts to ads
                result = conn.exec_driver_sql(
                    """
                    INSERT INTO ads (ad_id, link_type, name, image_url, link, updated_at)
                    SELECT post_id, 'post', title, NULL, url, updated_at
                    FROM posts
                    WHERE post_id NOT IN (SELECT ad_id FROM ads)
                    """
                )
                migrated = result.rowcount if hasattr(result, 'rowcount') else 0
                log.info(f"  ✓ Migrated {migrated} posts to ads table")
            else:
                log.info("  ✓ No posts table found (nothing to migrate)")
        except Exception as e:
            log.error(f"  ✗ Failed to migrate posts: {e}")
            raise
        
        # Step 3: Add link_type column to ads_products table
        log.info("Step 3: Adding link_type column to ads_products table")
        try:
            rows = conn.exec_driver_sql(
                """
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ads_products' AND COLUMN_NAME = 'link_type'
                """
            ).fetchall()
            if not rows:
                conn.exec_driver_sql("ALTER TABLE ads_products ADD COLUMN link_type VARCHAR(16) NOT NULL DEFAULT 'ad'")
                conn.exec_driver_sql("CREATE INDEX idx_ads_products_link_type ON ads_products(link_type)")
                log.info("  ✓ Added link_type column to ads_products table")
            else:
                log.info("  ✓ link_type column already exists in ads_products")
        except Exception as e:
            log.error(f"  ✗ Failed to add link_type to ads_products: {e}")
            raise
        
        # Step 4: Migrate posts_products to ads_products
        log.info("Step 4: Migrating posts_products to ads_products")
        try:
            # Check if posts_products table exists
            posts_products_exists = conn.exec_driver_sql(
                """
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'posts_products'
                LIMIT 1
                """
            ).fetchone()
            
            if posts_products_exists:
                # Migrate posts_products to ads_products
                result = conn.exec_driver_sql(
                    """
                    INSERT INTO ads_products (ad_id, link_type, product_id, sku, auto_linked, created_at)
                    SELECT post_id, 'post', product_id, sku, auto_linked, created_at
                    FROM posts_products
                    WHERE post_id NOT IN (SELECT ad_id FROM ads_products)
                    """
                )
                migrated = result.rowcount if hasattr(result, 'rowcount') else 0
                log.info(f"  ✓ Migrated {migrated} posts_products to ads_products table")
            else:
                log.info("  ✓ No posts_products table found (nothing to migrate)")
        except Exception as e:
            log.error(f"  ✗ Failed to migrate posts_products: {e}")
            raise
        
        # Step 5: Add last_link_type and last_link_id to conversations
        log.info("Step 5: Adding last_link_type and last_link_id to conversations table")
        try:
            # Check if columns exist
            rows = conn.exec_driver_sql(
                """
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'conversations' AND COLUMN_NAME IN ('last_link_type', 'last_link_id')
                """
            ).fetchall()
            existing_cols = {str(r[0]).lower() for r in rows or []}
            
            if 'last_link_type' not in existing_cols:
                conn.exec_driver_sql("ALTER TABLE conversations ADD COLUMN last_link_type VARCHAR(16) NULL")
                conn.exec_driver_sql("CREATE INDEX idx_conversations_link_type ON conversations(last_link_type)")
                log.info("  ✓ Added last_link_type column")
            else:
                log.info("  ✓ last_link_type column already exists")
            
            if 'last_link_id' not in existing_cols:
                conn.exec_driver_sql("ALTER TABLE conversations ADD COLUMN last_link_id VARCHAR(128) NULL")
                conn.exec_driver_sql("CREATE INDEX idx_conversations_link_id ON conversations(last_link_id)")
                log.info("  ✓ Added last_link_id column")
            else:
                log.info("  ✓ last_link_id column already exists")
        except Exception as e:
            log.error(f"  ✗ Failed to add link columns to conversations: {e}")
            raise
        
        # Step 6: Migrate existing last_ad_id to last_link_type/last_link_id
        log.info("Step 6: Migrating existing last_ad_id to last_link_type/last_link_id")
        try:
            result = conn.exec_driver_sql(
                """
                UPDATE conversations
                SET last_link_type = 'ad', last_link_id = last_ad_id
                WHERE last_ad_id IS NOT NULL AND (last_link_id IS NULL OR last_link_id != last_ad_id)
                """
            )
            migrated = result.rowcount if hasattr(result, 'rowcount') else 0
            log.info(f"  ✓ Migrated {migrated} conversations from last_ad_id to last_link_type/last_link_id")
        except Exception as e:
            log.error(f"  ✗ Failed to migrate conversations: {e}")
            raise
        
        log.info("Migration completed successfully!")

if __name__ == "__main__":
    try:
        migrate()
    except Exception as e:
        log.error(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

