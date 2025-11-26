#!/usr/bin/env python3
"""
Quick fix script for conversation 3120 - creates missing ads_products entry
if story is linked to product.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from sqlalchemy import text

conv_id = 3120
story_id = "18120589918539447"
story_key = f"story:{story_id}"

with get_session() as session:
    print(f"=== Fixing Story Link for Conversation {conv_id} ===\n")
    
    # 1. Check if story is linked to product
    sp = session.exec(
        text("SELECT story_id, product_id, sku FROM stories_products WHERE story_id = :sid").bindparams(sid=story_id)
    ).first()
    
    if not sp:
        print(f"❌ Story {story_id} is NOT linked to any product!")
        print(f"\nPlease link the story to a product first:")
        print(f"   https://hma.cdn.com.tr/stories/{story_id}/edit")
        sys.exit(1)
    
    product_id = sp[1] if isinstance(sp, (list, tuple)) else getattr(sp, 'product_id', None)
    sku = sp[2] if isinstance(sp, (list, tuple)) else getattr(sp, 'sku', None)
    
    print(f"✅ Story {story_id} is linked to product {product_id}")
    if sku:
        print(f"   SKU: {sku}")
    
    # 2. Check if ads_products entry exists
    ap = session.exec(
        text("SELECT product_id FROM ads_products WHERE ad_id = :aid AND link_type = 'story'").bindparams(aid=story_key)
    ).first()
    
    if ap:
        existing_pid = ap[0] if isinstance(ap, (list, tuple)) else getattr(ap, 'product_id', None)
        if existing_pid == product_id:
            print(f"\n✅ ads_products entry already exists and matches!")
            print(f"   Product ID: {existing_pid}")
            sys.exit(0)
        else:
            print(f"\n⚠️  ads_products entry exists but product_id mismatch:")
            print(f"   Current: {existing_pid}, Expected: {product_id}")
            print(f"   Updating...")
    else:
        print(f"\n❌ ads_products entry MISSING - creating it...")
    
    # 3. Ensure ads entry exists
    try:
        session.exec(
            text("""
                INSERT INTO ads(ad_id, link_type, name, image_url, link, updated_at)
                VALUES(:id, 'story', :name, NULL, NULL, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    link_type = 'story',
                    updated_at = CURRENT_TIMESTAMP
            """).bindparams(id=story_key, name=f"Story {story_id}")
        )
    except Exception:
        try:
            session.exec(
                text("""
                    INSERT OR IGNORE INTO ads(ad_id, link_type, name, image_url, link, updated_at)
                    VALUES(:id, 'story', :name, NULL, NULL, CURRENT_TIMESTAMP)
                """).bindparams(id=story_key, name=f"Story {story_id}")
            )
        except Exception:
            pass
    
    # 4. Create/update ads_products entry
    try:
        session.exec(
            text("""
                INSERT INTO ads_products(ad_id, link_type, product_id, sku, auto_linked)
                VALUES(:aid, 'story', :pid, :sku, 0)
                ON DUPLICATE KEY UPDATE
                    product_id = VALUES(product_id),
                    sku = VALUES(sku),
                    link_type = 'story',
                    auto_linked = 0
            """).bindparams(aid=story_key, pid=int(product_id), sku=(sku or None))
        )
        session.commit()
        print(f"✅ Created/Updated ads_products entry!")
        print(f"   ad_id: {story_key}")
        print(f"   product_id: {product_id}")
    except Exception as e:
        try:
            session.exec(
                text("""
                    INSERT OR REPLACE INTO ads_products(ad_id, link_type, product_id, sku, auto_linked)
                    VALUES(:aid, 'story', :pid, :sku, 0)
                """).bindparams(aid=story_key, pid=int(product_id), sku=(sku or None))
            )
            session.commit()
            print(f"✅ Created ads_products entry (SQLite mode)!")
        except Exception as e2:
            session.rollback()
            print(f"❌ Failed: {e2}")
            sys.exit(1)
    
    # 5. Verify
    verify = session.exec(
        text("SELECT product_id FROM ads_products WHERE ad_id = :aid AND link_type = 'story'").bindparams(aid=story_key)
    ).first()
    
    if verify:
        print(f"\n✅ Verification successful!")
        print(f"   Conversation {conv_id} should now work with AI replies.")
    else:
        print(f"\n❌ Verification failed - entry still missing!")

