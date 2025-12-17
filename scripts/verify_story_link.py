#!/usr/bin/env python3
"""
Verify story link status and check if sync is working correctly.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from sqlalchemy import text

story_id = "18120589918539447"
story_key = f"story:{story_id}"
conv_id = 3120

with get_session() as session:
    print(f"=== Verifying Story Link for Conversation {conv_id} ===\n")
    
    # Check all components
    sp = session.exec(
        text("SELECT product_id, sku FROM stories_products WHERE story_id = :sid").bindparams(sid=story_id)
    ).first()
    
    ap = session.exec(
        text("SELECT product_id FROM ads_products WHERE ad_id = :aid AND link_type = 'story'").bindparams(aid=story_key)
    ).first()
    
    conv = session.exec(
        text("SELECT last_link_id, last_link_type FROM conversations WHERE id = :cid").bindparams(cid=conv_id)
    ).first()
    
    print("Component Status:")
    print(f"  1. stories_products: {'✅ Linked' if sp else '❌ NOT linked'}")
    if sp:
        print(f"     Product ID: {sp[0] if isinstance(sp, (list, tuple)) else getattr(sp, 'product_id', None)}")
    
    print(f"  2. ads_products: {'✅ Exists' if ap else '❌ MISSING'}")
    if ap:
        print(f"     Product ID: {ap[0] if isinstance(ap, (list, tuple)) else getattr(ap, 'product_id', None)}")
    
    if conv:
        link_id = conv[0] if isinstance(conv, (list, tuple)) else getattr(conv, 'last_link_id', None)
        link_type = conv[1] if isinstance(conv, (list, tuple)) else getattr(conv, 'last_link_type', None)
        print(f"  3. conversation.last_link_id: {link_id}")
        print(f"  4. conversation.last_link_type: {link_type}")
    
    print("\n=== Analysis ===")
    
    if not sp:
        print("❌ Story is NOT linked to any product")
        print("\nAction required:")
        print(f"  1. Go to: https://hma.cdn.com.tr/stories/{story_id}/edit")
        print(f"  2. Select a product and save")
        print(f"  3. After linking, run this script again to verify")
    elif not ap:
        print("⚠️  Story is linked but ads_products entry is MISSING!")
        print("\nThis is a sync issue. The manual linking should have created it.")
        print("Let me check if we can fix it...")
        
        # Try to create it
        product_id = sp[0] if isinstance(sp, (list, tuple)) else getattr(sp, 'product_id', None)
        sku = sp[1] if isinstance(sp, (list, tuple)) else getattr(sp, 'sku', None)
        
        if product_id:
            try:
                session.exec(
                    text("""
                        INSERT INTO ads_products(ad_id, link_type, product_id, sku, auto_linked)
                        VALUES(:aid, 'story', :pid, :sku, 0)
                        ON DUPLICATE KEY UPDATE
                            product_id = VALUES(product_id),
                            sku = VALUES(sku),
                            link_type = 'story'
                    """).bindparams(aid=story_key, pid=int(product_id), sku=(sku or None))
                )
                session.commit()
                print(f"✅ Created ads_products entry for product {product_id}!")
            except Exception as e:
                try:
                    session.exec(
                        text("""
                            INSERT OR REPLACE INTO ads_products(ad_id, link_type, product_id, sku, auto_linked)
                            VALUES(:aid, 'story', :pid, :sku, 0)
                        """).bindparams(aid=story_key, pid=int(product_id), sku=(sku or None))
                    )
                    session.commit()
                    print("✅ Created ads_products entry!")
                except Exception as e2:
                    session.rollback()
                    print(f"❌ Failed to create: {e2}")
    else:
        sp_pid = sp[0] if isinstance(sp, (list, tuple)) else getattr(sp, 'product_id', None)
        ap_pid = ap[0] if isinstance(ap, (list, tuple)) else getattr(ap, 'product_id', None)
        
        if sp_pid == ap_pid:
            print("✅ All components are correctly linked and synced!")
            print(f"   Product ID: {sp_pid}")
            print("\n✅ Conversation should work with AI replies now.")
        else:
            print(f"⚠️  Product ID mismatch: stories_products={sp_pid}, ads_products={ap_pid}")
            print("   They should match!")

