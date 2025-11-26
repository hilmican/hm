#!/usr/bin/env python3
"""
Fix script for conversation 3120 - ensures story is properly linked to product.
"""
import os
import sys
from pathlib import Path

# Add app to path
sys.path.insert(0, str(Path(__file__).parent))

from app.db import get_session
from sqlalchemy import text

def main():
    conversation_id = 3120
    story_id = "18120589918539447"
    story_key = f"story:{story_id}"
    
    print(f"=== Fixing Conversation {conversation_id} ===\n")
    
    with get_session() as session:
        # 1. Get conversation details
        conv = session.exec(
            text("""
                SELECT id, last_link_id, last_link_type
                FROM conversations 
                WHERE id = :cid
            """).bindparams(cid=conversation_id)
        ).first()
        
        if not conv:
            print(f"❌ Conversation {conversation_id} not found!")
            return
        
        conv_id = conv[0] if isinstance(conv, (list, tuple)) else getattr(conv, 'id', None)
        last_link_id = conv[1] if isinstance(conv, (list, tuple)) else getattr(conv, 'last_link_id', None)
        last_link_type = conv[2] if isinstance(conv, (list, tuple)) else getattr(conv, 'last_link_type', None)
        
        print(f"Current state:")
        print(f"  last_link_id: {last_link_id}")
        print(f"  last_link_type: {last_link_type}")
        print()
        
        # 2. Check stories_products
        story_product = session.exec(
            text("""
                SELECT story_id, product_id, sku
                FROM stories_products
                WHERE story_id = :sid
            """).bindparams(sid=story_id)
        ).first()
        
        if not story_product:
            print(f"❌ Story {story_id} is NOT linked to any product in stories_products!")
            print("   You need to manually link the story to a product first.")
            print("   Go to: https://hma.cdn.com.tr/stories/{story_id}/edit")
            return
        
        sp_story_id = story_product[0] if isinstance(story_product, (list, tuple)) else getattr(story_product, 'story_id', None)
        sp_product_id = story_product[1] if isinstance(story_product, (list, tuple)) else getattr(story_product, 'product_id', None)
        sp_sku = story_product[2] if isinstance(story_product, (list, tuple)) else getattr(story_product, 'sku', None)
        
        print(f"✅ Story {sp_story_id} is linked to Product ID: {sp_product_id}")
        if sp_sku:
            print(f"   SKU: {sp_sku}")
        print()
        
        # 3. Check/ensure ads_products entry exists
        ad_product = session.exec(
            text("""
                SELECT ad_id, link_type, product_id, sku
                FROM ads_products
                WHERE ad_id = :ad_id AND link_type = 'story'
            """).bindparams(ad_id=story_key)
        ).first()
        
        if not ad_product:
            print(f"⚠️  Missing ads_products entry for {story_key}")
            print(f"   Creating entry...")
            
            try:
                session.exec(
                    text("""
                        INSERT INTO ads_products(ad_id, link_type, product_id, sku, auto_linked)
                        VALUES(:ad_id, 'story', :product_id, :sku, 1)
                    """).bindparams(
                        ad_id=story_key,
                        product_id=int(sp_product_id),
                        sku=sp_sku
                    )
                )
                session.commit()
                print(f"✅ Created ads_products entry")
            except Exception as e:
                print(f"❌ Failed to create ads_products entry: {e}")
                session.rollback()
                # Try UPDATE instead
                try:
                    session.exec(
                        text("""
                            INSERT INTO ads_products(ad_id, link_type, product_id, sku, auto_linked)
                            VALUES(:ad_id, 'story', :product_id, :sku, 1)
                            ON DUPLICATE KEY UPDATE
                                product_id = VALUES(product_id),
                                sku = VALUES(sku),
                                link_type = 'story',
                                auto_linked = 1
                        """).bindparams(
                            ad_id=story_key,
                            product_id=int(sp_product_id),
                            sku=sp_sku
                        )
                    )
                    session.commit()
                    print(f"✅ Created/Updated ads_products entry (MySQL)")
                except Exception as e2:
                    print(f"❌ Still failed: {e2}")
                    return
        else:
            print(f"✅ ads_products entry already exists")
        print()
        
        # 4. Ensure conversation has correct last_link_id and last_link_type
        needs_update = False
        if last_link_id != story_key:
            print(f"⚠️  Conversation last_link_id mismatch: {last_link_id} != {story_key}")
            needs_update = True
        if last_link_type != 'story':
            print(f"⚠️  Conversation last_link_type mismatch: {last_link_type} != 'story'")
            needs_update = True
        
        if needs_update:
            print(f"   Updating conversation...")
            try:
                session.exec(
                    text("""
                        UPDATE conversations
                        SET last_link_id = :link_id,
                            last_link_type = :link_type
                        WHERE id = :cid
                    """).bindparams(
                        link_id=story_key,
                        link_type='story',
                        cid=conversation_id
                    )
                )
                session.commit()
                print(f"✅ Updated conversation last_link_id and last_link_type")
            except Exception as e:
                print(f"❌ Failed to update conversation: {e}")
                return
        else:
            print(f"✅ Conversation already has correct last_link_id and last_link_type")
        print()
        
        # 5. Verify the fix
        print("=== Verifying Fix ===")
        verify_conv = session.exec(
            text("""
                SELECT c.last_link_id, c.last_link_type
                FROM conversations c
                WHERE c.id = :cid
            """).bindparams(cid=conversation_id)
        ).first()
        
        verify_ad = session.exec(
            text("""
                SELECT ap.product_id, p.name, p.slug
                FROM ads_products ap
                LEFT JOIN product p ON ap.product_id = p.id
                WHERE ap.ad_id = :aid AND ap.link_type = 'story'
            """).bindparams(aid=story_key)
        ).first()
        
        if verify_conv and verify_ad:
            vc_link_id = verify_conv[0] if isinstance(verify_conv, (list, tuple)) else getattr(verify_conv, 'last_link_id', None)
            vc_link_type = verify_conv[1] if isinstance(verify_conv, (list, tuple)) else getattr(verify_conv, 'last_link_type', None)
            
            vp_product_id = verify_ad[0] if isinstance(verify_ad, (list, tuple)) else getattr(verify_ad, 'product_id', None)
            vp_name = verify_ad[1] if isinstance(verify_ad, (list, tuple)) else getattr(verify_ad, 'name', None)
            vp_slug = verify_ad[2] if isinstance(verify_ad, (list, tuple)) else getattr(verify_ad, 'slug', None)
            
            print(f"✅ Verification successful!")
            print(f"   Conversation link: {vc_link_id} ({vc_link_type})")
            print(f"   Product: {vp_name} (ID: {vp_product_id}, Slug: {vp_slug})")
            print()
            print("The conversation should now be linked to the product.")
            print("Try refreshing the AI reply or triggering a new reply.")
        else:
            print(f"❌ Verification failed - something is still wrong")

if __name__ == "__main__":
    main()

