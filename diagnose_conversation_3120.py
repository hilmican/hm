#!/usr/bin/env python3
"""
Diagnostic script to investigate why conversation 3120 is not linked to a product.
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
    story_id_from_ui = "18120589918539447"
    story_key = f"story:{story_id_from_ui}"
    
    print(f"=== Diagnosing Conversation {conversation_id} ===\n")
    
    with get_session() as session:
        # 1. Check conversation details
        print("1. CONVERSATION DETAILS")
        print("-" * 50)
        conv = session.exec(
            text("""
                SELECT id, last_link_id, last_link_type, last_ad_id,
                       ig_sender_id, ig_recipient_id, ig_user_id
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
        last_ad_id = conv[3] if isinstance(conv, (list, tuple)) else getattr(conv, 'last_ad_id', None)
        
        print(f"  ID: {conv_id}")
        print(f"  Last Link ID: {last_link_id}")
        print(f"  Last Link Type: {last_link_type}")
        print(f"  Last Ad ID: {last_ad_id}")
        print()
        
        # 2. Check messages with story_id
        print("2. MESSAGES WITH STORY_ID")
        print("-" * 50)
        messages = session.exec(
            text("""
                SELECT id, ig_message_id, story_id, story_url, direction, text,
                       timestamp_ms
                FROM message
                WHERE conversation_id = :cid AND story_id IS NOT NULL
                ORDER BY timestamp_ms DESC
            """).bindparams(cid=conversation_id)
        ).all()
        
        if messages:
            for msg in messages:
                msg_id = msg[0] if isinstance(msg, (list, tuple)) else getattr(msg, 'id', None)
                msg_story_id = msg[2] if isinstance(msg, (list, tuple)) else getattr(msg, 'story_id', None)
                msg_story_url = msg[3] if isinstance(msg, (list, tuple)) else getattr(msg, 'story_url', None)
                msg_dir = msg[4] if isinstance(msg, (list, tuple)) else getattr(msg, 'direction', None)
                print(f"  Message ID: {msg_id}")
                print(f"    Story ID: {msg_story_id}")
                print(f"    Story URL: {msg_story_url[:80] if msg_story_url else None}...")
                print(f"    Direction: {msg_dir}")
                print()
        else:
            print("  ⚠️  No messages with story_id found!")
            print()
        
        # 3. Check stories_products table
        print("3. STORY -> PRODUCT LINK (stories_products table)")
        print("-" * 50)
        story_product = session.exec(
            text("""
                SELECT story_id, product_id, sku, auto_linked, confidence
                FROM stories_products
                WHERE story_id = :sid
            """).bindparams(sid=story_id_from_ui)
        ).first()
        
        if story_product:
            sp_story_id = story_product[0] if isinstance(story_product, (list, tuple)) else getattr(story_product, 'story_id', None)
            sp_product_id = story_product[1] if isinstance(story_product, (list, tuple)) else getattr(story_product, 'product_id', None)
            sp_sku = story_product[2] if isinstance(story_product, (list, tuple)) else getattr(story_product, 'sku', None)
            sp_auto = story_product[3] if isinstance(story_product, (list, tuple)) else getattr(story_product, 'auto_linked', None)
            print(f"  ✅ Story {sp_story_id} linked to Product ID: {sp_product_id}")
            print(f"     SKU: {sp_sku}")
            print(f"     Auto-linked: {sp_auto}")
        else:
            print(f"  ❌ Story {story_id_from_ui} NOT found in stories_products table!")
        print()
        
        # 4. Check ads_products table with story key
        print("4. AD -> PRODUCT LINK (ads_products table)")
        print("-" * 50)
        ad_product = session.exec(
            text("""
                SELECT ap.ad_id, ap.link_type, ap.product_id, ap.sku, ap.auto_linked
                FROM ads_products ap
                WHERE ap.ad_id = :ad_id AND ap.link_type = 'story'
            """).bindparams(ad_id=story_key)
        ).first()
        
        if ad_product:
            ap_ad_id = ad_product[0] if isinstance(ad_product, (list, tuple)) else getattr(ad_product, 'ad_id', None)
            ap_link_type = ad_product[1] if isinstance(ad_product, (list, tuple)) else getattr(ad_product, 'link_type', None)
            ap_product_id = ad_product[2] if isinstance(ad_product, (list, tuple)) else getattr(ad_product, 'product_id', None)
            print(f"  ✅ Ad {ap_ad_id} (type: {ap_link_type}) linked to Product ID: {ap_product_id}")
        else:
            print(f"  ❌ Ad {story_key} NOT found in ads_products table!")
            
            # Check what's actually in ads_products for this story
            all_story_ads = session.exec(
                text("""
                    SELECT ad_id, link_type, product_id
                    FROM ads_products
                    WHERE ad_id LIKE :pattern
                """).bindparams(pattern=f"%{story_id_from_ui}%")
            ).all()
            
            if all_story_ads:
                print(f"  Found similar entries:")
                for entry in all_story_ads:
                    print(f"    - {entry}")
            else:
                print(f"  No similar entries found.")
        print()
        
        # 5. Check ads table
        print("5. AD ENTRY (ads table)")
        print("-" * 50)
        ad_entry = session.exec(
            text("""
                SELECT ad_id, link_type, name, image_url, link
                FROM ads
                WHERE ad_id = :ad_id
            """).bindparams(ad_id=story_key)
        ).first()
        
        if ad_entry:
            print(f"  ✅ Ad entry found: {ad_entry[0]}")
            print(f"     Type: {ad_entry[1]}")
            print(f"     Name: {ad_entry[2]}")
        else:
            print(f"  ❌ Ad {story_key} NOT found in ads table!")
        print()
        
        # 6. Check what _detect_focus_product would find
        print("6. WHAT _detect_focus_product WOULD FIND")
        print("-" * 50)
        if last_link_id and last_link_type:
            print(f"  Checking for link_id={last_link_id}, link_type={last_link_type}")
            focus_check = session.exec(
                text("""
                    SELECT ap.sku, ap.product_id, p.slug, p.name
                    FROM ads_products ap
                    LEFT JOIN product p ON ap.product_id = p.id
                    WHERE ap.ad_id = :id AND ap.link_type = :link_type
                    LIMIT 1
                """).bindparams(id=str(last_link_id), link_type=str(last_link_type))
            ).first()
            
            if focus_check:
                print(f"  ✅ Would find product:")
                print(f"     SKU: {focus_check[0]}")
                print(f"     Product ID: {focus_check[1]}")
                print(f"     Slug: {focus_check[2]}")
                print(f"     Name: {focus_check[3]}")
            else:
                print(f"  ❌ Would NOT find product (this is the problem!)")
        else:
            print(f"  ❌ Conversation has no last_link_id/last_link_type set!")
        print()
        
        # 7. SUMMARY
        print("=== SUMMARY ===")
        print("-" * 50)
        issues = []
        
        if not last_link_id or last_link_id != story_key:
            issues.append(f"Conversation last_link_id mismatch: expected '{story_key}', got '{last_link_id}'")
        
        if not last_link_type or last_link_type != 'story':
            issues.append(f"Conversation last_link_type mismatch: expected 'story', got '{last_link_type}'")
        
        if not story_product:
            issues.append(f"Story {story_id_from_ui} not linked to product in stories_products table")
        
        if not ad_product:
            issues.append(f"Story ad {story_key} not linked to product in ads_products table")
        
        if issues:
            print("❌ ISSUES FOUND:")
            for i, issue in enumerate(issues, 1):
                print(f"  {i}. {issue}")
        else:
            print("✅ All checks passed! (but AI still not working - check other factors)")

if __name__ == "__main__":
    main()

