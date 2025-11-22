#!/usr/bin/env python3
"""Check shadow replies and AI instructions for conversations 2211 and 2212"""
import sys
sys.path.insert(0, '.')

from app.db import get_session
from sqlalchemy import text as _text
from app.models import Product

def check_conversation(conversation_id: int):
    print(f"\n{'='*60}")
    print(f"Conversation {conversation_id}")
    print(f"{'='*60}")
    
    with get_session() as session:
        # Get shadow replies
        print("\n--- Shadow Replies ---")
        rows = session.exec(
            _text(
                """
                SELECT id, conversation_id, reply_text, model, confidence, reason, 
                       status, created_at, LEFT(reply_text, 200) as preview
                FROM ai_shadow_reply 
                WHERE conversation_id = :cid 
                ORDER BY created_at DESC 
                LIMIT 5
                """
            ).params(cid=int(conversation_id))
        ).all()
        
        if not rows:
            print("No shadow replies found")
        else:
            for i, row in enumerate(rows, 1):
                row_dict = row._asdict() if hasattr(row, '_asdict') else dict(row)
                reply_text = row_dict.get('reply_text') or row_dict.get(2) or ''
                preview = row_dict.get('preview') or reply_text[:200] if reply_text else ''
                
                print(f"\nShadow Reply #{i}:")
                print(f"  ID: {row_dict.get('id') or row_dict.get(0)}")
                print(f"  Status: {row_dict.get('status') or row_dict.get(6)}")
                print(f"  Created: {row_dict.get('created_at') or row_dict.get(7)}")
                print(f"  Preview (first 200 chars): {repr(preview)}")
                print(f"  Full text length: {len(reply_text)}")
                print(f"  Contains literal \\\\n: {'\\\\\\\\n' in str(reply_text)}")
                print(f"  Contains literal \\n: {'\\\\n' in str(reply_text)}")
                print(f"  Contains actual \\n: {'\\n' in str(reply_text)}")
                
                # Show hex representation of first 100 chars to see escape sequences
                text_str = str(reply_text)[:100]
                hex_str = ' '.join(f'{ord(c):02x}' for c in text_str)
                print(f"  Hex (first 100 chars): {hex_str[:200]}")
        
        # Get product info and AI instructions
        print("\n--- Product & AI Instructions ---")
        product_row = session.exec(
            _text(
                """
                SELECT p.id, p.name, p.slug, p.ai_system_msg,
                       (SELECT product_id FROM ads_products ap 
                        INNER JOIN conversations c ON c.last_ad_id = ap.ad_id 
                        WHERE c.id = :cid LIMIT 1) as linked_product_id
                FROM conversations c
                LEFT JOIN ads_products ap ON ap.ad_id = c.last_ad_id
                LEFT JOIN product p ON p.id = ap.product_id
                WHERE c.id = :cid
                LIMIT 1
                """
            ).params(cid=int(conversation_id))
        ).first()
        
        if product_row:
            row_dict = product_row._asdict() if hasattr(product_row, '_asdict') else dict(product_row)
            product_id = row_dict.get('id') or row_dict.get(0)
            product_name = row_dict.get('name') or row_dict.get(1)
            ai_system_msg = row_dict.get('ai_system_msg') or row_dict.get(3)
            
            print(f"  Product ID: {product_id}")
            print(f"  Product Name: {product_name}")
            
            if ai_system_msg:
                print(f"  AI System Message (first 300 chars):")
                print(f"    {repr(str(ai_system_msg)[:300])}")
                print(f"  Contains literal \\\\n: {'\\\\\\\\n' in str(ai_system_msg)}")
                print(f"  Contains literal \\n: {'\\\\n' in str(ai_system_msg)}")
                print(f"  Contains actual \\n: {'\\n' in str(ai_system_msg)}")
            else:
                print("  No AI system message found")
        else:
            print("  No product found for this conversation")

if __name__ == "__main__":
    check_conversation(2211)
    check_conversation(2212)

