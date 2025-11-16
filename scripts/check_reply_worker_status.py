#!/usr/bin/env python3
"""Diagnostic script to check why reply-worker isn't generating replies for a conversation."""
import sys
from app.db import get_session
from sqlalchemy import text as _text

def check_conversation(conversation_id: int):
    with get_session() as session:
        # Check conversation
        conv = session.exec(
            _text("""
                SELECT id, last_ad_id, igba_id, ig_user_id, last_message_at, last_message_timestamp_ms
                FROM conversations
                WHERE id = :cid
            """).params(cid=conversation_id)
        ).first()
        
        if not conv:
            print(f"❌ Conversation {conversation_id} not found")
            return
        
        print(f"✅ Conversation {conversation_id} found")
        last_ad_id = conv[1] if len(conv) > 1 else None
        print(f"   last_ad_id: {last_ad_id}")
        
        # Check if ad is linked to product
        if last_ad_id:
            ad_product = session.exec(
                _text("""
                    SELECT ap.ad_id, ap.product_id, p.name as product_name
                    FROM ads_products ap
                    LEFT JOIN product p ON ap.product_id = p.id
                    WHERE ap.ad_id = :ad_id
                """).params(ad_id=str(last_ad_id))
            ).first()
            
            if ad_product:
                product_id = ad_product[1] if len(ad_product) > 1 else None
                product_name = ad_product[2] if len(ad_product) > 2 else None
                print(f"✅ Ad {last_ad_id} is linked to product: {product_id} ({product_name})")
            else:
                print(f"❌ Ad {last_ad_id} is NOT linked to any product")
                print(f"   → This is why reply-worker isn't processing this conversation")
                print(f"   → Solution: Link the ad to a product via /ads/{last_ad_id}/edit")
        else:
            print(f"❌ Conversation has no last_ad_id")
            print(f"   → This is why reply-worker isn't processing this conversation")
            print(f"   → Solution: The conversation needs an ad linked to a product")
        
        # Check ai_shadow_state
        shadow = session.exec(
            _text("""
                SELECT conversation_id, status, last_inbound_ms, next_attempt_at, updated_at, postpone_count
                FROM ai_shadow_state
                WHERE conversation_id = :cid
            """).params(cid=conversation_id)
        ).first()
        
        if shadow:
            print(f"\n✅ ai_shadow_state entry exists:")
            print(f"   status: {shadow[1] if len(shadow) > 1 else 'N/A'}")
            print(f"   last_inbound_ms: {shadow[2] if len(shadow) > 2 else 'N/A'}")
            print(f"   next_attempt_at: {shadow[3] if len(shadow) > 3 else 'N/A'}")
            print(f"   updated_at: {shadow[4] if len(shadow) > 4 else 'N/A'}")
            print(f"   postpone_count: {shadow[5] if len(shadow) > 5 else 'N/A'}")
        else:
            print(f"\n❌ No ai_shadow_state entry found")
            print(f"   → This confirms why reply-worker isn't processing it")
            print(f"   → Reason: touch_shadow_state() only creates entries when ad is linked to product")
        
        # Check recent messages
        msgs = session.exec(
            _text("""
                SELECT id, ig_message_id, text, timestamp_ms, direction, ad_id
                FROM message
                WHERE conversation_id = :cid
                ORDER BY timestamp_ms DESC
                LIMIT 5
            """).params(cid=conversation_id)
        ).all()
        
        print(f"\n📨 Recent messages ({len(msgs)}):")
        for msg in msgs:
            msg_id = msg[0] if len(msg) > 0 else None
            ig_msg_id = msg[1] if len(msg) > 1 else None
            text = (msg[2] or "")[:50] if len(msg) > 2 else ""
            ts = msg[3] if len(msg) > 3 else None
            direction = msg[4] if len(msg) > 4 else None
            ad_id = msg[5] if len(msg) > 5 else None
            print(f"   [{direction}] {ts}: {text}... (ad_id: {ad_id})")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/check_reply_worker_status.py <conversation_id>")
        sys.exit(1)
    
    try:
        cid = int(sys.argv[1])
        check_conversation(cid)
    except ValueError:
        print(f"Error: Invalid conversation_id: {sys.argv[1]}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

