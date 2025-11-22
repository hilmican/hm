#!/usr/bin/env python3
"""Diagnostic script to check why ad-to-product auto-linking didn't work for a conversation."""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from sqlalchemy import text
import json

def diagnose_conversation(conversation_id: int):
    """Check conversation messages and ad data."""
    with get_session() as session:
        # Get conversation info
        conv = session.exec(
            text("SELECT id, last_ad_id, last_ad_title, last_ad_link FROM conversations WHERE id=:id").params(id=conversation_id)
        ).first()
        
        if not conv:
            print(f"❌ Conversation {conversation_id} not found")
            return
        
        conv_id = conv.id if hasattr(conv, "id") else conv[0]
        last_ad_id = conv.last_ad_id if hasattr(conv, "last_ad_id") else (conv[1] if len(conv) > 1 else None)
        last_ad_title = conv.last_ad_title if hasattr(conv, "last_ad_title") else (conv[2] if len(conv) > 2 else None)
        last_ad_link = conv.last_ad_link if hasattr(conv, "last_ad_link") else (conv[3] if len(conv) > 3 else None)
        
        print(f"📋 Conversation {conv_id}:")
        print(f"   Last ad_id: {last_ad_id}")
        print(f"   Last ad_title: {last_ad_title}")
        print(f"   Last ad_link: {last_ad_link}")
        print()
        
        # Get all messages with ad data
        messages = session.exec(
            text("""
                SELECT id, ig_message_id, ad_id, ad_title, ad_name, ad_link, referral_json, text, timestamp_ms
                FROM message
                WHERE conversation_id=:cid AND ad_id IS NOT NULL
                ORDER BY timestamp_ms DESC
            """).params(cid=conversation_id)
        ).all()
        
        if not messages:
            print("⚠️  No messages with ad_id found in this conversation")
            # Check if there are any messages at all
            any_msg = session.exec(
                text("SELECT COUNT(*) FROM message WHERE conversation_id=:cid").params(cid=conversation_id)
            ).first()
            count = any_msg[0] if isinstance(any_msg, tuple) else (getattr(any_msg, "count", None) if hasattr(any_msg, "count") else None)
            print(f"   Total messages in conversation: {count}")
            return
        
        print(f"📨 Found {len(messages)} message(s) with ad data:")
        print()
        
        for msg in messages:
            msg_id = msg.id if hasattr(msg, "id") else msg[0]
            ig_msg_id = msg.ig_message_id if hasattr(msg, "ig_message_id") else msg[1]
            ad_id = msg.ad_id if hasattr(msg, "ad_id") else msg[2]
            ad_title = msg.ad_title if hasattr(msg, "ad_title") else msg[3]
            ad_name = msg.ad_name if hasattr(msg, "ad_name") else msg[4]
            ad_link = msg.ad_link if hasattr(msg, "ad_link") else msg[5]
            referral_json = msg.referral_json if hasattr(msg, "referral_json") else msg[6]
            msg_text = msg.text if hasattr(msg, "text") else msg[7]
            ts_ms = msg.timestamp_ms if hasattr(msg, "timestamp_ms") else msg[8]
            
            print(f"   Message ID: {msg_id} (IG: {ig_msg_id})")
            print(f"   Timestamp: {ts_ms}")
            print(f"   Text: {msg_text[:100] if msg_text else '(empty)'}")
            print(f"   ad_id: {ad_id}")
            print(f"   ad_title: {ad_title}")
            print(f"   ad_name: {ad_name}")
            print(f"   ad_link: {ad_link}")
            
            # Check referral_json for additional ad_title
            ad_title_from_json = None
            if referral_json:
                try:
                    ref_data = json.loads(referral_json) if isinstance(referral_json, str) else referral_json
                    if isinstance(ref_data, dict):
                        ads_ctx = ref_data.get("ads_context_data") or {}
                        if isinstance(ads_ctx, dict):
                            ad_title_from_json = ads_ctx.get("ad_title")
                        if not ad_title_from_json:
                            ad_title_from_json = ref_data.get("ad_title") or ref_data.get("headline") or ref_data.get("source")
                except Exception as e:
                    print(f"   ⚠️  Error parsing referral_json: {e}")
            
            if ad_title_from_json:
                print(f"   ad_title (from referral_json): {ad_title_from_json}")
            
            # Determine what would be passed to _auto_link_ad
            ad_title_final = ad_title or ad_title_from_json
            ad_text = ad_title_final or ad_name
            
            print(f"   → ad_title_final (for auto-link): {ad_title_final}")
            print(f"   → ad_text (for auto-link): {ad_text}")
            
            if not ad_text:
                print(f"   ❌ PROBLEM: No ad_title or ad_name available - auto-link would be skipped!")
            else:
                print(f"   ✓ ad_text available: '{ad_text}'")
            
            # Check if ad is already linked
            if ad_id:
                linked = session.exec(
                    text("SELECT ad_id, product_id, auto_linked FROM ads_products WHERE ad_id=:id").params(id=str(ad_id))
                ).first()
                
                if linked:
                    prod_id = linked.product_id if hasattr(linked, "product_id") else (linked[1] if len(linked) > 1 else None)
                    auto_linked = linked.auto_linked if hasattr(linked, "auto_linked") else (linked[2] if len(linked) > 2 else None)
                    print(f"   📎 Already linked to product_id: {prod_id} (auto_linked: {auto_linked})")
                else:
                    print(f"   ❌ NOT linked to any product")
            
            print()
        
        # Check AI client status
        print("🤖 AI Client Status:")
        try:
            from app.services.ai import AIClient, get_ai_model_from_settings
            model = get_ai_model_from_settings()
            ai = AIClient(model=model)
            if ai.enabled:
                print(f"   ✓ AI is enabled (model: {ai.model})")
            else:
                print(f"   ❌ AI is NOT enabled (OPENAI_API_KEY not set or OpenAI not available)")
        except Exception as e:
            print(f"   ❌ Error checking AI client: {e}")
        
        print()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python diagnose_ad_linking.py <conversation_id>")
        sys.exit(1)
    
    try:
        conv_id = int(sys.argv[1])
        diagnose_conversation(conv_id)
    except ValueError:
        print(f"Error: conversation_id must be a number, got: {sys.argv[1]}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

