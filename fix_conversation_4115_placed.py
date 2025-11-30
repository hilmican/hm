#!/usr/bin/env python3
"""Fix conversation 4115 to be marked as placed by AI order detection."""

import sys
import os

# Add the project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.db import get_session
from app.models import Message, Conversation, AiOrderCandidate
from app.services.ai_orders import _update_candidate
from app.services.ai_orders_detection import analyze_conversation_for_order_candidate
from sqlmodel import select
import datetime as dt
import json

def main():
    conversation_id = 4115
    
    print(f"Fixing conversation {conversation_id}...")
    print("=" * 80)
    
    with get_session() as session:
        # Verify conversation exists
        conversation = session.exec(
            select(Conversation).where(Conversation.id == conversation_id)
        ).first()
        
        if not conversation:
            print(f"❌ ERROR: Conversation {conversation_id} not found!")
            return
        
        print(f"✅ Conversation {conversation_id} found")
        
        # Check if order confirmation message exists
        order_msgs = session.exec(
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .where(Message.text.like('%Siparişinizi aldığımız%'))
            .order_by(Message.timestamp_ms.desc())
            .limit(1)
        ).all()
        
        if not order_msgs:
            print("❌ ERROR: Order confirmation message not found!")
            print("   Cannot automatically detect order placement.")
            return
        
        order_msg = order_msgs[0]
        order_timestamp_ms = order_msg.timestamp_ms
        order_datetime = dt.datetime.utcfromtimestamp(order_timestamp_ms / 1000.0) if order_timestamp_ms else None
        
        print(f"✅ Order confirmation found:")
        print(f"   - Timestamp: {order_datetime}")
        print(f"   - Message: {order_msg.text[:100]}...")
        print()
        
        # Re-analyze the conversation with AI to get updated status
        print("Re-analyzing conversation with AI...")
        try:
            result = analyze_conversation_for_order_candidate(conversation_id)
            new_status = result.get("status", "interested")
            
            print(f"✅ AI Analysis complete:")
            print(f"   - Detected status: {new_status}")
            
            if new_status != "placed":
                print(f"   ⚠️  WARNING: AI detected status as '{new_status}', not 'placed'")
                print(f"   This might be because:")
                print(f"   - The order confirmation message format wasn't recognized")
                print(f"   - Other messages in the conversation confused the AI")
                print()
                print("Proceeding to manually mark as 'placed' based on order confirmation message...")
                new_status = "placed"
            
        except Exception as e:
            print(f"⚠️  AI analysis failed: {e}")
            print("Proceeding to manually mark as 'placed' based on order confirmation message...")
            new_status = "placed"
        
        print()
        
        # Build order payload
        order_payload = result if 'result' in locals() else {}
        
        # Update the candidate to "placed"
        print(f"Updating AiOrderCandidate status to '{new_status}'...")
        
        status_reason = "Order confirmed via message (re-detected after order placement)"
        if 'result' in locals() and result.get("notes"):
            status_reason = result.get("notes")
        
        _update_candidate(
            conversation_id=conversation_id,
            status=new_status,
            note=status_reason,
            payload=order_payload if isinstance(order_payload, dict) else {},
            mark_placed=(new_status == "placed"),
        )
        
        print(f"✅ Successfully updated conversation {conversation_id} to status '{new_status}'")
        
        # Verify the update
        updated_candidate = session.exec(
            select(AiOrderCandidate).where(AiOrderCandidate.conversation_id == conversation_id)
        ).first()
        
        if updated_candidate:
            print()
            print("Verification:")
            print(f"   - Status: {updated_candidate.status}")
            print(f"   - Placed At: {updated_candidate.placed_at}")
            print(f"   - Updated At: {updated_candidate.updated_at}")
        
        print()
        print("=" * 80)
        print("✅ Done! Conversation 4115 is now marked as 'placed'")

if __name__ == "__main__":
    main()

