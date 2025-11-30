#!/usr/bin/env python3
"""Diagnose why conversation 4115 is not marked as placed by AI order detection."""

import os
import sys
import json
import datetime as dt

# Add the project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.db import get_session
from app.models import Conversation, Message, AiOrderCandidate, IGUser
from sqlmodel import select
from sqlalchemy import func

def format_datetime(dt_obj):
    if dt_obj is None:
        return None
    return dt_obj.isoformat()

def format_timestamp_ms(ts_ms):
    if ts_ms is None:
        return None
    try:
        dt_obj = dt.datetime.utcfromtimestamp(ts_ms / 1000.0)
        return dt_obj.isoformat()
    except Exception:
        return str(ts_ms)

def main():
    conversation_id = 4115
    message_id_to_check = "17841469611452957"
    
    print(f"=" * 80)
    print(f"Diagnosing Conversation {conversation_id}")
    print(f"=" * 80)
    print()
    
    with get_session() as session:
        # Check conversation exists
        conversation = session.exec(
            select(Conversation).where(Conversation.id == conversation_id)
        ).first()
        
        if not conversation:
            print(f"❌ ERROR: Conversation {conversation_id} not found!")
            return
        
        print(f"✅ Conversation {conversation_id} found")
        print(f"   - IG User ID: {conversation.ig_user_id}")
        print(f"   - Graph Conversation ID: {conversation.graph_conversation_id}")
        print(f"   - Last Message At: {format_datetime(conversation.last_message_at)}")
        print()
        
        # Check IG User
        if conversation.ig_user_id:
            ig_user = session.exec(
                select(IGUser).where(IGUser.id == conversation.ig_user_id)
            ).first()
            if ig_user:
                print(f"✅ IG User found: @{ig_user.username} ({ig_user.contact_name})")
            else:
                print(f"⚠️  IG User ID {conversation.ig_user_id} not found")
            print()
        
        # Check AiOrderCandidate
        candidate = session.exec(
            select(AiOrderCandidate).where(AiOrderCandidate.conversation_id == conversation_id)
        ).first()
        
        if candidate:
            print(f"✅ AiOrderCandidate exists:")
            print(f"   - Status: {candidate.status}")
            print(f"   - Status Reason: {candidate.status_reason}")
            print(f"   - Placed At: {format_datetime(candidate.placed_at)}")
            print(f"   - Last Status At: {format_datetime(candidate.last_status_at)}")
            print(f"   - Created At: {format_datetime(candidate.created_at)}")
            print(f"   - Updated At: {format_datetime(candidate.updated_at)}")
            
            if candidate.order_payload_json:
                try:
                    payload = json.loads(candidate.order_payload_json)
                    print(f"   - Order Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")
                except Exception as e:
                    print(f"   - Order Payload (parse error): {e}")
            
            if candidate.status_history_json:
                try:
                    history = json.loads(candidate.status_history_json)
                    print(f"   - Status History: {json.dumps(history, indent=2, ensure_ascii=False)}")
                except Exception as e:
                    print(f"   - Status History (parse error): {e}")
            
            if candidate.status != "placed":
                print(f"\n❌ ISSUE: Status is '{candidate.status}', not 'placed'!")
        else:
            print(f"❌ ISSUE: No AiOrderCandidate exists for conversation {conversation_id}")
        print()
        
        # Check messages - especially the order confirmation message
        print(f"Checking messages...")
        messages = session.exec(
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.timestamp_ms.asc())
        ).all()
        
        print(f"   Total messages: {len(messages)}")
        
        # Find the specific order confirmation message
        order_confirmation = None
        for msg in messages:
            if str(msg.ig_message_id) == message_id_to_check:
                order_confirmation = msg
                break
        
        if order_confirmation:
            print(f"\n✅ Found order confirmation message {message_id_to_check}:")
            print(f"   - Direction: {order_confirmation.direction}")
            print(f"   - Timestamp: {format_timestamp_ms(order_confirmation.timestamp_ms)}")
            print(f"   - Text: {order_confirmation.text[:200]}...")
            print()
        
        # Show last 10 messages
        print(f"Last 10 messages:")
        for msg in messages[-10:]:
            direction_marker = "➡️" if msg.direction == "out" else "⬅️"
            timestamp = format_timestamp_ms(msg.timestamp_ms)
            text_preview = (msg.text or "")[:100]
            msg_id_marker = " ⭐ ORDER CONFIRMATION" if str(msg.ig_message_id) == message_id_to_check else ""
            print(f"   {direction_marker} [{timestamp}] {text_preview}{msg_id_marker}")
        print()
        
        # Check message timestamps for date range filtering
        if messages:
            first_msg_ts = messages[0].timestamp_ms
            last_msg_ts = messages[-1].timestamp_ms
            first_dt = format_timestamp_ms(first_msg_ts)
            last_dt = format_timestamp_ms(last_msg_ts)
            
            print(f"Message timestamp range:")
            print(f"   - First message: {first_dt} (ms: {first_msg_ts})")
            print(f"   - Last message: {last_dt} (ms: {last_msg_ts})")
            print()
            
            # Check if this conversation would be picked up by detection
            # The detection filters by message timestamp_ms
            today = dt.date.today()
            seven_days_ago = today - dt.timedelta(days=7)
            
            start_dt = dt.datetime.combine(seven_days_ago, dt.time.min)
            end_dt = dt.datetime.combine(today + dt.timedelta(days=1), dt.time.min)
            start_ms = int(start_dt.timestamp() * 1000)
            end_ms = int(end_dt.timestamp() * 1000)
            
            print(f"Detection date range (last 7 days):")
            print(f"   - Start: {start_dt.isoformat()} (ms: {start_ms})")
            print(f"   - End: {end_dt.isoformat()} (ms: {end_ms})")
            
            if first_msg_ts and last_msg_ts:
                is_in_range = (first_msg_ts >= start_ms or last_msg_ts >= start_ms) and (first_msg_ts < end_ms or last_msg_ts < end_ms)
                print(f"   - Conversation in range: {is_in_range}")
            
            print()
        
        # Summary
        print(f"=" * 80)
        print("SUMMARY:")
        print(f"=" * 80)
        if candidate:
            if candidate.status == "placed":
                print(f"✅ Conversation is marked as 'placed'")
            else:
                print(f"❌ Conversation status is '{candidate.status}', should be 'placed'")
                print(f"   Recommendation: Re-run AI detection or manually update status")
        else:
            print(f"❌ No AiOrderCandidate exists")
            print(f"   Recommendation: Run AI order detection for this conversation")
        
        if order_confirmation:
            print(f"✅ Order confirmation message found: {message_id_to_check}")
            print(f"   Message text contains order details")

if __name__ == "__main__":
    main()

