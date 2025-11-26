#!/usr/bin/env python3
"""
Script to manually trigger auto-linking for a story that hasn't been linked yet.
Useful when auto-link failed or needs to be retried.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from app.services.ingest import _auto_link_story_reply
from sqlalchemy import text

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 trigger_story_autolink.py <story_id> [message_id]")
        print("\nExample:")
        print("  python3 trigger_story_autolink.py 18120589918539447 38254")
        sys.exit(1)
    
    story_id = sys.argv[1]
    message_id = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    print(f"=== Triggering Auto-Link for Story {story_id} ===\n")
    
    with get_session() as session:
        # Check if already linked
        existing = session.exec(
            text("SELECT product_id FROM stories_products WHERE story_id = :sid").bindparams(sid=story_id)
        ).first()
        
        if existing:
            pid = existing[0] if isinstance(existing, (list, tuple)) else getattr(existing, 'product_id', None)
            if pid:
                print(f"⚠️  Story {story_id} is already linked to product {pid}")
                print("   Use the migration script or manual linking if you need to update it.")
                return
        
        # Find message with this story_id
        if not message_id:
            msg = session.exec(
                text("""
                    SELECT id, story_id, story_url, text
                    FROM message
                    WHERE story_id = :sid
                    ORDER BY timestamp_ms DESC
                    LIMIT 1
                """).bindparams(sid=story_id)
            ).first()
            
            if msg:
                message_id = msg[0] if isinstance(msg, (list, tuple)) else getattr(msg, 'id', None)
                story_url = msg[2] if isinstance(msg, (list, tuple)) else getattr(msg, 'story_url', None)
                message_text = msg[3] if isinstance(msg, (list, tuple)) else getattr(msg, 'text', None)
                print(f"Found message {message_id} with story {story_id}")
            else:
                print(f"❌ No message found with story_id {story_id}")
                print("   Please provide a message_id manually.")
                return
        else:
            # Get message details
            msg = session.exec(
                text("SELECT story_id, story_url, text FROM message WHERE id = :mid").bindparams(mid=message_id)
            ).first()
            
            if not msg:
                print(f"❌ Message {message_id} not found")
                return
            
            story_url = msg[1] if isinstance(msg, (list, tuple)) else getattr(msg, 'story_url', None)
            message_text = msg[2] if isinstance(msg, (list, tuple)) else getattr(msg, 'text', None)
        
        print(f"\nAttempting to auto-link story {story_id}...")
        print(f"Message ID: {message_id}")
        print(f"Story URL: {story_url[:80] if story_url else 'None'}...")
        print()
        
        # Trigger auto-link
        try:
            _auto_link_story_reply(int(message_id), story_id, story_url, message_text)
            
            # Check if it worked
            result = session.exec(
                text("SELECT product_id, confidence FROM stories_products WHERE story_id = :sid").bindparams(sid=story_id)
            ).first()
            
            if result:
                pid = result[0] if isinstance(result, (list, tuple)) else getattr(result, 'product_id', None)
                conf = result[1] if isinstance(result, (list, tuple)) else getattr(result, 'confidence', None)
                print(f"✅ Success! Story linked to product {pid}")
                if conf:
                    print(f"   Confidence: {conf}")
            else:
                print("❌ Auto-link completed but no product was linked.")
                print("   This might mean:")
                print("   - AI couldn't match the story to any product")
                print("   - Confidence was below the threshold (0.7)")
                print("   - No product images match the story")
                print("\n   You may need to manually link the story via the UI.")
                
        except Exception as e:
            print(f"❌ Error during auto-link: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()

