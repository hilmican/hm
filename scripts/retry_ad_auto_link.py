#!/usr/bin/env python3
"""Script to retry auto-linking for ads that weren't linked, using referral_json data."""

import sys
import os
from pathlib import Path
import json

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from sqlalchemy import text
from app.services.ingest import _auto_link_ad

def retry_ad_linking(ad_id: str = None, conversation_id: int = None, dry_run: bool = False):
    """Retry auto-linking for ads that weren't linked."""
    with get_session() as session:
        if ad_id:
            # Link specific ad
            query = """
                SELECT m.ad_id, m.ad_title, m.ad_name, m.referral_json
                FROM message m
                WHERE m.ad_id = :ad_id
                ORDER BY m.timestamp_ms DESC
                LIMIT 1
            """
            params = {"ad_id": str(ad_id)}
        elif conversation_id:
            # Link ads from specific conversation
            query = """
                SELECT m.ad_id, m.ad_title, m.ad_name, m.referral_json
                FROM message m
                WHERE m.conversation_id = :cid AND m.ad_id IS NOT NULL
                ORDER BY m.timestamp_ms DESC
            """
            params = {"cid": int(conversation_id)}
        else:
            # Find all unlinked ads with referral_json
            query = """
                SELECT DISTINCT m.ad_id, m.ad_title, m.ad_name, m.referral_json
                FROM message m
                LEFT JOIN ads_products ap ON ap.ad_id = m.ad_id
                WHERE m.ad_id IS NOT NULL
                  AND m.referral_json IS NOT NULL
                  AND ap.product_id IS NULL
                ORDER BY m.timestamp_ms DESC
                LIMIT 100
            """
            params = {}
        
        messages = session.exec(text(query).params(**params)).all()
        
        if not messages:
            print("No messages found")
            return
        
        print(f"Found {len(messages)} message(s) to process")
        print()
        
        linked_count = 0
        skipped_count = 0
        
        for msg in messages:
            ad_id_val = msg.ad_id if hasattr(msg, "ad_id") else msg[0]
            ad_title = msg.ad_title if hasattr(msg, "ad_title") else msg[1]
            ad_name = msg.ad_name if hasattr(msg, "ad_name") else msg[2]
            referral_json = msg.referral_json if hasattr(msg, "referral_json") else msg[3]
            
            if not ad_id_val:
                continue
            
            # Extract better ad_title from referral_json (same logic as fixed code)
            ad_title_final = ad_title
            if referral_json:
                try:
                    ref_data = json.loads(referral_json) if isinstance(referral_json, str) else referral_json
                    if isinstance(ref_data, dict):
                        ads_ctx = ref_data.get("ads_context_data") or {}
                        if isinstance(ads_ctx, dict):
                            ctx_title = ads_ctx.get("ad_title")
                            if ctx_title and ctx_title.strip() and ctx_title.strip().upper() not in ("ADS", "AD", "ADVERTISEMENT"):
                                ad_title_final = ctx_title
                        if not ad_title_final or ad_title_final.strip().upper() in ("ADS", "AD", "ADVERTISEMENT"):
                            ad_title_final = ref_data.get("ad_title") or ref_data.get("headline") or ref_data.get("source") or ad_title_final
                except Exception as e:
                    print(f"  ⚠️  Error parsing referral_json for ad {ad_id_val}: {e}")
            
            print(f"Ad ID: {ad_id_val}")
            print(f"  Original ad_title: {ad_title}")
            print(f"  Final ad_title: {ad_title_final}")
            print(f"  ad_name: {ad_name}")
            
            if not ad_title_final and not ad_name:
                print(f"  ⚠️  Skipping: no title or name available")
                skipped_count += 1
                print()
                continue
            
            if dry_run:
                print(f"  [DRY RUN] Would call _auto_link_ad with ad_title='{ad_title_final}', ad_name='{ad_name}'")
            else:
                try:
                    _auto_link_ad(session, str(ad_id_val), ad_title_final, ad_name)
                    # Check if it was linked
                    linked = session.exec(
                        text("SELECT product_id FROM ads_products WHERE ad_id=:id").params(id=str(ad_id_val))
                    ).first()
                    if linked:
                        prod_id = linked.product_id if hasattr(linked, "product_id") else linked[0]
                        print(f"  ✓ Linked to product_id: {prod_id}")
                        linked_count += 1
                    else:
                        print(f"  ⚠️  Auto-link was called but no product was linked (low confidence?)")
                        skipped_count += 1
                except Exception as e:
                    print(f"  ❌ Error: {e}")
                    skipped_count += 1
            
            print()
        
        print(f"Summary: {linked_count} linked, {skipped_count} skipped")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Retry auto-linking for ads")
    parser.add_argument("--ad-id", help="Specific ad ID to link")
    parser.add_argument("--conversation-id", type=int, help="Conversation ID to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually link, just show what would be done")
    args = parser.parse_args()
    
    try:
        retry_ad_linking(
            ad_id=args.ad_id,
            conversation_id=args.conversation_id,
            dry_run=args.dry_run
        )
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

