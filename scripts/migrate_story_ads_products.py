#!/usr/bin/env python3
"""
Migration script to ensure all stories linked to products in stories_products
also have corresponding entries in ads_products.

This fixes the issue where stories were linked to products but the AI reply
system couldn't find them because it queries ads_products.
"""
import os
import sys
from pathlib import Path

# Add app to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import get_session
from sqlalchemy import text

def _story_link_key(story_id: str) -> str:
    """Convert story_id to story link key format."""
    sid = str(story_id or "").strip()
    if not sid:
        return ""
    return sid if sid.startswith("story:") else f"story:{sid}"


def main():
    print("=== Story -> Product Link Migration ===\n")
    print("Fixing missing ads_products entries for stories...\n")
    
    fixed_count = 0
    skipped_count = 0
    error_count = 0
    errors = []
    
    with get_session() as session:
        # Find all stories that are linked to products
        print("1. Finding all stories linked to products...")
        stories = session.exec(
            text("""
                SELECT story_id, product_id, sku, auto_linked
                FROM stories_products
                WHERE product_id IS NOT NULL
                ORDER BY story_id
            """)
        ).all()
        
        total_stories = len(stories) if stories else 0
        print(f"   Found {total_stories} story-product links\n")
        
        if total_stories == 0:
            print("No stories to process.")
            return
        
        print("2. Checking and fixing ads_products entries...\n")
        
        for story_row in stories:
            try:
                story_id = story_row[0] if isinstance(story_row, (list, tuple)) else getattr(story_row, 'story_id', None)
                product_id = story_row[1] if isinstance(story_row, (list, tuple)) else getattr(story_row, 'product_id', None)
                sku = story_row[2] if isinstance(story_row, (list, tuple)) else getattr(story_row, 'sku', None)
                auto_linked = story_row[3] if isinstance(story_row, (list, tuple)) else getattr(story_row, 'auto_linked', None)
                
                if not story_id or not product_id:
                    skipped_count += 1
                    continue
                
                story_key = _story_link_key(str(story_id))
                if not story_key:
                    skipped_count += 1
                    continue
                
                # Check if ads_products entry exists
                existing = session.exec(
                    text("""
                        SELECT product_id FROM ads_products
                        WHERE ad_id = :aid AND link_type = 'story'
                        LIMIT 1
                    """).bindparams(aid=str(story_key))
                ).first()
                
                if existing:
                    existing_pid = existing[0] if isinstance(existing, (list, tuple)) else getattr(existing, 'product_id', None)
                    # Check if product_id matches
                    if existing_pid == product_id:
                        # Already exists and matches - skip
                        skipped_count += 1
                        continue
                    else:
                        # Exists but product_id doesn't match - update it
                        print(f"   Updating story {story_id}: product_id {existing_pid} -> {product_id}")
                        try:
                            session.exec(
                                text("""
                                    UPDATE ads_products
                                    SET product_id = :pid, sku = :sku, auto_linked = :auto
                                    WHERE ad_id = :aid AND link_type = 'story'
                                """).bindparams(
                                    aid=str(story_key),
                                    pid=int(product_id),
                                    sku=(sku or None),
                                    auto=(1 if auto_linked else 0)
                                )
                            )
                            session.commit()
                            fixed_count += 1
                        except Exception as e:
                            session.rollback()
                            error_count += 1
                            error_msg = f"Story {story_id}: Update failed - {str(e)[:100]}"
                            errors.append(error_msg)
                            print(f"   ❌ {error_msg}")
                else:
                    # Missing - create it
                    print(f"   Creating ads_products entry for story {story_id} -> product {product_id}")
                    try:
                        # First ensure ads entry exists
                        session.exec(
                            text("""
                                INSERT INTO ads(ad_id, link_type, name, image_url, link, updated_at)
                                VALUES(:id, 'story', :name, NULL, NULL, CURRENT_TIMESTAMP)
                                ON DUPLICATE KEY UPDATE
                                    link_type = 'story',
                                    updated_at = CURRENT_TIMESTAMP
                            """).bindparams(
                                id=str(story_key),
                                name=f"Story {story_id}"
                            )
                        )
                    except Exception:
                        try:
                            session.exec(
                                text("""
                                    INSERT OR IGNORE INTO ads(ad_id, link_type, name, image_url, link, updated_at)
                                    VALUES(:id, 'story', :name, NULL, NULL, CURRENT_TIMESTAMP)
                                """).bindparams(
                                    id=str(story_key),
                                    name=f"Story {story_id}"
                                )
                            )
                        except Exception:
                            pass  # Ads entry might already exist, continue anyway
                    
                    # Now create ads_products entry
                    try:
                        session.exec(
                            text("""
                                INSERT INTO ads_products(ad_id, link_type, product_id, sku, auto_linked)
                                VALUES(:aid, 'story', :pid, :sku, :auto)
                                ON DUPLICATE KEY UPDATE
                                    product_id = VALUES(product_id),
                                    sku = VALUES(sku),
                                    link_type = 'story',
                                    auto_linked = VALUES(auto_linked)
                            """).bindparams(
                                aid=str(story_key),
                                pid=int(product_id),
                                sku=(sku or None),
                                auto=(1 if auto_linked else 0)
                            )
                        )
                        session.commit()
                        fixed_count += 1
                    except Exception:
                        try:
                            session.exec(
                                text("""
                                    INSERT OR REPLACE INTO ads_products(ad_id, link_type, product_id, sku, auto_linked)
                                    VALUES(:aid, 'story', :pid, :sku, :auto)
                                """).bindparams(
                                    aid=str(story_key),
                                    pid=int(product_id),
                                    sku=(sku or None),
                                    auto=(1 if auto_linked else 0)
                                )
                            )
                            session.commit()
                            fixed_count += 1
                        except Exception as e:
                            session.rollback()
                            error_count += 1
                            error_msg = f"Story {story_id}: Insert failed - {str(e)[:100]}"
                            errors.append(error_msg)
                            print(f"   ❌ {error_msg}")
                
            except Exception as e:
                error_count += 1
                error_msg = f"Story {story_id if 'story_id' in locals() else 'unknown'}: Error - {str(e)[:100]}"
                errors.append(error_msg)
                print(f"   ❌ {error_msg}")
                session.rollback()
        
        print("\n=== Migration Summary ===")
        print(f"Total stories processed: {total_stories}")
        print(f"✅ Fixed/Created: {fixed_count}")
        print(f"⏭️  Skipped (already correct): {skipped_count}")
        print(f"❌ Errors: {error_count}")
        
        if errors:
            print(f"\nErrors encountered ({len(errors)}):")
            for err in errors[:10]:  # Show first 10 errors
                print(f"  - {err}")
            if len(errors) > 10:
                print(f"  ... and {len(errors) - 10} more")
        
        print("\n✅ Migration complete!")
        
        if fixed_count > 0:
            print(f"\n💡 Note: {fixed_count} conversations with these stories should now work with AI replies.")


if __name__ == "__main__":
    main()

