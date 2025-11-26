#!/bin/bash
# Simple diagnostic script to check conversation 3120

echo "=== Checking Conversation 3120 ==="
echo ""

# Try to exec into a pod and run Python to query the database
kubectl exec -n hm hm-app-9bc8c8dfc-2t8xz -- python3 << 'PYEOF'
import os
import sys
sys.path.insert(0, '/app')
from app.db import get_session
from sqlalchemy import text

conv_id = 3120
story_id = "18120589918539447"
story_key = f"story:{story_id}"

with get_session() as session:
    # Check conversation
    conv = session.exec(text("""
        SELECT id, convo_id, last_link_id, last_link_type, last_ad_id
        FROM conversations WHERE id = :cid
    """).bindparams(cid=conv_id)).first()
    
    if conv:
        print(f"Conversation {conv[0]}:")
        print(f"  last_link_id: {conv[2]}")
        print(f"  last_link_type: {conv[3]}")
        print(f"  last_ad_id: {conv[4]}")
        
        # Check ads_products
        link_id = conv[2]
        link_type = conv[3]
        
        if link_id and link_type:
            ap = session.exec(text("""
                SELECT ap.product_id, p.name, p.slug
                FROM ads_products ap
                LEFT JOIN product p ON ap.product_id = p.id
                WHERE ap.ad_id = :aid AND ap.link_type = :lt
            """).bindparams(aid=str(link_id), lt=str(link_type))).first()
            
            if ap:
                print(f"\n✅ Found product link:")
                print(f"   Product ID: {ap[0]}")
                print(f"   Product Name: {ap[1]}")
                print(f"   Product Slug: {ap[2]}")
            else:
                print(f"\n❌ NO product link found in ads_products")
                print(f"   Looking for: ad_id='{link_id}', link_type='{link_type}'")
        
        # Check stories_products
        sp = session.exec(text("""
            SELECT product_id FROM stories_products WHERE story_id = :sid
        """).bindparams(sid=story_id)).first()
        
        if sp:
            print(f"\n✅ Story {story_id} linked to product {sp[0]} in stories_products")
        else:
            print(f"\n❌ Story {story_id} NOT linked in stories_products")
    else:
        print(f"❌ Conversation {conv_id} not found!")
PYEOF

