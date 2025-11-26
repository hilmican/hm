# Migration Scripts

## migrate_story_ads_products.py

This script fixes a synchronization issue where stories linked to products in `stories_products` were missing corresponding entries in `ads_products`. This prevented the AI reply system from finding products for conversations that reference stories.

### What it does:

1. Finds all stories that are linked to products in `stories_products`
2. Checks if corresponding entries exist in `ads_products`
3. Creates missing entries or updates mismatched product_ids
4. Ensures the `ads` table has entries for all stories

### When to run:

- After deploying the fix to `_auto_link_story_reply` in `app/services/ingest.py`
- If you notice conversations with stories that show "Bağlı story" but AI can't find the product
- To fix historical data where stories were manually linked but `ads_products` entries are missing

### How to run:

#### Option 1: Run locally (if you have database access)
```bash
python3 scripts/migrate_story_ads_products.py
```

#### Option 2: Run in Kubernetes pod
```bash
# Copy script to pod
kubectl cp scripts/migrate_story_ads_products.py hm/hm-app-<pod-name>:/tmp/migrate.py

# Run in pod
kubectl exec -n hm hm-app-<pod-name> -- python3 /tmp/migrate.py
```

#### Option 3: Run in a dedicated job pod
```bash
# Get current app pod name
kubectl get pods -n hm | grep hm-app

# Copy script and run
kubectl cp scripts/migrate_story_ads_products.py hm/$(kubectl get pods -n hm -l app=hm-app -o jsonpath='{.items[0].metadata.name}'):/tmp/migrate.py
kubectl exec -n hm $(kubectl get pods -n hm -l app=hm-app -o jsonpath='{.items[0].metadata.name}') -- python3 /tmp/migrate.py
```

### Expected output:

```
=== Story -> Product Link Migration ===

Fixing missing ads_products entries for stories...

1. Finding all stories linked to products...
   Found 42 story-product links

2. Checking and fixing ads_products entries...
   Creating ads_products entry for story 18120589918539447 -> product 123
   ...

=== Migration Summary ===
Total stories processed: 42
✅ Fixed/Created: 15
⏭️  Skipped (already correct): 27
❌ Errors: 0

✅ Migration complete!

💡 Note: 15 conversations with these stories should now work with AI replies.
```

### Notes:

- The script is idempotent - it's safe to run multiple times
- It only processes stories that have a `product_id` in `stories_products`
- It preserves the `auto_linked` flag and `sku` from the original story-product link
- Errors are reported but don't stop the migration

