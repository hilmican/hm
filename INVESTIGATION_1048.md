# Investigation: Conversation 1048 - Why No Product Auto-Assignment

## Summary
Conversation 1048 at `https://hma.cdn.com.tr/ig/inbox/1048` shows no product assignment, even though the first message contains an Instagram post attachment.

## Database Investigation Results

### Conversation Details
- **Conversation ID**: 1048
- **User ID**: 4242189045993205 (@maksudmemmedov.88)
- **Total Messages**: 7
- **Last ad_id**: None
- **Last ad_title**: None

### Key Finding: Instagram Post Attachment

**Message 6932** (first message from user, timestamp: 1763412113011):
- **Direction**: in
- **Text**: (empty)
- **Attachments**: 2 items
  - `type: share`, `media_id: 17916189101920962`
  - `type: ig_post`, `media_id: 17916189101920962`

### Root Cause: Post Already Manually Linked

The Instagram post `17916189101920962` **IS linked** to `product_id: 3`, but:
- **auto_linked**: `0` (manually linked, not automatic)
- **Original message_id**: `3711` (different conversation)
- **Current message_id**: `6932` (conversation 1048)

### Why Auto-Linking Was Skipped

When message 6932 was inserted with the post attachment:

1. **Auto-link function was called** (`_auto_link_instagram_post` at line 1457)
2. **Function checked if post already linked** (line 710-715):
   ```python
   existing = session.exec(
       _sql_text("SELECT post_id FROM posts_products WHERE post_id=:pid")
   ).first()
   if existing:
       _log.debug("ingest: post %s already linked, skipping", post_id)
       return  # ← Exits here
   ```
3. **Found existing link** → Function returned early without attempting auto-link
4. **Result**: Post was not auto-linked for this conversation

### Why It Wasn't Auto-Linked Originally

The post was **manually linked** to product_id 3 (not automatically), which means:
- Either the auto-link function failed when message 3711 was originally inserted
- Or it was linked manually through the UI
- Or the original message 3711 no longer exists in the database

### Current Status

- ✅ Post `17916189101920962` exists in `posts` table
- ✅ Post is linked to `product_id: 3` in `posts_products` table
- ❌ Link was created manually (`auto_linked=0`)
- ❌ Conversation 1048 doesn't show product assignment because the link wasn't created for this conversation

## Solution

### Option 1: Retry Auto-Linking for This Post

Since the post is already linked, you could:
1. Check if the product assignment is correct
2. If not, unlink and retry auto-linking
3. Or manually assign the conversation to the product

### Option 2: Fix Auto-Link Logic

The current logic prevents re-linking posts that are already linked, even if they were manually linked. Consider:
- Checking `auto_linked` flag before skipping
- Or allowing re-auto-linking if `auto_linked=0`

### Option 3: Manual Assignment

Since the post is already linked to product_id 3, you can manually assign conversation 1048 to that product through the UI.

## Code References

- Auto-link function: `app/services/ingest.py:678` (`_auto_link_instagram_post`)
- Skip logic: `app/services/ingest.py:710-715`
- Call site: `app/services/ingest.py:1456-1457`

