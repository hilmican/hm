# Story Product Linking - Verification & Auto-Link Flow

## Overview

This document verifies that the story-to-product linking system works correctly and automatically syncs `ads_products` entries so AI replies can find products.

## Auto-Link Flow

### 1. When Messages Are Ingested

**Location**: `app/services/ingest.py` → `handle_ingest()` → `_insert_message()`

When a message with `story_id` is ingested:

1. Message is inserted with `story_id` and `story_url`
2. Conversation's `last_link_id` is set to `story:{story_id}` (line 598-602)
3. Conversation's `last_link_type` is set to `'story'` (line 601)
4. **`_auto_link_story_reply()` is called** (line 2231-2237) for inbound messages with story_id

### 2. Auto-Link Function Behavior

**Location**: `app/services/ingest.py` → `_auto_link_story_reply()`

**Flow**:

1. **Check if story already linked** (lines 1248-1332):
   - If story is in `stories_products` with a `product_id`:
     - ✅ **NEW**: Checks if `ads_products` entry exists
     - ✅ **NEW**: Creates `ads_products` entry if missing
     - ✅ **NEW**: Updates `ads_products` if product_id doesn't match
     - ✅ **NEW**: Ensures `ads` entry exists
     - Returns early (story already processed)

2. **If not linked, try AI matching** (lines 1334-1400):
   - Downloads/caches story media
   - Uses AI to match story image to products
   - If match found (confidence ≥ 0.7):
     - Creates entry in `stories_products`
     - Creates entry in `ads_products` (lines 1391-1415)
     - Creates entry in `ads` table

### 3. AI Reply System

**Location**: `app/services/ai_ig.py` → `_detect_focus_product()`

When AI tries to reply:

1. Gets conversation's `last_link_id` and `last_link_type` (lines 98-110)
2. Queries `ads_products` table with `ad_id = last_link_id` and `link_type = last_link_type` (lines 117-127)
3. If found, returns product slug/name/ID for AI context

## Key Fixes Applied

### Fix 1: Sync ads_products When Story Already Linked

**Problem**: If a story was already linked in `stories_products`, the function returned early without checking `ads_products`.

**Solution**: Added sync logic (lines 1254-1332) that:
- Checks if `ads_products` entry exists
- Creates it if missing
- Updates it if product_id doesn't match

### Fix 2: Image URL Type Handling

**Problem**: Image URLs could be passed in wrong format causing errors.

**Solution**: Added type checking and validation (lines 1364-1376) to ensure all image URLs are valid strings.

## Verification Checklist

- [x] Messages with story_id trigger auto-link function
- [x] Auto-link creates entries in both `stories_products` and `ads_products`
- [x] If story already linked, `ads_products` is synced automatically
- [x] Conversation's `last_link_id` is set correctly
- [x] AI reply system queries `ads_products` correctly
- [x] Manual story linking (via UI) also creates `ads_products` entries

## Testing

### Test 1: New Story Message

1. Send a message with a story reply
2. Wait for ingestion
3. Check:
   - `stories_products` has entry (if AI matched)
   - `ads_products` has entry with `ad_id = 'story:{story_id}'`
   - Conversation has `last_link_id = 'story:{story_id}'`

### Test 2: Already-Linked Story

1. Manually link a story to a product
2. Send a new message with that story
3. Check:
   - `ads_products` entry exists (sync should create it)
   - AI can find the product

### Test 3: AI Reply

1. Ensure story is linked to product
2. Send a message in that conversation
3. AI should be able to reply with product context

## Manual Linking

**Location**: `app/routers/stories.py` → `save_story_mapping()`

When a story is manually linked via the UI:

1. Creates/updates entry in `stories_products` (lines 108-143)
2. Creates/updates entry in `ads` table (lines 144-172)
3. Creates/updates entry in `ads_products` (lines 173-199)

✅ Manual linking already creates `ads_products` entries correctly.

## Migration

For existing stories that were linked before the fix:

Run: `scripts/migrate_story_ads_products.py`

This will:
- Find all stories in `stories_products`
- Create missing `ads_products` entries
- Update mismatched product_ids

## Summary

✅ **The system now automatically ensures `ads_products` entries exist whenever:**
- A story is auto-linked via AI matching
- A story is manually linked via UI
- An existing story link is detected during message ingestion

✅ **This ensures AI replies can always find products for conversations with stories.**

