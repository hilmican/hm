# Two-Stage Agent/Serializer System - Activation Guide

## Current Status

✅ **The system is already implemented and active by default**

The two-stage architecture is built into `draft_reply()` function. Every time `worker_reply.py` calls `draft_reply()`, it automatically uses:
1. **Agent Stage**: `generate_chat()` with tools enabled (free-text DM output)
2. **Serializer Stage**: `generate_json()` with JSON schema enforcement

## Verification Checklist

### 1. Core Files Present
- ✅ `app/services/ai_reply.py` - Two-stage implementation
- ✅ `app/services/ai.py` - `generate_chat()` method exists
- ✅ `app/services/prompts.py` - `get_serializer_prompt()` function
- ✅ `app/services/prompts/AGENT_SERIALIZER_PROMPT.txt` - Serializer prompt
- ✅ `app/services/prompts/REVISED_GLOBAL_SYSTEM_PROMPT.txt` - Agent prompt (DM-focused)

### 2. Worker Integration
- ✅ `scripts/worker_reply.py` calls `draft_reply()` - No changes needed

### 3. New Tools Registered
- ✅ `change_focus_product` - Product focus switching
- ✅ `add_cart_item` - Cart management with upsell flags
- ✅ `analyze_customer_image` - Image analysis
- ✅ `send_product_image_to_customer` - Product image sending

## How to Activate/Verify

### Option 1: Quick Verification (Recommended)

Run the verification script:
```bash
python scripts/verify_two_stage_system.py
```

This will:
- Check all required files exist
- Verify function signatures
- Test prompt loading
- Validate tool schemas

### Option 2: Manual Testing

1. **Check logs** when worker processes a conversation:
   ```bash
   # Watch worker logs
   tail -f logs/worker_reply.log | grep "ai_shadow"
   ```

2. **Look for two-stage indicators**:
   - `agent_raw` in debug_meta
   - `serializer_request_payload` in debug_meta
   - Function callbacks from new tools

3. **Test with a real conversation**:
   - Use a test conversation ID
   - Trigger `draft_reply()` via API or worker
   - Check `ai_shadow_reply` table for `json_meta` field containing debug_meta

### Option 3: API Testing

Create a test script to call `draft_reply()` directly:
```python
from app.services.ai_reply import draft_reply

result = draft_reply(
    conversation_id=YOUR_TEST_CONV_ID,
    include_meta=True
)

# Check for two-stage indicators
assert "debug_meta" in result
assert "agent_raw" in result["debug_meta"]
assert "serializer_request_payload" in result["debug_meta"]
```

## Monitoring

### Key Metrics to Watch

1. **Agent Stage Success Rate**
   - Check logs for `generate_chat` calls
   - Monitor tool call frequency

2. **Serializer Stage Success Rate**
   - Check logs for `generate_json` calls
   - Monitor JSON schema compliance

3. **New Tool Usage**
   - `change_focus_product` calls
   - `add_cart_item` calls (especially with `is_upsell: true`)
   - `send_product_image_to_customer` calls

4. **Error Patterns**
   - Tool handler failures
   - Serializer JSON parsing errors
   - Missing context errors

### Database Queries

```sql
-- Check recent replies with two-stage metadata
SELECT 
    conversation_id,
    created_at,
    json_meta->'debug_meta'->'agent_raw' IS NOT NULL as has_agent_stage,
    json_meta->'debug_meta'->'serializer_request_payload' IS NOT NULL as has_serializer_stage,
    actions_json->'function_callbacks' as callbacks
FROM ai_shadow_reply
WHERE created_at > NOW() - INTERVAL '1 hour'
ORDER BY created_at DESC
LIMIT 20;

-- Check new tool usage
SELECT 
    conversation_id,
    created_at,
    jsonb_array_elements(actions_json->'function_callbacks')->>'name' as tool_name
FROM ai_shadow_reply
WHERE actions_json->'function_callbacks' IS NOT NULL
    AND created_at > NOW() - INTERVAL '1 day'
ORDER BY created_at DESC;
```

## Rollback Plan (If Needed)

If you need to temporarily disable the two-stage system:

1. **Quick Rollback**: Modify `draft_reply()` to skip Serializer stage
   ```python
   # In app/services/ai_reply.py, around line 1718
   # Comment out Serializer stage and use agent_reply_text directly
   ```

2. **Feature Flag Approach**: Add environment variable
   ```python
   USE_TWO_STAGE = os.getenv("USE_TWO_STAGE_AI", "true").lower() == "true"
   if USE_TWO_STAGE:
       # Two-stage flow
   else:
       # Legacy single-stage flow
   ```

## Testing Scenarios

### 1. Basic Flow Test
- Customer asks about product
- Agent generates DM
- Serializer maps to JSON

### 2. Tool Call Test
- Customer provides height/weight
- Agent calls `set_customer_measurements`
- Serializer includes tool results in state

### 3. Multi-Product Test
- Customer says "ikisinide istiyorum"
- Agent calls `add_cart_item` twice
- Serializer includes cart in state

### 4. Upsell Test
- Customer completes size/color selection
- Agent offers upsell (from `upsell_config`)
- Customer accepts → `add_cart_item` with `is_upsell: true`

### 5. Admin Escalation Test
- Customer requests exchange/return
- Agent calls `yoneticiye_bildirim_gonder`
- Serializer sets `should_reply: false`

### 6. Image Request Test
- Customer asks "mavisini atar mısın?"
- Agent calls `send_product_image_to_customer`
- Backend adds image to reply

## Configuration

### Environment Variables

- `OPENAI_API_KEY` - Required for AI calls
- `AI_SHADOW_MODEL` - Model to use (default: gpt-4o-mini)
- `AI_REPLY_TEMPERATURE` - Temperature setting
- `PROMPT_REFRESH_SECONDS` - Prompt cache refresh (default: 5)
- `AGENT_SERIALIZER_PROMPT_FILE` - Override serializer prompt path
- `GLOBAL_SYSTEM_PROMPT_FILE` - Override agent prompt path

### Prompt Hot-Reload

Prompts are cached but auto-reload when files change:
- Edit `app/services/prompts/AGENT_SERIALIZER_PROMPT.txt`
- Edit `app/services/prompts/REVISED_GLOBAL_SYSTEM_PROMPT.txt`
- Changes take effect within `PROMPT_REFRESH_SECONDS` (default 5s)

## Next Steps

1. ✅ **Verify system is working** - Run verification script
2. ✅ **Monitor initial conversations** - Watch logs for first few hours
3. ✅ **Test edge cases** - Multi-product, upsell, escalation scenarios
4. ✅ **Tune prompts** - Adjust based on real-world performance
5. ✅ **Add upsell config** - Configure `upsell_config` in context JSON

## Support

If issues arise:
1. Check `ai_shadow_reply.json_meta.debug_meta` for detailed logs
2. Review worker logs for errors
3. Verify OpenAI API key and quota
4. Check prompt files are readable
5. Ensure all tools have handlers registered

