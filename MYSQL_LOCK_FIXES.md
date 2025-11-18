# MySQL Lock Contention Fixes

## Problem Identified

The `message` table was experiencing lock contention causing:
- Lock wait timeouts (1205 errors)
- Long-running INSERT queries (40+ seconds)
- Service unavailability (hma.cdn.com.tr couldn't be accessed)

## Root Causes

1. **Race Condition in INSERT Logic**: The code was doing `SELECT` then `INSERT`, creating a window where multiple processes could try to insert the same message simultaneously, causing lock contention on the unique index.

2. **No Query Timeouts**: Queries could run indefinitely, holding locks.

3. **No Transaction Isolation Level**: Default isolation level (REPEATABLE READ) can cause more lock contention than necessary.

4. **No Connection Pool Limits**: Unlimited connections could exhaust database resources.

## Fixes Implemented

### 1. Database Engine Configuration (`app/db.py`)

**Added:**
- Connection pool limits:
  - `pool_size`: 10 (default, configurable via `DB_POOL_SIZE`)
  - `max_overflow`: 5 (default, configurable via `DB_MAX_OVERFLOW`)
  - `pool_timeout`: 30 seconds (default, configurable via `DB_POOL_TIMEOUT`)
  - `pool_recycle`: 3600 seconds (1 hour, configurable via `DB_POOL_RECYCLE`)

- Query timeouts:
  - `read_timeout`: 30 seconds (default, configurable via `DB_QUERY_TIMEOUT`)
  - `write_timeout`: 30 seconds (default, configurable via `DB_QUERY_TIMEOUT`)

- Transaction isolation level:
  - Set to `READ COMMITTED` (default, configurable via `DB_ISOLATION_LEVEL`)
  - Applied on each new connection via event listener
  - Reduces lock contention compared to REPEATABLE READ

### 2. INSERT Race Condition Fix (`app/services/ingest.py`)

**Changed:**
- Replaced `SELECT` then `session.add()` pattern with `INSERT IGNORE`
- This makes the insert operation atomic and prevents race conditions
- If a duplicate is detected, `INSERT IGNORE` silently skips it without holding locks
- Added proper error handling and fallback logic

**Before:**
```python
exists = session.exec(text("SELECT id FROM message WHERE ig_message_id = :mid")).first()
if exists:
    return None
row = Message(...)
session.add(row)
session.flush()
```

**After:**
```python
# Fast path check (optimization)
exists = session.exec(text("SELECT id FROM message WHERE ig_message_id = :mid LIMIT 1")).first()
if exists:
    return None

# Atomic INSERT IGNORE - no race condition
stmt = text("INSERT IGNORE INTO message (...) VALUES (...)")
session.exec(stmt)
session.flush()
# Fetch inserted ID
```

### 3. Index Verification

**Verified:**
- `ig_message_id` has a UNIQUE index (already in place)
- All necessary indexes exist for query performance
- No additional indexes needed

## Configuration Options

You can customize these settings via environment variables:

```bash
# Connection pool
DB_POOL_SIZE=10              # Base pool size
DB_MAX_OVERFLOW=5            # Additional connections allowed
DB_POOL_TIMEOUT=30           # Seconds to wait for connection
DB_POOL_RECYCLE=3600         # Recycle connections after N seconds

# Query timeouts
DB_QUERY_TIMEOUT=30          # Read/write timeout in seconds

# Transaction isolation
DB_ISOLATION_LEVEL=READ COMMITTED  # Options: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
```

## Expected Benefits

1. **Reduced Lock Contention**: `INSERT IGNORE` eliminates race conditions
2. **Faster Queries**: `READ COMMITTED` isolation level reduces lock wait times
3. **Better Resource Management**: Connection pool limits prevent connection exhaustion
4. **Automatic Recovery**: Query timeouts prevent queries from hanging indefinitely
5. **Improved Reliability**: Service should remain available even under high load

## Monitoring

To monitor the effectiveness of these fixes:

1. **Check for lock waits:**
   ```sql
   SELECT * FROM information_schema.innodb_lock_waits;
   ```

2. **Monitor long-running queries:**
   ```sql
   SELECT * FROM information_schema.processlist WHERE time > 5 AND command != 'Sleep';
   ```

3. **Check connection usage:**
   ```sql
   SHOW STATUS LIKE 'Threads_connected';
   SHOW VARIABLES LIKE 'max_connections';
   ```

4. **Monitor lock wait timeouts:**
   - Check application logs for "Lock wait timeout exceeded" errors
   - Should see significant reduction after these fixes

## Deployment

1. **Deploy the code changes** to your Kubernetes cluster
2. **Restart worker pods** to apply new connection pool settings:
   ```bash
   kubectl rollout restart deployment -n hm hm-worker-ingest
   kubectl rollout restart deployment -n hm hm-worker-reply
   kubectl rollout restart deployment -n hm hm-worker-enrich
   kubectl rollout restart deployment -n hm hm-worker-media
   ```

3. **Monitor** for any issues in the first few hours after deployment

## Rollback Plan

If issues occur, you can rollback by:
1. Reverting the code changes
2. Restarting pods
3. The database changes (isolation level, timeouts) are session-level and will reset on connection close

## Additional Recommendations

1. **Monitor connection pool usage**: If you see connection pool exhaustion, increase `DB_POOL_SIZE`
2. **Adjust timeouts**: If legitimate queries take longer than 30 seconds, increase `DB_QUERY_TIMEOUT`
3. **Consider read replicas**: For read-heavy workloads, consider using MySQL read replicas
4. **Add application-level retries**: For transient lock wait errors, add exponential backoff retries

