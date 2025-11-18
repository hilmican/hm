#!/usr/bin/env python3
"""
Diagnostic script to check MySQL table locks, long-running queries, and system status.
Helps identify which table might be causing issues.
"""
import os
import sys
from sqlalchemy import create_engine, text
from datetime import datetime
import json

# Get database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("MYSQL_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL or MYSQL_URL must be set")
    sys.exit(1)

if not (DATABASE_URL.startswith("mysql+") or DATABASE_URL.startswith("mysql://")):
    print("ERROR: Only MySQL databases are supported")
    sys.exit(1)

print("=" * 80)
print(f"MySQL Diagnostic Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
print()

try:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})
    
    with engine.connect() as conn:
        # 1. Check for table locks
        print("1. TABLE LOCKS")
        print("-" * 80)
        try:
            result = conn.execute(text("""
                SELECT 
                    r.trx_id waiting_trx_id,
                    r.trx_mysql_thread_id waiting_thread,
                    r.trx_query waiting_query,
                    b.trx_id blocking_trx_id,
                    b.trx_mysql_thread_id blocking_thread,
                    b.trx_query blocking_query,
                    l.lock_table,
                    l.lock_mode,
                    l.lock_type,
                    TIMESTAMPDIFF(SECOND, r.trx_wait_started, NOW()) as wait_seconds
                FROM information_schema.innodb_lock_waits w
                INNER JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id
                INNER JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id
                INNER JOIN information_schema.innodb_locks l ON l.lock_id = w.requested_lock_id
                ORDER BY wait_seconds DESC
            """))
            locks = result.fetchall()
            if locks:
                print(f"⚠️  Found {len(locks)} table lock(s):")
                for lock in locks:
                    print(f"   Waiting Thread: {lock[1]}")
                    print(f"   Blocking Thread: {lock[4]}")
                    print(f"   Table: {lock[6]}")
                    print(f"   Lock Type: {lock[8]} ({lock[7]})")
                    print(f"   Wait Time: {lock[9]} seconds")
                    print(f"   Waiting Query: {lock[2][:100] if lock[2] else 'N/A'}...")
                    print(f"   Blocking Query: {lock[5][:100] if lock[5] else 'N/A'}...")
                    print()
            else:
                print("✓ No table locks detected")
        except Exception as e:
            print(f"⚠️  Could not check table locks: {e}")
            # Try alternative query for older MySQL versions
            try:
                result = conn.execute(text("SHOW ENGINE INNODB STATUS"))
                status = result.fetchone()
                if status and status[2]:
                    print("   (Showing INNODB STATUS output - check for 'LOCK WAIT' sections)")
                    # Parse for lock info
                    status_text = status[2]
                    if "LOCK WAIT" in status_text or "lock wait" in status_text.lower():
                        print("   ⚠️  Lock wait detected in INNODB STATUS")
            except:
                pass
        print()
        
        # 2. Check for long-running queries
        print("2. LONG-RUNNING QUERIES (> 5 seconds)")
        print("-" * 80)
        try:
            result = conn.execute(text("""
                SELECT 
                    id,
                    user,
                    host,
                    db,
                    command,
                    time as duration_seconds,
                    state,
                    LEFT(info, 200) as query
                FROM information_schema.processlist
                WHERE time > 5
                  AND command != 'Sleep'
                  AND id != CONNECTION_ID()
                ORDER BY time DESC
            """))
            long_queries = result.fetchall()
            if long_queries:
                print(f"⚠️  Found {len(long_queries)} long-running query/queries:")
                for q in long_queries:
                    print(f"   Process ID: {q[0]}")
                    print(f"   User: {q[1]}@{q[2]}")
                    print(f"   Database: {q[3]}")
                    print(f"   Duration: {q[5]} seconds")
                    print(f"   State: {q[6]}")
                    print(f"   Query: {q[7]}")
                    print()
            else:
                print("✓ No long-running queries detected")
        except Exception as e:
            print(f"⚠️  Could not check long-running queries: {e}")
        print()
        
        # 3. Check all active processes
        print("3. ALL ACTIVE PROCESSES")
        print("-" * 80)
        try:
            result = conn.execute(text("""
                SELECT 
                    id,
                    user,
                    host,
                    db,
                    command,
                    time as duration_seconds,
                    state,
                    LEFT(info, 150) as query
                FROM information_schema.processlist
                WHERE id != CONNECTION_ID()
                ORDER BY time DESC
                LIMIT 20
            """))
            processes = result.fetchall()
            if processes:
                print(f"Found {len(processes)} active process(es):")
                for p in processes:
                    print(f"   [{p[0]}] {p[1]}@{p[2]} | DB: {p[3]} | {p[4]} | {p[5]}s | {p[6]}")
                    if p[7]:
                        print(f"      Query: {p[7]}")
            else:
                print("✓ No other active processes")
        except Exception as e:
            print(f"⚠️  Could not check processes: {e}")
        print()
        
        # 4. Check table-level locks (MyISAM or explicit locks)
        print("4. TABLE-LEVEL LOCKS")
        print("-" * 80)
        try:
            result = conn.execute(text("""
                SELECT 
                    table_schema,
                    table_name,
                    lock_type,
                    lock_mode,
                    lock_status
                FROM performance_schema.table_lock_waits_summary_by_table
                WHERE sum_timer_wait > 0
                ORDER BY sum_timer_wait DESC
                LIMIT 10
            """))
            table_locks = result.fetchall()
            if table_locks:
                print(f"⚠️  Found table-level lock activity:")
                for tl in table_locks:
                    print(f"   {tl[0]}.{tl[1]} - {tl[2]} ({tl[3]}) - {tl[4]}")
            else:
                print("✓ No table-level lock activity detected")
        except Exception as e:
            print(f"   (Table-level lock info not available: {e})")
        print()
        
        # 5. Check for locked tables (FLUSH TABLES WITH READ LOCK, etc.)
        print("5. LOCKED TABLES")
        print("-" * 80)
        try:
            result = conn.execute(text("SHOW OPEN TABLES WHERE In_use > 0"))
            locked = result.fetchall()
            if locked:
                print(f"⚠️  Found {len(locked)} locked table(s):")
                for lt in locked:
                    print(f"   {lt[0]}.{lt[1]} - In_use: {lt[2]}, Name_locked: {lt[3]}")
            else:
                print("✓ No locked tables")
        except Exception as e:
            print(f"⚠️  Could not check locked tables: {e}")
        print()
        
        # 6. Check database connection count
        print("6. CONNECTION STATISTICS")
        print("-" * 80)
        try:
            result = conn.execute(text("SHOW STATUS LIKE 'Threads_connected'"))
            threads_connected = result.fetchone()
            if threads_connected:
                print(f"   Threads Connected: {threads_connected[1]}")
            
            result = conn.execute(text("SHOW STATUS LIKE 'Max_used_connections'"))
            max_used = result.fetchone()
            if max_used:
                print(f"   Max Used Connections: {max_used[1]}")
            
            result = conn.execute(text("SHOW VARIABLES LIKE 'max_connections'"))
            max_conn = result.fetchone()
            if max_conn:
                print(f"   Max Connections: {max_conn[1]}")
        except Exception as e:
            print(f"⚠️  Could not check connection stats: {e}")
        print()
        
        # 7. Check for deadlocks
        print("7. RECENT DEADLOCKS")
        print("-" * 80)
        try:
            result = conn.execute(text("SHOW ENGINE INNODB STATUS"))
            status = result.fetchone()
            if status and status[2]:
                status_text = status[2]
                if "LATEST DETECTED DEADLOCK" in status_text:
                    print("⚠️  Recent deadlock detected in INNODB STATUS")
                    # Extract deadlock section
                    deadlock_start = status_text.find("LATEST DETECTED DEADLOCK")
                    if deadlock_start != -1:
                        deadlock_section = status_text[deadlock_start:deadlock_start+2000]
                        print("   (Check MySQL error log for full details)")
                        # Try to extract table names
                        import re
                        tables = re.findall(r'`(\w+)`\.`(\w+)`', deadlock_section)
                        if tables:
                            print(f"   Tables involved: {', '.join([f'{t[0]}.{t[1]}' for t in set(tables)])}")
                else:
                    print("✓ No recent deadlocks")
        except Exception as e:
            print(f"⚠️  Could not check for deadlocks: {e}")
        print()
        
        # 8. List all tables and their status
        print("8. TABLE STATUS SUMMARY")
        print("-" * 80)
        try:
            result = conn.execute(text("""
                SELECT 
                    table_name,
                    table_rows,
                    data_length,
                    index_length,
                    engine
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                ORDER BY table_name
            """))
            tables = result.fetchall()
            print(f"Found {len(tables)} tables in database:")
            for t in tables[:20]:  # Show first 20
                rows = f"{t[1]:,}" if t[1] else "N/A"
                size_mb = ((t[2] or 0) + (t[3] or 0)) / 1024 / 1024
                print(f"   {t[0]:30s} | Rows: {rows:>12s} | Size: {size_mb:>8.2f} MB | Engine: {t[4]}")
            if len(tables) > 20:
                print(f"   ... and {len(tables) - 20} more tables")
        except Exception as e:
            print(f"⚠️  Could not list tables: {e}")
        print()
        
        # 9. Test basic connectivity
        print("9. CONNECTIVITY TEST")
        print("-" * 80)
        try:
            start = datetime.now()
            conn.execute(text("SELECT 1"))
            elapsed = (datetime.now() - start).total_seconds()
            print(f"✓ Database is reachable (response time: {elapsed*1000:.2f}ms)")
        except Exception as e:
            print(f"✗ Database connectivity issue: {e}")
        print()
        
except Exception as e:
    print(f"✗ ERROR: Could not connect to database: {e}")
    print(f"   DATABASE_URL: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else 'hidden'}")
    sys.exit(1)

print("=" * 80)
print("Diagnostic complete. Check the output above for any issues.")
print("=" * 80)

