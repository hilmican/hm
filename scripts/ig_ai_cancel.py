#!/usr/bin/env python3
"""
Cancel IG AI processing runs and remove queued jobs without using the UI.

Usage examples:

  # Cancel ALL active runs
  python scripts/ig_ai_cancel.py

  # Cancel specific runs by id
  python scripts/ig_ai_cancel.py --runs 12 15 18

Environment:
  - APP_DB_PATH: override SQLite path (default: data/app.db)
  - REDIS_URL: Redis url for queue (default: redis://localhost:6379/0)
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from typing import Iterable, List, Tuple
import time


def _db_path(cli_override: str | None = None) -> str:
    # Priority: CLI --db > APP_DB_PATH > detected project root /app/data/app.db > relative data/app.db
    if cli_override:
        return cli_override
    p = os.getenv("APP_DB_PATH")
    if p:
        return p
    # Try project root relative to this file
    here = os.path.abspath(os.path.dirname(__file__))
    root = os.path.abspath(os.path.join(here, ".."))
    candidates = [
        os.path.join(root, "data", "app.db"),   # /app/data/app.db in container
        "/app/data/app.db",
        os.path.join("data", "app.db"),          # relative fallback
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    # default best-guess
    return os.path.join(root, "data", "app.db")


def _connect_db(db_path: str | None = None) -> sqlite3.Connection:
    path = _db_path(db_path)
    conn = sqlite3.connect(path, timeout=float(os.getenv("SQLITE_BUSY_TIMEOUT", "60")))
    conn.row_factory = sqlite3.Row
    try:
        # Helpful pragmas to reduce lock contention
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute(f"PRAGMA busy_timeout={int(float(os.getenv('SQLITE_BUSY_TIMEOUT', '60'))*1000)};")
    except Exception:
        pass
    return conn


def _connect_redis():
    try:
        from redis import Redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        return Redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _select_active_runs(conn: sqlite3.Connection, only_ids: Iterable[int] | None) -> List[sqlite3.Row]:
    cur = conn.cursor()
    if only_ids:
        ids = list({int(x) for x in only_ids})
        qmarks = ",".join(["?"] * len(ids))
        cur.execute(
            f"""
            SELECT id, job_id
            FROM ig_ai_run
            WHERE (completed_at IS NULL) AND (cancelled_at IS NULL) AND id IN ({qmarks})
            ORDER BY id DESC
            """,
            ids,
        )
    else:
        cur.execute(
            """
            SELECT id, job_id
            FROM ig_ai_run
            WHERE (completed_at IS NULL) AND (cancelled_at IS NULL)
            ORDER BY id DESC
            """
        )
    return list(cur.fetchall())


def _cancel_runs(conn: sqlite3.Connection, rows: List[sqlite3.Row], *, retries: int = 10, backoff: float = 0.2) -> None:
    cur = conn.cursor()
    # Process one run at a time to reduce lock contention
    for r in rows:
        run_id = int(r["id"]) if r["id"] is not None else None
        job_id = int(r["job_id"]) if r["job_id"] is not None else None
        if run_id is None:
            continue
        # Mark cancelled/completed
        _begin_immediate(conn, retries=retries, backoff=backoff)
        _exec_retry(cur, "UPDATE ig_ai_run SET cancelled_at=CURRENT_TIMESTAMP, completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP) WHERE id=?", (run_id,), retries=retries, backoff=backoff)
        # Delete queued job by id
        try:
            if job_id:
                _exec_retry(cur, "DELETE FROM jobs WHERE id=?", (job_id,), retries=retries, backoff=backoff)
        except Exception:
            pass
        # Delete by (kind,key) fallback
        try:
            _exec_retry(cur, "DELETE FROM jobs WHERE kind='ig_ai_process_run' AND key=?", (str(run_id),), retries=retries, backoff=backoff)
        except Exception:
            pass
        conn.commit()


def _exec_retry(cur: sqlite3.Cursor, sql: str, params, *, retries: int, backoff: float) -> None:
    attempt = 0
    delay = max(0.05, backoff)
    while True:
        try:
            cur.execute(sql, params)
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if ("database is locked" in msg or "busy" in msg) and attempt < retries:
                time.sleep(delay)
                delay = min(delay * 1.7, 2.0)
                attempt += 1
                continue
            raise


def _begin_immediate(conn: sqlite3.Connection, *, retries: int, backoff: float) -> None:
    """Acquire a write lock quickly using BEGIN IMMEDIATE with retries."""
    attempt = 0
    delay = max(0.05, backoff)
    while True:
        try:
            conn.execute("BEGIN IMMEDIATE;")
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if ("database is locked" in msg or "busy" in msg) and attempt < retries:
                time.sleep(delay)
                delay = min(delay * 1.7, 2.0)
                attempt += 1
                continue
            # If we already have a transaction, ignore
            if "cannot start a transaction within a transaction" in msg:
                return
            raise


def _purge_redis_messages(rows: List[sqlite3.Row]) -> Tuple[int, int]:
    client = _connect_redis()
    if not client:
        return (0, 0)
    keys = {str(int(r["id"])) for r in rows if r["id"] is not None}
    jids = {str(int(r["job_id"])) for r in rows if r["job_id"] is not None}
    removed = 0
    total = 0
    try:
        arr = client.lrange("jobs:ig_ai_process_run", 0, -1) or []
        total = len(arr)
        for msg in arr:
            try:
                data = json.loads(msg)
            except Exception:
                # If unparsable, drop it to be safe
                client.lrem("jobs:ig_ai_process_run", 1, msg)
                removed += 1
                continue
            mid = str(data.get("id")) if data.get("id") is not None else None
            mkey = str(data.get("key")) if data.get("key") is not None else None
            if (mid and mid in jids) or (mkey and mkey in keys):
                client.lrem("jobs:ig_ai_process_run", 1, msg)
                removed += 1
    except Exception:
        # non-fatal
        pass
    return (removed, total)


def main() -> None:
    ap = argparse.ArgumentParser(description="Cancel IG AI processing runs and purge queued jobs")
    ap.add_argument("--runs", nargs="*", type=int, help="Specific run ids to cancel (default: all active)")
    ap.add_argument("--db", type=str, default=None, help="Path to SQLite DB (default auto-detect)")
    ap.add_argument("--retries", type=int, default=10, help="DB lock retries (default 10)")
    ap.add_argument("--backoff", type=float, default=0.2, help="Initial backoff seconds (default 0.2)")
    args = ap.parse_args()

    conn = _connect_db(args.db)
    try:
        rows = _select_active_runs(conn, args.runs)
        if not rows:
            print("No active runs found.")
            return
        print(f"Cancelling {len(rows)} run(s): {[int(r['id']) for r in rows]}")
        _cancel_runs(conn, rows, retries=int(args.retries), backoff=float(args.backoff))
        r_removed, r_total = _purge_redis_messages(rows)
        if r_total:
            print(f"Redis queue: removed {r_removed}/{r_total} pending messages for ig_ai_process_run")
        print("Done.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()


