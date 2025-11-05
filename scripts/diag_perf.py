#!/usr/bin/env python3
"""
Performance diagnostics for the app (SQLite + Redis + hotspot queries).

Usage:
  python scripts/diag_perf.py            # auto-detects DB and Redis
  python scripts/diag_perf.py --db /app/data/app.db --redis redis://localhost:6379/0

Outputs a human-readable summary and a JSON blob at the end.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from typing import Any, Dict


def autodetect_db(path: str | None) -> str:
    if path:
        return path
    env = os.getenv("APP_DB_PATH")
    if env:
        return env
    here = os.path.abspath(os.path.dirname(__file__))
    root = os.path.abspath(os.path.join(here, ".."))
    for c in [os.path.join(root, "data", "app.db"), "/app/data/app.db", os.path.join("data", "app.db")]:
        if os.path.exists(c):
            return c
    return os.path.join(root, "data", "app.db")


def connect_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=float(os.getenv("SQLITE_BUSY_TIMEOUT", "30")))
    conn.row_factory = sqlite3.Row
    return conn


def connect_redis(url: str | None):
    try:
        from redis import Redis
        u = url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        return Redis.from_url(u, decode_responses=True)
    except Exception:
        return None


def time_query(cur: sqlite3.Cursor, sql: str, params: dict | tuple | None = None) -> tuple[float, list[sqlite3.Row]]:
    t0 = time.perf_counter()
    if isinstance(params, dict):
        cur.execute(sql, params)
    elif isinstance(params, tuple):
        cur.execute(sql, params)
    else:
        cur.execute(sql)
    rows = cur.fetchall()
    dt = (time.perf_counter() - t0) * 1000.0
    return dt, rows


def explain(cur: sqlite3.Cursor, sql: str) -> str:
    try:
        cur.execute("EXPLAIN QUERY PLAN " + sql)
        return " | ".join(["; ".join(map(str, r)) for r in cur.fetchall()])
    except Exception as e:
        return f"(explain failed: {e})"


def main() -> None:
    ap = argparse.ArgumentParser(description="App performance diagnostics")
    ap.add_argument("--db", type=str, default=None, help="Path to SQLite DB")
    ap.add_argument("--redis", type=str, default=None, help="Redis URL")
    args = ap.parse_args()

    db_path = autodetect_db(args.db)
    conn = connect_db(db_path)
    cur = conn.cursor()

    out: Dict[str, Any] = {"db_path": db_path}

    # PRAGMAs
    def pragma(name: str) -> Any:
        try:
            cur.execute(f"PRAGMA {name}")
            r = cur.fetchone()
            return r[0] if r else None
        except Exception:
            return None

    out["pragmas"] = {
        "journal_mode": pragma("journal_mode"),
        "synchronous": pragma("synchronous"),
        "busy_timeout(ms)": pragma("busy_timeout"),
        "page_count": pragma("page_count"),
        "page_size": pragma("page_size"),
    }

    # Table sizes
    sizes: Dict[str, int] = {}
    for t in ("message", "conversations", "jobs", "attachments", "order", "client"):
        try:
            cur.execute(f"SELECT COUNT(1) FROM {t}")
            sizes[t] = int(cur.fetchone()[0])
        except Exception:
            sizes[t] = -1
    out["table_counts"] = sizes

    # Indexes
    cur.execute("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index'")
    idx = [{"name": r[0], "table": r[1], "sql": r[2]} for r in cur.fetchall()]
    out["indexes"] = idx

    # Hotspot query timings
    timings: Dict[str, float] = {}
    plans: Dict[str, str] = {}

    # Eligible conversations (like processor)
    from datetime import datetime, timedelta
    cutoff = (datetime.utcnow() - timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S")
    sql_conv = (
        "SELECT convo_id FROM conversations WHERE ai_processed_at IS NULL AND last_message_at <= :cutoff "
        "ORDER BY last_message_at DESC LIMIT 200"
    )
    dt_ms, _ = time_query(cur, sql_conv, {"cutoff": cutoff})
    timings["eligible_conversations_ms"] = round(dt_ms, 2)
    plans["eligible_conversations_plan"] = explain(cur, sql_conv.replace(":cutoff", f"'{cutoff}'"))

    # Latest messages for a sample conversation
    try:
        cur.execute("SELECT convo_id FROM conversations ORDER BY last_message_at DESC LIMIT 1")
        row = cur.fetchone()
        any_cid = row[0] if row else None
    except Exception:
        any_cid = None
    if any_cid:
        sql_msgs = (
            "SELECT ig_message_id, timestamp_ms FROM message WHERE conversation_id = :cid ORDER BY timestamp_ms DESC LIMIT 200"
        )
        dt2, _ = time_query(cur, sql_msgs, {"cid": any_cid})
        timings["thread_messages_ms"] = round(dt2, 2)
        plans["thread_messages_plan"] = explain(cur, sql_msgs.replace(":cid", f"'{any_cid}'"))

    out["query_timings_ms"] = timings
    out["query_plans"] = plans

    # Redis queue depths
    rclient = connect_redis(args.redis)
    qdepth: Dict[str, int] = {}
    if rclient:
        for k in [
            "jobs:ingest",
            "jobs:enrich_user",
            "jobs:enrich_page",
            "jobs:fetch_media",
            "jobs:ig_ai_process_run",
        ]:
            try:
                qdepth[k] = int(rclient.llen(k))
            except Exception:
                qdepth[k] = -1
    out["queue_depths"] = qdepth

    # Heuristics / suggestions
    suggestions: list[str] = []
    idx_names = {i["name"] for i in idx if i.get("name")}
    if "idx_conversations_last_message_at" not in idx_names:
        suggestions.append("Add index on conversations(last_message_at) [already in init_db if recent]")
    if "idx_message_conv_ts" not in idx_names:
        suggestions.append("Add index on message(conversation_id, timestamp_ms)")
    if qdepth.get("jobs:ingest", 0) > 1000:
        suggestions.append("Ingest queue is backed up; scale workers or reduce webhook rate")
    out["suggestions"] = suggestions

    # Human-readable summary
    print("=== SQLite ===")
    for k, v in out["pragmas"].items():
        print(f"{k}: {v}")
    print("\n=== Table counts ===")
    for k, v in sizes.items():
        print(f"{k}: {v}")
    print("\n=== Query timings (ms) ===")
    for k, v in timings.items():
        print(f"{k}: {v}")
    if suggestions:
        print("\n=== Suggestions ===")
        for s in suggestions:
            print("- " + s)
    print("\n=== JSON ===")
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


