import os
import sqlite3
import json
from typing import Optional

from sqlmodel import create_engine


def _normalize_ms(val: Optional[object]) -> Optional[int]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            v = float(val)
        else:
            s = str(val).strip()
            if s.replace(".", "", 1).isdigit():
                v = float(s)
            else:
                return None
        return int(v if v >= 10_000_000_000 else v * 1000)
    except Exception:
        return None


def _extract_ts_ms(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    ts = None
    try:
        if isinstance(obj, dict):
            ts = obj.get("timestamp") or ((obj.get("message") or {}).get("timestamp"))
    except Exception:
        ts = None
    return _normalize_ms(ts)


def main() -> None:
    db_url = os.getenv("DATABASE_URL") or os.getenv("MYSQL_URL")
    sqlite_path = os.getenv("SQLITE_PATH", "data/app.db")
    if not db_url:
        raise SystemExit("DATABASE_URL or MYSQL_URL is required")
    if not os.path.exists(sqlite_path):
        raise SystemExit(f"SQLite file not found: {sqlite_path}")

    engine = create_engine(db_url, pool_pre_ping=True)
    con_sqlite = sqlite3.connect(sqlite_path)
    con_sqlite.row_factory = sqlite3.Row

    updated = 0
    scanned = 0
    missing = 0
    with engine.begin() as conn, con_sqlite:
        cur = con_sqlite.cursor()
        cur.execute("SELECT ig_message_id, raw_json FROM message WHERE raw_json IS NOT NULL")
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for r in rows:
                scanned += 1
                igid = r["ig_message_id"]
                raw = r["raw_json"]
                if not igid or not raw:
                    continue
                # update raw_json; compute timestamp if applicable
                ts = _extract_ts_ms(raw)
                try:
                    if ts is not None:
                        res = conn.exec_driver_sql(
                            "UPDATE message SET raw_json=:raw, timestamp_ms=:ts WHERE ig_message_id=:igid",
                            {"raw": raw, "ts": int(ts), "igid": str(igid)},
                        )
                    else:
                        res = conn.exec_driver_sql(
                            "UPDATE message SET raw_json=:raw WHERE ig_message_id=:igid",
                            {"raw": raw, "igid": str(igid)},
                        )
                    if getattr(res, "rowcount", 0) == 0:
                        missing += 1
                    else:
                        updated += 1
                except Exception:
                    # continue best-effort
                    continue

    print(f"reimport_message_raw_json: scanned={scanned} updated={updated} missing={missing}")


if __name__ == "__main__":
    main()


