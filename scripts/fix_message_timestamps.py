import json
import os
from typing import Optional

from sqlmodel import create_engine


def _extract_ts_ms(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    # common locations
    cand = None
    try:
        if isinstance(obj, dict):
            cand = obj.get("timestamp") or (
                (obj.get("message") or {}).get("timestamp")
            )
    except Exception:
        cand = None
    try:
        if cand is None:
            return None
        # accept int, float, or numeric string
        if isinstance(cand, (int, float)):
            val = float(cand)
        else:
            s = str(cand).strip()
            if s.replace(".", "", 1).isdigit():
                val = float(s)
            else:
                return None
        # normalize to milliseconds
        return int(val if val >= 10_000_000_000 else val * 1000)
    except Exception:
        return None


def main() -> None:
    db_url = os.getenv("DATABASE_URL") or os.getenv("MYSQL_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL or MYSQL_URL is required")
    engine = create_engine(db_url, pool_pre_ping=True)

    fixed = 0
    scanned = 0
    with engine.begin() as conn:
        rows = conn.exec_driver_sql(
            "SELECT id, raw_json FROM message WHERE timestamp_ms IS NULL OR timestamp_ms >= 2147480000"
        ).fetchall()
        for mid, raw in rows:
            scanned += 1
            ts = _extract_ts_ms(raw)
            if ts is None:
                continue
            try:
                conn.exec_driver_sql(
                    "UPDATE message SET timestamp_ms = :ts WHERE id = :id",
                    {"ts": int(ts), "id": int(mid)},
                )
                fixed += 1
            except Exception:
                pass

    print(f"fix_message_timestamps: scanned={scanned} fixed={fixed}")


if __name__ == "__main__":
    main()


