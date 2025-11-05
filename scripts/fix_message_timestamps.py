import json
import os
from typing import Optional, Tuple

from sqlmodel import SQLModel, create_engine
from sqlalchemy import text


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
            return int(cand)
        s = str(cand)
        if s.isdigit():
            return int(s)
        return None
    except Exception:
        return None


def main() -> None:
    db_url = os.getenv("DATABASE_URL") or os.getenv("MYSQL_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL or MYSQL_URL is required")
    engine = create_engine(db_url, pool_pre_ping=True)
    # Ensure metadata is loaded (Message model etc.)
    try:
        import app.models  # noqa: F401
        SQLModel.metadata.create_all(engine)
    except Exception:
        pass

    fixed = 0
    scanned = 0
    with engine.begin() as conn:
        # upgrade column type proactively (idempotent)
        try:
            row = conn.exec_driver_sql(
                """
                SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'message' AND COLUMN_NAME = 'timestamp_ms'
                """
            ).fetchone()
            if row is not None and str(row[0]).lower() != "bigint":
                conn.exec_driver_sql("ALTER TABLE message MODIFY COLUMN timestamp_ms BIGINT")
        except Exception:
            pass

        # Fetch candidate rows: INT clamp value or NULL
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


