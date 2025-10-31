from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy import text
import os
import time
import sqlite3

DB_PATH = Path("data/app.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
DATABASE_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(
    DATABASE_URL,
    connect_args={
        "check_same_thread": False,
        # increase sqlite busy timeout to reduce 'database is locked' errors on startup
        "timeout": float(os.getenv("SQLITE_BUSY_TIMEOUT", "30")),
    },
)


def init_db() -> None:
    def _wait_for_sqlite_available() -> None:
        """Ensure we can obtain a write lock before proceeding with DDL/migrations.

        This guards against overlapping writers during rolling updates where the
        previous pod may still be finalizing WAL writes.
        """
        retries = int(os.getenv("DB_INIT_RETRIES", "10"))
        backoff = float(os.getenv("DB_INIT_BACKOFF", "0.5"))
        busy_timeout_s = float(os.getenv("SQLITE_BUSY_TIMEOUT", "30"))
        last: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                conn = sqlite3.connect(str(DB_PATH), timeout=busy_timeout_s)
                # set helpful pragmas
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_s*1000)};")
                # optional performance tuning when enabled
                if os.getenv("SQLITE_TUNE", "1") != "0":
                    try:
                        conn.execute("PRAGMA synchronous=NORMAL;")
                        conn.execute("PRAGMA temp_store=MEMORY;")
                        conn.execute("PRAGMA mmap_size=268435456;")  # 256MB
                        conn.execute("PRAGMA cache_size=-262144;")    # ~256MB page cache
                        conn.execute("PRAGMA journal_size_limit=134217728;")  # 128MB
                    except Exception:
                        pass
                # try to acquire writer lock
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute("ROLLBACK;")
                conn.close()
                return
            except Exception as e:
                last = e
                msg = str(e).lower()
                locked = isinstance(e, sqlite3.OperationalError) or ("database is locked" in msg)
                if locked and attempt < retries:
                    try:
                        print(f"[DB INIT] waiting for SQLite lock release ({attempt}/{retries})...")
                    except Exception:
                        pass
                    time.sleep(backoff)
                    backoff = min(backoff * 1.5, 5.0)
                    continue
                raise

    _wait_for_sqlite_available()
    retries = int(os.getenv("DB_INIT_RETRIES", "10"))
    backoff = float(os.getenv("DB_INIT_BACKOFF", "0.5"))
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            SQLModel.metadata.create_all(engine)
            # lightweight migrations for existing SQLite DBs
            with engine.begin() as conn:
                # Apply SQLite PRAGMAs once at startup if enabled
                try:
                    if (getattr(engine, "url", None) and getattr(engine.url, "get_backend_name", lambda: "")() == "sqlite") and os.getenv("SQLITE_TUNE", "1") != "0":
                        conn.exec_driver_sql("PRAGMA journal_mode=WAL")
                        conn.exec_driver_sql("PRAGMA synchronous=NORMAL")
                        conn.exec_driver_sql("PRAGMA temp_store=MEMORY")
                        conn.exec_driver_sql("PRAGMA mmap_size=268435456")
                        conn.exec_driver_sql("PRAGMA cache_size=-262144")
                        conn.exec_driver_sql("PRAGMA journal_size_limit=134217728")
                except Exception:
                    pass
                def column_exists(table: str, column: str) -> bool:
                    rows = conn.exec_driver_sql(f"PRAGMA table_info('{table}')").fetchall()
                    # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
                    return any(r[1] == column for r in rows)

                # Client.height_cm / weight_kg
                if not column_exists("client", "height_cm"):
                    conn.exec_driver_sql("ALTER TABLE client ADD COLUMN height_cm INTEGER")
                if not column_exists("client", "weight_kg"):
                    conn.exec_driver_sql("ALTER TABLE client ADD COLUMN weight_kg INTEGER")

                # Order.data_date (DATE)
                if not column_exists("order", "data_date"):
                    conn.exec_driver_sql('ALTER TABLE "order" ADD COLUMN data_date DATE')
                # Order.total_cost (REAL)
                if not column_exists("order", "total_cost"):
                    conn.exec_driver_sql('ALTER TABLE "order" ADD COLUMN total_cost REAL')

                # Order.shipping_fee (REAL)
                if not column_exists("order", "shipping_fee"):
                    conn.exec_driver_sql('ALTER TABLE "order" ADD COLUMN shipping_fee REAL')

                # ImportRow.row_hash index for dedup/idempotency (best-effort)
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_importrow_row_hash ON importrow(row_hash)")
                except Exception:
                    pass

                # ImportRun.data_date (DATE)
                if not column_exists("importrun", "data_date"):
                    conn.exec_driver_sql("ALTER TABLE importrun ADD COLUMN data_date DATE")

                # OrderItem table (lightweight create-if-missing for SQLite)
                # Detect by presence of a known column on the table name
                try:
                    rows = conn.exec_driver_sql("PRAGMA table_info('orderitem')").fetchall()
                    orderitem_exists = any(rows)
                except Exception:
                    orderitem_exists = False
                if not orderitem_exists:
                    conn.exec_driver_sql(
				"""
				CREATE TABLE IF NOT EXISTS orderitem (
					id INTEGER PRIMARY KEY,
					order_id INTEGER,
					item_id INTEGER,
					quantity INTEGER DEFAULT 1,
					created_at DATETIME,
					FOREIGN KEY(order_id) REFERENCES "order"(id),
					FOREIGN KEY(item_id) REFERENCES item(id)
				)
				"""
			)

                # Inventory/Variant fields on item
                if not column_exists("item", "product_id"):
                    conn.exec_driver_sql("ALTER TABLE item ADD COLUMN product_id INTEGER")
                if not column_exists("item", "size"):
                    conn.exec_driver_sql("ALTER TABLE item ADD COLUMN size TEXT")
                if not column_exists("item", "color"):
                    conn.exec_driver_sql("ALTER TABLE item ADD COLUMN color TEXT")
                # stop creating pack_type/pair_multiplier for new DBs; legacy columns remain if present
                if not column_exists("item", "price"):
                    conn.exec_driver_sql("ALTER TABLE item ADD COLUMN price REAL")
                if not column_exists("item", "cost"):
                    conn.exec_driver_sql("ALTER TABLE item ADD COLUMN cost REAL")
                if not column_exists("item", "status"):
                    conn.exec_driver_sql("ALTER TABLE item ADD COLUMN status TEXT")

                # User table and columns (created by metadata, but ensure columns exist for old DBs)
                if not column_exists("user", "username"):
                    # create table if absent by invoking metadata create again (safe) then fallback columns
                    SQLModel.metadata.create_all(engine)
                if not column_exists("user", "password_hash"):
                    conn.exec_driver_sql("ALTER TABLE user ADD COLUMN password_hash TEXT")
                if not column_exists("user", "role"):
                    conn.exec_driver_sql("ALTER TABLE user ADD COLUMN role TEXT")
                if not column_exists("user", "failed_attempts"):
                    conn.exec_driver_sql("ALTER TABLE user ADD COLUMN failed_attempts INTEGER DEFAULT 0")
                if not column_exists("user", "locked_until"):
                    conn.exec_driver_sql("ALTER TABLE user ADD COLUMN locked_until DATETIME")

                # Client.status
                if not column_exists("client", "status"):
                    conn.exec_driver_sql("ALTER TABLE client ADD COLUMN status TEXT")

                # Payment fee fields and net_amount
                for col, coltype in [
                    ("fee_komisyon", "REAL"),
                    ("fee_hizmet", "REAL"),
                    ("fee_kargo", "REAL"),
                    ("fee_iade", "REAL"),
                    ("fee_erken_odeme", "REAL"),
                    ("net_amount", "REAL"),
                ]:
                    if not column_exists("payment", col):
                        conn.exec_driver_sql(f"ALTER TABLE payment ADD COLUMN {col} {coltype} DEFAULT 0")

                # Product.default_color lightweight migration
                if not column_exists("product", "default_color"):
                    conn.exec_driver_sql("ALTER TABLE product ADD COLUMN default_color TEXT")

                # Message table lightweight migrations
                try:
                    rows = conn.exec_driver_sql("PRAGMA table_info('message')").fetchall()
                    message_exists = any(rows)
                except Exception:
                    message_exists = False
                if message_exists:
                    if not column_exists("message", "conversation_id"):
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN conversation_id TEXT")
                    if not column_exists("message", "direction"):
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN direction TEXT")
                    if not column_exists("message", "sender_username"):
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN sender_username TEXT")
                    # ads/referral columns
                    if not column_exists("message", "ad_id"):
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN ad_id TEXT")
                    if not column_exists("message", "ad_link"):
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN ad_link TEXT")
                    if not column_exists("message", "ad_title"):
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN ad_title TEXT")
                    if not column_exists("message", "referral_json"):
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN referral_json TEXT")

                # Instagram ingestion tables (create-if-missing)
                # raw_events archive for auditing and replay
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS raw_events (
                        id INTEGER PRIMARY KEY,
                        received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        object TEXT NOT NULL,
                        entry_id TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        sig256 TEXT NOT NULL,
                        uniq_hash TEXT UNIQUE
                    )
                    """
                )
                # ig_accounts reference
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS ig_accounts (
                        igba_id TEXT PRIMARY KEY,
                        username TEXT,
                        name TEXT,
                        profile_pic_url TEXT,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                # ig_users reference
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS ig_users (
                        ig_user_id TEXT PRIMARY KEY,
                        username TEXT,
                        name TEXT,
                        profile_pic_url TEXT,
                        last_seen_at DATETIME,
                        fetched_at DATETIME,
                        fetch_status TEXT,
                        fetch_error TEXT
                    )
                    """
                )
                # conversations table
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS conversations (
                        convo_id TEXT PRIMARY KEY,
                        igba_id TEXT NOT NULL,
                        ig_user_id TEXT NOT NULL,
                        last_message_at DATETIME NOT NULL,
                        unread_count INTEGER NOT NULL DEFAULT 0,
                        hydrated_at DATETIME,
                        UNIQUE (igba_id, ig_user_id)
                    )
                    """
                )
                # attachments table (1..N per message)
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS attachments (
                        id INTEGER PRIMARY KEY,
                        message_id INTEGER NOT NULL,
                        kind TEXT NOT NULL,
                        graph_id TEXT,
                        position INTEGER,
                        mime TEXT,
                        size_bytes INTEGER,
                        checksum_sha256 TEXT,
                        storage_path TEXT,
                        thumb_path TEXT,
                        fetched_at DATETIME,
                        fetch_status TEXT,
                        fetch_error TEXT,
                        FOREIGN KEY(message_id) REFERENCES message(id) ON DELETE CASCADE
                    )
                    """
                )
                # jobs table for lightweight queue dedupe/observability
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        id INTEGER PRIMARY KEY,
                        kind TEXT NOT NULL,
                        key TEXT NOT NULL,
                        run_after DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        max_attempts INTEGER NOT NULL DEFAULT 8,
                        payload TEXT,
                        UNIQUE (kind, key)
                    )
                    """
                )
                # Helpful indexes
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_attachments_graph_id ON attachments(graph_id)")
                except Exception:
                    pass
                # Additional indexes to speed up dashboard and common lookups
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_payment_order_id ON payment(order_id)")
                except Exception:
                    pass
                # Date-based filtering on payment reports
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_payment_date ON payment(date)")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_payment_client_id ON payment(client_id)")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_importrow_matched_client ON importrow(matched_client_id)")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_importrow_matched_order ON importrow(matched_order_id)")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_importrow_import_run ON importrow(import_run_id)")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_stockmovement_item_id ON stockmovement(item_id)")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_orderitem_order_id ON orderitem(order_id)")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_orderitem_item_id ON orderitem(item_id)")
                except Exception:
                    pass
                # Order filtering and joins
                try:
                    conn.exec_driver_sql('CREATE INDEX IF NOT EXISTS idx_order_shipment_date ON "order"(shipment_date)')
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql('CREATE INDEX IF NOT EXISTS idx_order_data_date ON "order"(data_date)')
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql('CREATE INDEX IF NOT EXISTS idx_order_client_id ON "order"(client_id)')
                except Exception:
                    pass
                # Helpful composite index for inbox (latest message per conversation)
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_message_conv_ts ON message(conversation_id, timestamp_ms)")
                except Exception:
                    pass
                # Lightweight migrations for conversations.hydrated_at (pre-existing tables)
                try:
                    rows = conn.exec_driver_sql("PRAGMA table_info('conversations')").fetchall()
                    has_hydrated = any(r[1] == 'hydrated_at' for r in rows)
                    if not has_hydrated:
                        conn.exec_driver_sql("ALTER TABLE conversations ADD COLUMN hydrated_at DATETIME")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_ig_users_fetched_at ON ig_users(fetched_at)")
                except Exception:
                    pass
            return
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            is_locked = isinstance(e, sqlite3.OperationalError) or ("database is locked" in msg)
            if is_locked and attempt < retries:
                try:
                    print(f"[DB INIT] database locked; retry {attempt}/{retries} after {backoff:.2f}s")
                except Exception:
                    pass
                time.sleep(backoff)
                backoff = min(backoff * 1.5, 5.0)
                continue
            # other errors or last attempt -> re-raise
            raise
    # exhausted retries
    raise last_err if last_err else RuntimeError("DB init failed")


@contextmanager
def get_session() -> Iterator[Session]:
	session = Session(engine)
	try:
		yield session
		session.commit()
	except Exception:
		session.rollback()
		raise
	finally:
		session.close()


def reset_db() -> None:
    """Reset DB but preserve users.

    Backs up rows from the `user` table (if it exists), recreates the DB,
    then restores the users to keep credentials intact.
    """
    # Backup existing users before dropping DB
    existing_users = []
    try:
        from .models import User  # local import to avoid circulars at module import time
        try:
            from sqlmodel import Session as _Session, select as _select
            with _Session(engine) as _sess:
                try:
                    rows = _sess.exec(_select(User)).all()
                    for u in rows:
                        existing_users.append({
                            "id": u.id,
                            "username": u.username,
                            "password_hash": u.password_hash,
                            "role": u.role,
                            "failed_attempts": u.failed_attempts,
                            "locked_until": u.locked_until,
                            "created_at": u.created_at,
                            "updated_at": u.updated_at,
                        })
                except Exception:
                    # table may not exist; ignore
                    pass
        except Exception:
            pass
    except Exception:
        # models import failed; proceed without backup
        pass

    engine.dispose()
    if DB_PATH.exists():
        DB_PATH.unlink()
    SQLModel.metadata.create_all(engine)

    # Restore users if any
    if existing_users:
        try:
            from .models import User  # re-import after re-create
            with Session(engine) as _sess:
                for data in existing_users:
                    try:
                        _sess.add(User(**data))
                    except Exception:
                        # If explicit id insertion fails, drop id and retry
                        data_no_id = dict(data)
                        data_no_id.pop("id", None)
                        _sess.add(User(**data_no_id))
                _sess.commit()
        except Exception:
            # If restore fails, continue with empty users rather than aborting reset
            pass
