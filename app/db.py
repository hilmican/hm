from contextlib import contextmanager
from typing import Iterator

from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy import text
import os
import time
import sqlite3

# Require explicit database URL; no implicit SQLite fallback
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("MYSQL_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL (or MYSQL_URL) must be set; SQLite fallback is disabled")

if DATABASE_URL.startswith("mysql+") or DATABASE_URL.startswith("mysql://"):
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
else:
    engine = create_engine(
        DATABASE_URL,
        connect_args={
            "check_same_thread": False,
            # increase sqlite busy timeout to reduce 'database is locked' errors on startup
            "timeout": float(os.getenv("SQLITE_BUSY_TIMEOUT", "30")),
        },
    )


def init_db() -> None:
    retries = int(os.getenv("DB_INIT_RETRIES", "10"))
    backoff = float(os.getenv("DB_INIT_BACKOFF", "0.5"))
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            SQLModel.metadata.create_all(engine)
            # For non-SQLite backends (e.g., MySQL), we skip SQLite-only migrations
            try:
                backend = getattr(engine.url, "get_backend_name", lambda: "")()
            except Exception:
                backend = ""
            if backend != "sqlite":
                # Minimal MySQL-safe migrations
                if backend == "mysql":
                    with engine.begin() as conn:
                        # Ensure client.merged_into_client_id exists
                        try:
                            row = conn.exec_driver_sql(
                                """
                                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'client' AND COLUMN_NAME = 'merged_into_client_id'
                                LIMIT 1
                                """
                            ).fetchone()
                            if row is None:
                                try:
                                    conn.exec_driver_sql("ALTER TABLE client ADD COLUMN merged_into_client_id INT NULL")
                                except Exception:
                                    pass
                                try:
                                    conn.exec_driver_sql("CREATE INDEX idx_client_merged_into ON client(merged_into_client_id)")
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        # Ensure raw_events table exists (MySQL)
                        try:
                            row = conn.exec_driver_sql(
                                """
                                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'raw_events'
                                LIMIT 1
                                """
                            ).fetchone()
                            if row is None:
                                conn.exec_driver_sql(
                                    """
                                    CREATE TABLE raw_events (
                                        id INT PRIMARY KEY AUTO_INCREMENT,
                                        received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                        object VARCHAR(255) NOT NULL,
                                        entry_id VARCHAR(255) NOT NULL,
                                        payload LONGTEXT NOT NULL,
                                        sig256 VARCHAR(255) NOT NULL,
                                        uniq_hash VARCHAR(255) UNIQUE
                                    )
                                    """
                                )
                        except Exception:
                            pass
                        # ai_conversations is deprecated; keep legacy tables untouched on MySQL
                        # Up-size potentially long text fields
                        try:
                            row = conn.exec_driver_sql(
                                """
                                SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'importrow' AND COLUMN_NAME = 'mapped_json'
                                """
                            ).fetchone()
                            if row is not None:
                                dtype = str(row[0]).lower()
                                maxlen = row[1]
                                if dtype in ("varchar", "char") or dtype == "text":
                                    conn.exec_driver_sql("ALTER TABLE importrow MODIFY COLUMN mapped_json LONGTEXT")
                        except Exception:
                            pass
                        try:
                            row = conn.exec_driver_sql(
                                """
                                SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'importrow' AND COLUMN_NAME = 'message'
                                """
                            ).fetchone()
                            if row is not None:
                                dtype = str(row[0]).lower()
                                if dtype in ("varchar", "char"):
                                    conn.exec_driver_sql("ALTER TABLE importrow MODIFY COLUMN message TEXT")
                        except Exception:
                            pass
                        # Ensure message.timestamp_ms is BIGINT to hold ms since epoch
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
                        # Ensure message.raw_json is LONGTEXT to avoid truncation
                        try:
                            row = conn.exec_driver_sql(
                                """
                                SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'message' AND COLUMN_NAME = 'raw_json'
                                """
                            ).fetchone()
                            if row is not None:
                                dtype = str(row[0]).lower()
                                # if varchar/char or TEXT, upgrade to LONGTEXT
                                if dtype in ("varchar", "char", "text", "tinytext", "mediumtext"):
                                    conn.exec_driver_sql("ALTER TABLE message MODIFY COLUMN raw_json LONGTEXT")
                        except Exception:
                            pass
                        # Ensure message.referral_json is LONGTEXT as well (ads context blobs can be large)
                        try:
                            row = conn.exec_driver_sql(
                                """
                                SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'message' AND COLUMN_NAME = 'referral_json'
                                """
                            ).fetchone()
                            if row is not None:
                                dtype = str(row[0]).lower()
                                if dtype in ("varchar", "char", "text", "tinytext", "mediumtext"):
                                    conn.exec_driver_sql("ALTER TABLE message MODIFY COLUMN referral_json LONGTEXT")
                        except Exception:
                            pass
                        # Ensure new ad metadata columns exist (MySQL)
                        try:
                            rows = conn.exec_driver_sql(
                                """
                                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'message'
                                """
                            ).fetchall()
                            have_cols = {str(r[0]).lower() for r in rows or []}
                            if 'ad_image_url' not in have_cols:
                                conn.exec_driver_sql("ALTER TABLE message ADD COLUMN ad_image_url TEXT NULL")
                            if 'ad_name' not in have_cols:
                                conn.exec_driver_sql("ALTER TABLE message ADD COLUMN ad_name TEXT NULL")
                            # Ensure AI lifecycle columns exist
                            if 'ai_status' not in have_cols:
                                conn.exec_driver_sql("ALTER TABLE message ADD COLUMN ai_status VARCHAR(16) NULL")
                            if 'ai_json' not in have_cols:
                                conn.exec_driver_sql("ALTER TABLE message ADD COLUMN ai_json LONGTEXT NULL")
                        except Exception:
                            pass
                        # Ensure order.notes is LONGTEXT to prevent overflow from appended notes
                        try:
                            row = conn.exec_driver_sql(
                                """
                                SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'order' AND COLUMN_NAME = 'notes'
                                """
                            ).fetchone()
                            if row is not None:
                                dtype = str(row[0]).lower()
                                if dtype in ("varchar", "char", "text", "tinytext", "mediumtext"):
                                    conn.exec_driver_sql("ALTER TABLE `order` MODIFY COLUMN notes LONGTEXT NULL")
                        except Exception:
                            pass
                        # Ensure ig_ai_run table exists (MySQL)
                        try:
                            conn.exec_driver_sql(
                                """
                                CREATE TABLE IF NOT EXISTS ig_ai_run (
                                    id INTEGER PRIMARY KEY AUTO_INCREMENT,
                                    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    completed_at DATETIME NULL,
                                    cancelled_at DATETIME NULL,
                                    job_id INTEGER NULL,
                                    date_from DATE NULL,
                                    date_to DATE NULL,
                                    min_age_minutes INTEGER NOT NULL DEFAULT 60,
                                    conversations_considered INTEGER NOT NULL DEFAULT 0,
                                    conversations_processed INTEGER NOT NULL DEFAULT 0,
                                    orders_linked INTEGER NOT NULL DEFAULT 0,
                                    purchases_detected INTEGER NOT NULL DEFAULT 0,
                                    purchases_unlinked INTEGER NOT NULL DEFAULT 0,
                                    errors_json LONGTEXT NULL
                                )
                                """
                            )
                        except Exception:
                            pass
                        # Optional history of AI results per conversation/run
                        try:
                            conn.exec_driver_sql(
                                """
                                CREATE TABLE IF NOT EXISTS ig_ai_result (
                                    id INTEGER PRIMARY KEY AUTO_INCREMENT,
                                    convo_id VARCHAR(128) NOT NULL,
                                    run_id INTEGER NOT NULL,
                                    status VARCHAR(32) NULL,
                                    ai_json LONGTEXT NULL,
                                    linked_order_id INTEGER NULL,
                                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    INDEX idx_ig_ai_res_convo (convo_id),
                                    INDEX idx_ig_ai_res_run (run_id)
                                )
                                """
                            )
                        except Exception:
                            pass
                        # Dedicated debug runs per conversation
                        try:
                            conn.exec_driver_sql(
                                """
                                CREATE TABLE IF NOT EXISTS ig_ai_debug_run (
                                    id INTEGER PRIMARY KEY AUTO_INCREMENT,
                                    conversation_id VARCHAR(128) NOT NULL,
                                    job_id INTEGER NULL,
                                    ai_run_id INTEGER NULL,
                                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                                    ai_model VARCHAR(128) NULL,
                                    system_prompt LONGTEXT NULL,
                                    user_prompt LONGTEXT NULL,
                                    raw_response LONGTEXT NULL,
                                    extracted_json LONGTEXT NULL,
                                    logs_json LONGTEXT NULL,
                                    error_message LONGTEXT NULL,
                                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    started_at DATETIME NULL,
                                    completed_at DATETIME NULL,
                                    INDEX idx_ig_ai_debug_convo (conversation_id),
                                    INDEX idx_ig_ai_debug_status (status),
                                    INDEX idx_ig_ai_debug_job (job_id),
                                    INDEX idx_ig_ai_debug_run (ai_run_id)
                                )
                                """
                            )
                        except Exception:
                            pass
                        # Ensure ig_accounts table exists (MySQL) used by enrichers
                        try:
                            conn.exec_driver_sql(
                                """
                                CREATE TABLE IF NOT EXISTS ig_accounts (
                                    igba_id VARCHAR(64) PRIMARY KEY,
                                    username TEXT NULL,
                                    name TEXT NULL,
                                    profile_pic_url TEXT NULL,
                                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                                )
                                """
                            )
                        except Exception:
                            pass
                        # ads cache table (MySQL)
                        try:
                            conn.exec_driver_sql(
                                """
                                CREATE TABLE IF NOT EXISTS ads (
                                    ad_id VARCHAR(128) PRIMARY KEY,
                                    name TEXT NULL,
                                    image_url TEXT NULL,
                                    link TEXT NULL,
                                    fetch_status TEXT NULL,
                                    fetch_error TEXT NULL,
                                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                                )
                                """
                            )
                        except Exception:
                            pass
                        # ads to product mapping table (MySQL)
                        try:
                            conn.exec_driver_sql(
                                """
                                CREATE TABLE IF NOT EXISTS ads_products (
                                    ad_id VARCHAR(128) PRIMARY KEY,
                                    product_id INTEGER NULL,
                                    sku VARCHAR(128) NULL,
                                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    INDEX idx_ads_products_product (product_id),
                                    INDEX idx_ads_products_sku (sku)
                                )
                                """
                            )
                        except Exception:
                            pass
                        # Stories cache and mapping (MySQL)
                        try:
                            conn.exec_driver_sql(
                                """
                                CREATE TABLE IF NOT EXISTS stories (
                                    story_id VARCHAR(128) PRIMARY KEY,
                                    url TEXT NULL,
                                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                                )
                                """
                            )
                        except Exception:
                            pass
                        try:
                            conn.exec_driver_sql(
                                """
                                CREATE TABLE IF NOT EXISTS stories_products (
                                    story_id VARCHAR(128) PRIMARY KEY,
                                    product_id INTEGER NULL,
                                    sku VARCHAR(128) NULL,
                                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    INDEX idx_stories_products_product (product_id),
                                    INDEX idx_stories_products_sku (sku)
                                )
                                """
                            )
                        except Exception:
                            pass
                        # Ensure message has story_id/story_url (MySQL)
                        try:
                            rows = conn.exec_driver_sql(
                                """
                                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'message'
                                """
                            ).fetchall()
                            have_cols = {str(r[0]).lower() for r in rows or []}
                            if 'story_id' not in have_cols:
                                conn.exec_driver_sql("ALTER TABLE message ADD COLUMN story_id VARCHAR(128) NULL")
                            if 'story_url' not in have_cols:
                                conn.exec_driver_sql("ALTER TABLE message ADD COLUMN story_url TEXT NULL")
                        except Exception:
                            pass
                        # latest_messages deprecated: creation removed
                        # conversations AI/contact columns now live on ig_users; only ensure
                        # graph_conversation_id and useful indexes exist on conversations.
                        try:
                            rows = conn.exec_driver_sql(
                                """
                                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'conversations'
                                """
                            ).fetchall()
                            have_cols = {str(r[0]).lower() for r in rows or []}
                            if 'graph_conversation_id' not in have_cols:
                                conn.exec_driver_sql("ALTER TABLE conversations ADD COLUMN graph_conversation_id VARCHAR(128) NULL")
                            else:
                                # Widen graph_conversation_id if too short for full Graph CIDs
                                try:
                                    row_len = conn.exec_driver_sql(
                                        """
                                        SELECT CHARACTER_MAXIMUM_LENGTH
                                        FROM INFORMATION_SCHEMA.COLUMNS
                                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'conversations' AND COLUMN_NAME = 'graph_conversation_id'
                                        """
                                    ).fetchone()
                                    maxlen = int(row_len[0]) if row_len and row_len[0] is not None else None
                                    if maxlen is not None and maxlen < 256:
                                        conn.exec_driver_sql("ALTER TABLE conversations MODIFY COLUMN graph_conversation_id VARCHAR(512) NULL")
                                except Exception:
                                    pass
                            # helpful indexes for inbox/AI
                            try:
                                conn.exec_driver_sql("CREATE INDEX idx_conversations_last_message_at ON conversations(last_message_at)")
                            except Exception:
                                pass
                            try:
                                conn.exec_driver_sql("CREATE INDEX idx_message_conv_ts ON message(conversation_id, timestamp_ms)")
                            except Exception:
                                pass
                            try:
                                conn.exec_driver_sql("CREATE INDEX idx_ig_users_username ON ig_users(username)")
                            except Exception:
                                pass
                        except Exception:
                            pass
                        # Ensure `order`.ig_conversation_id exists for linking back to IG threads
                        try:
                            rows = conn.exec_driver_sql(
                                """
                                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'order'
                                """
                            ).fetchall()
                            have_cols = {str(r[0]).lower() for r in rows or []}
                            if 'ig_conversation_id' not in have_cols:
                                conn.exec_driver_sql("ALTER TABLE `order` ADD COLUMN ig_conversation_id VARCHAR(128) NULL")
                            try:
                                conn.exec_driver_sql("CREATE INDEX idx_order_ig_conversation_id ON `order`(ig_conversation_id)")
                            except Exception:
                                pass
                            # Ensure paid_by_bank_transfer exists (MySQL bool -> TINYINT)
                            if 'paid_by_bank_transfer' not in have_cols:
                                try:
                                    conn.exec_driver_sql("ALTER TABLE `order` ADD COLUMN paid_by_bank_transfer TINYINT(1) NULL DEFAULT 0")
                                except Exception:
                                    pass
                                try:
                                    conn.exec_driver_sql("CREATE INDEX idx_order_paid_by_bank_transfer ON `order`(paid_by_bank_transfer)")
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        # ai_conversations is deprecated; new deployments should use conversations only
                        # Ensure product AI prompt fields exist (MySQL)
                        try:
                            rows = conn.exec_driver_sql(
                                """
                                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'product'
                                """
                            ).fetchall()
                            have_cols = {str(r[0]).lower() for r in rows or []}
                            if 'ai_system_msg' not in have_cols:
                                conn.exec_driver_sql("ALTER TABLE product ADD COLUMN ai_system_msg LONGTEXT NULL")
                            if 'ai_prompt_msg' not in have_cols:
                                conn.exec_driver_sql("ALTER TABLE product ADD COLUMN ai_prompt_msg LONGTEXT NULL")
                            if 'ai_tags' not in have_cols:
                                conn.exec_driver_sql("ALTER TABLE product ADD COLUMN ai_tags JSON NULL")
                        except Exception:
                            pass
                        # Message timestamp and composite index to accelerate counts and latest-per-conversation lookups
                        try:
                            conn.exec_driver_sql("CREATE INDEX idx_message_ts ON message(timestamp_ms)")
                        except Exception:
                            pass
                        try:
                            conn.exec_driver_sql("CREATE INDEX idx_message_conv_ts ON message(conversation_id, timestamp_ms)")
                        except Exception:
                            pass
                        # Normalize blank strings to NULL so COALESCE logic behaves consistently
                        try:
                            conn.exec_driver_sql("UPDATE conversations SET contact_name=NULL WHERE contact_name=''")
                        except Exception:
                            pass
                        try:
                            conn.exec_driver_sql("UPDATE conversations SET contact_phone=NULL WHERE contact_phone=''")
                        except Exception:
                            pass
                        try:
                            conn.exec_driver_sql("UPDATE conversations SET contact_address=NULL WHERE contact_address=''")
                        except Exception:
                            pass
                        try:
                            conn.exec_driver_sql("UPDATE conversations SET ai_json=NULL WHERE ai_json=''")
                        except Exception:
                            pass
                        try:
                            conn.exec_driver_sql("UPDATE `order` SET ig_conversation_id=NULL WHERE ig_conversation_id=''")
                        except Exception:
                            pass
                        # Ensure jobs.id is AUTO_INCREMENT and unique(kind,key) exists (MySQL)
                        try:
                            row = conn.exec_driver_sql(
                                """
                                SELECT COLUMN_KEY, EXTRA FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'jobs' AND COLUMN_NAME = 'id'
                                """
                            ).fetchone()
                            # Make id auto-increment primary key when missing
                            if row is not None:
                                colkey = str(row[0] or '').lower()
                                extra = str(row[1] or '').lower()
                                if 'auto_increment' not in extra:
                                    # Try a series of compatible ALTERs across MySQL variants
                                    try:
                                        conn.exec_driver_sql("ALTER TABLE `jobs` MODIFY COLUMN `id` INT NOT NULL")
                                    except Exception:
                                        pass
                                    try:
                                        conn.exec_driver_sql("ALTER TABLE `jobs` DROP PRIMARY KEY")
                                    except Exception:
                                        pass
                                    try:
                                        conn.exec_driver_sql("ALTER TABLE `jobs` CHANGE `id` `id` INT NOT NULL AUTO_INCREMENT")
                                    except Exception:
                                        # Fallback single-step
                                        try:
                                            conn.exec_driver_sql("ALTER TABLE `jobs` MODIFY COLUMN `id` INT NOT NULL AUTO_INCREMENT")
                                        except Exception:
                                            pass
                                    try:
                                        conn.exec_driver_sql("ALTER TABLE `jobs` ADD PRIMARY KEY (`id`)")
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                        try:
                            # Add UNIQUE(kind,key) if missing
                            idx = conn.exec_driver_sql(
                                """
                                SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'jobs' AND CONSTRAINT_TYPE='UNIQUE'
                                """
                            ).fetchall()
                            have = {str(r[0]) for r in (idx or [])}
                            if 'uq_jobs_kind_key' not in have:
                                conn.exec_driver_sql("ALTER TABLE `jobs` ADD CONSTRAINT `uq_jobs_kind_key` UNIQUE (`kind`,`key`)")
                        except Exception:
                            pass
                        # Ensure user.preferred_language exists
                        try:
                            row = conn.exec_driver_sql(
                                """
                                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user' AND COLUMN_NAME = 'preferred_language'
                                LIMIT 1
                                """
                            ).fetchone()
                            if row is None:
                                try:
                                    conn.exec_driver_sql("ALTER TABLE user ADD COLUMN preferred_language VARCHAR(8) NULL")
                                except Exception:
                                    pass
                                try:
                                    conn.exec_driver_sql("CREATE INDEX idx_user_pref_lang ON user(preferred_language)")
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        # AI Shadow tables (MySQL)
                        try:
                            conn.exec_driver_sql(
                                """
                                CREATE TABLE IF NOT EXISTS ai_shadow_state (
                                    convo_id VARCHAR(128) PRIMARY KEY,
                                    last_inbound_ms BIGINT NULL,
                                    next_attempt_at DATETIME NULL,
                                    postpone_count INT NOT NULL DEFAULT 0,
                                    status VARCHAR(32) NULL,
                                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    INDEX idx_ai_shadow_next_attempt (next_attempt_at),
                                    INDEX idx_ai_shadow_status (status),
                                    INDEX idx_ai_shadow_postpone (postpone_count)
                                )
                                """
                            )
                        except Exception:
                            pass
                        try:
                            conn.exec_driver_sql(
                                """
                                CREATE TABLE IF NOT EXISTS ai_shadow_reply (
                                    id INTEGER PRIMARY KEY AUTO_INCREMENT,
                                    convo_id VARCHAR(128) NOT NULL,
                                    reply_text LONGTEXT NULL,
                                    model VARCHAR(128) NULL,
                                    confidence DOUBLE NULL,
                                    reason VARCHAR(128) NULL,
                                    json_meta LONGTEXT NULL,
                                    attempt_no INT NULL DEFAULT 0,
                                    status VARCHAR(32) NULL,
                                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    INDEX idx_ai_shadow_reply_convo (convo_id),
                                    INDEX idx_ai_shadow_reply_created (created_at),
                                    INDEX idx_ai_shadow_reply_status (status)
                                )
                                """
                            )
                        except Exception:
                            pass
                return
            # lightweight migrations for existing SQLite DBs
            with engine.begin() as conn:
                # Apply SQLite PRAGMAs once at startup if enabled
                # SQLite PRAGMAs are no-ops in MySQL deployments; kept only for legacy compatibility.
                def column_exists(table: str, column: str) -> bool:
                    try:
                        backend = getattr(engine.url, "get_backend_name", lambda: "")()
                    except Exception:
                        backend = ""
                    if backend == "sqlite":
                        rows = conn.exec_driver_sql(f"PRAGMA table_info('{table}')").fetchall()
                        # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
                        return any(r[1] == column for r in rows)
                    # MySQL / others: use INFORMATION_SCHEMA when available
                    try:
                        dbname = conn.exec_driver_sql("SELECT DATABASE()").fetchone()[0]
                        q = (
                            "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
                            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s LIMIT 1"
                        )
                        res = conn.exec_driver_sql(q, (dbname, table, column)).fetchone()
                        return bool(res)
                    except Exception:
                        # Fallback: assume not exists to avoid crashing during startup
                        return False

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

                # Order.return_or_switch_date (DATE)
                if not column_exists("order", "return_or_switch_date"):
                    conn.exec_driver_sql('ALTER TABLE "order" ADD COLUMN return_or_switch_date DATE')
                # Optional helpful index for filtering
                try:
                    conn.exec_driver_sql('CREATE INDEX IF NOT EXISTS idx_order_return_or_switch_date ON "order"(return_or_switch_date)')
                except Exception:
                    pass

                # Order.paid_by_bank_transfer (INTEGER as boolean)
                if not column_exists("order", "paid_by_bank_transfer"):
                    conn.exec_driver_sql('ALTER TABLE "order" ADD COLUMN paid_by_bank_transfer INTEGER DEFAULT 0')
                try:
                    conn.exec_driver_sql('CREATE INDEX IF NOT EXISTS idx_order_paid_by_bank_transfer ON "order"(paid_by_bank_transfer)')
                except Exception:
                    pass

                # Order.ig_conversation_id (TEXT)
                if not column_exists("order", "ig_conversation_id"):
                    conn.exec_driver_sql('ALTER TABLE "order" ADD COLUMN ig_conversation_id TEXT')
                try:
                    conn.exec_driver_sql('CREATE INDEX IF NOT EXISTS idx_order_ig_conversation_id ON "order"(ig_conversation_id)')
                except Exception:
                    pass

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
                if not column_exists("user", "preferred_language"):
                    try:
                        conn.exec_driver_sql("ALTER TABLE user ADD COLUMN preferred_language TEXT")
                    except Exception:
                        pass

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
                    if not column_exists("message", "ad_image_url"):
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN ad_image_url TEXT")
                    if not column_exists("message", "ad_name"):
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN ad_name TEXT")
                    if not column_exists("message", "referral_json"):
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN referral_json TEXT")

                # Instagram ingestion tables (create-if-missing)
                # raw_events archive for auditing and replay
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS raw_events (
                        id INTEGER PRIMARY KEY AUTO_INCREMENT,
                        received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        object VARCHAR(255) NOT NULL,
                        entry_id VARCHAR(255) NOT NULL,
                        payload LONGTEXT NOT NULL,
                        sig256 VARCHAR(255) NOT NULL,
                        uniq_hash VARCHAR(255) UNIQUE
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
                # ads cache
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS ads (
                        ad_id TEXT PRIMARY KEY,
                        name TEXT,
                        image_url TEXT,
                        link TEXT,
                        fetch_status TEXT,
                        fetch_error TEXT,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                # ai_conversations is deprecated on SQLite as well; keep legacy tables untouched
                # Ensure conversations AI/contact columns exist (idempotent ALTERs)
                try:
                    rows = conn.exec_driver_sql("PRAGMA table_info('conversations')").fetchall()
                    have = {r[1] for r in rows}
                    add_cols: list[tuple[str, str]] = []
                    if 'contact_name' not in have:
                        add_cols.append(("contact_name", "TEXT"))
                    if 'contact_phone' not in have:
                        add_cols.append(("contact_phone", "TEXT"))
                    if 'contact_address' not in have:
                        add_cols.append(("contact_address", "TEXT"))
                    if 'ai_status' not in have:
                        add_cols.append(("ai_status", "TEXT"))
                    if 'ai_json' not in have:
                        add_cols.append(("ai_json", "TEXT"))
                    if 'ai_processed_at' not in have:
                        add_cols.append(("ai_processed_at", "DATETIME"))
                    if 'ai_process_time' not in have:
                        add_cols.append(("ai_process_time", "DATETIME"))
                    if 'linked_order_id' not in have:
                        add_cols.append(("linked_order_id", "INTEGER"))
                    if 'ai_run_id' not in have:
                        add_cols.append(("ai_run_id", "INTEGER"))
                    if 'graph_conversation_id' not in have:
                        add_cols.append(("graph_conversation_id", "TEXT"))
                    for name, typ in add_cols:
                        try:
                            conn.exec_driver_sql(f"ALTER TABLE conversations ADD COLUMN {name} {typ}")
                        except Exception:
                            pass
                    # helpful indexes
                    try:
                        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_conversations_ai_processed ON conversations(ai_processed_at)")
                    except Exception:
                        pass
                    try:
                        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_conversations_ai_process_time ON conversations(ai_process_time)")
                    except Exception:
                        pass
                except Exception:
                    pass
                # attachments table (1..N per message)
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS attachments (
                        id INTEGER PRIMARY KEY AUTO_INCREMENT,
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
                # ig_ai_run table for batch tracking
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS ig_ai_run (
                        id INTEGER PRIMARY KEY,
                        started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        completed_at DATETIME,
                        cancelled_at DATETIME,
                        job_id INTEGER,
                        date_from DATE,
                        date_to DATE,
                        min_age_minutes INTEGER,
                        conversations_considered INTEGER DEFAULT 0,
                        conversations_processed INTEGER DEFAULT 0,
                        orders_linked INTEGER DEFAULT 0,
                        purchases_detected INTEGER DEFAULT 0,
                        purchases_unlinked INTEGER DEFAULT 0,
                        errors_json TEXT
                    )
                    """
                )
                # Ensure columns exist for older DBs
                # Optional: history of AI results per conversation/run (SQLite)
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS ig_ai_result (
                        id INTEGER PRIMARY KEY,
                        convo_id TEXT NOT NULL,
                        run_id INTEGER NOT NULL,
                        status TEXT,
                        ai_json TEXT,
                        linked_order_id INTEGER,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                try:
                    rows = conn.exec_driver_sql("PRAGMA table_info('ig_ai_run')").fetchall()
                    have = {r[1] for r in rows}
                    if 'cancelled_at' not in have:
                        conn.exec_driver_sql("ALTER TABLE ig_ai_run ADD COLUMN cancelled_at DATETIME")
                    if 'job_id' not in have:
                        conn.exec_driver_sql("ALTER TABLE ig_ai_run ADD COLUMN job_id INTEGER")
                except Exception:
                    pass
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
                # StockMovement.reason column (TEXT)
                if not column_exists("stockmovement", "reason"):
                    try:
                        conn.exec_driver_sql("ALTER TABLE stockmovement ADD COLUMN reason TEXT")
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
                # Backfill status rename stitched -> switched
                try:
                    conn.exec_driver_sql("UPDATE \"order\" SET status='switched' WHERE status='stitched'")
                except Exception:
                    pass
                # Helpful composite index for inbox (latest message per conversation)
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_message_conv_ts ON message(conversation_id, timestamp_ms)")
                except Exception:
                    pass
                # Username index for labels
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_ig_users_username ON ig_users(username)")
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
                # Helpful indexes for IG AI processing
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_conversations_last_message_at ON conversations(last_message_at)")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_ig_users_fetched_at ON ig_users(fetched_at)")
                except Exception:
                    pass
                # AI Shadow tables (SQLite)
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS ai_shadow_state (
                        convo_id TEXT PRIMARY KEY,
                        last_inbound_ms BIGINT,
                        next_attempt_at DATETIME,
                        postpone_count INTEGER DEFAULT 0,
                        status TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_ai_shadow_next_attempt ON ai_shadow_state(next_attempt_at)")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_ai_shadow_status ON ai_shadow_state(status)")
                except Exception:
                    pass
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS ai_shadow_reply (
                        id INTEGER PRIMARY KEY,
                        convo_id TEXT NOT NULL,
                        reply_text TEXT,
                        model TEXT,
                        confidence REAL,
                        reason TEXT,
                        json_meta TEXT,
                        attempt_no INTEGER DEFAULT 0,
                        status TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_ai_shadow_reply_convo ON ai_shadow_reply(convo_id)")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_ai_shadow_reply_created ON ai_shadow_reply(created_at)")
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_ai_shadow_reply_status ON ai_shadow_reply(status)")
                except Exception:
                    pass
                # latest_messages table deprecated: creation removed
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
    session = Session(engine, expire_on_commit=False)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
