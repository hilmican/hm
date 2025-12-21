from contextlib import contextmanager
from typing import Iterator

from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy import text, event
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import Engine
import os
import time

# Require MySQL database URL
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("MYSQL_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL (or MYSQL_URL) must be set; MySQL is required")

if not (DATABASE_URL.startswith("mysql+") or DATABASE_URL.startswith("mysql://")):
    raise RuntimeError("Only MySQL databases are supported. DATABASE_URL must start with mysql+ or mysql://")

# Connection pool settings to prevent too many connections
pool_size = int(os.getenv("DB_POOL_SIZE", "10"))
max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "5"))
pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", "30"))
pool_recycle = int(os.getenv("DB_POOL_RECYCLE", "3600"))  # Recycle connections after 1 hour

# Query timeout in seconds (default 30 seconds)
query_timeout = int(os.getenv("DB_QUERY_TIMEOUT", "30"))

# Transaction isolation level (READ COMMITTED reduces lock contention)
isolation_level = os.getenv("DB_ISOLATION_LEVEL", "READ COMMITTED")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=pool_size,
    max_overflow=max_overflow,
    pool_timeout=pool_timeout,
    pool_recycle=pool_recycle,
    connect_args={
        "connect_timeout": 10,
        "read_timeout": query_timeout,
        "write_timeout": query_timeout,
        "init_command": f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}",
    },
)

# Set isolation level on each connection
@event.listens_for(Engine, "connect")
def set_isolation_level(dbapi_conn, connection_record):
    """Set transaction isolation level on each new connection."""
    try:
        cursor = dbapi_conn.cursor()
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
        cursor.close()
    except Exception:
        # Best-effort; don't fail if isolation level can't be set
        pass


def init_db() -> None:
    retries = int(os.getenv("DB_INIT_RETRIES", "10"))
    backoff = float(os.getenv("DB_INIT_BACKOFF", "0.5"))
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            # Handle concurrent DDL (e.g., table creation) gracefully: MySQL error 1684 (concurrent DDL) and 1050 (table already exists)
            for _ in range(3):
                try:
                    SQLModel.metadata.create_all(engine)
                    break
                except OperationalError as oe:
                    code = None
                    try:
                        code = oe.orig.args[0] if oe.orig and hasattr(oe.orig, "args") else None
                    except Exception:
                        code = None
                    if code == 1684:
                        # Concurrent DDL operation - retry after delay
                        time.sleep(2.0)
                        continue
                    elif code == 1050:
                        # Table already exists - this is fine, continue with initialization
                        break
                    raise
            else:
                # If we exhaust retries without break, re-raise last error
                raise last_err or RuntimeError("create_all failed after retries")
            # MySQL-only migrations
            with engine.begin() as conn:
                def _exec_ddl(sql: str) -> None:
                    try:
                        conn.exec_driver_sql(sql)
                    except Exception:
                        pass

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
                # Ensure ai_order_candidates detection bookkeeping columns exist
                try:
                    rows = conn.exec_driver_sql(
                        """
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ai_order_candidates'
                        """
                    ).fetchall()
                    have_cols = {str(r[0]).lower() for r in rows or []}
                    if 'last_detected_at' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE ai_order_candidates ADD COLUMN last_detected_at DATETIME NULL")
                        conn.exec_driver_sql("CREATE INDEX idx_ai_order_candidates_last_detected_at ON ai_order_candidates(last_detected_at)")
                    if 'last_detected_message_ts_ms' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE ai_order_candidates ADD COLUMN last_detected_message_ts_ms BIGINT NULL")
                        conn.exec_driver_sql("CREATE INDEX idx_ai_order_candidates_last_detected_msg ON ai_order_candidates(last_detected_message_ts_ms)")
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
                    # Ensure product_id column exists for per-message product focus tracking
                    if 'product_id' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN product_id INT NULL")
                        conn.exec_driver_sql("ALTER TABLE message ADD INDEX idx_message_product_id (product_id)")
                        conn.exec_driver_sql("ALTER TABLE message ADD FOREIGN KEY (product_id) REFERENCES product(id) ON DELETE SET NULL")
                    # Ensure message_category column exists for message categorization
                    if 'message_category' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN message_category VARCHAR(32) NULL")
                        conn.exec_driver_sql("ALTER TABLE message ADD INDEX idx_message_category (message_category)")
                    # Ensure sender_type column exists for AI vs human detection
                    if 'sender_type' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE message ADD COLUMN sender_type VARCHAR(16) NULL")
                        conn.exec_driver_sql("ALTER TABLE message ADD INDEX idx_message_sender_type (sender_type)")
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
                # ads cache table (MySQL) - unified for ads and posts
                try:
                    conn.exec_driver_sql(
                        """
                        CREATE TABLE IF NOT EXISTS ads (
                            ad_id VARCHAR(128) PRIMARY KEY,
                            link_type VARCHAR(16) NOT NULL DEFAULT 'ad',
                            name TEXT NULL,
                            image_url TEXT NULL,
                            link TEXT NULL,
                            fetch_status TEXT NULL,
                            fetch_error TEXT NULL,
                            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_ads_link_type (link_type)
                        )
                        """
                    )
                except Exception:
                    pass
                # Add link_type column if it doesn't exist (migration support)
                try:
                    rows = conn.exec_driver_sql(
                        """
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ads' AND COLUMN_NAME = 'link_type'
                        """
                    ).fetchall()
                    if not rows:
                        conn.exec_driver_sql("ALTER TABLE ads ADD COLUMN link_type VARCHAR(16) NOT NULL DEFAULT 'ad'")
                        conn.exec_driver_sql("CREATE INDEX idx_ads_link_type ON ads(link_type)")
                except Exception:
                    pass
                # ads to product mapping table (MySQL) - unified for ads and posts
                try:
                    conn.exec_driver_sql(
                        """
                        CREATE TABLE IF NOT EXISTS ads_products (
                            ad_id VARCHAR(128) PRIMARY KEY,
                            link_type VARCHAR(16) NOT NULL DEFAULT 'ad',
                            product_id INTEGER NULL,
                            sku VARCHAR(128) NULL,
                            auto_linked TINYINT(1) NOT NULL DEFAULT 0,
                            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_ads_products_product (product_id),
                            INDEX idx_ads_products_sku (sku),
                            INDEX idx_ads_products_auto_linked (auto_linked),
                            INDEX idx_ads_products_link_type (link_type)
                        )
                        """
                    )
                except Exception:
                    pass
                # Add link_type column if it doesn't exist (migration support)
                try:
                    rows = conn.exec_driver_sql(
                        """
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ads_products' AND COLUMN_NAME = 'link_type'
                        """
                    ).fetchall()
                    if not rows:
                        conn.exec_driver_sql("ALTER TABLE ads_products ADD COLUMN link_type VARCHAR(16) NOT NULL DEFAULT 'ad'")
                        conn.exec_driver_sql("CREATE INDEX idx_ads_products_link_type ON ads_products(link_type)")
                except Exception:
                    pass
                # Add auto_linked column to ads_products if it doesn't exist
                try:
                    rows = conn.exec_driver_sql(
                        """
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ads_products'
                        """
                    ).fetchall()
                    have_cols = {str(r[0]).lower() for r in rows or []}
                    if 'auto_linked' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE ads_products ADD COLUMN auto_linked TINYINT(1) NOT NULL DEFAULT 0")
                        conn.exec_driver_sql("CREATE INDEX idx_ads_products_auto_linked ON ads_products(auto_linked)")
                except Exception:
                    pass
                # Stories cache and mapping (MySQL)
                try:
                    conn.exec_driver_sql(
                        """
                        CREATE TABLE IF NOT EXISTS stories (
                            story_id VARCHAR(128) PRIMARY KEY,
                            url TEXT NULL,
                            media_path TEXT NULL,
                            media_thumb_path TEXT NULL,
                            media_mime VARCHAR(64) NULL,
                            media_checksum VARCHAR(128) NULL,
                            media_fetched_at DATETIME NULL,
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
                            auto_linked TINYINT(1) NOT NULL DEFAULT 0,
                            confidence FLOAT NULL,
                            ai_result_json LONGTEXT NULL,
                            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_stories_products_product (product_id),
                            INDEX idx_stories_products_sku (sku)
                        )
                        """
                    )
                except Exception:
                    pass
                # Add media metadata columns on stories table if missing
                try:
                    rows = conn.exec_driver_sql(
                        """
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'stories'
                        """
                    ).fetchall()
                    have_cols = {str(r[0]).lower() for r in rows or []}
                    if 'media_path' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE stories ADD COLUMN media_path TEXT NULL")
                    if 'media_thumb_path' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE stories ADD COLUMN media_thumb_path TEXT NULL")
                    if 'media_mime' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE stories ADD COLUMN media_mime VARCHAR(64) NULL")
                    if 'media_checksum' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE stories ADD COLUMN media_checksum VARCHAR(128) NULL")
                    if 'media_fetched_at' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE stories ADD COLUMN media_fetched_at DATETIME NULL")
                except Exception:
                    pass
                # Ensure stories_products has AI metadata columns
                try:
                    rows = conn.exec_driver_sql(
                        """
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'stories_products'
                        """
                    ).fetchall()
                    have_cols = {str(r[0]).lower() for r in rows or []}
                    if 'auto_linked' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE stories_products ADD COLUMN auto_linked TINYINT(1) NOT NULL DEFAULT 0")
                    if 'confidence' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE stories_products ADD COLUMN confidence FLOAT NULL")
                    if 'ai_result_json' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE stories_products ADD COLUMN ai_result_json LONGTEXT NULL")
                except Exception:
                    pass
                # Posts cache and mapping (MySQL)
                try:
                    conn.exec_driver_sql(
                        """
                        CREATE TABLE IF NOT EXISTS posts (
                            post_id VARCHAR(128) PRIMARY KEY,
                            ig_post_media_id VARCHAR(128) NULL,
                            title TEXT NULL,
                            url TEXT NULL,
                            message_id INTEGER NULL,
                            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_posts_media_id (ig_post_media_id),
                            INDEX idx_posts_message (message_id)
                        )
                        """
                    )
                except Exception:
                    pass
                try:
                    conn.exec_driver_sql(
                        """
                        CREATE TABLE IF NOT EXISTS posts_products (
                            post_id VARCHAR(128) PRIMARY KEY,
                            product_id INTEGER NULL,
                            sku VARCHAR(128) NULL,
                            auto_linked TINYINT(1) NOT NULL DEFAULT 0,
                            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_posts_products_product (product_id),
                            INDEX idx_posts_products_sku (sku),
                            INDEX idx_posts_products_auto_linked (auto_linked)
                        )
                        """
                    )
                except Exception:
                    pass
                # Add auto_linked column to posts_products if it doesn't exist
                try:
                    rows = conn.exec_driver_sql(
                        """
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'posts_products'
                        """
                    ).fetchall()
                    have_cols = {str(r[0]).lower() for r in rows or []}
                    if 'auto_linked' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE posts_products ADD COLUMN auto_linked TINYINT(1) NOT NULL DEFAULT 0")
                        conn.exec_driver_sql("CREATE INDEX idx_posts_products_auto_linked ON posts_products(auto_linked)")
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
                    # Ensure partial payment merging fields exist (MySQL)
                    if 'merged_into_order_id' not in have_cols:
                        try:
                            conn.exec_driver_sql("ALTER TABLE `order` ADD COLUMN merged_into_order_id INT NULL")
                        except Exception:
                            pass
                        try:
                            conn.exec_driver_sql("CREATE INDEX idx_order_merged_into_order_id ON `order`(merged_into_order_id)")
                        except Exception:
                            pass
                    if 'is_partial_payment' not in have_cols:
                        try:
                            conn.exec_driver_sql("ALTER TABLE `order` ADD COLUMN is_partial_payment TINYINT(1) NULL DEFAULT 0")
                        except Exception:
                            pass
                        try:
                            conn.exec_driver_sql("CREATE INDEX idx_order_is_partial_payment ON `order`(is_partial_payment)")
                        except Exception:
                            pass
                    if 'partial_payment_group_id' not in have_cols:
                        try:
                            conn.exec_driver_sql("ALTER TABLE `order` ADD COLUMN partial_payment_group_id INT NULL")
                        except Exception:
                            pass
                        try:
                            conn.exec_driver_sql("CREATE INDEX idx_order_partial_payment_group_id ON `order`(partial_payment_group_id)")
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
                    if 'pretext_id' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE product ADD COLUMN pretext_id INT NULL")
                        conn.exec_driver_sql("CREATE INDEX idx_product_pretext_id ON product(pretext_id)")
                    if 'ai_reply_sending_enabled' not in have_cols:
                        conn.exec_driver_sql("ALTER TABLE product ADD COLUMN ai_reply_sending_enabled TINYINT(1) NOT NULL DEFAULT 1")
                except Exception:
                    pass
                # Ensure ai_pretext table exists
                try:
                    conn.exec_driver_sql(
                        """
                        CREATE TABLE IF NOT EXISTS ai_pretext (
                            id INTEGER PRIMARY KEY AUTO_INCREMENT,
                            name VARCHAR(255) NOT NULL,
                            content LONGTEXT NOT NULL,
                            is_default TINYINT(1) NOT NULL DEFAULT 0,
                            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            INDEX idx_ai_pretext_name (name),
                            INDEX idx_ai_pretext_default (is_default)
                        )
                        """
                    )
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

                # Ensure attachments table exists and attachments.id is AUTO_INCREMENT (MySQL)
                # This fixes "Field 'id' doesn't have a default value" errors when inserting attachments.
                try:
                    conn.exec_driver_sql(
                        """
                        CREATE TABLE IF NOT EXISTS attachments (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            message_id INT NOT NULL,
                            kind VARCHAR(32) NOT NULL,
                            graph_id VARCHAR(255) NULL,
                            position INT NULL,
                            mime VARCHAR(255) NULL,
                            size_bytes BIGINT NULL,
                            checksum_sha256 VARCHAR(255) NULL,
                            storage_path TEXT NULL,
                            thumb_path TEXT NULL,
                            fetched_at DATETIME NULL,
                            fetch_status VARCHAR(32) NULL,
                            fetch_error TEXT NULL,
                            INDEX idx_attachments_message_id (message_id),
                            INDEX idx_attachments_graph_id (graph_id)
                        )
                        """
                    )
                except Exception:
                    # Table may already exist with different definition; ignore and fix id below.
                    pass
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT COLUMN_KEY, EXTRA
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = 'attachments'
                          AND COLUMN_NAME = 'id'
                        """
                    ).fetchone()
                    if row is not None:
                        colkey = str(row[0] or "").lower()
                        extra = str(row[1] or "").lower()
                        if "auto_increment" not in extra:
                            # Align with SQLModel model: make id an AUTO_INCREMENT primary key.
                            try:
                                conn.exec_driver_sql("ALTER TABLE `attachments` MODIFY COLUMN `id` INT NOT NULL")
                            except Exception:
                                pass
                            try:
                                # Drop existing PK if it's on a different column; safe to ignore failures.
                                if colkey != "pri":
                                    conn.exec_driver_sql("ALTER TABLE `attachments` DROP PRIMARY KEY")
                            except Exception:
                                pass
                            try:
                                conn.exec_driver_sql(
                                    "ALTER TABLE `attachments` CHANGE `id` `id` INT NOT NULL AUTO_INCREMENT"
                                )
                            except Exception:
                                # Fallback single-step for some MySQL variants
                                try:
                                    conn.exec_driver_sql(
                                        "ALTER TABLE `attachments` MODIFY COLUMN `id` INT NOT NULL AUTO_INCREMENT"
                                    )
                                except Exception:
                                    pass
                            try:
                                conn.exec_driver_sql("ALTER TABLE `attachments` ADD PRIMARY KEY (`id`)")
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
                # AI Shadow tables (MySQL) - canonical key is conversations.id (INT)
                try:
                    conn.exec_driver_sql(
                        """
                        CREATE TABLE IF NOT EXISTS ai_shadow_state (
                            conversation_id INT PRIMARY KEY,
                            last_inbound_ms BIGINT NULL,
                            next_attempt_at DATETIME NULL,
                            postpone_count INT NOT NULL DEFAULT 0,
                            status VARCHAR(32) NULL,
                            ai_images_sent TINYINT(1) NOT NULL DEFAULT 0,
                            state_json LONGTEXT NULL,
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
                            conversation_id INT NOT NULL,
                            reply_text LONGTEXT NULL,
                            model VARCHAR(128) NULL,
                            confidence DOUBLE NULL,
                            reason VARCHAR(128) NULL,
                            json_meta LONGTEXT NULL,
                            actions_json LONGTEXT NULL,
                            state_json LONGTEXT NULL,
                            attempt_no INT NULL DEFAULT 0,
                            status VARCHAR(32) NULL,
                            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_ai_shadow_reply_conversation (conversation_id),
                            INDEX idx_ai_shadow_reply_created (created_at),
                            INDEX idx_ai_shadow_reply_status (status)
                        )
                        """
                    )
                except Exception:
                    pass
                # Backfill columns for legacy tables
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ai_shadow_state' AND COLUMN_NAME = 'ai_images_sent'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql("ALTER TABLE ai_shadow_state ADD COLUMN ai_images_sent TINYINT(1) NOT NULL DEFAULT 0")
                except Exception:
                    pass
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ai_shadow_state' AND COLUMN_NAME = 'state_json'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql("ALTER TABLE ai_shadow_state ADD COLUMN state_json LONGTEXT NULL")
                except Exception:
                    pass
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ai_shadow_reply' AND COLUMN_NAME = 'actions_json'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql("ALTER TABLE ai_shadow_reply ADD COLUMN actions_json LONGTEXT NULL")
                except Exception:
                    pass
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ai_shadow_reply' AND COLUMN_NAME = 'state_json'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql("ALTER TABLE ai_shadow_reply ADD COLUMN state_json LONGTEXT NULL")
                except Exception:
                    pass
                # Add first_reply_notified_at column to ai_shadow_state
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ai_shadow_state' AND COLUMN_NAME = 'first_reply_notified_at'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql("ALTER TABLE ai_shadow_state ADD COLUMN first_reply_notified_at DATETIME NULL")
                except Exception:
                    pass
                # Create system_settings table
                _exec_ddl(
                    """
                    CREATE TABLE IF NOT EXISTS system_settings (
                        `key` VARCHAR(128) PRIMARY KEY,
                        value TEXT NOT NULL,
                        description TEXT NULL,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                    """
                )

                # New Instagram permission tables
                _exec_ddl(
                    """
                    CREATE TABLE IF NOT EXISTS ig_profile_snapshot (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        igba_id VARCHAR(64) NOT NULL,
                        username VARCHAR(255) NULL,
                        name VARCHAR(255) NULL,
                        profile_picture_url TEXT NULL,
                        biography LONGTEXT NULL,
                        followers_count INT NULL,
                        follows_count INT NULL,
                        media_count INT NULL,
                        website VARCHAR(512) NULL,
                        refreshed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        expires_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_ig_profile_snapshot_igba (igba_id),
                        INDEX idx_ig_profile_snapshot_refreshed (refreshed_at),
                        INDEX idx_ig_profile_snapshot_expires (expires_at)
                    )
                    """
                )
                _exec_ddl(
                    """
                    CREATE TABLE IF NOT EXISTS ig_insights_snapshot (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        scope VARCHAR(32) NOT NULL,
                        subject_id VARCHAR(128) NOT NULL,
                        cache_key VARCHAR(191) NOT NULL,
                        payload_json LONGTEXT NOT NULL,
                        captured_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        expires_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_ig_insights_scope_subject (scope, subject_id),
                        INDEX idx_ig_insights_cache_key (cache_key),
                        INDEX idx_ig_insights_expires (expires_at)
                    )
                    """
                )
                _exec_ddl(
                    """
                    CREATE TABLE IF NOT EXISTS ig_scheduled_post (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        media_type VARCHAR(16) NOT NULL DEFAULT 'PHOTO',
                        caption LONGTEXT NULL,
                        media_payload_json LONGTEXT NULL,
                        scheduled_at DATETIME NULL,
                        status VARCHAR(32) NOT NULL DEFAULT 'draft',
                        error_message LONGTEXT NULL,
                        ig_container_id VARCHAR(255) NULL,
                        ig_media_id VARCHAR(255) NULL,
                        created_by_user_id INT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_ig_scheduled_post_status (status),
                        INDEX idx_ig_scheduled_post_sched (scheduled_at),
                        INDEX idx_ig_scheduled_post_creator (created_by_user_id)
                    )
                    """
                )
                _exec_ddl(
                    """
                    CREATE TABLE IF NOT EXISTS ig_publishing_audit (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        scheduled_post_id INT NOT NULL,
                        action VARCHAR(32) NOT NULL,
                        status VARCHAR(16) NOT NULL,
                        payload_json LONGTEXT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_ig_publishing_audit_post (scheduled_post_id),
                        INDEX idx_ig_publishing_audit_action (action),
                        INDEX idx_ig_publishing_audit_status (status)
                    )
                    """
                )
                _exec_ddl(
                    """
                    CREATE TABLE IF NOT EXISTS ig_comment_action (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        media_id VARCHAR(128) NULL,
                        comment_id VARCHAR(128) NOT NULL,
                        action VARCHAR(32) NOT NULL,
                        actor_user_id INT NULL,
                        payload_json LONGTEXT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_ig_comment_action_media (media_id),
                        INDEX idx_ig_comment_action_comment (comment_id),
                        INDEX idx_ig_comment_action_action (action),
                        INDEX idx_ig_comment_action_created (created_at)
                    )
                    """
                )
                _exec_ddl(
                    """
                    CREATE TABLE IF NOT EXISTS conversation_assignment (
                        conversation_id INT PRIMARY KEY,
                        assignee_user_id INT NULL,
                        note LONGTEXT NULL,
                        updated_by_user_id INT NULL,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_conversation_assignment_assignee (assignee_user_id)
                    )
                    """
                )
                _exec_ddl(
                    """
                    CREATE TABLE IF NOT EXISTS ig_canned_response (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        title VARCHAR(255) NOT NULL,
                        body LONGTEXT NOT NULL,
                        tags VARCHAR(255) NULL,
                        language VARCHAR(16) NULL,
                        is_active TINYINT(1) NOT NULL DEFAULT 1,
                        created_by_user_id INT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_ig_canned_response_title (title),
                        INDEX idx_ig_canned_response_active (is_active)
                    )
                    """
                )
                _exec_ddl(
                    """
                    CREATE TABLE IF NOT EXISTS ig_dm_order_draft (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        conversation_id INT NOT NULL,
                        status VARCHAR(32) NOT NULL DEFAULT 'draft',
                        payload_json LONGTEXT NOT NULL,
                        created_by_user_id INT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_ig_dm_order_conversation (conversation_id),
                        INDEX idx_ig_dm_order_status (status)
                    )
                    """
                )
                # Admin mesajları tablosu
                try:
                    conn.exec_driver_sql(
                        """
                        CREATE TABLE IF NOT EXISTS admin_messages (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            conversation_id INT NOT NULL,
                            message TEXT NOT NULL,
                            message_type VARCHAR(16) NOT NULL DEFAULT 'info',
                            is_read BOOLEAN NOT NULL DEFAULT FALSE,
                            read_by_user_id INT NULL,
                            read_at DATETIME NULL,
                            metadata_json LONGTEXT NULL,
                            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_admin_msg_conversation (conversation_id),
                            INDEX idx_admin_msg_type (message_type),
                            INDEX idx_admin_msg_read (is_read),
                            INDEX idx_admin_msg_created (created_at),
                            FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE,
                            FOREIGN KEY (read_by_user_id) REFERENCES user(id) ON DELETE SET NULL
                        )
                        """
                    )
                except Exception:
                    pass

                # Pushover recipients for admin notifications
                try:
                    conn.exec_driver_sql(
                        """
                        CREATE TABLE IF NOT EXISTS admin_pushover_recipient (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            label VARCHAR(191) NOT NULL,
                            user_key VARCHAR(191) NOT NULL,
                            is_active BOOLEAN NOT NULL DEFAULT TRUE,
                            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_admin_pushover_label (label),
                            INDEX idx_admin_pushover_active (is_active)
                        )
                        """
                    )
                except Exception:
                    pass

                # Ensure stockmovement.unit_cost exists for tracking purchase costs
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'stockmovement' AND COLUMN_NAME = 'unit_cost'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql("ALTER TABLE stockmovement ADD COLUMN unit_cost DOUBLE NULL")
                except Exception:
                    pass

                # Ensure account table exists
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'account'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql(
                            """
                            CREATE TABLE account (
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                name VARCHAR(191) NOT NULL,
                                type VARCHAR(64) NOT NULL,
                                iban VARCHAR(191) NULL,
                                initial_balance DOUBLE NOT NULL DEFAULT 0.0,
                                notes TEXT NULL,
                                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                INDEX idx_account_name (name),
                                INDEX idx_account_type (type),
                                INDEX idx_account_active (is_active)
                            )
                            """
                        )
                except Exception:
                    pass

                # Ensure income table exists
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'income'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql(
                            """
                            CREATE TABLE income (
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                account_id INT NOT NULL,
                                amount DOUBLE NOT NULL,
                                date DATE NULL,
                                source VARCHAR(64) NOT NULL,
                                reference VARCHAR(191) NULL,
                                notes TEXT NULL,
                                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                INDEX idx_income_account (account_id),
                                INDEX idx_income_date (date),
                                INDEX idx_income_source (source),
                                FOREIGN KEY (account_id) REFERENCES account(id) ON DELETE RESTRICT
                            )
                            """
                        )
                except Exception:
                    pass

                # Ensure orderpayment table exists
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orderpayment'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql(
                            """
                            CREATE TABLE orderpayment (
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                income_id INT NOT NULL,
                                order_id INT NOT NULL,
                                expected_amount DOUBLE NULL,
                                collected_amount DOUBLE NULL,
                                collected_at DATETIME NULL,
                                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                INDEX idx_orderpayment_income (income_id),
                                INDEX idx_orderpayment_order (order_id),
                                INDEX idx_orderpayment_collected (collected_at),
                                FOREIGN KEY (income_id) REFERENCES income(id) ON DELETE RESTRICT,
                                FOREIGN KEY (order_id) REFERENCES `order`(id) ON DELETE RESTRICT
                            )
                            """
                        )
                except Exception:
                    pass

                # Ensure incomehistorylog table exists
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'incomehistorylog'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql(
                            """
                            CREATE TABLE incomehistorylog (
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                income_id INT NOT NULL,
                                action VARCHAR(32) NOT NULL,
                                old_data_json TEXT NULL,
                                new_data_json TEXT NULL,
                                user_id INT NULL,
                                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                INDEX idx_incomehistorylog_income (income_id),
                                INDEX idx_incomehistorylog_action (action),
                                INDEX idx_incomehistorylog_user (user_id),
                                INDEX idx_incomehistorylog_created (created_at),
                                FOREIGN KEY (income_id) REFERENCES income(id) ON DELETE CASCADE,
                                FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE SET NULL
                            )
                            """
                        )
                except Exception:
                    pass

                # Update incomehistorylog foreign key to CASCADE if it exists but has wrong constraint
                try:
                    # Check if foreign key exists and what its delete rule is
                    fk_row = conn.exec_driver_sql(
                        """
                        SELECT CONSTRAINT_NAME, DELETE_RULE
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                        JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                        ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                        WHERE kcu.TABLE_SCHEMA = DATABASE()
                        AND kcu.TABLE_NAME = 'incomehistorylog'
                        AND kcu.COLUMN_NAME = 'income_id'
                        AND kcu.REFERENCED_TABLE_NAME = 'income'
                        LIMIT 1
                        """
                    ).fetchone()
                    if fk_row and fk_row[1] != 'CASCADE':
                        # Drop old constraint and recreate with CASCADE
                        constraint_name = fk_row[0]
                        conn.exec_driver_sql(f"ALTER TABLE incomehistorylog DROP FOREIGN KEY {constraint_name}")
                        conn.exec_driver_sql(
                            "ALTER TABLE incomehistorylog "
                            "ADD CONSTRAINT incomehistorylog_ibfk_1 "
                            "FOREIGN KEY (income_id) REFERENCES income(id) ON DELETE CASCADE"
                        )
                except Exception:
                    pass

                # Ensure costhistorylog table exists
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'costhistorylog'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql(
                            """
                            CREATE TABLE costhistorylog (
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                cost_id INT NOT NULL,
                                action VARCHAR(32) NOT NULL,
                                old_data_json TEXT NULL,
                                new_data_json TEXT NULL,
                                user_id INT NULL,
                                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                INDEX idx_costhistorylog_cost (cost_id),
                                INDEX idx_costhistorylog_action (action),
                                INDEX idx_costhistorylog_user (user_id),
                                INDEX idx_costhistorylog_created (created_at),
                                FOREIGN KEY (cost_id) REFERENCES cost(id) ON DELETE CASCADE,
                                FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE SET NULL
                            )
                            """
                        )
                except Exception:
                    pass

                # Update costhistorylog foreign key to CASCADE if it exists but has wrong constraint
                try:
                    # Check if foreign key exists and what its delete rule is
                    fk_row = conn.exec_driver_sql(
                        """
                        SELECT CONSTRAINT_NAME, DELETE_RULE
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                        JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                        ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                        WHERE kcu.TABLE_SCHEMA = DATABASE()
                        AND kcu.TABLE_NAME = 'costhistorylog'
                        AND kcu.COLUMN_NAME = 'cost_id'
                        AND kcu.REFERENCED_TABLE_NAME = 'cost'
                        LIMIT 1
                        """
                    ).fetchone()
                    if fk_row and fk_row[1] != 'CASCADE':
                        # Drop old constraint and recreate with CASCADE
                        constraint_name = fk_row[0]
                        conn.exec_driver_sql(f"ALTER TABLE costhistorylog DROP FOREIGN KEY {constraint_name}")
                        conn.exec_driver_sql(
                            "ALTER TABLE costhistorylog "
                            "ADD CONSTRAINT costhistorylog_ibfk_1 "
                            "FOREIGN KEY (cost_id) REFERENCES cost(id) ON DELETE CASCADE"
                        )
                except Exception:
                    pass

                # Ensure cost.account_id exists
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cost' AND COLUMN_NAME = 'account_id'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql("ALTER TABLE cost ADD COLUMN account_id INT NULL")
                        conn.exec_driver_sql("CREATE INDEX idx_cost_account ON cost(account_id)")
                        conn.exec_driver_sql(
                            "ALTER TABLE cost ADD CONSTRAINT fk_cost_account FOREIGN KEY (account_id) REFERENCES account(id) ON DELETE SET NULL"
                        )
                except Exception:
                    pass

                # Ensure supplier table exists
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'supplier'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql(
                            """
                            CREATE TABLE supplier (
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                name VARCHAR(255) NOT NULL,
                                phone VARCHAR(255) NULL,
                                address TEXT NULL,
                                tax_id VARCHAR(255) NULL,
                                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                INDEX idx_supplier_name (name)
                            )
                            """
                        )
                except Exception:
                    pass

                # Ensure cost.supplier_id exists
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cost' AND COLUMN_NAME = 'supplier_id'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql("ALTER TABLE cost ADD COLUMN supplier_id INT NULL")
                        conn.exec_driver_sql("CREATE INDEX idx_cost_supplier ON cost(supplier_id)")
                        conn.exec_driver_sql(
                            "ALTER TABLE cost ADD CONSTRAINT fk_cost_supplier FOREIGN KEY (supplier_id) REFERENCES supplier(id) ON DELETE SET NULL"
                        )
                except Exception:
                    pass

                # Ensure cost.product_id exists
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cost' AND COLUMN_NAME = 'product_id'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql("ALTER TABLE cost ADD COLUMN product_id INT NULL")
                        conn.exec_driver_sql("CREATE INDEX idx_cost_product ON cost(product_id)")
                        conn.exec_driver_sql(
                            "ALTER TABLE cost ADD CONSTRAINT fk_cost_product FOREIGN KEY (product_id) REFERENCES product(id) ON DELETE SET NULL"
                        )
                except Exception:
                    pass

                # Ensure cost.quantity exists
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cost' AND COLUMN_NAME = 'quantity'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql("ALTER TABLE cost ADD COLUMN quantity INT NULL")
                except Exception:
                    pass

                # Ensure cost.is_payment_to_supplier exists
                try:
                    row = conn.exec_driver_sql(
                        """
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cost' AND COLUMN_NAME = 'is_payment_to_supplier'
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.exec_driver_sql("ALTER TABLE cost ADD COLUMN is_payment_to_supplier BOOLEAN NOT NULL DEFAULT FALSE")
                        conn.exec_driver_sql("CREATE INDEX idx_cost_payment_to_supplier ON cost(is_payment_to_supplier)")
                except Exception:
                    pass

                # Ensure "Genel Giderler" supplier exists
                try:
                    from sqlmodel import select
                    from app.models import Supplier
                    with Session(engine) as session:
                        existing = session.exec(select(Supplier).where(Supplier.name == "Genel Giderler")).first()
                        if not existing:
                            session.add(Supplier(name="Genel Giderler"))
                            session.commit()
                except Exception:
                    pass

                # Ensure supplier_payment_allocation table exists
                try:
                    conn.exec_driver_sql(
                        """
                        CREATE TABLE IF NOT EXISTS supplierpaymentallocation (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            payment_cost_id INT NOT NULL,
                            debt_cost_id INT NOT NULL,
                            amount DOUBLE NOT NULL,
                            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_payment_allocation_payment (payment_cost_id),
                            INDEX idx_payment_allocation_debt (debt_cost_id),
                            FOREIGN KEY (payment_cost_id) REFERENCES cost(id) ON DELETE CASCADE,
                            FOREIGN KEY (debt_cost_id) REFERENCES cost(id) ON DELETE CASCADE
                        )
                        """
                    )
                except Exception:
                    pass

                return
        except Exception as e:
            last_err = e
            if attempt < retries:
                try:
                    print(f"[DB INIT] error on attempt {attempt}/{retries}: {str(e)[:200]}; retrying after {backoff:.2f}s")
                except Exception:
                    pass
                time.sleep(backoff)
                backoff = min(backoff * 1.5, 5.0)
                continue
            # last attempt -> re-raise
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
