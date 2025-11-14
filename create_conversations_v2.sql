/* --------------------------------------------------------------------
   MySQL schema (DATABASE_URL starts with mysql:// or mysql+...)
   -------------------------------------------------------------------- */

DROP TABLE IF EXISTS ai_conversations;

DROP TABLE IF EXISTS conversations;

CREATE TABLE conversations (
    id INT PRIMARY KEY AUTO_INCREMENT,
    igba_id VARCHAR(64) NOT NULL,
    ig_user_id VARCHAR(64) NOT NULL,
    graph_conversation_id VARCHAR(128) NULL,

    -- Inbox / AI summary (formerly in ai_conversations)
    last_message_id INT NULL,
    last_message_timestamp_ms BIGINT NULL,
    last_message_text LONGTEXT NULL,
    last_message_direction VARCHAR(8) NULL,
    last_sender_username TEXT NULL,
    ig_sender_id VARCHAR(64) NULL,
    ig_recipient_id VARCHAR(64) NULL,
    last_ad_id VARCHAR(128) NULL,
    last_ad_link TEXT NULL,
    last_ad_title TEXT NULL,

    -- Hydration / unread state
    last_message_at DATETIME NULL,
    unread_count INT NOT NULL DEFAULT 0,
    hydrated_at DATETIME NULL,

    -- Helpful indexes
    INDEX idx_conversations_ig (igba_id, ig_user_id),
    INDEX idx_conversations_graph (graph_conversation_id),
    INDEX idx_conversations_last_ts (last_message_timestamp_ms),
    INDEX idx_conversations_last_at (last_message_at)
);

DROP TABLE IF EXISTS ig_users;

CREATE TABLE ig_users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    ig_user_id VARCHAR(64) NOT NULL UNIQUE,

    username VARCHAR(255),
    name VARCHAR(255),
    profile_pic_url TEXT,
    last_seen_at DATETIME,
    fetched_at DATETIME,
    fetch_status VARCHAR(32),
    fetch_error TEXT,

    -- Contact / CRM fields
    contact_name TEXT,
    contact_phone VARCHAR(64),
    contact_address TEXT,
    linked_order_id INT NULL,
    ai_status VARCHAR(32),
    ai_json LONGTEXT,

    -- Helpful indexes
    INDEX idx_ig_users_ig_user_id (ig_user_id),
    INDEX idx_ig_users_username (username),
    INDEX idx_ig_users_fetched_at (fetched_at),
    INDEX idx_ig_users_linked_order (linked_order_id)
);
