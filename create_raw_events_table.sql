-- Create the missing raw_events table in MySQL
-- Run this on your MySQL database: appdb_h

CREATE TABLE IF NOT EXISTS raw_events (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    object VARCHAR(255) NOT NULL,
    entry_id VARCHAR(255) NOT NULL,
    payload LONGTEXT NOT NULL,
    sig256 VARCHAR(255) NOT NULL,
    uniq_hash VARCHAR(255) UNIQUE
);

-- Verify the table was created
SHOW TABLES LIKE 'raw_events';
DESCRIBE raw_events;
