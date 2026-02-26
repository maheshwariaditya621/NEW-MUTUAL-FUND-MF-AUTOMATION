-- Migration: 024_notification_logs
-- Description: Table to store system alerts and telegram notification history

CREATE TABLE IF NOT EXISTS notification_logs (
    notification_id BIGSERIAL PRIMARY KEY,
    level VARCHAR(20) NOT NULL, -- 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    category VARCHAR(50) NOT NULL, -- 'EXTRACTION', 'MERGE', 'SYSTEM', 'AUM_CHECK'
    content TEXT NOT NULL,
    channel VARCHAR(50) DEFAULT 'TELEGRAM',
    status VARCHAR(20) DEFAULT 'SENT', -- 'SENT', 'FAILED', 'PENDING'
    error_details TEXT,
    created_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_notification_logs_created_at ON notification_logs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_notification_logs_level ON notification_logs (level);