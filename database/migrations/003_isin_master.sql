-- ============================================================
-- ISIN Master Table Migration
-- Date: 2026-02-11
-- ============================================================

-- 1. Create isin_master table
CREATE TABLE IF NOT EXISTS isin_master (
    isin VARCHAR(12) PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    nse_symbol VARCHAR(20),
    bse_code VARCHAR(20),
    sector TEXT,
    industry TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Add Index to ISIN for fast lookups
CREATE INDEX IF NOT EXISTS idx_isin_master_search ON isin_master (isin);

CREATE INDEX IF NOT EXISTS idx_isin_master_name ON isin_master (canonical_name);

-- 3. Update companies to have a foreign key to isin_master if possible
-- (Optional: for now we just keep them in sync via logic)