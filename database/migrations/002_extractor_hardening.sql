-- ============================================================
-- Extractor Hardening & Lineage Migration
-- Date: 2026-02-11
-- ============================================================

-- 1. Create extraction_runs if it doesn't exist
CREATE TABLE IF NOT EXISTS extraction_runs (
    run_id SERIAL PRIMARY KEY,
    amc_id BIGINT REFERENCES amcs (amc_id),
    period_id BIGINT REFERENCES periods (period_id),
    file_name TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    extractor_version TEXT NOT NULL,
    header_fingerprint TEXT,
    rows_read INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    rows_filtered INTEGER DEFAULT 0,
    total_value NUMERIC(20, 2) DEFAULT 0,
    processing_time_seconds FLOAT,
    processing_timestamp_utc TIMESTAMP DEFAULT(now() at time zone 'utc'),
    status TEXT, -- 'SUCCESS', 'FAILED'
    error_log TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Add symbols to companies table
ALTER TABLE companies
ADD COLUMN IF NOT EXISTS nse_symbol VARCHAR(20),
ADD COLUMN IF NOT EXISTS bse_code VARCHAR(20);

-- 3. Create scheme_master for raw-to-canonical mapping
CREATE TABLE IF NOT EXISTS scheme_master (
    raw_sheet_name TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    plan_type VARCHAR(10) NOT NULL,
    option_type VARCHAR(10) NOT NULL,
    is_reinvest BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Add Index to ISIN (already exists but ensuring)
CREATE INDEX IF NOT EXISTS idx_companies_isin_lookup ON companies (isin);

-- 5. Add Constraints for Data Integrity
ALTER TABLE equity_holdings ALTER COLUMN snapshot_id SET NOT NULL;

ALTER TABLE equity_holdings ALTER COLUMN company_id SET NOT NULL;

-- isin column doesn't exist in equity_holdings (it's in companies)
-- Note: isin is usually in companies, but if we had it in holdings...
-- Checking current holdings table
-- it has snapshot_id, company_id, quantity, market_value_inr, percent_of_nav

-- Ensure NOT NULL on market_value_inr
ALTER TABLE equity_holdings
ALTER COLUMN market_value_inr
SET
    NOT NULL;