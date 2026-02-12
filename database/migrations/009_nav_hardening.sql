-- ============================================================
-- MIGRATION 009: NAV HARDENING & PERFORMANCE
-- Date: 2026-02-12
-- Author: Antigravity
-- ============================================================

BEGIN;

-- 1. PRECISION UPDATE FOR NAV
-- Using NUMERIC(18,6) to be absolutely safe against overflow while maintaining 6 decimals
ALTER TABLE nav_history ALTER COLUMN nav_value TYPE NUMERIC(18, 6);

-- 2. LINK NAV HISTORY TO INTERNAL SCHEME ID
ALTER TABLE nav_history
ADD COLUMN IF NOT EXISTS scheme_id BIGINT REFERENCES schemes (scheme_id);

-- 3. IDCW/DIVIDEND ADJUSTMENTS TABLE
CREATE TABLE IF NOT EXISTS scheme_dividends (
    dividend_id SERIAL PRIMARY KEY,
    scheme_id BIGINT NOT NULL REFERENCES schemes (scheme_id),
    record_date DATE NOT NULL,
    dividend_value NUMERIC(18, 6) NOT NULL,
    created_at TIMESTAMP DEFAULT(now() at time zone 'utc'),
    UNIQUE (scheme_id, record_date)
);

-- 4. SCHEME ALIAS / MERGE TRACKING
CREATE TABLE IF NOT EXISTS scheme_alias (
    alias_id SERIAL PRIMARY KEY,
    old_amfi_code VARCHAR(50) NOT NULL,
    new_amfi_code VARCHAR(50) NOT NULL,
    effective_date DATE NOT NULL,
    action_type VARCHAR(50) DEFAULT 'MERGER',
    notes TEXT,
    created_at TIMESTAMP DEFAULT(now() at time zone 'utc')
);

-- 5. PRECOMPUTED RETURNS (Performance Layer)
CREATE TABLE IF NOT EXISTS scheme_returns (
    scheme_id BIGINT PRIMARY KEY REFERENCES schemes (scheme_id),
    latest_nav_date DATE NOT NULL,
    latest_nav_value NUMERIC(18, 6),
    return_1d NUMERIC(12, 4),
    return_1m NUMERIC(12, 4),
    return_3m NUMERIC(12, 4),
    return_6m NUMERIC(12, 4),
    return_1y NUMERIC(12, 4),
    return_3y NUMERIC(12, 4),
    return_5y NUMERIC(12, 4),
    cagr_since_inception NUMERIC(12, 4),
    updated_at TIMESTAMP DEFAULT(now() at time zone 'utc')
);

-- 6. INDEX FOR MAPPING
-- We will add the UNIQUE constraint on (scheme_id, nav_date) after populating scheme_id
CREATE INDEX IF NOT EXISTS idx_nav_scheme_id_date ON nav_history (scheme_id, nav_date);

COMMIT;