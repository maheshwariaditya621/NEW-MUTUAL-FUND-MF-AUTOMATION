-- ============================================================
-- MIGRATION 008: AMFI NAV PIPELINE
-- Date: 2026-02-12
-- Author: Antigravity
-- ============================================================

BEGIN;

-- 1. NAV HISTORY TABLE
CREATE TABLE IF NOT EXISTS nav_history (
    nav_id BIGSERIAL PRIMARY KEY,
    scheme_code VARCHAR(50) NOT NULL,
    isin_growth VARCHAR(12),
    isin_div_payout VARCHAR(12),
    isin_div_reinv VARCHAR(12),
    scheme_name TEXT NOT NULL,
    nav_value NUMERIC(15, 6) NOT NULL,
    nav_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Uniqueness: One NAV per scheme code per date
    CONSTRAINT uq_nav_entry UNIQUE (scheme_code, nav_date)
);

-- Indexes for performance
CREATE INDEX idx_nav_scheme_code ON nav_history (scheme_code);

CREATE INDEX idx_nav_isin_growth ON nav_history (isin_growth);

CREATE INDEX idx_nav_date ON nav_history (nav_date);

CREATE INDEX idx_nav_scheme_date ON nav_history (scheme_code, nav_date);

COMMENT ON
TABLE nav_history IS 'Historical and daily NAV data from AMFI';

-- 2. SCHEME TO AMFI MAPPING (Optional but useful for lookup)
-- We might want to link our internal scheme_id to amfi_scheme_code
ALTER TABLE schemes ADD COLUMN IF NOT EXISTS amfi_code VARCHAR(50);

COMMIT;