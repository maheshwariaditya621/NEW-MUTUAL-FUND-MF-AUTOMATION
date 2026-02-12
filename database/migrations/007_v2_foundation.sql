-- ============================================================
-- MIGRATION 007: V2 FOUNDATION (Credibility & Lifecycle)
-- Date: 2026-02-12
-- Author: Antigravity
-- ============================================================

BEGIN;

-- 1. SCHEME LIFECYCLE ENHANCEMENTS
ALTER TABLE schemes
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE,
ADD COLUMN IF NOT EXISTS inception_date DATE,
ADD COLUMN IF NOT EXISTS closure_date DATE,
ADD COLUMN IF NOT EXISTS merged_into_id BIGINT REFERENCES schemes (scheme_id);

COMMENT ON COLUMN schemes.is_active IS 'Soft-delete flag; false if scheme is closed or merged';

COMMENT ON COLUMN schemes.merged_into_id IS 'Reference to the new scheme ID if this one was merged';

-- 2. PERIOD STATUS & LOCKING
ALTER TABLE periods
ADD COLUMN IF NOT EXISTS period_status VARCHAR(20) DEFAULT 'OPEN' CHECK (
    period_status IN ('OPEN', 'FINAL')
);

COMMENT ON COLUMN periods.period_status IS 'FINAL status locks the month from reprocessing unless forced';

-- 3. EXTRACTION LINEAGE (Git Tracking)
ALTER TABLE extraction_runs
ADD COLUMN IF NOT EXISTS git_commit_hash VARCHAR(40);

COMMENT ON COLUMN extraction_runs.git_commit_hash IS 'Git commit of the code that performed this run';

-- 4. CORPORATE ACTIONS TABLE
CREATE TABLE IF NOT EXISTS corporate_actions (
    action_id SERIAL PRIMARY KEY,
    old_isin VARCHAR(12) NOT NULL REFERENCES isin_master (isin),
    new_isin VARCHAR(12) NOT NULL REFERENCES isin_master (isin),
    effective_date DATE NOT NULL,
    action_type VARCHAR(50) NOT NULL, -- 'MERGER', 'NAME_CHANGE', 'ISIN_REPLACEMENT'
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_corp_actions_old_isin ON corporate_actions (old_isin);

CREATE INDEX idx_corp_actions_new_isin ON corporate_actions (new_isin);

CREATE INDEX idx_corp_actions_date ON corporate_actions (effective_date);

COMMENT ON
TABLE corporate_actions IS 'Tracks stock/company identity migrations to preserve lineage';

COMMIT;