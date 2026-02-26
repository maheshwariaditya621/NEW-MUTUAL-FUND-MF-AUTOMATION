-- Migration: 023_scheme_resolution_engine
-- Description: Tables to support automated scheme rename detection and resolution

-- 1. Scheme Aliases (Tier 1: Fast Path)
-- Maps various incoming AMC scheme names to a single canonical scheme_id
CREATE TABLE IF NOT EXISTS scheme_aliases (
    alias_id BIGSERIAL PRIMARY KEY,
    amc_id INTEGER NOT NULL REFERENCES amcs (amc_id) ON DELETE CASCADE,
    alias_name VARCHAR(255) NOT NULL,
    canonical_scheme_id INTEGER NOT NULL REFERENCES schemes (scheme_id) ON DELETE CASCADE,
    plan_type VARCHAR(50) NOT NULL,
    option_type VARCHAR(50) NOT NULL,
    is_reinvest BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        approved_by VARCHAR(100), -- Who approved this link (e.g. 'manual', 'system-auto-85%', or user name)
        detection_method VARCHAR(50), -- 'TEXT_SIMILARITY', 'PORTFOLIO_OVERLAP', 'MANUAL'
        UNIQUE (
            amc_id,
            alias_name,
            plan_type,
            option_type,
            is_reinvest
        )
);

CREATE INDEX IF NOT EXISTS idx_scheme_aliases_lookup ON scheme_aliases (
    amc_id,
    alias_name,
    plan_type,
    option_type,
    is_reinvest
);

-- 2. Pending Scheme Merges (For User Approval)
-- Stores detected renames that haven't been approved yet
CREATE TABLE IF NOT EXISTS pending_scheme_merges (
    merge_id BIGSERIAL PRIMARY KEY,
    amc_id INTEGER NOT NULL REFERENCES amcs (amc_id) ON DELETE CASCADE,
    new_scheme_name VARCHAR(255) NOT NULL,
    old_scheme_id INTEGER NOT NULL REFERENCES schemes (scheme_id) ON DELETE CASCADE,
    plan_type VARCHAR(50) NOT NULL,
    option_type VARCHAR(50) NOT NULL,
    is_reinvest BOOLEAN DEFAULT FALSE,
    confidence_score FLOAT NOT NULL, -- 0.0 to 1.0
    detection_method VARCHAR(50), -- 'TEXT_SIMILARITY', 'PORTFOLIO_OVERLAP'
    metadata JSONB, -- Store overlap details or text diff
    created_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(20) DEFAULT 'PENDING', -- 'PENDING', 'APPROVED', 'REJECTED'
        UNIQUE (
            amc_id,
            new_scheme_name,
            plan_type,
            option_type,
            is_reinvest
        )
);

-- 3. Resolution Audit Log
-- For tracking how schemes were resolved during loading
CREATE TABLE IF NOT EXISTS scheme_resolution_log (
    log_id BIGSERIAL PRIMARY KEY,
    amc_id INTEGER NOT NULL,
    incoming_name VARCHAR(255) NOT NULL,
    resolved_scheme_id INTEGER,
    method VARCHAR(50), -- 'EXACT_MATCH', 'ALIAS_MATCH', 'PENDING_NEW'
    period_id INTEGER,
    created_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT CURRENT_TIMESTAMP
);