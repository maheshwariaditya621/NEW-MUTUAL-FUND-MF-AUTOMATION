-- ============================================================
-- MIGRATION 032: FUND MANAGER INTELLIGENCE SYSTEM
-- Description: Tables for normalized fund managers, scheme-manager assignments,
--              sync history, change tracking, and unmatched review queue.
-- ============================================================

BEGIN;

-- 1. FUND MANAGERS MASTER
CREATE TABLE IF NOT EXISTS fund_managers (
    manager_id BIGSERIAL PRIMARY KEY,
    canonical_name TEXT NOT NULL UNIQUE,
    bio TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fund_managers_name ON fund_managers (canonical_name);

-- 2. SCHEME TO FUND MANAGER ASSIGNMENTS (Many-to-Many + Historical Tracking)
CREATE TABLE IF NOT EXISTS scheme_fund_managers (
    id BIGSERIAL PRIMARY KEY,
    scheme_id BIGINT NOT NULL REFERENCES schemes(scheme_id) ON DELETE CASCADE,
    manager_id BIGINT NOT NULL REFERENCES fund_managers(manager_id) ON DELETE CASCADE,
    source_manager_name TEXT NOT NULL,
    role VARCHAR(100) DEFAULT 'Fund Manager',
    is_primary BOOLEAN DEFAULT FALSE,
    tenure_start_date DATE,
    tenure_end_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    source_doc_date DATE,
    source_url VARCHAR(500),
    verification_status VARCHAR(50) DEFAULT 'verified', -- 'verified', 'unverified', 'pending_review', 'ambiguous'
    match_method VARCHAR(50), -- 'amfi_code', 'isin', 'validated_mapping', 'manual_approval'
    first_observed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_confirmed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_scheme_manager_tenure UNIQUE (scheme_id, manager_id, tenure_start_date)
);

CREATE INDEX IF NOT EXISTS idx_sfm_scheme_active ON scheme_fund_managers (scheme_id, is_active);
CREATE INDEX IF NOT EXISTS idx_sfm_manager_id ON scheme_fund_managers (manager_id);
CREATE INDEX IF NOT EXISTS idx_sfm_status ON scheme_fund_managers (verification_status);

-- 3. SYNCHRONIZATION AUDIT & JOB EXECUTION LEDGER
CREATE TABLE IF NOT EXISTS fund_manager_sync_history (
    job_id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) DEFAULT 'running', -- 'running', 'success', 'warning', 'failed'
    source_endpoint VARCHAR(500) NOT NULL,
    source_doc_date DATE,
    records_processed INT DEFAULT 0,
    exact_identifier_matches INT DEFAULT 0,
    verified_matches INT DEFAULT 0,
    unmatched_records INT DEFAULT 0,
    ambiguous_records INT DEFAULT 0,
    manager_additions INT DEFAULT 0,
    manager_departures INT DEFAULT 0,
    warnings_count INT DEFAULT 0,
    errors TEXT,
    summary_json JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_fm_sync_started ON fund_manager_sync_history (started_at DESC);

-- 4. FUND MANAGER CHANGE LOG (Audit trail of appointments and transitions)
CREATE TABLE IF NOT EXISTS fund_manager_change_log (
    log_id BIGSERIAL PRIMARY KEY,
    scheme_id BIGINT NOT NULL REFERENCES schemes(scheme_id) ON DELETE CASCADE,
    manager_id BIGINT NOT NULL REFERENCES fund_managers(manager_id) ON DELETE CASCADE,
    change_type VARCHAR(50) NOT NULL, -- 'new_appointment', 'verified_departure', 'tenure_update', 'source_correction', 'manual_approval'
    change_date DATE,
    details JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fm_change_scheme ON fund_manager_change_log (scheme_id);
CREATE INDEX IF NOT EXISTS idx_fm_change_date ON fund_manager_change_log (created_at DESC);

-- 5. UNMATCHED / AMBIGUOUS SCHEMES REVIEW QUEUE
CREATE TABLE IF NOT EXISTS fund_manager_unmatched_queue (
    queue_id BIGSERIAL PRIMARY KEY,
    scheme_id BIGINT REFERENCES schemes(scheme_id) ON DELETE SET NULL,
    source_scheme_code VARCHAR(50),
    source_scheme_name VARCHAR(500),
    source_isin VARCHAR(50),
    source_amc VARCHAR(255),
    raw_manager_string TEXT,
    parsed_managers JSONB DEFAULT '[]'::jsonb,
    candidate_scheme_id BIGINT REFERENCES schemes(scheme_id) ON DELETE SET NULL,
    candidate_match_reason VARCHAR(255),
    confidence_score NUMERIC(5, 2),
    review_status VARCHAR(50) DEFAULT 'pending', -- 'pending', 'approved', 'rejected', 'ignored'
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMP WITH TIME ZONE,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fmu_status ON fund_manager_unmatched_queue (review_status);
CREATE INDEX IF NOT EXISTS idx_fmu_candidate ON fund_manager_unmatched_queue (candidate_scheme_id);

-- Register Migration in schema_migrations
INSERT INTO schema_migrations (filename, applied_at)
VALUES ('032_fund_manager_intelligence.sql', CURRENT_TIMESTAMP)
ON CONFLICT (filename) DO NOTHING;

COMMIT;
