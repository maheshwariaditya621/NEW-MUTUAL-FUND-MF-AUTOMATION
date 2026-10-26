-- ============================================================
-- MIGRATION 033: Automated Earnings Calendar & Results Tracking
-- Date: 2026-10-14
-- ============================================================

BEGIN;

-- 1. EARNINGS CALENDAR (Forward-Looking Board Meetings & Expected Results)
CREATE TABLE IF NOT EXISTS earnings_calendar (
    event_id                BIGSERIAL PRIMARY KEY,
    company_id              BIGINT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    isin                    VARCHAR(12) NOT NULL,
    exchange_symbol         VARCHAR(50),
    bse_code                VARCHAR(20),
    financial_year          VARCHAR(10),
    fiscal_period           VARCHAR(10),
    meeting_date            DATE NOT NULL,
    meeting_time            VARCHAR(50),
    expected_results_date   DATE,
    raw_purpose             TEXT,
    normalized_event_type   VARCHAR(50) NOT NULL DEFAULT 'BOARD_MEETING_RESULTS',
    event_status            VARCHAR(50) NOT NULL DEFAULT 'SCHEDULED',
    source_url              TEXT,
    source_record_id        VARCHAR(100) NOT NULL DEFAULT 'DEFAULT',
    source_name             VARCHAR(50) NOT NULL,
    verification_status     VARCHAR(50) NOT NULL DEFAULT 'VERIFIED_EXCHANGE',
    consensus_eps           NUMERIC(10, 2),
    consensus_eps_high      NUMERIC(10, 2),
    consensus_eps_low       NUMERIC(10, 2),
    consensus_revenue_inr   NUMERIC(18, 2),
    first_detected_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_verified_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_earnings_calendar_source UNIQUE (company_id, source_name, meeting_date, source_record_id)
);

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_company ON earnings_calendar(company_id);
CREATE INDEX IF NOT EXISTS idx_earnings_calendar_meeting_date ON earnings_calendar(meeting_date ASC);
CREATE INDEX IF NOT EXISTS idx_earnings_calendar_status ON earnings_calendar(event_status);
CREATE INDEX IF NOT EXISTS idx_earnings_calendar_fy_period ON earnings_calendar(financial_year, fiscal_period);

-- 2. EARNINGS CALENDAR HISTORY (Append-Only Audit Log of Date & Status Changes)
CREATE TABLE IF NOT EXISTS earnings_calendar_history (
    history_id              BIGSERIAL PRIMARY KEY,
    event_id                BIGINT NOT NULL REFERENCES earnings_calendar(event_id) ON DELETE CASCADE,
    company_id              BIGINT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    old_meeting_date        DATE,
    new_meeting_date        DATE,
    old_status              VARCHAR(50),
    new_status              VARCHAR(50),
    change_reason           TEXT,
    detected_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_evidence         TEXT
);

CREATE INDEX IF NOT EXISTS idx_calendar_history_event ON earnings_calendar_history(event_id);
CREATE INDEX IF NOT EXISTS idx_calendar_history_company ON earnings_calendar_history(company_id);

-- 3. EARNINGS RESULTS FILINGS (Actual Published Financial Results)
CREATE TABLE IF NOT EXISTS earnings_results_filings (
    filing_id               BIGSERIAL PRIMARY KEY,
    company_id              BIGINT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    calendar_event_id       BIGINT REFERENCES earnings_calendar(event_id) ON DELETE SET NULL,
    canonical_announcement_id BIGINT REFERENCES canonical_announcements(canonical_id) ON DELETE SET NULL,
    exchange                VARCHAR(10) NOT NULL CHECK (exchange IN ('NSE', 'BSE', 'BOTH')),
    source_filing_id        VARCHAR(100) NOT NULL,
    financial_year          VARCHAR(10),
    fiscal_period           VARCHAR(10),
    result_type             VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN',
    audit_status            VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN',
    submission_timestamp    TIMESTAMP WITH TIME ZONE,
    dissemination_timestamp TIMESTAMP WITH TIME ZONE,
    detected_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    filing_title            TEXT NOT NULL,
    filing_url              TEXT,
    attachment_url          TEXT,
    attachment_filename     VARCHAR(255),
    is_revision             BOOLEAN NOT NULL DEFAULT FALSE,
    revision_type           VARCHAR(50),
    parent_filing_id        BIGINT REFERENCES earnings_results_filings(filing_id) ON DELETE SET NULL,
    verification_status     VARCHAR(50) NOT NULL DEFAULT 'VERIFIED_EXCHANGE',
    raw_metadata            JSONB,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_results_filing_source UNIQUE (exchange, source_filing_id)
);

CREATE INDEX IF NOT EXISTS idx_results_filings_company ON earnings_results_filings(company_id);
CREATE INDEX IF NOT EXISTS idx_results_filings_dissem ON earnings_results_filings(dissemination_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_results_filings_detected ON earnings_results_filings(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_results_filings_calendar ON earnings_results_filings(calendar_event_id);
CREATE INDEX IF NOT EXISTS idx_results_filings_fy_period ON earnings_results_filings(financial_year, fiscal_period);

-- 4. EARNINGS SYNC STATE (Operational Observability & Health Tracking)
CREATE TABLE IF NOT EXISTS earnings_sync_state (
    source_key              VARCHAR(50) PRIMARY KEY,
    last_attempted_at       TIMESTAMP WITH TIME ZONE,
    last_successful_at      TIMESTAMP WITH TIME ZONE,
    status                  VARCHAR(20) NOT NULL DEFAULT 'IDLE',
    events_count            INTEGER NOT NULL DEFAULT 0,
    results_count           INTEGER NOT NULL DEFAULT 0,
    last_error              TEXT,
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Seed initial sync state keys
INSERT INTO earnings_sync_state (source_key, status)
VALUES 
    ('BSE_BOARD_MEETINGS', 'IDLE'),
    ('BSE_RESULTS_FEED', 'IDLE'),
    ('NSE_BOARD_MEETINGS', 'IDLE'),
    ('NSE_RESULTS_FEED', 'IDLE'),
    ('TIGZIG_CONSENSUS', 'IDLE'),
    ('RECONCILIATION_ENGINE', 'IDLE')
ON CONFLICT (source_key) DO NOTHING;

-- Seed permissions for admin and existing users
UPDATE users 
SET permissions = permissions || '["view_earnings"]'::jsonb
WHERE NOT (permissions @> '["view_earnings"]'::jsonb);

COMMIT;

SELECT '✅ Migration 033: Automated Earnings Calendar & Results Tracking tables created successfully!' AS status;
