-- ============================================================
-- MIGRATION 029: Corporate Announcements Module Foundation
-- Date: 2026-09-12
-- ============================================================

BEGIN;

-- 1. MASTER OFFICE WATCHLIST
CREATE TABLE IF NOT EXISTS master_watchlist (
    master_id       BIGSERIAL PRIMARY KEY,
    company_id      BIGINT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    notes           TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    added_by        INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_master_watchlist_company UNIQUE (company_id)
);

CREATE INDEX IF NOT EXISTS idx_master_watchlist_active ON master_watchlist(is_active) WHERE is_active = TRUE;

-- 2. CANONICAL ANNOUNCEMENTS (Unified UI Representation)
CREATE TABLE IF NOT EXISTS canonical_announcements (
    canonical_id          BIGSERIAL PRIMARY KEY,
    company_id            BIGINT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    isin                  VARCHAR(12) NOT NULL,
    company_name          VARCHAR(255) NOT NULL,
    primary_timestamp     TIMESTAMP WITH TIME ZONE NOT NULL,
    title                 TEXT NOT NULL,
    summary_text          TEXT,
    has_nse               BOOLEAN NOT NULL DEFAULT FALSE,
    has_bse               BOOLEAN NOT NULL DEFAULT FALSE,
    categories            TEXT[] NOT NULL DEFAULT '{Other}',
    is_revision           BOOLEAN NOT NULL DEFAULT FALSE,
    revision_type         VARCHAR(50),
    parent_canonical_id   BIGINT REFERENCES canonical_announcements(canonical_id) ON DELETE SET NULL,
    dedup_signature       VARCHAR(64) NOT NULL UNIQUE,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_canonical_company ON canonical_announcements(company_id);
CREATE INDEX IF NOT EXISTS idx_canonical_timestamp ON canonical_announcements(primary_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_canonical_categories ON canonical_announcements USING GIN(categories);
CREATE INDEX IF NOT EXISTS idx_canonical_parent ON canonical_announcements(parent_canonical_id);

-- 3. ANNOUNCEMENT SOURCES (Raw Exchange Ingestions)
CREATE TABLE IF NOT EXISTS announcement_sources (
    source_id             BIGSERIAL PRIMARY KEY,
    canonical_id          BIGINT NOT NULL REFERENCES canonical_announcements(canonical_id) ON DELETE CASCADE,
    exchange              VARCHAR(10) NOT NULL CHECK (exchange IN ('NSE', 'BSE')),
    source_announcement_id VARCHAR(100) NOT NULL,
    company_id            BIGINT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    exchange_symbol       VARCHAR(50),
    raw_subject           TEXT NOT NULL,
    raw_details           TEXT,
    raw_category          VARCHAR(100),
    raw_subcategory       VARCHAR(100),
    submission_timestamp  TIMESTAMP WITH TIME ZONE,
    dissemination_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    time_difference       VARCHAR(20),
    detected_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    inserted_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    has_xbrl              BOOLEAN NOT NULL DEFAULT FALSE,
    raw_payload           JSONB,
    CONSTRAINT uq_source_exchange_id UNIQUE (exchange, source_announcement_id)
);

CREATE INDEX IF NOT EXISTS idx_sources_canonical ON announcement_sources(canonical_id);
CREATE INDEX IF NOT EXISTS idx_sources_dissem ON announcement_sources(dissemination_timestamp DESC);

-- 4. ANNOUNCEMENT ATTACHMENTS (Document Lifecycle)
CREATE TABLE IF NOT EXISTS announcement_attachments (
    attachment_id         BIGSERIAL PRIMARY KEY,
    source_id             BIGINT NOT NULL REFERENCES announcement_sources(source_id) ON DELETE CASCADE,
    canonical_id          BIGINT NOT NULL REFERENCES canonical_announcements(canonical_id) ON DELETE CASCADE,
    exchange              VARCHAR(10) NOT NULL,
    original_file_name    VARCHAR(255) NOT NULL,
    source_file_url       TEXT NOT NULL,
    local_file_path       TEXT,
    file_size_bytes       BIGINT,
    sha256_hash           VARCHAR(64),
    mime_type             VARCHAR(100) DEFAULT 'application/pdf',
    lifecycle_stage       VARCHAR(20) NOT NULL DEFAULT 'HOT' CHECK (lifecycle_stage IN ('HOT', 'WARM', 'PURGED', 'FAILED')),
    is_locally_available  BOOLEAN NOT NULL DEFAULT FALSE,
    download_attempts     INTEGER NOT NULL DEFAULT 0,
    download_error        TEXT,
    downloaded_at         TIMESTAMP WITH TIME ZONE,
    purged_at             TIMESTAMP WITH TIME ZONE,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_attachment_source_url UNIQUE (source_id, source_file_url)
);

CREATE INDEX IF NOT EXISTS idx_attachments_canonical ON announcement_attachments(canonical_id);
CREATE INDEX IF NOT EXISTS idx_attachments_lifecycle ON announcement_attachments(lifecycle_stage, is_locally_available);

-- 5. ANNOUNCEMENT SYNC STATE (Durable Watermarking)
CREATE TABLE IF NOT EXISTS announcement_sync_state (
    sync_key              VARCHAR(50) PRIMARY KEY,
    exchange              VARCHAR(10) NOT NULL,
    scrip_code            VARCHAR(20),
    last_checkpoint_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seq_id           BIGINT,
    last_poll_status      VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',
    last_poll_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error            TEXT,
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Seed initial admin permissions if not already present
UPDATE users 
SET permissions = permissions || '["view_announcements", "manage_master_watchlist"]'::jsonb
WHERE role = 'admin' AND NOT (permissions @> '["view_announcements"]'::jsonb);

COMMIT;

SELECT '✅ Migration 029: Corporate Announcements tables created successfully!' AS status;
