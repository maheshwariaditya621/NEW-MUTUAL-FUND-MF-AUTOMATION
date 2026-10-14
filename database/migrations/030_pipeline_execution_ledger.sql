-- ============================================================
-- MIGRATION 030: PIPELINE EXECUTION LEDGER & MASTER RUNS
-- ============================================================
-- Description: Creates persistent ledger tables for mutual fund AMC automation.
-- Tracks master batch runs and granular per-AMC executions with full lineage,
-- state transitions, retry telemetry, and duplicate prevention.
-- Date: 2026-09-13
-- Status: Production-Ready
-- ============================================================

BEGIN;

-- 1. Master Pipeline Runs Table
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    target_year INTEGER NOT NULL CHECK (target_year >= 2020 AND target_year <= 2100),
    target_month INTEGER NOT NULL CHECK (target_month >= 1 AND target_month <= 12),
    trigger_type VARCHAR(32) NOT NULL DEFAULT 'SCHEDULED' CHECK (trigger_type IN ('SCHEDULED', 'MANUAL', 'CLI')),
    status VARCHAR(32) NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    concurrency INTEGER NOT NULL DEFAULT 2 CHECK (concurrency >= 1),
    dry_run BOOLEAN NOT NULL DEFAULT FALSE,
    redo BOOLEAN NOT NULL DEFAULT FALSE,
    total_amcs INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    pid INTEGER,
    hostname VARCHAR(128),
    error_message TEXT,
    summary_json JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_period ON pipeline_runs (target_year, target_month);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs (status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON pipeline_runs (started_at DESC);

-- 2. Per-AMC Pipeline Execution Ledger Table
CREATE TABLE IF NOT EXISTS pipeline_execution_ledger (
    ledger_id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    amc_slug VARCHAR(64) NOT NULL,
    amc_id BIGINT REFERENCES amcs(amc_id),
    year INTEGER NOT NULL CHECK (year >= 2020 AND year <= 2100),
    month INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
    period_id BIGINT REFERENCES periods(period_id),
    stage VARCHAR(32) NOT NULL DEFAULT 'INIT' CHECK (stage IN ('INIT', 'DOWNLOAD', 'MERGE', 'EXTRACT', 'LOAD', 'COMPLETED')),
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'RETRY_PENDING', 'SKIPPED', 'CANCELLED')),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    max_retries INTEGER NOT NULL DEFAULT 3 CHECK (max_retries >= 0),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    duration_seconds NUMERIC(10, 2) DEFAULT 0,
    rows_fetched INTEGER DEFAULT 0,
    rows_extracted INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    rows_updated INTEGER DEFAULT 0,
    duplicates_prevented INTEGER DEFAULT 0,
    file_hash VARCHAR(64),
    error_type VARCHAR(64),
    error_message TEXT,
    stack_trace TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ledger_run_amc UNIQUE (run_id, amc_slug)
);

CREATE INDEX IF NOT EXISTS idx_ledger_amc_period ON pipeline_execution_ledger (amc_slug, year, month);
CREATE INDEX IF NOT EXISTS idx_ledger_run_id ON pipeline_execution_ledger (run_id);
CREATE INDEX IF NOT EXISTS idx_ledger_status ON pipeline_execution_ledger (status);
CREATE INDEX IF NOT EXISTS idx_ledger_created_at ON pipeline_execution_ledger (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_success_lookup ON pipeline_execution_ledger (amc_slug, year, month, status);

-- 3. Register Migration in schema_migrations
INSERT INTO schema_migrations (filename, applied_at)
VALUES ('030_pipeline_execution_ledger.sql', CURRENT_TIMESTAMP)
ON CONFLICT (filename) DO NOTHING;

COMMIT;
