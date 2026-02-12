-- ============================================================
-- MIGRATION 012: BENCHMARK HISTORY LINEAGE
-- Date: 2026-02-12
-- Author: Antigravity
-- Description: Adds source tracking to benchmark history.
-- ============================================================

BEGIN;

-- 1. Add lineage columns to benchmark_history
ALTER TABLE benchmark_history
ADD COLUMN IF NOT EXISTS source_file TEXT,
ADD COLUMN IF NOT EXISTS imported_at TIMESTAMP DEFAULT(now() at time zone 'utc');

-- 2. Ensure IS_TRI is strictly enforced for future usage
-- (We trust the seeder for now, but this is a reminder)

COMMIT;