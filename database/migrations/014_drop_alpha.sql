-- ============================================================
-- MIGRATION 014: DROP ALPHA METRICS
-- Date: 2026-02-12
-- Author: Antigravity
-- Description: Drops the computed_alpha_metrics table as Phase C analytics was cancelled.
-- ============================================================

BEGIN;

DROP TABLE IF EXISTS computed_alpha_metrics;

COMMIT;