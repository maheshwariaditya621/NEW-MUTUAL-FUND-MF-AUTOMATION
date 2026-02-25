-- ============================================================
-- MIGRATION 021: Add shares_outstanding to companies
-- Date: 2026-02-25
-- ============================================================

BEGIN;

ALTER TABLE companies
ADD COLUMN IF NOT EXISTS shares_outstanding BIGINT,
ADD COLUMN IF NOT EXISTS shares_last_updated_at TIMESTAMP;

COMMENT ON COLUMN companies.shares_outstanding IS 'Total number of shares outstanding for the company';

COMMENT ON COLUMN companies.shares_last_updated_at IS 'Timestamp of when shares outstanding was last fetched';

COMMIT;