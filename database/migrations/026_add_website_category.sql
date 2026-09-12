-- ============================================================
-- MIGRATION 026: WEBSITE CATEGORY ENHANCEMENTS
-- Date: 2026-03-08
-- Author: Antigravity
-- ============================================================

BEGIN;

-- 1. ADD WEBSITE CATEGORY COLUMNS TO SCHEMES
ALTER TABLE schemes
ADD COLUMN IF NOT EXISTS website_category VARCHAR(100),
ADD COLUMN IF NOT EXISTS website_sub_category VARCHAR(100);

COMMENT ON COLUMN schemes.website_category IS 'Top-level category for website display (e.g., Equity Funds, Debt Funds)';

COMMENT ON COLUMN schemes.website_sub_category IS 'Granular category for website filter (e.g., Large Cap, Flexi Cap)';

-- Create index for performance on categorization filters
CREATE INDEX IF NOT EXISTS idx_schemes_website_cat ON schemes (
    website_category,
    website_sub_category
);

COMMIT;