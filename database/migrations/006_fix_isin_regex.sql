-- ============================================================
-- Fix ISIN Regex Constraint
-- Date: 2026-02-12
-- ============================================================

-- The user specified Position 9-10 (Index 8-9) must be "10" for Equity.
-- The previous regex was ^INE[A-Z0-9]{6}10[A-Z0-9]{1}$ (index 9-10 = 10)
-- Correct regex: ^INE[A-Z0-9]{5}10[A-Z0-9]{2}$ (index 8-9 = 10)

ALTER TABLE companies DROP CONSTRAINT IF EXISTS chk_isin_format;

ALTER TABLE companies
ADD CONSTRAINT chk_isin_format CHECK (
    isin ~ '^INE[A-Z0-9]{5}10[A-Z0-9]{2}$'
);

COMMENT ON CONSTRAINT chk_isin_format ON companies IS 'Ensures ISIN starts with INE, is 12 chars, and has security code 10 at positions 9-10 (equity)';