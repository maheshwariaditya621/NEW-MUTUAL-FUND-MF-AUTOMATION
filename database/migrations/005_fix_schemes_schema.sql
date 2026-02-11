-- ============================================================
-- Fix Schemes Schema
-- Date: 2026-02-12
-- ============================================================

-- Add is_reinvest to schemes table
ALTER TABLE schemes
ADD COLUMN IF NOT EXISTS is_reinvest BOOLEAN DEFAULT FALSE;

-- Update the uniqueness constraint to include is_reinvest
-- First drop the old one
ALTER TABLE schemes DROP CONSTRAINT IF EXISTS uq_scheme;

-- Add the new comprehensive one
ALTER TABLE schemes
ADD CONSTRAINT uq_scheme UNIQUE (
    amc_id,
    scheme_name,
    plan_type,
    option_type,
    is_reinvest
);

COMMENT ON COLUMN schemes.is_reinvest IS 'Whether this is a reinvestment option (IDCW)';