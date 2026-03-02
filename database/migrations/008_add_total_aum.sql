-- ============================================================
-- MIGRATION 008: Add Total Net Assets to Scheme Snapshots
-- Date: 2026-02-27
-- Author: Antigravity
-- ============================================================

BEGIN;

ALTER TABLE scheme_snapshots
ADD COLUMN IF NOT EXISTS total_net_assets_inr NUMERIC(20, 2);

COMMENT ON COLUMN scheme_snapshots.total_net_assets_inr IS 'The total AUM (Grand Total) extracted from the AMC Excel footer, distinct from total_value_inr which is the sum of equity holdings.';

COMMIT;