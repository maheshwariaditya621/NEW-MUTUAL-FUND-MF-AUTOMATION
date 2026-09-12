-- ============================================================
-- MIGRATION 020: Corporate Actions Adjustment Engine Foundation
-- Date: 2026-03-07
-- Description:
--   1. Enhances corporate_actions with numerator/denominator columns.
--   2. Creates adjustment_factors table.
--   3. Creates reprocessing_queue table.
--   4. Adds adj_quantity column to equity_holdings.
-- ============================================================

BEGIN;

-- ============================================================
-- 1. Enhance corporate_actions
-- ============================================================

ALTER TABLE corporate_actions
ADD COLUMN IF NOT EXISTS numerator NUMERIC(10, 4),
ADD COLUMN IF NOT EXISTS denominator NUMERIC(10, 4),
ADD COLUMN IF NOT EXISTS is_applied BOOLEAN NOT NULL DEFAULT FALSE;

-- Backfill numerator/denominator from ratio_factor for backwards compat
UPDATE corporate_actions
SET
    numerator = ratio_factor,
    denominator = 1
WHERE
    numerator IS NULL
    AND denominator IS NULL
    AND ratio_factor IS NOT NULL;

-- Drop old action_type check constraint if present (we no longer restrict at DB level)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'corporate_actions_action_type_check'
    ) THEN
        ALTER TABLE corporate_actions DROP CONSTRAINT corporate_actions_action_type_check;
    END IF;
END
$$;

COMMENT ON COLUMN corporate_actions.action_type IS 'SPLIT | BONUS | DIVIDEND | MERGER | RIGHTS';

COMMENT ON COLUMN corporate_actions.numerator IS 'New shares per unit (e.g., 5 for a 1:5 split)';

COMMENT ON COLUMN corporate_actions.denominator IS 'Old shares per unit (e.g., 1 for a 1:5 split)';

COMMENT ON COLUMN corporate_actions.is_applied IS 'TRUE once the adjustment engine has processed this action';

-- ============================================================
-- 2. Create adjustment_factors table
-- ============================================================

CREATE TABLE IF NOT EXISTS adjustment_factors (
    factor_id       BIGSERIAL PRIMARY KEY,
    isin            VARCHAR(12) NOT NULL,
    effective_date  DATE NOT NULL,
    qty_factor      NUMERIC(18, 10) NOT NULL DEFAULT 1.0,
    price_factor    NUMERIC(18, 10) NOT NULL DEFAULT 1.0,
    source_ca_ids   INTEGER[],
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (isin, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_adj_factors_isin_date ON adjustment_factors (isin, effective_date);

COMMENT ON
TABLE adjustment_factors IS 'Computed cumulative adjustment multipliers per ISIN per date. Derived from corporate_actions. safe to recompute at any time.';

COMMENT ON COLUMN adjustment_factors.qty_factor IS 'Multiply raw_quantity by this to get adj_quantity. e.g., 5.0 for a pre-split 1:5 record.';

COMMENT ON COLUMN adjustment_factors.price_factor IS 'Multiply raw price by this to get adjusted price (= 1/qty_factor).';

-- ============================================================
-- 3. Create reprocessing_queue table
-- ============================================================

CREATE TABLE IF NOT EXISTS reprocessing_queue (
    queue_id SERIAL PRIMARY KEY,
    isin VARCHAR(12) NOT NULL,
    reason VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (
        status IN (
            'pending',
            'running',
            'done',
            'failed'
        )
    ),
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_reprocess_status ON reprocessing_queue (status, triggered_at);

CREATE INDEX IF NOT EXISTS idx_reprocess_isin ON reprocessing_queue (isin);

COMMENT ON
TABLE reprocessing_queue IS 'Queue of ISINs whose adj_quantity values need recalculation after new corporate actions or monthly data loads.';

-- ============================================================
-- 4. Add adj_quantity to equity_holdings
-- ============================================================

ALTER TABLE equity_holdings
ADD COLUMN IF NOT EXISTS adj_quantity NUMERIC(20, 4);

COMMENT ON COLUMN equity_holdings.quantity IS 'Raw share count exactly as reported in the AMC disclosure. Never modified.';

COMMENT ON COLUMN equity_holdings.adj_quantity IS 'Backward-adjusted share count after applying corporate action factors. NULL means engine has not run yet. Use this for charting and trending, not raw quantity.';

COMMIT;

SELECT 'Migration 020 applied successfully' AS status;