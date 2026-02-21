-- ============================================================
-- MIGRATION 019: Corporate Actions Enhancements (v2)
-- Date: 2026-02-17
-- ============================================================

BEGIN;

-- 1. Add status, confidence and source to corporate_actions
ALTER TABLE corporate_actions
ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PROPOSED' CHECK (
    status IN (
        'PROPOSED',
        'CONFIRMED',
        'REJECTED'
    )
),
ADD COLUMN IF NOT EXISTS confidence_score FLOAT DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS source TEXT;

-- 2. Add Unique Constraint to enable UPSERT/Double-Lock logic
-- This prevents duplicate entries for the same entity, date, and type.
ALTER TABLE corporate_actions
DROP CONSTRAINT IF EXISTS unique_entity_date_action;

ALTER TABLE corporate_actions
ADD CONSTRAINT unique_entity_date_action UNIQUE (
    entity_id,
    effective_date,
    action_type
);

COMMIT;