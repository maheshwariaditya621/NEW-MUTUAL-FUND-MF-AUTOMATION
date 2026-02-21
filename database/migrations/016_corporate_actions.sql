-- ============================================================
-- MIGRATION 016: Corporate Actions Registry
-- Date: 2026-02-17
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS corporate_actions (
    id SERIAL PRIMARY KEY,
    entity_id INTEGER NOT NULL REFERENCES corporate_entities (entity_id),
    effective_date DATE NOT NULL,
    ratio_factor FLOAT NOT NULL,
    action_type TEXT NOT NULL, -- 'SPLIT', 'BONUS', 'MERGER'
    old_isin TEXT,
    new_isin TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_corporate_actions_entity_id ON corporate_actions (entity_id);

CREATE INDEX IF NOT EXISTS idx_corporate_actions_date ON corporate_actions (effective_date);

COMMENT ON
TABLE corporate_actions IS 'Registry of corporate actions used to normalize historical share counts';

COMMIT;