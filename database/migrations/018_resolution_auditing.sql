-- ============================================================
-- MIGRATION 018: Resolution Auditing
-- Date: 2026-02-17
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS resolution_audit (
    audit_id SERIAL PRIMARY KEY,
    isin VARCHAR(12),
    raw_name TEXT,
    resolved_entity_id INTEGER REFERENCES corporate_entities (entity_id),
    resolution_tier TEXT, -- e.g., 'TIER 1 (ISIN)', 'TIER 4 (FUZZY)'
    details TEXT, -- JSON or string with more info
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_resolution_audit_isin ON resolution_audit (isin);

CREATE INDEX IF NOT EXISTS idx_resolution_audit_entity_id ON resolution_audit (resolved_entity_id);

COMMIT;