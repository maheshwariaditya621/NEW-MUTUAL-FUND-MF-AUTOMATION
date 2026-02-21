-- ============================================================
-- MIGRATION 017: Security Master Strengthening & Data Continuity
-- Date: 2026-02-17
-- ============================================================

BEGIN;

-- 1. Strengthen corporate_entities (The Identity Layer)
ALTER TABLE corporate_entities
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_corporate_entities_canonical_name_trgm ON corporate_entities USING gin (canonical_name gin_trgm_ops);

-- 2. Strengthen isin_master (The Temporal Mapping Layer)
-- Add auditing and period tracking
ALTER TABLE isin_master
ADD COLUMN IF NOT EXISTS first_seen_period_id INTEGER,
ADD COLUMN IF NOT EXISTS last_seen_period_id INTEGER,
ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Ensure an ISIN isn't linked to the same entity multiple times
-- Also ensures faster lookup on (entity_id, isin)
ALTER TABLE isin_master DROP CONSTRAINT IF EXISTS unique_entity_isin;

ALTER TABLE isin_master
ADD CONSTRAINT unique_entity_isin UNIQUE (entity_id, isin);

-- 3. Indexes for tiered resolution speed
CREATE INDEX IF NOT EXISTS idx_isin_master_isin ON isin_master (isin);

CREATE INDEX IF NOT EXISTS idx_isin_master_entity_id ON isin_master (entity_id);

-- 4. Enable pg_trgm for fuzzy matching if not already enabled
CREATE EXTENSION IF NOT EXISTS pg_trgm;

COMMIT;