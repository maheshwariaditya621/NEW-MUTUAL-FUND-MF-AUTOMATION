-- ============================================================
-- MIGRATION 015: Corporate Entities & Entity Decoupling
-- Date: 2026-02-16
-- ============================================================

BEGIN;

-- 1. Create Corporate Entities table (The Logical Entity)
CREATE TABLE IF NOT EXISTS corporate_entities (
    entity_id SERIAL PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    group_symbol VARCHAR(20) UNIQUE, -- The persistent anchor (e.g., 'KOTAKBANK')
    sector TEXT,
    industry TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Add entity_id reference to companies table
ALTER TABLE companies
ADD COLUMN IF NOT EXISTS entity_id INTEGER REFERENCES corporate_entities (entity_id);

-- 3. Add same reference to isin_master for consistency
ALTER TABLE isin_master
ADD COLUMN IF NOT EXISTS entity_id INTEGER REFERENCES corporate_entities (entity_id);

-- 4. Indexing for aggregation performance
CREATE INDEX IF NOT EXISTS idx_companies_entity_id ON companies (entity_id);

CREATE INDEX IF NOT EXISTS idx_isin_master_entity_id ON isin_master (entity_id);

COMMENT ON
TABLE corporate_entities IS 'Logical business entities that may have multiple ISINs or Symbols over time';

COMMENT ON COLUMN companies.entity_id IS 'Link to the logical corporate entity for aggregation across ISIN changes';

COMMIT;