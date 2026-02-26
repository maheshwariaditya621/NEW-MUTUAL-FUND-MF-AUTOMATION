-- Migration: 022_scheme_mappings
-- Description: Create lookup table for canonical scheme name resolution

CREATE TABLE IF NOT EXISTS scheme_name_mappings (
    mapping_id SERIAL PRIMARY KEY,
    amc_id INTEGER NOT NULL REFERENCES amcs (amc_id) ON DELETE CASCADE,
    source_name VARCHAR(255) NOT NULL,
    canonical_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (amc_id, source_name)
);

CREATE INDEX IF NOT EXISTS idx_scheme_mappings_lookup ON scheme_name_mappings (amc_id, source_name);