-- ============================================================
-- Sector & Company Master Migration
-- Date: 2026-02-11
-- ============================================================

-- 1. Sector Master Table
-- Maps raw AMC sector names (e.g., 'FINANCE') to canonical ones (e.g., 'Financial Services')
CREATE TABLE IF NOT EXISTS sector_master (
    raw_sector_name TEXT PRIMARY KEY,
    canonical_sector TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Company Master Table
-- The analytical entity behind instruments (ISINs)
CREATE TABLE IF NOT EXISTS company_master (
    company_id SERIAL PRIMARY KEY,
    isin VARCHAR(12) NOT NULL REFERENCES isin_master (isin),
    canonical_name TEXT NOT NULL,
    sector TEXT,
    industry TEXT,
    first_seen_date DATE,
    last_seen_date DATE,
    is_listed BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (isin)
);

-- 3. Initial Sector Seeding (Common AMFI/AMC categories)
INSERT INTO
    sector_master (
        raw_sector_name,
        canonical_sector
    )
VALUES (
        'FINANCE',
        'Financial Services'
    ),
    (
        'FINANCIAL SERVICES',
        'Financial Services'
    ),
    ('BANKS', 'Financial Services'),
    (
        'IT',
        'Information Technology'
    ),
    (
        'INFORMATION TECHNOLOGY',
        'Information Technology'
    ),
    (
        'AUTO',
        'Automobile and Auto Components'
    ),
    (
        'AUTOMOBILE',
        'Automobile and Auto Components'
    ),
    ('PHARMA', 'Healthcare'),
    (
        'PHARMACEUTICALS',
        'Healthcare'
    ),
    ('HEALTHCARE', 'Healthcare'),
    (
        'CONSUMER GOODS',
        'Fast Moving Consumer Goods'
    ),
    (
        'FMCG',
        'Fast Moving Consumer Goods'
    ),
    (
        'OIL & GAS',
        'Oil, Gas & Consumable Fuels'
    ),
    (
        'ENERGY',
        'Oil, Gas & Consumable Fuels'
    ),
    (
        'CONSTRUCTION',
        'Construction'
    ),
    ('CHEMICALS', 'Chemicals'),
    ('METALS', 'Metals & Mining'),
    (
        'METALS & MINING',
        'Metals & Mining'
    ) ON CONFLICT (raw_sector_name) DO
UPDATE
SET
    canonical_sector = EXCLUDED.canonical_sector;