-- ============================================================
-- MIGRATION 034: Bulk & Block Deals Tracking Module
-- Date: 2026-10-26
-- ============================================================

BEGIN;

-- 1. MARKET DEALS TABLE (Normalized Bulk & Block Deals)
CREATE TABLE IF NOT EXISTS market_deals (
    deal_id                 BIGSERIAL PRIMARY KEY,
    deal_type               VARCHAR(10) NOT NULL CHECK (deal_type IN ('BULK', 'BLOCK')),
    exchange                VARCHAR(10) NOT NULL CHECK (exchange IN ('NSE', 'BSE')),
    trade_date              DATE NOT NULL,
    company_id              BIGINT REFERENCES companies(company_id) ON DELETE SET NULL,
    isin                    VARCHAR(12),
    symbol                  VARCHAR(50) NOT NULL,
    security_name           VARCHAR(255) NOT NULL,
    client_name             VARCHAR(255) NOT NULL,
    investor_category       VARCHAR(50) NOT NULL DEFAULT 'OTHER', -- MUTUAL_FUND, FII_FPI, DII, PROMOTER, AIF, OTHER
    action                  VARCHAR(10) NOT NULL CHECK (action IN ('BUY', 'SELL')),
    quantity                BIGINT NOT NULL CHECK (quantity > 0),
    trade_price             NUMERIC(14, 4) NOT NULL CHECK (trade_price >= 0),
    trade_value_inr         NUMERIC(18, 2) NOT NULL DEFAULT 0.00,
    pct_equity_traded       NUMERIC(8, 4), -- % of total shares outstanding
    source_fingerprint      VARCHAR(64) NOT NULL, -- SHA256 deterministic hash for idempotency
    source_record_id        VARCHAR(100),
    source_name             VARCHAR(50) NOT NULL,
    ingestion_mode          VARCHAR(20) NOT NULL DEFAULT 'AUTOMATED', -- AUTOMATED, MANUAL_CSV
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_market_deal_fingerprint UNIQUE (source_fingerprint)
);

-- Core search & filter indexes
CREATE INDEX IF NOT EXISTS idx_market_deals_trade_date ON market_deals(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_deals_type_exchange ON market_deals(deal_type, exchange);
CREATE INDEX IF NOT EXISTS idx_market_deals_company ON market_deals(company_id);
CREATE INDEX IF NOT EXISTS idx_market_deals_symbol ON market_deals(symbol);
CREATE INDEX IF NOT EXISTS idx_market_deals_isin ON market_deals(isin);
CREATE INDEX IF NOT EXISTS idx_market_deals_client ON market_deals(client_name);
CREATE INDEX IF NOT EXISTS idx_market_deals_category ON market_deals(investor_category);
CREATE INDEX IF NOT EXISTS idx_market_deals_action ON market_deals(action);

-- 2. INGESTION RUNS (Operational Audit & Reconciliation Ledger)
CREATE TABLE IF NOT EXISTS market_deal_ingestion_runs (
    run_id                  BIGSERIAL PRIMARY KEY,
    run_mode                VARCHAR(20) NOT NULL DEFAULT 'AUTOMATED', -- AUTOMATED, BACKFILL, MANUAL_CSV
    exchange                VARCHAR(10) NOT NULL,                     -- NSE, BSE, BOTH
    deal_type               VARCHAR(10) NOT NULL,                     -- BULK, BLOCK, ALL
    from_date               DATE,
    to_date                 DATE,
    start_time              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time                TIMESTAMP WITH TIME ZONE,
    status                  VARCHAR(20) NOT NULL DEFAULT 'RUNNING',  -- RUNNING, SUCCESS, PARTIAL, FAILED
    records_fetched         INTEGER NOT NULL DEFAULT 0,
    records_inserted        INTEGER NOT NULL DEFAULT 0,
    records_updated         INTEGER NOT NULL DEFAULT 0,
    records_skipped         INTEGER NOT NULL DEFAULT 0,
    records_rejected        INTEGER NOT NULL DEFAULT 0,
    error_summary           TEXT,
    metadata                JSONB
);

CREATE INDEX IF NOT EXISTS idx_deal_runs_start ON market_deal_ingestion_runs(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_deal_runs_status ON market_deal_ingestion_runs(status);

-- 3. DEAL SYNC STATE (Checkpointing & Health Monitoring)
CREATE TABLE IF NOT EXISTS deal_sync_state (
    source_key              VARCHAR(50) PRIMARY KEY,
    last_attempted_at       TIMESTAMP WITH TIME ZONE,
    last_successful_at      TIMESTAMP WITH TIME ZONE,
    status                  VARCHAR(20) NOT NULL DEFAULT 'IDLE',
    earliest_date_covered   DATE,
    latest_date_covered     DATE,
    total_deals_count       INTEGER NOT NULL DEFAULT 0,
    last_error              TEXT,
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Seed initial sync state keys
INSERT INTO deal_sync_state (source_key, status)
VALUES 
    ('NSE_BULK_DEALS', 'IDLE'),
    ('NSE_BLOCK_DEALS', 'IDLE'),
    ('BSE_BULK_DEALS', 'IDLE'),
    ('BSE_BLOCK_DEALS', 'IDLE'),
    ('MANUAL_CSV_IMPORT', 'IDLE')
ON CONFLICT (source_key) DO NOTHING;

-- Grant permissions to existing users
UPDATE users 
SET permissions = permissions || '["view_deals"]'::jsonb
WHERE NOT (permissions @> '["view_deals"]'::jsonb);

-- Record in schema_migrations
INSERT INTO schema_migrations (filename, applied_at)
VALUES ('034_bulk_block_deals.sql', CURRENT_TIMESTAMP)
ON CONFLICT DO NOTHING;

COMMIT;
