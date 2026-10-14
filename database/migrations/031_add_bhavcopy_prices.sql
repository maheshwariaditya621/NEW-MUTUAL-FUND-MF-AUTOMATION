-- Migration 031: Add Bhavcopy EOD prices and metadata to companies table

ALTER TABLE companies 
ADD COLUMN IF NOT EXISTS close_price NUMERIC(12, 2),
ADD COLUMN IF NOT EXISTS prev_close NUMERIC(12, 2),
ADD COLUMN IF NOT EXISTS price_date DATE,
ADD COLUMN IF NOT EXISTS price_change_pct NUMERIC(6, 2);

-- Index on isin and close_price for instant price lookups
CREATE INDEX IF NOT EXISTS idx_companies_isin_price ON companies(isin, close_price);

INSERT INTO schema_migrations (filename, applied_at)
VALUES ('031_add_bhavcopy_prices.sql', CURRENT_TIMESTAMP)
ON CONFLICT (filename) DO NOTHING;
