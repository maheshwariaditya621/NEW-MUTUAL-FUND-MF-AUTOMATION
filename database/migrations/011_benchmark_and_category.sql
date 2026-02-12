-- ============================================================
-- MIGRATION 011: BENCHMARK & CATEGORY INFRASTRUCTURE
-- Date: 2026-02-12
-- Author: Antigravity
-- ============================================================

BEGIN;

-- 1. BENCHMARK MASTER
CREATE TABLE IF NOT EXISTS benchmark_master (
    benchmark_id SERIAL PRIMARY KEY,
    benchmark_name TEXT NOT NULL,
    index_symbol TEXT UNIQUE, -- e.g., NIFTY_50_TRI
    provider TEXT, -- NSE, BSE
    is_tri BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT(now() at time zone 'utc'),
    updated_at TIMESTAMP DEFAULT(now() at time zone 'utc')
);

-- 2. BENCHMARK HISTORY
CREATE TABLE IF NOT EXISTS benchmark_history (
    benchmark_id INTEGER REFERENCES benchmark_master (benchmark_id),
    nav_date DATE NOT NULL,
    index_value NUMERIC(18, 6) NOT NULL,
    PRIMARY KEY (benchmark_id, nav_date)
);

-- 3. SCHEME CATEGORY MASTER
-- Mapped by AMFI scheme code as requested
CREATE TABLE IF NOT EXISTS scheme_category_master (
    amfi_code TEXT PRIMARY KEY,
    broad_category TEXT, -- Equity, Hybrid, etc.
    scheme_category TEXT, -- Large Cap, Flexi Cap, etc.
    sub_category TEXT,
    created_at TIMESTAMP DEFAULT(now() at time zone 'utc'),
    updated_at TIMESTAMP DEFAULT(now() at time zone 'utc')
);

-- 4. SCHEME BENCHMARK HISTORY (Temporal Mapping)
CREATE TABLE IF NOT EXISTS scheme_benchmark_history (
    history_id SERIAL PRIMARY KEY,
    scheme_id INTEGER REFERENCES schemes (scheme_id),
    benchmark_id INTEGER REFERENCES benchmark_master (benchmark_id),
    start_date DATE NOT NULL,
    end_date DATE, -- NULL means active
    created_at TIMESTAMP DEFAULT(now() at time zone 'utc'),
    updated_at TIMESTAMP DEFAULT(now() at time zone 'utc'),
    UNIQUE (scheme_id, start_date)
);

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_bench_hist_date ON benchmark_history (nav_date);

CREATE INDEX IF NOT EXISTS idx_scheme_bench_scheme ON scheme_benchmark_history (scheme_id);

COMMIT;