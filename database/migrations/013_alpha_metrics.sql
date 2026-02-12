-- ============================================================
-- MIGRATION 013: COMPUTED ALPHA METRICS
-- Date: 2026-02-12
-- Author: Antigravity
-- Description: Stores pre-computed alpha (excess return) metrics.
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS computed_alpha_metrics (
    metric_id SERIAL PRIMARY KEY,
    scheme_id INTEGER REFERENCES schemes (scheme_id),
    benchmark_id INTEGER REFERENCES benchmark_master (benchmark_id),
    calculation_date DATE NOT NULL,

-- Status Flag (e.g. 'CALCULATED', 'NO_BENCHMARK', 'INSUFFICIENT_HISTORY')
metrics_status TEXT DEFAULT 'CALCULATED',

-- Point-to-Point Excess Returns (Fund Return - Benchmark Return)
alpha_1m NUMERIC(10, 4),
alpha_3m NUMERIC(10, 4),
alpha_6m NUMERIC(10, 4),
alpha_1y NUMERIC(10, 4),
alpha_3y NUMERIC(10, 4),
alpha_5y NUMERIC(10, 4),

-- Rolling Metric Summaries (e.g. % of times Alpha > 0 in last 3 years)
rolling_win_rate_1y NUMERIC(5, 2), -- % of rolling 1Y periods where extraction > 0
    rolling_win_rate_3y NUMERIC(5, 2),
    
    created_at TIMESTAMP DEFAULT (now() at time zone 'utc'),
    updated_at TIMESTAMP DEFAULT (now() at time zone 'utc'),
    
    UNIQUE (scheme_id, calculation_date)
);

-- Index for fast retrieval by scheme
CREATE INDEX IF NOT EXISTS idx_alpha_scheme_date ON computed_alpha_metrics (scheme_id, calculation_date);

COMMIT;