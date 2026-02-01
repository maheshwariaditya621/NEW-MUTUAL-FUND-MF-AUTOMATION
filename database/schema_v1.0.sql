-- ============================================================
-- MUTUAL FUND PORTFOLIO ANALYTICS PLATFORM
-- FINAL SCHEMA v1.0 (LOCKED)
-- ============================================================
-- Database: PostgreSQL 14+
-- Status: Production-Ready
-- Date: 2026-02-01
--
-- IMPORTANT: This schema is LOCKED for production use.
-- Structural changes should NOT be made unless absolutely necessary.
-- ============================================================

-- ============================================================
-- TABLE 1: amcs
-- Asset Management Companies (mutual fund houses)
-- ============================================================

CREATE TABLE amcs (
    amc_id BIGSERIAL PRIMARY KEY,
    amc_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_amcs_name ON amcs (amc_name);

COMMENT ON
TABLE amcs IS 'Master table of Asset Management Companies (mutual fund houses)';

COMMENT ON COLUMN amcs.amc_name IS 'Full official name (e.g., "ICICI Prudential Mutual Fund")';

-- ============================================================
-- TABLE 2: schemes
-- Mutual fund schemes
-- ============================================================

CREATE TABLE schemes (
    scheme_id       BIGSERIAL PRIMARY KEY,
    amc_id          BIGINT NOT NULL REFERENCES amcs(amc_id) ON DELETE RESTRICT,
    scheme_name     VARCHAR(500) NOT NULL,
    plan_type       VARCHAR(10) NOT NULL CHECK (plan_type IN ('Direct', 'Regular')),
    option_type     VARCHAR(10) NOT NULL CHECK (option_type IN ('Growth', 'Dividend', 'IDCW')),
    scheme_category VARCHAR(100),
    scheme_code     VARCHAR(50),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

-- Uniqueness: One scheme per (AMC, scheme_name, plan_type, option_type)
CONSTRAINT uq_scheme UNIQUE (amc_id, scheme_name, plan_type, option_type)
);

CREATE INDEX idx_schemes_amc ON schemes (amc_id);

CREATE INDEX idx_schemes_name ON schemes (scheme_name);

COMMENT ON TABLE schemes IS 'Master table of mutual fund schemes';

COMMENT ON COLUMN schemes.scheme_name IS 'Canonical scheme name WITHOUT plan/option suffixes';

COMMENT ON COLUMN schemes.plan_type IS 'Exactly "Direct" or "Regular"';

COMMENT ON COLUMN schemes.option_type IS 'Exactly "Growth", "Dividend", or "IDCW"';

COMMENT ON CONSTRAINT uq_scheme ON schemes IS 'Ensures one unique scheme per (AMC, name, plan, option)';

-- ============================================================
-- TABLE 3: periods
-- Monthly portfolio disclosure periods
-- ============================================================

CREATE TABLE periods (
    period_id       BIGSERIAL PRIMARY KEY,
    year            INTEGER NOT NULL CHECK (year >= 2020 AND year <= 2100),
    month           INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
    period_end_date DATE NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

-- Uniqueness: One record per (year, month)
CONSTRAINT uq_period UNIQUE (year, month) );

CREATE INDEX idx_periods_date ON periods (period_end_date);

COMMENT ON
TABLE periods IS 'Master table of monthly portfolio disclosure periods';

COMMENT ON COLUMN periods.year IS '4-digit year (e.g., 2025)';

COMMENT ON COLUMN periods.month IS 'Month number (1-12)';

COMMENT ON COLUMN periods.period_end_date IS 'Last day of the month (e.g., 2025-01-31)';

-- ============================================================
-- TABLE 4: companies
-- Publicly listed companies (equity only)
-- ============================================================

CREATE TABLE companies (
    company_id      BIGSERIAL PRIMARY KEY,
    isin            CHAR(12) NOT NULL UNIQUE,
    company_name    VARCHAR(255) NOT NULL,
    exchange_symbol VARCHAR(20),
    sector          VARCHAR(100),
    industry        VARCHAR(100),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

-- ISIN validation: Must start with 'INE', exactly 12 chars, security code '10'
CONSTRAINT chk_isin_format CHECK (
        isin ~ '^INE[A-Z0-9]{6}10[A-Z0-9]{1}$'
    )
);

CREATE UNIQUE INDEX idx_companies_isin ON companies (isin);

CREATE INDEX idx_companies_name ON companies (company_name);

CREATE INDEX idx_companies_sector ON companies (sector);

COMMENT ON
TABLE companies IS 'Master table of publicly listed companies (equity only)';

COMMENT ON COLUMN companies.isin IS '12-character ISIN code (e.g., "INE002A01018")';

COMMENT ON COLUMN companies.company_name IS 'Official company name';

COMMENT ON CONSTRAINT chk_isin_format ON companies IS 'Ensures ISIN starts with INE, is 12 chars, and has security code 10 (equity)';

-- ============================================================
-- TABLE 5: scheme_snapshots
-- Tracks successfully loaded scheme-period combinations
-- ============================================================

CREATE TABLE scheme_snapshots (
    snapshot_id     BIGSERIAL PRIMARY KEY,
    scheme_id       BIGINT NOT NULL REFERENCES schemes(scheme_id) ON DELETE CASCADE,
    period_id       BIGINT NOT NULL REFERENCES periods(period_id) ON DELETE CASCADE,
    total_holdings  INTEGER NOT NULL CHECK (total_holdings >= 0),
    total_value_inr NUMERIC(20, 2) NOT NULL CHECK (total_value_inr >= 0),
    holdings_count  INTEGER NOT NULL CHECK (holdings_count >= 0),
    loaded_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

-- Uniqueness: One snapshot per (scheme, period)
CONSTRAINT uq_snapshot UNIQUE (scheme_id, period_id) );

CREATE INDEX idx_snapshots_scheme ON scheme_snapshots (scheme_id);

CREATE INDEX idx_snapshots_period ON scheme_snapshots (period_id);

CREATE INDEX idx_snapshots_loaded ON scheme_snapshots (loaded_at);

COMMENT ON
TABLE scheme_snapshots IS 'Tracks successfully loaded scheme-period combinations';

COMMENT ON COLUMN scheme_snapshots.total_holdings IS 'Total number of equity holdings rows in this snapshot (includes zero-value positions)';

COMMENT ON COLUMN scheme_snapshots.total_value_inr IS 'Total portfolio value in INR';

COMMENT ON COLUMN scheme_snapshots.holdings_count IS 'Count of distinct companies held. Due to UNIQUE(snapshot_id, company_id) constraint, this normally equals total_holdings. Difference can only occur in future schema versions if constraint is relaxed.';

COMMENT ON CONSTRAINT uq_snapshot ON scheme_snapshots IS 'Ensures one snapshot per scheme per period';

-- ============================================================
-- TABLE 6: equity_holdings
-- Individual equity holdings (main fact table)
-- ============================================================

CREATE TABLE equity_holdings (
    holding_id      BIGSERIAL PRIMARY KEY,
    snapshot_id     BIGINT NOT NULL REFERENCES scheme_snapshots(snapshot_id) ON DELETE CASCADE,
    company_id      BIGINT NOT NULL REFERENCES companies(company_id) ON DELETE RESTRICT,
    quantity        BIGINT NOT NULL CHECK (quantity >= 0),
    market_value_inr NUMERIC(20, 2) NOT NULL CHECK (market_value_inr >= 0),
    percent_of_nav  NUMERIC(8, 4) NOT NULL CHECK (percent_of_nav >= 0 AND percent_of_nav <= 100),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

-- Uniqueness: One holding per (snapshot, company)
CONSTRAINT uq_holding UNIQUE (snapshot_id, company_id) );

-- Indexes for analytics queries
CREATE INDEX idx_holdings_snapshot ON equity_holdings (snapshot_id);

CREATE INDEX idx_holdings_company ON equity_holdings (company_id);

CREATE INDEX idx_holdings_value ON equity_holdings (market_value_inr DESC);

CREATE INDEX idx_holdings_nav ON equity_holdings (percent_of_nav DESC);

-- Composite index for common join pattern
CREATE INDEX idx_holdings_snapshot_company ON equity_holdings (snapshot_id, company_id);

COMMENT ON
TABLE equity_holdings IS 'Individual equity holdings (main fact table)';

COMMENT ON COLUMN equity_holdings.quantity IS 'Number of shares held (can be 0 for exited positions or very small positions)';

COMMENT ON COLUMN equity_holdings.market_value_inr IS 'Market value in Indian Rupees (₹), can be 0 for exited positions';

COMMENT ON COLUMN equity_holdings.percent_of_nav IS 'Percentage of NAV (0-100 scale, e.g., 3.06 = 3.06%), can be 0 for exited positions or rounding';

COMMENT ON CONSTRAINT uq_holding ON equity_holdings IS 'Ensures one holding per company per snapshot';

-- ============================================================
-- END OF SCHEMA v1.0
-- ============================================================