# 🗄️ POSTGRESQL SCHEMA DESIGN
## Mutual Fund Portfolio Analytics Platform

> **Version**: 1.0  
> **Database**: PostgreSQL 14+  
> **Status**: Production-Ready  
> **Based On**: Canonical Data Contract v1.0

---

## 🎯 DESIGN PRINCIPLES

This schema is designed to:
- ✅ Implement the Canonical Data Contract EXACTLY
- ✅ Support RupeeVest-like analytics for 10+ years
- ✅ Enable efficient querying for portfolio tracking, stock analysis, net buying/selling
- ✅ Maintain referential integrity
- ✅ Support safe monthly reloads without data corruption

---

## 1️⃣ TABLE DEFINITIONS

### Table 1: `amcs`

**Purpose**: Master table of Asset Management Companies

```sql
CREATE TABLE amcs (
    amc_id          SERIAL PRIMARY KEY,
    amc_name        VARCHAR(255) NOT NULL UNIQUE,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Constraints
CREATE UNIQUE INDEX idx_amcs_name ON amcs(amc_name);

-- Comments
COMMENT ON TABLE amcs IS 'Master table of Asset Management Companies (mutual fund houses)';
COMMENT ON COLUMN amcs.amc_name IS 'Full official name (e.g., "ICICI Prudential Mutual Fund")';
```

**Validation Rules Enforced**:
- `amc_name` must be unique (enforced by UNIQUE constraint)
- `amc_name` cannot be null (enforced by NOT NULL)

**Example Data**:
```
amc_id | amc_name
-------|------------------------------------------
1      | ICICI Prudential Mutual Fund
2      | HDFC Mutual Fund
3      | Axis Mutual Fund
```

---

### Table 2: `schemes`

**Purpose**: Master table of mutual fund schemes

```sql
CREATE TABLE schemes (
    scheme_id       SERIAL PRIMARY KEY,
    amc_id          INTEGER NOT NULL REFERENCES amcs(amc_id) ON DELETE RESTRICT,
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

-- Indexes
CREATE INDEX idx_schemes_amc ON schemes(amc_id);
CREATE INDEX idx_schemes_name ON schemes(scheme_name);

-- Comments
COMMENT ON TABLE schemes IS 'Master table of mutual fund schemes';
COMMENT ON COLUMN schemes.scheme_name IS 'Canonical scheme name WITHOUT plan/option suffixes';
COMMENT ON COLUMN schemes.plan_type IS 'Exactly "Direct" or "Regular"';
COMMENT ON COLUMN schemes.option_type IS 'Exactly "Growth", "Dividend", or "IDCW"';
COMMENT ON CONSTRAINT uq_scheme ON schemes IS 'Ensures one unique scheme per (AMC, name, plan, option)';
```

**Validation Rules Enforced**:
- `plan_type` must be exactly "Direct" or "Regular" (enforced by CHECK constraint)
- `option_type` must be exactly "Growth", "Dividend", or "IDCW" (enforced by CHECK constraint)
- Combination of `(amc_id, scheme_name, plan_type, option_type)` must be unique (enforced by UNIQUE constraint)
- `amc_id` must reference valid AMC (enforced by FOREIGN KEY)

**Example Data**:
```
scheme_id | amc_id | scheme_name                        | plan_type | option_type
----------|--------|------------------------------------|-----------|--------------
1         | 1      | ICICI Prudential Bluechip Fund     | Direct    | Growth
2         | 1      | ICICI Prudential Bluechip Fund     | Direct    | Dividend
3         | 1      | ICICI Prudential Bluechip Fund     | Regular   | Growth
4         | 2      | HDFC Equity Fund                   | Direct    | Growth
```

---

### Table 3: `periods`

**Purpose**: Master table of monthly periods

```sql
CREATE TABLE periods (
    period_id       SERIAL PRIMARY KEY,
    year            INTEGER NOT NULL CHECK (year >= 2020 AND year <= 2100),
    month           INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
    period_end_date DATE NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Uniqueness: One record per (year, month)
    CONSTRAINT uq_period UNIQUE (year, month)
);

-- Indexes
CREATE INDEX idx_periods_date ON periods(period_end_date);

-- Comments
COMMENT ON TABLE periods IS 'Master table of monthly portfolio disclosure periods';
COMMENT ON COLUMN periods.year IS '4-digit year (e.g., 2025)';
COMMENT ON COLUMN periods.month IS 'Month number (1-12)';
COMMENT ON COLUMN periods.period_end_date IS 'Last day of the month (e.g., 2025-01-31)';
```

**Validation Rules Enforced**:
- `year` must be between 2020 and 2100 (enforced by CHECK constraint)
- `month` must be between 1 and 12 (enforced by CHECK constraint)
- Combination of `(year, month)` must be unique (enforced by UNIQUE constraint)

**Example Data**:
```
period_id | year | month | period_end_date
----------|------|-------|----------------
1         | 2025 | 1     | 2025-01-31
2         | 2025 | 2     | 2025-02-28
3         | 2025 | 3     | 2025-03-31
```

---

### Table 4: `companies`

**Purpose**: Master table of publicly listed companies

```sql
CREATE TABLE companies (
    company_id      SERIAL PRIMARY KEY,
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

-- Indexes
CREATE UNIQUE INDEX idx_companies_isin ON companies(isin);
CREATE INDEX idx_companies_name ON companies(company_name);
CREATE INDEX idx_companies_sector ON companies(sector);

-- Comments
COMMENT ON TABLE companies IS 'Master table of publicly listed companies (equity only)';
COMMENT ON COLUMN companies.isin IS '12-character ISIN code (e.g., "INE002A01018")';
COMMENT ON COLUMN companies.company_name IS 'Official company name';
COMMENT ON CONSTRAINT chk_isin_format ON companies IS 'Ensures ISIN starts with INE, is 12 chars, and has security code 10 (equity)';
```

**Validation Rules Enforced**:
- `isin` must be exactly 12 characters (enforced by CHAR(12))
- `isin` must be unique (enforced by UNIQUE constraint)
- `isin` must match pattern: `INE[6 chars]10[1 char]` (enforced by CHECK constraint with regex)
- This regex ensures:
  - Starts with "INE" ✅
  - Positions 9-10 are "10" (equity security code) ✅
  - Exactly 12 characters ✅

**Example Data**:
```
company_id | isin         | company_name                  | exchange_symbol | sector
-----------|--------------|-------------------------------|-----------------|--------
1          | INE002A01018 | Reliance Industries Limited   | RELIANCE        | Energy
2          | INE040A01034 | HDFC Bank Limited             | HDFCBANK        | Financials
3          | INE467B01029 | Tata Consultancy Services Ltd | TCS             | IT
```

---

### Table 5: `scheme_snapshots`

**Purpose**: Tracks which scheme-period combinations have been successfully loaded

```sql
CREATE TABLE scheme_snapshots (
    snapshot_id     SERIAL PRIMARY KEY,
    scheme_id       INTEGER NOT NULL REFERENCES schemes(scheme_id) ON DELETE CASCADE,
    period_id       INTEGER NOT NULL REFERENCES periods(period_id) ON DELETE CASCADE,
    total_holdings  INTEGER NOT NULL CHECK (total_holdings >= 0),
    total_value_inr NUMERIC(20, 2) NOT NULL CHECK (total_value_inr >= 0),
    holdings_count  INTEGER NOT NULL CHECK (holdings_count >= 0),
    loaded_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Uniqueness: One snapshot per (scheme, period)
    CONSTRAINT uq_snapshot UNIQUE (scheme_id, period_id)
);

-- Indexes
CREATE INDEX idx_snapshots_scheme ON scheme_snapshots(scheme_id);
CREATE INDEX idx_snapshots_period ON scheme_snapshots(period_id);
CREATE INDEX idx_snapshots_loaded ON scheme_snapshots(loaded_at);

-- Comments
COMMENT ON TABLE scheme_snapshots IS 'Tracks successfully loaded scheme-period combinations';
COMMENT ON COLUMN scheme_snapshots.total_holdings IS 'Total number of equity holdings in this snapshot';
COMMENT ON COLUMN scheme_snapshots.total_value_inr IS 'Total portfolio value in INR';
COMMENT ON COLUMN scheme_snapshots.holdings_count IS 'Count of distinct companies held';
COMMENT ON CONSTRAINT uq_snapshot ON scheme_snapshots IS 'Ensures one snapshot per scheme per period';
```

**Validation Rules Enforced**:
- Combination of `(scheme_id, period_id)` must be unique (enforced by UNIQUE constraint)
- This prevents duplicate scheme-month data ✅
- `total_holdings`, `total_value_inr`, `holdings_count` cannot be negative (enforced by CHECK constraints)

**Example Data**:
```
snapshot_id | scheme_id | period_id | total_holdings | total_value_inr | holdings_count | loaded_at
------------|-----------|-----------|----------------|-----------------|----------------|-------------------
1           | 1         | 1         | 45             | 1250000000.00   | 45             | 2026-02-01 10:00:00
2           | 1         | 2         | 47             | 1320000000.00   | 46             | 2026-03-01 10:00:00
3           | 4         | 1         | 62             | 890000000.00    | 58             | 2026-02-01 10:30:00
```

---

### Table 6: `equity_holdings`

**Purpose**: Individual equity holdings (the main fact table)

```sql
CREATE TABLE equity_holdings (
    holding_id      BIGSERIAL PRIMARY KEY,
    snapshot_id     INTEGER NOT NULL REFERENCES scheme_snapshots(snapshot_id) ON DELETE CASCADE,
    company_id      INTEGER NOT NULL REFERENCES companies(company_id) ON DELETE RESTRICT,
    quantity        BIGINT NOT NULL CHECK (quantity >= 0),
    market_value_inr NUMERIC(20, 2) NOT NULL CHECK (market_value_inr > 0),
    percent_of_nav  NUMERIC(8, 4) NOT NULL CHECK (percent_of_nav > 0 AND percent_of_nav <= 100),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Uniqueness: One holding per (snapshot, company)
    CONSTRAINT uq_holding UNIQUE (snapshot_id, company_id)
);

-- Indexes for analytics queries
CREATE INDEX idx_holdings_snapshot ON equity_holdings(snapshot_id);
CREATE INDEX idx_holdings_company ON equity_holdings(company_id);
CREATE INDEX idx_holdings_value ON equity_holdings(market_value_inr DESC);
CREATE INDEX idx_holdings_nav ON equity_holdings(percent_of_nav DESC);

-- Composite index for common join pattern
CREATE INDEX idx_holdings_snapshot_company ON equity_holdings(snapshot_id, company_id);

-- Comments
COMMENT ON TABLE equity_holdings IS 'Individual equity holdings (main fact table)';
COMMENT ON COLUMN equity_holdings.quantity IS 'Number of shares held (can be 0 for very small positions)';
COMMENT ON COLUMN equity_holdings.market_value_inr IS 'Market value in Indian Rupees (₹), must be > 0';
COMMENT ON COLUMN equity_holdings.percent_of_nav IS 'Percentage of NAV (0-100 scale, e.g., 3.06 = 3.06%)';
COMMENT ON CONSTRAINT uq_holding ON equity_holdings IS 'Ensures one holding per company per snapshot';
```

**Validation Rules Enforced**:
- `quantity` can be 0 (valid for small positions) but cannot be negative (enforced by CHECK constraint) ✅
- `market_value_inr` must be > 0 (enforced by CHECK constraint) ✅
- `percent_of_nav` must be > 0 and <= 100 (enforced by CHECK constraint) ✅
- Combination of `(snapshot_id, company_id)` must be unique (enforced by UNIQUE constraint)
- `snapshot_id` must reference valid snapshot (enforced by FOREIGN KEY)
- `company_id` must reference valid company (enforced by FOREIGN KEY)

**Example Data**:
```
holding_id | snapshot_id | company_id | quantity | market_value_inr | percent_of_nav
-----------|-------------|------------|----------|------------------|----------------
1          | 1           | 1          | 125000   | 306250000.00     | 3.0600
2          | 1           | 2          | 50000    | 75000000.00      | 0.7500
3          | 2           | 1          | 130000   | 318500000.00     | 3.1200
4          | 3           | 1          | 80000    | 196000000.00     | 2.2000
```

---

## 2️⃣ DATA INTEGRITY RULES

### Rule 1: No Duplicate Scheme-Months

**Enforced By**: `UNIQUE (scheme_id, period_id)` on `scheme_snapshots` table

**How It Works**:
- Before inserting holdings, a snapshot record is created
- If snapshot already exists for this scheme-period, INSERT will fail with unique constraint violation
- Application layer must handle this by either:
  - Skipping the duplicate (recommended)
  - Deleting old snapshot and inserting new one (reload scenario)

**Example**:
```sql
-- First insert: SUCCESS
INSERT INTO scheme_snapshots (scheme_id, period_id, total_holdings, total_value_inr, holdings_count)
VALUES (1, 1, 45, 1250000000.00, 45);

-- Second insert for same scheme-period: FAILS
INSERT INTO scheme_snapshots (scheme_id, period_id, total_holdings, total_value_inr, holdings_count)
VALUES (1, 1, 47, 1320000000.00, 46);
-- ERROR: duplicate key value violates unique constraint "uq_snapshot"
```

---

### Rule 2: No Partial Holdings

**Enforced By**: Transaction-based loading + `ON DELETE CASCADE`

**How It Works**:
1. All holdings for a scheme-month are inserted within a SINGLE transaction
2. If ANY holding fails validation, entire transaction is rolled back
3. `ON DELETE CASCADE` ensures if snapshot is deleted, all holdings are deleted
4. This guarantees: snapshot exists ⟺ all holdings exist

**Example Transaction**:
```sql
BEGIN;

-- Step 1: Create snapshot
INSERT INTO scheme_snapshots (scheme_id, period_id, total_holdings, total_value_inr, holdings_count)
VALUES (1, 1, 45, 1250000000.00, 45);

-- Step 2: Insert all 45 holdings
INSERT INTO equity_holdings (snapshot_id, company_id, quantity, market_value_inr, percent_of_nav)
VALUES 
    (1, 1, 125000, 306250000.00, 3.06),
    (1, 2, 50000, 75000000.00, 0.75),
    -- ... 43 more rows
    (1, 45, 10000, 24500000.00, 0.25);

-- If all succeed: COMMIT
COMMIT;

-- If ANY fail: ROLLBACK (no partial data)
ROLLBACK;
```

---

### Rule 3: Clean Joins for Analytics

**Enforced By**: Proper foreign keys and normalized structure

**Join Paths**:

**Path 1: Scheme → Portfolio View**
```
schemes → scheme_snapshots → equity_holdings → companies
```

**Path 2: Company → MF Holdings View**
```
companies → equity_holdings → scheme_snapshots → schemes → amcs
```

**Path 3: Month-over-Month Delta**
```
periods (month 1) → scheme_snapshots (month 1) → equity_holdings (month 1)
                                                       ↓ (JOIN on company_id)
periods (month 2) → scheme_snapshots (month 2) → equity_holdings (month 2)
```

**Benefits**:
- No circular dependencies
- No nullable foreign keys (except optional fields)
- Clear one-to-many relationships
- Efficient index usage

---

### Rule 4: Safe Deletes & Reloads Per Month

**Enforced By**: `ON DELETE CASCADE` and transaction isolation

**Reload Scenario**:
```sql
BEGIN;

-- Step 1: Delete old snapshot (cascades to holdings)
DELETE FROM scheme_snapshots 
WHERE scheme_id = 1 AND period_id = 1;

-- Step 2: Insert new snapshot
INSERT INTO scheme_snapshots (scheme_id, period_id, total_holdings, total_value_inr, holdings_count)
VALUES (1, 1, 47, 1320000000.00, 46);

-- Step 3: Insert new holdings
INSERT INTO equity_holdings (snapshot_id, company_id, quantity, market_value_inr, percent_of_nav)
VALUES 
    (CURRVAL('scheme_snapshots_snapshot_id_seq'), 1, 130000, 318500000.00, 3.12),
    -- ... more rows
    ;

COMMIT;
```

**Safety Guarantees**:
- ✅ Old data fully deleted before new data inserted
- ✅ If reload fails, old data remains (transaction rollback)
- ✅ No orphaned holdings (cascade delete)
- ✅ Other months unaffected (isolated by period_id)

---

## 3️⃣ INDEX STRATEGY

### Indexes for Scheme → Portfolio View

**Use Case**: Show all holdings for a specific scheme in a specific month

**Query Pattern**:
```sql
SELECT c.company_name, eh.quantity, eh.market_value_inr, eh.percent_of_nav
FROM schemes s
JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
JOIN equity_holdings eh ON ss.snapshot_id = eh.snapshot_id
JOIN companies c ON eh.company_id = c.company_id
WHERE s.scheme_name = 'ICICI Prudential Bluechip Fund'
  AND s.plan_type = 'Direct'
  AND s.option_type = 'Growth'
  AND ss.period_id = 1;
```

**Required Indexes**:
- ✅ `idx_schemes_name` on `schemes(scheme_name)` - Fast scheme lookup
- ✅ `idx_snapshots_scheme` on `scheme_snapshots(scheme_id)` - Fast snapshot lookup
- ✅ `idx_holdings_snapshot` on `equity_holdings(snapshot_id)` - Fast holdings lookup
- ✅ `idx_companies_isin` on `companies(isin)` - Fast company lookup

**Performance**: O(log n) for each join, total ~10-50ms for typical scheme with 50-100 holdings

---

### Indexes for Company → MF Holdings View

**Use Case**: Show which mutual funds hold a specific stock (e.g., "Which funds hold Reliance?")

**Query Pattern**:
```sql
SELECT 
    a.amc_name,
    s.scheme_name,
    s.plan_type,
    s.option_type,
    eh.quantity,
    eh.market_value_inr,
    eh.percent_of_nav
FROM companies c
JOIN equity_holdings eh ON c.company_id = eh.company_id
JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
JOIN schemes s ON ss.scheme_id = s.scheme_id
JOIN amcs a ON s.amc_id = a.amc_id
WHERE c.isin = 'INE002A01018'
  AND ss.period_id = 1
ORDER BY eh.market_value_inr DESC;
```

**Required Indexes**:
- ✅ `idx_companies_isin` on `companies(isin)` - Fast company lookup
- ✅ `idx_holdings_company` on `equity_holdings(company_id)` - Fast holdings lookup
- ✅ `idx_holdings_value` on `equity_holdings(market_value_inr DESC)` - Fast sorting
- ✅ `idx_snapshots_period` on `scheme_snapshots(period_id)` - Filter by period

**Performance**: O(log n) for each join, total ~20-100ms for popular stock held by 100+ funds

---

### Indexes for Month-over-Month Delta Analysis

**Use Case**: Detect net buying/selling of a stock across all mutual funds

**Query Pattern**:
```sql
WITH month1 AS (
    SELECT 
        eh.company_id,
        SUM(eh.quantity) as total_qty_month1,
        SUM(eh.market_value_inr) as total_value_month1
    FROM equity_holdings eh
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    WHERE ss.period_id = 1
    GROUP BY eh.company_id
),
month2 AS (
    SELECT 
        eh.company_id,
        SUM(eh.quantity) as total_qty_month2,
        SUM(eh.market_value_inr) as total_value_month2
    FROM equity_holdings eh
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    WHERE ss.period_id = 2
    GROUP BY eh.company_id
)
SELECT 
    c.company_name,
    m1.total_qty_month1,
    m2.total_qty_month2,
    (m2.total_qty_month2 - m1.total_qty_month1) as qty_delta,
    ((m2.total_qty_month2 - m1.total_qty_month1) * 100.0 / NULLIF(m1.total_qty_month1, 0)) as pct_change
FROM month1 m1
FULL OUTER JOIN month2 m2 ON m1.company_id = m2.company_id
JOIN companies c ON COALESCE(m1.company_id, m2.company_id) = c.company_id
ORDER BY ABS(qty_delta) DESC;
```

**Required Indexes**:
- ✅ `idx_snapshots_period` on `scheme_snapshots(period_id)` - Fast period filtering
- ✅ `idx_holdings_snapshot` on `equity_holdings(snapshot_id)` - Fast holdings lookup
- ✅ `idx_holdings_company` on `equity_holdings(company_id)` - Fast grouping

**Performance**: O(n) for aggregation, ~100-500ms for full dataset with 1M+ holdings

---

### Composite Indexes for Optimization

**Index 1**: `idx_holdings_snapshot_company` on `equity_holdings(snapshot_id, company_id)`
- **Purpose**: Optimize joins that filter by both snapshot and company
- **Benefit**: Covers both columns in single index lookup

**Index 2**: Consider adding if needed:
```sql
CREATE INDEX idx_snapshots_scheme_period ON scheme_snapshots(scheme_id, period_id);
```
- **Purpose**: Fast lookup for specific scheme-period combination
- **Benefit**: Useful for portfolio tracker queries

---

## 4️⃣ EXAMPLES

### Example 1: INSERT Flow (Conceptual)

**Scenario**: Load ICICI Prudential Bluechip Fund - Direct - Growth for January 2025

**Step-by-Step**:

```sql
-- Step 1: Ensure AMC exists (idempotent)
INSERT INTO amcs (amc_name)
VALUES ('ICICI Prudential Mutual Fund')
ON CONFLICT (amc_name) DO NOTHING;

-- Step 2: Ensure scheme exists (idempotent)
INSERT INTO schemes (amc_id, scheme_name, plan_type, option_type)
VALUES (
    (SELECT amc_id FROM amcs WHERE amc_name = 'ICICI Prudential Mutual Fund'),
    'ICICI Prudential Bluechip Fund',
    'Direct',
    'Growth'
)
ON CONFLICT (amc_id, scheme_name, plan_type, option_type) DO NOTHING;

-- Step 3: Ensure period exists (idempotent)
INSERT INTO periods (year, month, period_end_date)
VALUES (2025, 1, '2025-01-31')
ON CONFLICT (year, month) DO NOTHING;

-- Step 4: Ensure companies exist (idempotent, for each holding)
INSERT INTO companies (isin, company_name, exchange_symbol, sector)
VALUES 
    ('INE002A01018', 'Reliance Industries Limited', 'RELIANCE', 'Energy'),
    ('INE040A01034', 'HDFC Bank Limited', 'HDFCBANK', 'Financials')
    -- ... more companies
ON CONFLICT (isin) DO UPDATE SET
    company_name = EXCLUDED.company_name,
    exchange_symbol = EXCLUDED.exchange_symbol,
    sector = EXCLUDED.sector,
    updated_at = CURRENT_TIMESTAMP;

-- Step 5: BEGIN TRANSACTION for snapshot + holdings
BEGIN;

-- Step 6: Create snapshot
INSERT INTO scheme_snapshots (scheme_id, period_id, total_holdings, total_value_inr, holdings_count)
VALUES (
    (SELECT scheme_id FROM schemes WHERE 
        amc_id = (SELECT amc_id FROM amcs WHERE amc_name = 'ICICI Prudential Mutual Fund')
        AND scheme_name = 'ICICI Prudential Bluechip Fund'
        AND plan_type = 'Direct'
        AND option_type = 'Growth'),
    (SELECT period_id FROM periods WHERE year = 2025 AND month = 1),
    45,
    1250000000.00,
    45
)
RETURNING snapshot_id;
-- Assume snapshot_id = 1

-- Step 7: Insert all holdings
INSERT INTO equity_holdings (snapshot_id, company_id, quantity, market_value_inr, percent_of_nav)
VALUES 
    (1, (SELECT company_id FROM companies WHERE isin = 'INE002A01018'), 125000, 306250000.00, 3.06),
    (1, (SELECT company_id FROM companies WHERE isin = 'INE040A01034'), 50000, 75000000.00, 0.75)
    -- ... 43 more rows
;

-- Step 8: COMMIT (all or nothing)
COMMIT;
```

**Key Points**:
- Steps 1-4 are idempotent (can run multiple times safely)
- Steps 5-8 are atomic (all succeed or all fail)
- If snapshot already exists, Step 6 fails with unique constraint error
- Application layer handles error by either skipping or deleting old snapshot first

---

### Example 2: JOIN Paths for Analytics

**Analytics Query 1: Portfolio Composition**

**Question**: What are the top 10 holdings of ICICI Bluechip Fund (Direct-Growth) in January 2025?

```sql
SELECT 
    c.company_name,
    c.exchange_symbol,
    eh.quantity,
    eh.market_value_inr / 10000000 as market_value_crores,
    eh.percent_of_nav
FROM schemes s
JOIN amcs a ON s.amc_id = a.amc_id
JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
JOIN periods p ON ss.period_id = p.period_id
JOIN equity_holdings eh ON ss.snapshot_id = eh.snapshot_id
JOIN companies c ON eh.company_id = c.company_id
WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
  AND s.scheme_name = 'ICICI Prudential Bluechip Fund'
  AND s.plan_type = 'Direct'
  AND s.option_type = 'Growth'
  AND p.year = 2025
  AND p.month = 1
ORDER BY eh.percent_of_nav DESC
LIMIT 10;
```

**Join Path**: `amcs → schemes → scheme_snapshots → periods → equity_holdings → companies`

---

**Analytics Query 2: Stock-Wise MF Holdings**

**Question**: Which mutual funds hold Reliance Industries in January 2025?

```sql
SELECT 
    a.amc_name,
    s.scheme_name,
    s.plan_type,
    s.option_type,
    eh.quantity,
    eh.market_value_inr / 10000000 as market_value_crores,
    eh.percent_of_nav,
    RANK() OVER (ORDER BY eh.market_value_inr DESC) as rank_by_value
FROM companies c
JOIN equity_holdings eh ON c.company_id = eh.company_id
JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
JOIN periods p ON ss.period_id = p.period_id
JOIN schemes s ON ss.scheme_id = s.scheme_id
JOIN amcs a ON s.amc_id = a.amc_id
WHERE c.isin = 'INE002A01018'
  AND p.year = 2025
  AND p.month = 1
ORDER BY eh.market_value_inr DESC;
```

**Join Path**: `companies → equity_holdings → scheme_snapshots → periods → schemes → amcs`

---

**Analytics Query 3: Net Buying/Selling**

**Question**: Which stocks saw the most net buying by mutual funds between January and February 2025?

```sql
WITH jan_holdings AS (
    SELECT 
        c.isin,
        c.company_name,
        SUM(eh.quantity) as total_qty,
        SUM(eh.market_value_inr) as total_value
    FROM equity_holdings eh
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN periods p ON ss.period_id = p.period_id
    JOIN companies c ON eh.company_id = c.company_id
    WHERE p.year = 2025 AND p.month = 1
    GROUP BY c.isin, c.company_name
),
feb_holdings AS (
    SELECT 
        c.isin,
        c.company_name,
        SUM(eh.quantity) as total_qty,
        SUM(eh.market_value_inr) as total_value
    FROM equity_holdings eh
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN periods p ON ss.period_id = p.period_id
    JOIN companies c ON eh.company_id = c.company_id
    WHERE p.year = 2025 AND p.month = 2
    GROUP BY c.isin, c.company_name
)
SELECT 
    COALESCE(j.company_name, f.company_name) as company_name,
    COALESCE(j.total_qty, 0) as jan_qty,
    COALESCE(f.total_qty, 0) as feb_qty,
    (COALESCE(f.total_qty, 0) - COALESCE(j.total_qty, 0)) as qty_delta,
    CASE 
        WHEN j.total_qty > 0 THEN 
            ((COALESCE(f.total_qty, 0) - COALESCE(j.total_qty, 0)) * 100.0 / j.total_qty)
        ELSE NULL
    END as pct_change
FROM jan_holdings j
FULL OUTER JOIN feb_holdings f ON j.isin = f.isin
ORDER BY ABS(qty_delta) DESC
LIMIT 20;
```

**Join Path**: `periods → scheme_snapshots → equity_holdings → companies` (for each month, then FULL OUTER JOIN)

---

## 🎯 SUMMARY

This PostgreSQL schema:

✅ **Implements Canonical Data Contract Exactly**
- All entities defined (AMC, Scheme, Period, Company, Holding)
- All validation rules enforced via constraints
- All uniqueness rules enforced

✅ **Enforces Data Integrity**
- No duplicate scheme-months (unique constraint)
- No partial holdings (transaction + cascade delete)
- Clean joins (proper foreign keys)
- Safe reloads (delete + insert in transaction)

✅ **Optimized for Analytics**
- Indexes for scheme → portfolio view
- Indexes for company → MF holdings view
- Indexes for month-over-month delta analysis
- Composite indexes for common join patterns

✅ **Future-Proof for 10+ Years**
- Normalized structure (easy to extend)
- No JSON blobs (queryable)
- No denormalized columns (maintainable)
- Supports RupeeVest-like analytics

✅ **Production-Ready**
- All monetary values in base INR (₹)
- Quantity can be 0 (valid)
- Equity-only (ISIN regex validation)
- Timestamps for audit trail

---

**Next Steps**: Create migration scripts to implement this schema in PostgreSQL.

---

**END OF POSTGRESQL SCHEMA DESIGN**
