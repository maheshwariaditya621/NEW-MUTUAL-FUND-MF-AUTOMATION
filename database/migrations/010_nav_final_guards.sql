-- ============================================================
-- MIGRATION 010: FINAL NAV HARDENING & GUARDS
-- Date: 2026-02-12
-- Author: Antigravity
-- ============================================================

BEGIN;

-- 1. SCHEME INCEPTION DATE
-- Helps block NAVs before the fund actually launched
ALTER TABLE schemes ADD COLUMN IF NOT EXISTS inception_date DATE;

-- 2. HISTORICAL NAV LOCKS
-- Prevents accidental overwrites of past years
CREATE TABLE IF NOT EXISTS nav_history_locks (
    lock_id SERIAL PRIMARY KEY,
    lock_year INTEGER NOT NULL UNIQUE,
    is_locked BOOLEAN DEFAULT TRUE,
    locked_at TIMESTAMP DEFAULT(now() at time zone 'utc'),
    updated_at TIMESTAMP DEFAULT(now() at time zone 'utc')
);

-- 3. TRADING CALENDAR (NSE DAYS)
-- Crucial for benchmark alignment and gap detection
CREATE TABLE IF NOT EXISTS trading_calendar (
    trading_date DATE PRIMARY KEY,
    is_trading_day BOOLEAN DEFAULT TRUE,
    notes TEXT
);

-- 4. VARIANT ISOLATION VIEW (For Audit)
-- Helps detect mapping errors between Growth/IDCW vs Regular/Direct
CREATE OR REPLACE VIEW view_nav_collisions AS
SELECT n1.nav_date, n1.scheme_id as scheme_1, n2.scheme_id as scheme_2, n1.nav_value
FROM
    nav_history n1
    JOIN nav_history n2 ON n1.nav_date = n2.nav_date
    AND n1.nav_value = n2.nav_value
    AND n1.scheme_id < n2.scheme_id
    JOIN schemes s1 ON n1.scheme_id = s1.scheme_id
    JOIN schemes s2 ON n2.scheme_id = s2.scheme_id
WHERE
    s1.plan_type != s2.plan_type
    OR s1.option_type != s2.option_type;

COMMIT;