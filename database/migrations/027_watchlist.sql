-- ============================================================
-- MIGRATION 027: Personalized Smart Watchlist Tables
-- ============================================================
-- Description: Creates tables for user watchlist tracking
--   - user_watchlist: stores tracked stocks and mutual fund schemes per user
--   - user_watchlist_preferences: stores which insight modules are enabled per user
-- Date: 2026-03-09
-- ============================================================

BEGIN;

-- ============================================================
-- TABLE: user_watchlist
-- Each row is one tracked asset (stock or scheme) per user
-- ============================================================
CREATE TABLE IF NOT EXISTS user_watchlist (
    watchlist_id  BIGSERIAL PRIMARY KEY,
    user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    asset_type    VARCHAR(10) NOT NULL CHECK (asset_type IN ('stock', 'scheme')),
    company_id    BIGINT REFERENCES companies(company_id) ON DELETE CASCADE,   -- set when asset_type = 'stock'
    scheme_id     BIGINT REFERENCES schemes(scheme_id) ON DELETE CASCADE,      -- set when asset_type = 'scheme'
    added_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

-- One user can only watch each asset once
CONSTRAINT uq_watchlist_stock UNIQUE (user_id, company_id),
CONSTRAINT uq_watchlist_scheme UNIQUE (user_id, scheme_id),

-- Ensure exactly one of company_id/scheme_id is set
CONSTRAINT chk_watchlist_asset CHECK (
        (asset_type = 'stock'  AND company_id IS NOT NULL AND scheme_id IS NULL) OR
        (asset_type = 'scheme' AND scheme_id  IS NOT NULL AND company_id IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_watchlist_user ON user_watchlist (user_id);

CREATE INDEX IF NOT EXISTS idx_watchlist_company ON user_watchlist (company_id);

CREATE INDEX IF NOT EXISTS idx_watchlist_scheme ON user_watchlist (scheme_id);

CREATE INDEX IF NOT EXISTS idx_watchlist_asset_type ON user_watchlist (asset_type);

COMMENT ON
TABLE user_watchlist IS 'Tracks stocks and mutual fund schemes watched by each user';

COMMENT ON COLUMN user_watchlist.asset_type IS 'Either ''stock'' or ''scheme''';

COMMENT ON COLUMN user_watchlist.company_id IS 'FK to companies table; populated only for stock assets';

COMMENT ON COLUMN user_watchlist.scheme_id IS 'FK to schemes table; populated only for scheme assets';

-- ============================================================
-- TABLE: user_watchlist_preferences
-- One row per user; stores a JSON object of module toggles
-- ============================================================
CREATE TABLE IF NOT EXISTS user_watchlist_preferences (
    pref_id   BIGSERIAL PRIMARY KEY,
    user_id   INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE UNIQUE,
    prefs     JSONB NOT NULL DEFAULT '{
        "mf_buying":        true,
        "mf_selling":       true,
        "net_activity":     true,
        "top_holders":      false,
        "trend_indicator":  false,
        "popularity_score": false
    }'::jsonb,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_watchlist_prefs_user ON user_watchlist_preferences (user_id);

COMMENT ON
TABLE user_watchlist_preferences IS 'Stores per-user module toggle preferences for the watchlist dashboard';

COMMENT ON COLUMN user_watchlist_preferences.prefs IS 'JSONB object containing boolean flags for each insight module';

COMMIT;

SELECT '✅ Migration 027: Watchlist tables created successfully!' AS status;