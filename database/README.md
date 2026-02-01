# Database Module - README

## Overview

This module contains the PostgreSQL database schema and related files for the Mutual Fund Portfolio Analytics Platform.

---

## 📁 Files & Folders

### `migrations/`
**Numbered migration files for schema versioning**

Contains SQL migration scripts that create and evolve the database schema over time.

- **`001_create_schema_v1.sql`**: Initial schema creation (run this first)
- **`README.md`**: Migration system documentation

**To apply migrations**:
```bash
psql -U mf_admin -d mf_analytics -f database\migrations\001_create_schema_v1.sql
```

See `migrations/README.md` for detailed instructions.

---

### `schema_v1.0.sql`
**Reference copy of schema v1.0 (LOCKED)**

This is the complete PostgreSQL DDL (Data Definition Language) for documentation purposes.

**⚠️ IMPORTANT**: Do NOT run this file directly. Use the migration file instead (`migrations/001_create_schema_v1.sql`).

**Status**: LOCKED - Do not modify without strong justification

---

### `SCHEMA_CHANGELOG.md`
**Complete changelog of all changes from draft to v1.0**

Documents:
- What was changed
- Why it was changed
- Why this is now correct for real-world MF data
- Schema lock statement and long-term stability rules

**Read this** to understand the rationale behind the schema design.

---

## 🗄️ Schema Overview

### Tables (6 total)

1. **`amcs`** - Asset Management Companies
2. **`schemes`** - Mutual fund schemes
3. **`periods`** - Monthly disclosure periods
4. **`companies`** - Publicly listed companies (equity only)
5. **`scheme_snapshots`** - Tracks loaded scheme-period combinations
6. **`equity_holdings`** - Individual equity holdings (main fact table)

### Key Features

✅ **Allows zero values** for exited positions (market_value_inr, percent_of_nav)  
✅ **BIGSERIAL** for high-growth tables (future-proof for 10+ years)  
✅ **Strict constraints** prevent invalid data  
✅ **Foreign keys** enforce referential integrity  
✅ **Optimized indexes** for analytics queries  
✅ **Transaction-safe** loading (all-or-nothing)  

---

## 🔧 Key Constraints

### Exited Positions (IMPORTANT)

The schema **correctly allows zero values** for:
- `quantity >= 0` (can be 0 for exited positions)
- `market_value_inr >= 0` (can be 0 for exited positions)
- `percent_of_nav >= 0` (can be 0 for exited positions or rounding)

**Why?** When a mutual fund exits a position, the holding may still appear in the monthly report with all values as zero.

### ISIN Validation

```sql
CONSTRAINT chk_isin_format CHECK (
    isin ~ '^INE[A-Z0-9]{6}10[A-Z0-9]{1}$'
)
```

This ensures:
- Starts with "INE" (India)
- Exactly 12 characters
- Positions 9-10 are "10" (equity security code)

### Uniqueness Rules

- **No duplicate scheme-months**: `UNIQUE (scheme_id, period_id)` on `scheme_snapshots`
- **No duplicate holdings**: `UNIQUE (snapshot_id, company_id)` on `equity_holdings`
- **One scheme per combination**: `UNIQUE (amc_id, scheme_name, plan_type, option_type)` on `schemes`

---

## 📊 Common Queries

### Portfolio Composition
```sql
SELECT 
    c.company_name,
    eh.quantity,
    eh.market_value_inr,
    eh.percent_of_nav
FROM schemes s
JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
JOIN equity_holdings eh ON ss.snapshot_id = eh.snapshot_id
JOIN companies c ON eh.company_id = c.company_id
WHERE s.scheme_name = 'ICICI Prudential Bluechip Fund'
  AND s.plan_type = 'Direct'
  AND ss.period_id = 1
ORDER BY eh.percent_of_nav DESC;
```

### Stock-Wise MF Holdings
```sql
SELECT 
    s.scheme_name,
    eh.market_value_inr,
    eh.percent_of_nav
FROM companies c
JOIN equity_holdings eh ON c.company_id = eh.company_id
JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
JOIN schemes s ON ss.scheme_id = s.scheme_id
WHERE c.isin = 'INE002A01018'
  AND ss.period_id = 1
ORDER BY eh.market_value_inr DESC;
```

---

## 🔒 Schema Lock Policy

**This schema is LOCKED for production use.**

### Allowed Changes:
- ✅ Adding new columns (via ALTER TABLE)
- ✅ Adding new tables (for new features)
- ✅ Adding new indexes (for performance)
- ✅ Creating views or materialized views

### NOT Allowed:
- ❌ Removing tables or columns
- ❌ Changing data types (unless absolutely necessary)
- ❌ Modifying constraints (unless data proves them wrong)
- ❌ Renaming core entities

### Migration Process:
1. Create numbered migration file: `002_add_xyz.sql`
2. Document WHY the change is needed
3. Test on dev environment first
4. Apply to production during maintenance window

---

## 🚀 Next Steps

1. **Create database**: Run `schema_v1.0.sql`
2. **Implement connection pool**: In `database/connection.py`
3. **Create migration system**: In `database/migrations/`
4. **Build loader logic**: In `loaders/postgres_loader.py`

---

## 📚 Additional Documentation

- See `SCHEMA_CHANGELOG.md` for detailed change history
- See `../docs/POSTGRESQL_SCHEMA_DESIGN.md` for original design document
- See `schema_v1.0.sql` for the complete DDL

---

**Schema Version**: 1.0 (FINAL)  
**Last Updated**: 2026-02-01  
**Status**: Production-Ready
