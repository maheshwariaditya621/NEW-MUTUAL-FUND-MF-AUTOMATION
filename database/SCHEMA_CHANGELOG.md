# PostgreSQL Schema v1.0 - CHANGELOG

## Version: 1.0 (FINAL - LOCKED)
**Date**: 2026-02-01  
**Status**: Production-Ready  

---

## 🔧 CHANGES APPLIED

### 1️⃣ Fixed: `market_value_inr` Constraint in `equity_holdings`

**Previous (INCORRECT)**:
```sql
market_value_inr NUMERIC(20, 2) NOT NULL CHECK (market_value_inr > 0)
```

**New (CORRECT)**:
```sql
market_value_inr NUMERIC(20, 2) NOT NULL CHECK (market_value_inr >= 0)
```

**Why This Change?**
- **Real-world scenario**: When a mutual fund exits a position (sells all shares), the holding may still appear in the monthly report with:
  - `quantity = 0`
  - `market_value_inr = 0`
  - `percent_of_nav = 0`
- **Previous constraint was too strict**: It rejected valid exited positions
- **New constraint is correct**: Allows zero values for exited positions while still preventing negative values

**Example of valid data now accepted**:
```
Company: ABC Ltd
Quantity: 0 (exited)
Market Value: ₹0.00
Percent of NAV: 0.0000%
```

---

### 2️⃣ Fixed: `percent_of_nav` Constraint in `equity_holdings`

**Previous (INCORRECT)**:
```sql
percent_of_nav NUMERIC(8, 4) NOT NULL CHECK (percent_of_nav > 0 AND percent_of_nav <= 100)
```

**New (CORRECT)**:
```sql
percent_of_nav NUMERIC(8, 4) NOT NULL CHECK (percent_of_nav >= 0 AND percent_of_nav <= 100)
```

**Why This Change?**
- **Real-world scenario 1 (Exited positions)**: When a fund exits a position, `percent_of_nav = 0`
- **Real-world scenario 2 (Rounding)**: Very small holdings may round to 0.0000% (e.g., ₹100 in a ₹10,000 crore fund)
- **Previous constraint was too strict**: It rejected valid zero-percentage holdings
- **New constraint is correct**: Allows zero while still enforcing the 0-100% range

**Example of valid data now accepted**:
```
Company: XYZ Ltd
Quantity: 500 shares
Market Value: ₹12,500.00
Percent of NAV: 0.0000% (rounds to zero in a large fund)
```

---

### 3️⃣ Consistency: Changed to `BIGSERIAL` for High-Growth Tables

**Tables Updated**:
- `schemes.scheme_id`: `SERIAL` → `BIGSERIAL`
- `scheme_snapshots.snapshot_id`: `SERIAL` → `BIGSERIAL`
- `scheme_snapshots.scheme_id` (foreign key): `INTEGER` → `BIGINT`
- `equity_holdings.snapshot_id` (foreign key): `INTEGER` → `BIGINT`

**Why This Change?**
- **Long-term scalability**: These tables will grow significantly over 10+ years
- **schemes**: Thousands of schemes across all AMCs
- **scheme_snapshots**: Thousands of schemes × 120+ months = millions of rows
- **equity_holdings**: Millions of snapshots × 50-100 holdings each = hundreds of millions of rows
- **BIGSERIAL range**: 1 to 9,223,372,036,854,775,807 (more than enough for decades)
- **SERIAL range**: 1 to 2,147,483,647 (could be exhausted in high-volume scenarios)

**No logic change**: This is purely a capacity increase for future-proofing.

---

### 4️⃣ Clarification: `total_holdings` vs `holdings_count` in `scheme_snapshots`

**Both columns are retained** with clarified documentation:

- **`total_holdings`**: Total number of equity holding **rows** in the snapshot
  - Includes zero-value positions (exited holdings)
  - Includes duplicate companies if they appear multiple times (rare but possible)
  
- **`holdings_count`**: Count of **distinct companies** held
  - May differ from `total_holdings` if same company appears multiple times
  - Represents unique companies in the portfolio

**Why keep both?**
- **Data validation**: `total_holdings` should match the count of rows in `equity_holdings` for this snapshot
- **Analytics**: `holdings_count` is useful for "portfolio diversification" metrics
- **Minimal overhead**: Two integers per snapshot is negligible

**Example where they differ**:
```
Snapshot has 3 rows:
1. Reliance Industries - Class A shares
2. Reliance Industries - Class B shares (if such a scenario exists)
3. HDFC Bank

total_holdings = 3 (rows)
holdings_count = 2 (distinct companies: Reliance, HDFC)
```

---

## ✅ FINAL CONFIRMATION

### This Schema is Production-Ready

**Correctness**:
- ✅ Allows zero values for exited positions (real-world requirement)
- ✅ Enforces all business rules via database constraints
- ✅ Prevents negative values (data integrity)
- ✅ Prevents percentages > 100% (logical constraint)

**Scalability**:
- ✅ BIGSERIAL for high-growth tables (future-proof for 10+ years)
- ✅ Proper indexing for analytics queries
- ✅ Normalized structure (no JSON, no denormalization)

**Data Integrity**:
- ✅ Foreign keys enforce referential integrity
- ✅ Unique constraints prevent duplicates
- ✅ CASCADE deletes prevent orphaned records
- ✅ Transaction-based loading ensures all-or-nothing

**Analytics Support**:
- ✅ Supports RupeeVest-like portfolio tracking
- ✅ Supports stock-wise MF holdings analysis
- ✅ Supports month-over-month net buying/selling detection
- ✅ Optimized indexes for common query patterns

---

## 🔒 SCHEMA LOCK STATEMENT

**This schema (v1.0) should NOT require structural changes for many years if the following rules are followed**:

### Rules for Long-Term Stability:

1. **No Schema Changes Without Strong Justification**
   - Adding columns: OK if truly needed (use ALTER TABLE ADD COLUMN)
   - Changing constraints: AVOID unless data proves current constraints wrong
   - Removing tables/columns: NEVER (use deprecation flags instead)

2. **Handle New Requirements via Application Logic**
   - New validations → Add to application layer, not database constraints
   - New calculations → Create views or materialized views
   - New reports → Write new queries, don't change schema

3. **Extend, Don't Modify**
   - Need new data? Add new tables, don't modify existing ones
   - Need new relationships? Add junction tables
   - Need new attributes? Add lookup tables

4. **Version Control All Changes**
   - Use migration scripts (numbered: 001_initial.sql, 002_add_xyz.sql)
   - Never modify this base schema file
   - Document WHY each migration was needed

### Expected Lifespan:

**10+ years without structural changes** if:
- ✅ Mutual fund data structure remains similar (scheme → holdings)
- ✅ Monthly reporting continues as the standard
- ✅ Equity-only focus is maintained
- ✅ No major regulatory changes to data format

**Potential future additions** (via ALTER TABLE, not schema redesign):
- New asset classes (debt, hybrid) → Add new tables, not modify existing
- Additional metadata (fund manager, expense ratio) → Add columns to `schemes`
- Historical price data → Add new `stock_prices` table
- User portfolios → Add new `user_portfolios` table

---

## 📊 Summary

**What Changed**:
1. `market_value_inr >= 0` (was `> 0`) - Allows exited positions
2. `percent_of_nav >= 0` (was `> 0`) - Allows exited positions and rounding to zero
3. BIGSERIAL for `schemes`, `scheme_snapshots`, `equity_holdings` - Future-proof scalability
4. Clarified `total_holdings` vs `holdings_count` - Both serve different purposes

**Why It's Correct**:
- Reflects real-world mutual fund data (exits, rounding, small positions)
- Scales to millions of records over decades
- Maintains data integrity without being overly restrictive

**Why It's Final**:
- Covers all known use cases for MF portfolio analytics
- Normalized structure allows extension without modification
- Constraints are neither too strict nor too loose
- Proven design pattern (used by financial platforms worldwide)

---

**Schema v1.0 is LOCKED and ready for production deployment.**

---

*Document created: 2026-02-01*  
*Author: Staff Backend Engineer*  
*Status: FINAL*
