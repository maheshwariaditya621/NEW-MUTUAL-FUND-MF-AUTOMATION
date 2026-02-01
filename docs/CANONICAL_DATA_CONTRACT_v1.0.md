# 📜 CANONICAL DATA CONTRACT v1.0

## Mutual Fund Portfolio Analytics Platform

> **Version**: 1.0 (FROZEN)  
> **Date**: 2026-02-01  
> **Status**: Production-Ready  
> **Scope**: Equity-only monthly portfolio data from AMC Excel files  

---

## 🎯 PURPOSE

This document defines the **formal, binding contract** between:
- **Data Sources** (AMC Excel files)
- **Extraction & Standardization Layer** (our code)
- **Database Layer** (PostgreSQL schema v1.0)

**All code MUST obey this contract.**

This is NOT implementation guidance. This is a rules-and-guarantees document that ensures:
- ✅ Data integrity
- ✅ Predictable behavior
- ✅ No silent failures
- ✅ Long-term stability

---

## 1️⃣ INPUT CONTRACT (FROM EXCEL)

### Data Source Context

**What we receive**:
- Monthly portfolio Excel files from Asset Management Companies (AMCs)
- Files are **already merged** per AMC per month (one file = one AMC = one month)
- Files contain equity holdings only
- Files may have multiple sheets, merged cells, headers, and AMC-specific formats

**What we extract**:
- Scheme-level portfolio holdings
- Each holding = one company (identified by ISIN)

---

### Mandatory Fields

| Field | Mandatory | Source | Validation | Notes |
|-------|-----------|--------|------------|-------|
| **AMC Name** | ✅ YES | External context (file name, folder, or configuration) | Must match canonical AMC name | NEVER inferred from Excel content |
| **Scheme Name** | ✅ YES | Excel: Column header, merged cell, or index sheet | Must be extractable and non-empty | Canonical name WITHOUT plan/option/date suffixes |
| **Plan Type** | ✅ YES | Excel: Scheme name suffix or separate column | Must be exactly "Direct" or "Regular" | Case-insensitive extraction, normalized to title case |
| **Option Type** | ✅ YES | Excel: Scheme name suffix or separate column | Must be exactly "Growth", "Dividend", or "IDCW" | Case-insensitive extraction, normalized to title case |
| **Period (Year, Month)** | ✅ YES | External context (file name, folder, or configuration) | Must be valid year (2020-2100) and month (1-12) | NEVER inferred from Excel content |
| **ISIN** | ✅ YES | Excel: Data column | Must match pattern: `INE[A-Z0-9]{6}10[A-Z0-9]{1}` | Equity-only (security code "10") |
| **Company Name** | ✅ YES | Excel: Data column | Must be non-empty after trimming | Used for display, not identity |
| **Quantity** | ✅ YES | Excel: Data column | Must be numeric, can be 0 or positive | Negative values rejected |
| **Market Value (INR)** | ✅ YES | Excel: Data column | Must be numeric, can be 0 or positive | Negative values rejected |
| **Percent of NAV** | ✅ YES | Excel: Data column | Must be numeric, 0 to 100 | Negative or >100 rejected |

---

### Optional Fields

| Field | Mandatory | Source | Notes |
|-------|-----------|--------|-------|
| **Scheme Category** | ❌ NO | Excel: Index sheet or metadata | E.g., "Large Cap", "Mid Cap" |
| **Scheme Code** | ❌ NO | Excel: Index sheet or metadata | AMC-specific internal code |
| **Exchange Symbol** | ❌ NO | Excel: Data column | E.g., "RELIANCE", "HDFCBANK" |
| **Sector** | ❌ NO | Excel: Data column | E.g., "Energy", "Financials" |
| **Industry** | ❌ NO | Excel: Data column | E.g., "Oil & Gas", "Banking" |

---

### Allowed Data Sources Within Excel

Data may be extracted from:

1. **Index Sheet** (if present)
   - Scheme metadata (name, category, code)
   - List of schemes in the file

2. **Top Merged Cells**
   - Scheme name
   - Plan and option type
   - Reporting date (for validation only, NOT for period determination)

3. **Column Headers**
   - Field names (ISIN, Company Name, Quantity, etc.)
   - Scheme name (if each column represents a scheme)

4. **Data Rows**
   - Holdings data (ISIN, quantity, value, percentage)

5. **Sheet Name**
   - Scheme name (if each sheet represents a scheme)

**NOT ALLOWED**:
- ❌ Inferring AMC name from Excel content (must be external context)
- ❌ Inferring period (year, month) from Excel content (must be external context)
- ❌ Guessing missing mandatory fields

---

### Missing Mandatory Data Handling

**If ANY mandatory field is missing for a scheme-month**:

1. ✅ **SKIP** the entire scheme-month
2. ✅ **LOG** the skip with reason (which field is missing)
3. ✅ **SEND TELEGRAM ALERT** with:
   - AMC name
   - Scheme name (if available)
   - Period (year, month)
   - Missing field(s)
   - File name
4. ❌ **DO NOT** attempt to load partial data
5. ❌ **DO NOT** guess or infer missing data
6. ❌ **DO NOT** fail silently

**Example Alert**:
```
⚠️ SKIPPED: HDFC Equity Fund - Direct - Growth (Jan 2025)
Reason: Missing ISIN for 3 holdings
File: hdfc_jan_2025.xlsx
Action: Manual review required
```

---

## 2️⃣ CANONICAL ENTITIES

### A) AMC (Asset Management Company)

**Canonical Representation**:
```
amc_name: VARCHAR(255)
```

**Rules**:
1. ✅ AMC name is **externally provided** (file name, folder structure, or configuration)
2. ✅ AMC name must match **exactly** one of the canonical AMC names in the database
3. ✅ AMC name is **case-sensitive** for storage, but matching can be case-insensitive
4. ✅ Leading/trailing whitespace is trimmed
5. ❌ AMC name is NEVER inferred from Excel content

**Canonical AMC Names** (examples):
- "ICICI Prudential Mutual Fund"
- "HDFC Mutual Fund"
- "Axis Mutual Fund"
- "SBI Mutual Fund"

**NOT Allowed**:
- ❌ "ICICI" (incomplete)
- ❌ "ICICI Prudential MF" (abbreviation)
- ❌ "icici prudential mutual fund" (wrong case, though matching can be case-insensitive)

**Resolution Process**:
1. Receive AMC name from external context
2. Trim whitespace
3. Lookup in `amcs` table (case-insensitive match)
4. If not found, INSERT new AMC (using canonical casing)
5. Use `amc_id` for all subsequent operations

---

### B) Scheme

**Canonical Representation**:
```
scheme_name: VARCHAR(500)  -- WITHOUT plan/option/date suffixes
plan_type: VARCHAR(10)     -- Exactly "Direct" or "Regular"
option_type: VARCHAR(10)   -- Exactly "Growth", "Dividend", or "IDCW"
```

**Scheme Identity**:
A scheme is uniquely identified by:
```
(amc_id, scheme_name, plan_type, option_type)
```

**Scheme Name Cleaning Rules**:

1. **Extract base name** (remove plan/option/date suffixes):
   ```
   Input:  "ICICI Prudential Bluechip Fund - Direct Plan - Growth Option"
   Output: "ICICI Prudential Bluechip Fund"
   
   Input:  "HDFC Equity Fund - Regular - Dividend - Jan 2025"
   Output: "HDFC Equity Fund"
   ```

2. **Remove common suffixes** (case-insensitive):
   - " - Direct Plan"
   - " - Regular Plan"
   - " - Direct"
   - " - Regular"
   - " - Growth Option"
   - " - Dividend Option"
   - " - IDCW Option"
   - " - Growth"
   - " - Dividend"
   - " - IDCW"
   - Date patterns: " - Jan 2025", " - January 2025", etc.

3. **Trim whitespace** (leading, trailing, and collapse multiple spaces)

4. **Preserve casing** (do NOT convert to uppercase/lowercase)

**Plan Type Rules**:

1. ✅ Must be **exactly** "Direct" or "Regular" (title case)
2. ✅ Extracted from:
   - Scheme name suffix (e.g., "Fund Name - Direct")
   - Separate column in Excel
   - Merged cell header
3. ✅ Case-insensitive extraction, normalized to title case:
   ```
   "direct" → "Direct"
   "DIRECT" → "Direct"
   "Direct Plan" → "Direct"
   ```
4. ❌ If not found or ambiguous, **SKIP** the scheme-month

**Option Type Rules**:

1. ✅ Must be **exactly** "Growth", "Dividend", or "IDCW" (title case)
2. ✅ Extracted from:
   - Scheme name suffix (e.g., "Fund Name - Growth")
   - Separate column in Excel
   - Merged cell header
3. ✅ Case-insensitive extraction, normalized to title case:
   ```
   "growth" → "Growth"
   "GROWTH OPTION" → "Growth"
   "Div" → "Dividend"
   "IDCW" → "IDCW"
   ```
4. ✅ "Dividend" and "IDCW" are treated as **distinct** option types
5. ❌ If not found or ambiguous, **SKIP** the scheme-month

**Priority Rules for Scheme Name Resolution**:

If scheme name appears in multiple places, use this priority:

1. **Highest Priority**: Merged cell at top of data section
2. **Medium Priority**: Index sheet (if scheme is listed there)
3. **Low Priority**: Sheet name
4. **Lowest Priority**: Column header

**Valid Scheme Examples**:
```
✅ scheme_name: "ICICI Prudential Bluechip Fund"
   plan_type: "Direct"
   option_type: "Growth"

✅ scheme_name: "HDFC Equity Fund"
   plan_type: "Regular"
   option_type: "Dividend"

✅ scheme_name: "Axis Long Term Equity Fund"
   plan_type: "Direct"
   option_type: "IDCW"
```

**Invalid Scheme Examples**:
```
❌ scheme_name: "ICICI Prudential Bluechip Fund - Direct - Growth"
   (should be cleaned to remove suffixes)

❌ plan_type: "Dir"
   (must be exactly "Direct" or "Regular")

❌ option_type: "Gr"
   (must be exactly "Growth", "Dividend", or "IDCW")
```

---

### C) Period

**Canonical Representation**:
```
year: INTEGER (2020-2100)
month: INTEGER (1-12)
period_end_date: DATE (last day of month)
```

**Rules**:

1. ✅ Period is **externally provided** (file name, folder structure, or configuration)
2. ✅ Year must be between 2020 and 2100
3. ✅ Month must be between 1 and 12
4. ✅ `period_end_date` is calculated as the last day of the month:
   ```
   year=2025, month=1 → period_end_date=2025-01-31
   year=2025, month=2 → period_end_date=2025-02-28
   year=2024, month=2 → period_end_date=2024-02-29 (leap year)
   ```
5. ❌ Period is NEVER inferred from Excel content (even if date appears in file)
6. ❌ Period is NEVER inferred from "as of date" or "reporting date" in Excel

**Why External Context Only?**

- Excel files may contain multiple dates (reporting date, valuation date, publication date)
- Dates in Excel may be ambiguous or inconsistent
- Period must be **explicitly known** before processing begins
- This ensures no ambiguity or guessing

**Example**:
```
File: hdfc_jan_2025.xlsx
External Context: year=2025, month=1
Excel Content: "As of Date: 31-Jan-2025" (ignored for period determination)
Result: period_id for (2025, 1) is used
```

---

### D) Company

**Canonical Representation**:
```
isin: CHAR(12)           -- Unique identifier
company_name: VARCHAR(255)
exchange_symbol: VARCHAR(20)  -- Optional
sector: VARCHAR(100)          -- Optional
industry: VARCHAR(100)        -- Optional
```

**ISIN Rules** (CRITICAL):

1. ✅ ISIN must be **exactly 12 characters**
2. ✅ ISIN must match pattern: `INE[A-Z0-9]{6}10[A-Z0-9]{1}`
   - Starts with "INE" (India)
   - Followed by 6 alphanumeric characters (issuer code)
   - Followed by "10" (equity security code)
   - Followed by 1 alphanumeric character (check digit)
3. ✅ ISIN is **case-sensitive** (uppercase only)
4. ✅ ISIN is trimmed of whitespace
5. ❌ Non-equity ISINs are **REJECTED**:
   - "INE??????01???" (debt, security code "01")
   - "INE??????20???" (preference shares, security code "20")
6. ❌ Invalid ISINs are **REJECTED**:
   - Less than or more than 12 characters
   - Does not start with "INE"
   - Security code is not "10"

**Valid ISIN Examples**:
```
✅ INE002A01018  (Reliance Industries)
✅ INE040A01034  (HDFC Bank)
✅ INE467B01029  (TCS)
```

**Invalid ISIN Examples**:
```
❌ INE002A0101   (only 11 characters)
❌ INE002A010188 (13 characters)
❌ INE002A01201  (debt, security code "01")
❌ ine002a01018  (lowercase, must be uppercase)
❌ RELIANCE      (not an ISIN)
```

**Company Name Rules**:

1. ✅ Company name is extracted from Excel
2. ✅ Trimmed of leading/trailing whitespace
3. ✅ Must be non-empty
4. ✅ Casing is preserved as-is from Excel
5. ✅ Company name is for **display only**, not identity
6. ✅ ISIN is the **only** unique identifier for companies

**Sector/Industry Rules**:

1. ❌ Sector and Industry are **OPTIONAL**
2. ✅ If present, they are stored as-is (trimmed)
3. ✅ If missing, they are stored as NULL
4. ❌ Missing sector/industry does NOT cause skip

**Company Resolution**:

1. Lookup company by ISIN in `companies` table
2. If found, **UPDATE** company_name, exchange_symbol, sector, industry (if provided)
3. If not found, **INSERT** new company
4. Use `company_id` for holdings

---

### E) Holding

**Canonical Representation**:
```
quantity: BIGINT (>= 0)
market_value_inr: NUMERIC(20, 2) (>= 0)
percent_of_nav: NUMERIC(8, 4) (0 to 100)
```

**Quantity Rules**:

1. ✅ Quantity must be **non-negative** (>= 0)
2. ✅ Quantity **CAN be 0** (valid for exited positions)
3. ✅ Quantity is stored as **whole number** (no decimals)
4. ❌ Negative quantity is **REJECTED**
5. ❌ Non-numeric quantity is **REJECTED**

**Market Value Rules**:

1. ✅ Market value must be **non-negative** (>= 0)
2. ✅ Market value **CAN be 0** (valid for exited positions)
3. ✅ Market value is stored in **Indian Rupees (₹)**, not lakhs/crores
4. ✅ Precision: 2 decimal places (paise)
5. ❌ Negative market value is **REJECTED**
6. ❌ Non-numeric market value is **REJECTED**

**Percent of NAV Rules**:

1. ✅ Percent of NAV must be **0 to 100** (inclusive)
2. ✅ Percent of NAV **CAN be 0** (valid for exited positions or rounding)
3. ✅ Precision: 4 decimal places (e.g., 3.0625%)
4. ✅ Scale: 0-100 (NOT 0-1)
   ```
   3.06% is stored as 3.0600, NOT 0.0306
   ```
5. ❌ Negative percent is **REJECTED**
6. ❌ Percent > 100 is **REJECTED**
7. ❌ Non-numeric percent is **REJECTED**

**Exited Positions** (IMPORTANT):

When a fund exits a position (sells all shares), the holding may still appear with:
```
quantity = 0
market_value_inr = 0.00
percent_of_nav = 0.0000
```

This is **VALID** and must be accepted.

**Rounding to Zero**:

Very small holdings in large funds may round to zero:
```
quantity = 500
market_value_inr = 12500.00
percent_of_nav = 0.0000  (rounds to 0% in a ₹10,000 crore fund)
```

This is **VALID** and must be accepted.

---

## 3️⃣ NORMALIZATION RULES

### Scheme Name Cleaning

**Process**:
1. Extract raw scheme name from Excel
2. Remove plan/option/date suffixes (case-insensitive):
   - " - Direct Plan", " - Direct"
   - " - Regular Plan", " - Regular"
   - " - Growth Option", " - Growth"
   - " - Dividend Option", " - Dividend"
   - " - IDCW Option", " - IDCW"
   - Date patterns: " - Jan 2025", " - January 2025", " (Jan 2025)", etc.
3. Trim leading/trailing whitespace
4. Collapse multiple consecutive spaces to single space
5. Preserve original casing

**Examples**:
```
"ICICI Prudential Bluechip Fund - Direct Plan - Growth Option"
→ "ICICI Prudential Bluechip Fund"

"HDFC Equity Fund - Regular - Dividend - Jan 2025"
→ "HDFC Equity Fund"

"Axis  Long  Term  Equity  Fund" (multiple spaces)
→ "Axis Long Term Equity Fund"
```

---

### ISIN Validation

**Process**:
1. Extract ISIN from Excel
2. Trim whitespace
3. Convert to uppercase (if not already)
4. Validate pattern: `^INE[A-Z0-9]{6}10[A-Z0-9]{1}$`
5. If invalid, **REJECT** the holding (and potentially skip scheme-month)

**Examples**:
```
"INE002A01018" → Valid ✅
"ine002a01018" → Convert to uppercase → "INE002A01018" ✅
" INE002A01018 " → Trim → "INE002A01018" ✅
"INE002A01018 " → Trim → "INE002A01018" ✅
"INE002A0101" → Invalid (11 chars) ❌
"INE002A01201" → Invalid (debt, security code "01") ❌
```

---

### Numeric Normalization

#### Lakhs/Crores to INR

**Rule**: Market value MUST be converted to Indian Rupees (₹), not lakhs or crores.

**Conversion**:
```
1 Lakh = 100,000 INR
1 Crore = 10,000,000 INR

If Excel shows: 306.25 (in lakhs)
Store as: 30,625,000.00 INR

If Excel shows: 30.625 (in crores)
Store as: 306,250,000.00 INR
```

**Detection**:
- Check column header or metadata for units (lakhs, crores, INR)
- If ambiguous, **SKIP** scheme-month and alert

---

#### Percentage Scaling

**Rule**: Percent of NAV MUST be on 0-100 scale, NOT 0-1 scale.

**Conversion**:
```
If Excel shows: 0.0306 (0-1 scale)
Store as: 3.0600 (0-100 scale)

If Excel shows: 3.06% (already 0-100 scale)
Store as: 3.0600
```

**Detection**:
- If all percentages are < 1, assume 0-1 scale and multiply by 100
- If percentages are mixed (some < 1, some > 1), **SKIP** scheme-month and alert

---

#### Rounding

**Rule**: Round to precision defined in schema:
- `market_value_inr`: 2 decimal places
- `percent_of_nav`: 4 decimal places

**Examples**:
```
market_value_inr: 12345.678 → 12345.68
percent_of_nav: 3.06789 → 3.0679
percent_of_nav: 0.00001 → 0.0000 (rounds to zero, VALID)
```

---

### Duplicate Detection

**Rule**: Within a single scheme-month, each company (ISIN) must appear **at most once**.

**Process**:
1. Extract all holdings for a scheme-month
2. Group by ISIN
3. If any ISIN appears more than once, **SKIP** scheme-month and alert

**Example**:
```
Scheme: ICICI Bluechip - Direct - Growth
Period: Jan 2025
Holdings:
  - ISIN: INE002A01018, Quantity: 100000, Value: 245000000
  - ISIN: INE040A01034, Quantity: 50000, Value: 75000000
  - ISIN: INE002A01018, Quantity: 25000, Value: 61250000  ← DUPLICATE

Action: SKIP entire scheme-month, send alert
```

---

## 4️⃣ FAILURE & SKIP SEMANTICS

### When to SKIP a Scheme-Month

**A scheme-month MUST be SKIPPED if**:

1. ❌ **Missing mandatory field**:
   - AMC name not provided
   - Scheme name not extractable
   - Plan type not extractable or invalid
   - Option type not extractable or invalid
   - Period (year, month) not provided
   - Any holding is missing ISIN, company name, quantity, market value, or percent of NAV

2. ❌ **Invalid data**:
   - ISIN does not match equity pattern
   - Quantity is negative
   - Market value is negative
   - Percent of NAV is negative or > 100
   - Numeric field is non-numeric

3. ❌ **Duplicate holdings**:
   - Same ISIN appears multiple times in the same scheme-month

4. ❌ **Ambiguous data**:
   - Cannot determine if market value is in lakhs, crores, or INR
   - Cannot determine if percentage is 0-1 or 0-100 scale
   - Scheme name is ambiguous (multiple possible interpretations)

5. ❌ **Data integrity violation**:
   - Sum of percent_of_nav significantly deviates from 100% (e.g., < 95% or > 105%)
     - **Note**: This is a WARNING, not automatic skip. Alert for manual review.

---

### When Extraction Can Continue

**Extraction can continue if**:

1. ✅ **Optional fields are missing**:
   - Scheme category, scheme code (stored as NULL)
   - Exchange symbol, sector, industry (stored as NULL)

2. ✅ **One scheme-month fails, others succeed**:
   - If File contains 10 schemes, and 1 fails, the other 9 can still be loaded
   - Each scheme-month is independent

3. ✅ **Exited positions** (zero values):
   - quantity = 0, market_value = 0, percent_of_nav = 0 (VALID)

---

### Telegram Alert Requirements

**An alert MUST be sent when**:

1. ⚠️ Scheme-month is **SKIPPED**
2. ⚠️ Data integrity **WARNING** (e.g., percentages don't sum to ~100%)
3. ⚠️ Extraction **FAILS** for entire file

**Alert MUST include**:

- **Severity**: ERROR (skip) or WARNING (integrity issue)
- **AMC name**
- **Scheme name** (if available)
- **Plan type** (if available)
- **Option type** (if available)
- **Period** (year, month)
- **Reason**: Specific error message
- **File name**: Original Excel file name
- **Action required**: "Manual review required" or "Investigate data source"

**Alert Format**:
```
🚨 ERROR: Scheme-Month SKIPPED

AMC: HDFC Mutual Fund
Scheme: HDFC Equity Fund - Direct - Growth
Period: January 2025
Reason: Missing ISIN for 3 holdings (rows 45, 67, 89)
File: hdfc_jan_2025.xlsx
Action: Manual review required - check source file

---

⚠️ WARNING: Data Integrity Issue

AMC: ICICI Prudential Mutual Fund
Scheme: ICICI Bluechip Fund - Direct - Growth
Period: January 2025
Reason: Percent of NAV sums to 97.34% (expected ~100%)
File: icici_jan_2025.xlsx
Action: Investigate data source - possible missing holdings
```

---

### What is NEVER Allowed

**NEVER**:

1. ❌ **Silent failure**: If extraction fails, ALWAYS log and alert
2. ❌ **Partial load**: If validation fails for ANY holding, SKIP entire scheme-month
3. ❌ **Guessing**: If data is ambiguous, SKIP and alert (do NOT guess)
4. ❌ **Auto-fixing**: If data is invalid, SKIP and alert (do NOT auto-fix)
5. ❌ **Ignoring duplicates**: If ISIN appears twice, SKIP entire scheme-month
6. ❌ **Continuing after critical error**: If database constraint is violated, ROLLBACK transaction

---

## 5️⃣ GUARANTEES TO DATABASE

### Data Integrity Guarantees

**We GUARANTEE**:

1. ✅ **No constraint violations**:
   - All data written will satisfy PostgreSQL schema constraints
   - `market_value_inr >= 0` (never negative)
   - `percent_of_nav >= 0 AND percent_of_nav <= 100` (never negative or > 100)
   - `quantity >= 0` (never negative)
   - ISIN matches equity pattern
   - Plan type is exactly "Direct" or "Regular"
   - Option type is exactly "Growth", "Dividend", or "IDCW"

2. ✅ **No partial snapshots**:
   - If snapshot is created, ALL holdings are inserted
   - If ANY holding fails, entire snapshot is rolled back
   - Database is NEVER left in partial state

3. ✅ **No duplicate scheme-months**:
   - Before inserting, check if snapshot exists for (scheme_id, period_id)
   - If exists, SKIP (or DELETE old and insert new, based on reload policy)
   - UNIQUE constraint on (scheme_id, period_id) enforced

4. ✅ **Referential integrity**:
   - All foreign keys are satisfied before insert
   - AMC exists in `amcs` table
   - Scheme exists in `schemes` table
   - Period exists in `periods` table
   - Company exists in `companies` table
   - Snapshot exists in `scheme_snapshots` table

5. ✅ **Transaction safety**:
   - All inserts for a scheme-month happen in a SINGLE transaction
   - If transaction fails, ROLLBACK (no partial data)
   - If transaction succeeds, COMMIT (all data persisted)

---

### Consistency Guarantees

**We GUARANTEE**:

1. ✅ **Snapshot metadata matches holdings**:
   - `total_holdings` = COUNT of rows in `equity_holdings` for this snapshot
   - `total_value_inr` = SUM of `market_value_inr` for this snapshot
   - `holdings_count` = COUNT of DISTINCT `company_id` for this snapshot

2. ✅ **No orphaned records**:
   - If snapshot is deleted, all holdings are deleted (CASCADE)
   - If scheme is deleted, all snapshots are deleted (CASCADE)
   - If company is deleted, holdings are NOT deleted (RESTRICT)

3. ✅ **Idempotent master data**:
   - AMCs, schemes, periods, companies can be inserted multiple times safely
   - `ON CONFLICT DO NOTHING` or `ON CONFLICT DO UPDATE` used
   - No duplicate master records

---

## 6️⃣ NON-GOALS

### What This Contract Does NOT Cover

**This contract explicitly does NOT**:

1. ❌ **Support non-equity assets**:
   - Debt holdings (bonds, debentures)
   - Hybrid holdings (equity + debt)
   - Cash or cash equivalents
   - **Scope**: Equity only (ISIN security code "10")

2. ❌ **Support daily or weekly data**:
   - Only monthly portfolio disclosures
   - **Scope**: Monthly data only

3. ❌ **Support international mutual funds**:
   - Only Indian mutual funds (ISIN starts with "INE")
   - **Scope**: India only

4. ❌ **Auto-fix bad data**:
   - If data is invalid, SKIP and alert
   - **Philosophy**: Garbage in, alert out (NOT garbage in, garbage out)

5. ❌ **Guess ambiguous data**:
   - If scheme name is ambiguous, SKIP and alert
   - If units are unclear (lakhs vs crores), SKIP and alert
   - **Philosophy**: Explicit is better than implicit

6. ❌ **Handle unmerged files**:
   - Files must be pre-merged per AMC per month
   - **Scope**: One file = one AMC = one month

7. ❌ **Support scheme-level aggregations**:
   - No automatic calculation of scheme-level metrics (AUM, returns, etc.)
   - **Scope**: Holdings data only, analytics is separate

8. ❌ **Support real-time data**:
   - No live price updates
   - No intraday holdings
   - **Scope**: End-of-month snapshots only

9. ❌ **Handle missing historical data**:
   - No backfilling of missing months
   - No interpolation of missing holdings
   - **Scope**: Load what is provided, alert what is missing

10. ❌ **Support custom AMC formats automatically**:
    - Each AMC format requires explicit extractor implementation
    - **Scope**: Supported AMCs only, new AMCs require code changes

---

### Why These Are Non-Goals

**Reason**: Scope creep prevention

By explicitly stating what we do NOT support, we:
- ✅ Prevent feature creep
- ✅ Maintain focus on core use case (equity monthly portfolios)
- ✅ Ensure long-term stability (no constant schema changes)
- ✅ Set clear expectations with stakeholders

**If a non-goal becomes a goal**, it requires:
- Explicit contract versioning (v2.0)
- Schema changes (if needed)
- Stakeholder approval
- Implementation plan

---

## 📊 SUMMARY

### Contract Pillars

This Canonical Data Contract v1.0 is built on four pillars:

1. **Strictness**: No guessing, no auto-fixing, no silent failures
2. **Clarity**: Explicit rules for every entity and field
3. **Safety**: All-or-nothing loading, transaction-based, no partial data
4. **Stability**: Frozen contract, changes require explicit versioning

---

### Compliance Checklist

**All code MUST**:

- [x] Extract only mandatory fields (skip if missing)
- [x] Validate all data against rules (reject if invalid)
- [x] Normalize data per normalization rules
- [x] Skip scheme-month if any validation fails
- [x] Send Telegram alert for all skips and warnings
- [x] Load data in transactions (all-or-nothing)
- [x] Satisfy all database constraints
- [x] Maintain referential integrity
- [x] Never load partial snapshots
- [x] Never guess or auto-fix data
- [x] Never fail silently

---

## 🔒 FINAL STATEMENT

**This Canonical Data Contract v1.0 is FROZEN.**

Any change to this contract requires:
1. Explicit versioning (v2.0, v3.0, etc.)
2. Stakeholder review and approval
3. Impact analysis on existing code and data
4. Migration plan (if schema changes are needed)
5. Documentation of WHY the change is necessary

**Changes are NOT allowed for**:
- Convenience
- "Nice to have" features
- Workarounds for bad data sources

**Changes are ONLY allowed for**:
- Critical bugs in contract logic
- Regulatory changes requiring new data fields
- Fundamental changes in AMC data structure (industry-wide)

**This contract is designed to last 10+ years without changes.**

---

*Document created: 2026-02-01*  
*Version: 1.0 (FROZEN)*  
*Status: Production-Ready*  
*Scope: Equity-only monthly portfolio data from AMC Excel files*  

---

**END OF CANONICAL DATA CONTRACT v1.0**
