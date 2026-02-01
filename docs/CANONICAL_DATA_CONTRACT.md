# 📜 CANONICAL DATA CONTRACT
## Mutual Fund Portfolio Analytics Platform

> **Version**: 1.0  
> **Status**: FINAL  
> **Effective Date**: 2026-02-01  
> **Stability Guarantee**: This contract is designed to remain unchanged for 10+ years

---

## 🎯 PURPOSE

This document defines the **STRICT, IMMUTABLE** data contract that ALL AMC extractors must follow.

**Key Principles**:
- **AMC-Agnostic**: Works for any AMC, any format
- **Future-Proof**: Supports portfolio tracking, stock analysis, net buying/selling, debt funds
- **Zero Tolerance**: No partial data, no dirty data, no ambiguity
- **Skip, Don't Guess**: If data is unclear, skip the scheme-month and alert

---

## 1️⃣ CANONICAL ENTITIES

### Entity 1: AMC (Asset Management Company)

**Definition**: The mutual fund house that manages schemes.

**Required Fields**:
- `amc_name` - Full official name (e.g., "ICICI Prudential Mutual Fund")

**Uniqueness**: `amc_name` must be unique

**Validation Rules**:
- Must contain "Mutual Fund" or "Asset Management"
- Must be the OFFICIAL registered name (not abbreviations)
- Cannot be empty or null

**NOT Allowed**:
- Abbreviations (e.g., "ICICI MF" ❌)
- Short names (e.g., "HDFC" ❌)
- Variations (e.g., "ICICI Pru" ❌)

**Examples**:
- ✅ Valid: "ICICI Prudential Mutual Fund"
- ✅ Valid: "HDFC Mutual Fund"
- ❌ Invalid: "ICICI MF"
- ❌ Invalid: "Axis"

---

### Entity 2: Scheme

**Definition**: A specific mutual fund product offered by an AMC.

**Required Fields**:
- `scheme_name` - Canonical name (see Section 3 for rules)
- `amc_name` - Parent AMC
- `plan_type` - Either "Direct" or "Regular"
- `option_type` - Either "Growth", "Dividend", or "IDCW"

**Optional Fields**:
- `scheme_category` - e.g., "Large Cap", "Mid Cap", "Flexi Cap"
- `scheme_code` - AMC's internal code (if available)

**Uniqueness**: Combination of `(amc_name, scheme_name, plan_type, option_type)` must be unique

**Validation Rules**:
- `scheme_name` must NOT contain plan/option suffixes (see Section 3)
- `plan_type` must be exactly "Direct" or "Regular" (case-sensitive)
- `option_type` must be exactly "Growth", "Dividend", or "IDCW" (case-sensitive)

**NOT Allowed**:
- Scheme names with embedded plan/option (e.g., "HDFC Equity Fund - Direct Plan - Growth" ❌)
- Ambiguous plan types (e.g., "Dir", "Reg" ❌)
- Old terminology mixed with new (use "IDCW" not "Dividend Reinvestment" ❌)

**Examples**:
- ✅ Valid:
  - `scheme_name`: "ICICI Prudential Bluechip Fund"
  - `plan_type`: "Direct"
  - `option_type`: "Growth"
  
- ❌ Invalid:
  - `scheme_name`: "ICICI Prudential Bluechip Fund - Direct Plan - Growth"
  - `plan_type`: "Dir"
  - `option_type`: "Grw"

---

### Entity 3: Period (Month)

**Definition**: A specific month for which portfolio data is disclosed.

**Required Fields**:
- `year` - 4-digit year (e.g., 2025)
- `month` - Month number (1-12)
- `period_end_date` - Last day of the month (YYYY-MM-DD)

**Uniqueness**: Combination of `(year, month)` must be unique

**Validation Rules**:
- `year` must be >= 2020 and <= current year + 1
- `month` must be 1-12
- `period_end_date` must be the ACTUAL last day of that month (e.g., 2025-02-28, not 2025-02-30)

**NOT Allowed**:
- Inferring period from Excel text (e.g., "As on January 31, 2025" ❌)
- Using file download date as period ❌
- Partial months or date ranges ❌

**Source of Truth**: Period MUST be explicitly provided by the pipeline orchestrator, NOT extracted from Excel.

**Examples**:
- ✅ Valid:
  - `year`: 2025
  - `month`: 1
  - `period_end_date`: "2025-01-31"
  
- ❌ Invalid:
  - `year`: 25 (must be 4 digits)
  - `month`: "January" (must be numeric)
  - `period_end_date`: "2025-01-32" (invalid date)

---

### Entity 4: Company

**Definition**: A publicly listed company whose equity shares are held by mutual funds.

**Required Fields**:
- `isin` - 12-character ISIN code (e.g., "INE002A01018")
- `company_name` - Official company name

**Optional Fields**:
- `exchange_symbol` - Stock ticker (e.g., "RELIANCE")
- `sector` - Business sector (e.g., "Energy")
- `industry` - Specific industry (e.g., "Oil & Gas")

**Uniqueness**: `isin` must be unique

**Validation Rules**:
- `isin` must be EXACTLY 12 characters
- `isin` must start with "INE" (Indian Equity)
- `isin` positions 9-10 (0-indexed as 8-9) must be "10" (equity security code)
- `company_name` cannot be empty

**NOT Allowed**:
- Non-equity ISINs (debentures, bonds, rights, warrants) ❌
- ISINs starting with "IN9" (rights/partly paid) ❌
- ISINs with security codes other than "10" ❌

**Examples**:
- ✅ Valid:
  - `isin`: "INE002A01018" (Reliance Industries - equity)
  - `company_name`: "Reliance Industries Limited"
  
- ❌ Invalid:
  - `isin`: "INE040A08849" (HDFC Bank debenture - code "08")
  - `isin`: "IN9628A01026" (UPL rights issue - starts with "IN9")
  - `isin`: "INE002A" (too short)

---

### Entity 5: Holding (Equity Holding Record)

**Definition**: A single equity position held by a scheme in a specific period.

See Section 2 for complete structure.

---

## 2️⃣ CANONICAL HOLDING RECORD

This is the **MOST CRITICAL** structure. Every extractor must output holdings in this exact format.

### Field Definitions

| Field Name | Data Type | Required | Validation Rules |
|------------|-----------|----------|------------------|
| `amc_name` | Text | ✅ Yes | Must match Entity 1 rules |
| `scheme_name` | Text | ✅ Yes | Canonical name (see Section 3) |
| `plan_type` | Text | ✅ Yes | Exactly "Direct" or "Regular" |
| `option_type` | Text | ✅ Yes | Exactly "Growth", "Dividend", or "IDCW" |
| `year` | Integer | ✅ Yes | 4-digit year (e.g., 2025) |
| `month` | Integer | ✅ Yes | 1-12 |
| `isin` | Text | ✅ Yes | 12 chars, starts with "INE", security code "10" |
| `company_name` | Text | ✅ Yes | Cannot be empty |
| `quantity` | Integer | ✅ Yes | Can be 0, cannot be negative, cannot be null |
| `market_value_inr` | Decimal | ✅ Yes | In Indian Rupees (₹), must be > 0 |
| `percent_of_nav` | Decimal | ✅ Yes | 0-100 scale (e.g., 3.06 means 3.06%), must be > 0 |
| `sector` | Text | ⚠️ Optional | If available from Excel |
| `industry` | Text | ⚠️ Optional | If available from Excel |

### Validation Rules Per Field

**`amc_name`**:
- Must be full official name
- Cannot contain abbreviations

**`scheme_name`**:
- Must be canonical (no plan/option suffixes)
- See Section 3 for extraction rules

**`plan_type`**:
- Must be EXACTLY "Direct" or "Regular" (case-sensitive)
- If Excel says "Dir", "Direct Plan", "DP" → Normalize to "Direct"
- If Excel says "Reg", "Regular Plan", "RP" → Normalize to "Regular"
- If ambiguous or missing → SKIP this scheme-month

**`option_type`**:
- Must be EXACTLY "Growth", "Dividend", or "IDCW" (case-sensitive)
- If Excel says "Grw", "Gwth", "Growth Option" → Normalize to "Growth"
- If Excel says "Div", "Dividend Payout", "Income Distribution" → Normalize to "Dividend"
- If Excel says "IDCW", "Income Distribution cum Capital Withdrawal" → Normalize to "IDCW"
- If ambiguous or missing → SKIP this scheme-month

**`year` and `month`**:
- Must be provided by pipeline orchestrator
- NEVER infer from Excel content

**`isin`**:
- Must pass triple-check equity filter:
  1. Starts with "INE" ✅
  2. Length = 12 ✅
  3. Positions 9-10 = "10" ✅
- If any check fails → SKIP this row (not the whole scheme)

**`company_name`**:
- Cannot be empty, null, or whitespace
- If missing → SKIP this row

**`quantity`**:
- Must be an integer (whole number)
- CAN be 0 (valid for holdings with very small positions)
- CANNOT be negative
- CANNOT be null or missing
- If missing or invalid → SKIP this row

**`market_value_inr`**:
- Must be in Indian Rupees (₹)
- Must be > 0 (cannot be zero or negative)
- If Excel provides in Lakhs → Convert to Rupees: `value_inr = value_lakhs * 100,000`
- If Excel provides in Crores → Convert to Rupees: `value_inr = value_crores * 10,000,000`
- If missing or zero → SKIP this row

**`percent_of_nav`**:
- Must be on 0-100 scale (e.g., 3.06 means 3.06%)
- Must be > 0
- If Excel provides on 0-1 scale (e.g., 0.0306) → Convert: `pct_nav = pct_nav * 100`
- If missing or zero → SKIP this row

### Example: VALID Holding Record

```
amc_name: "ICICI Prudential Mutual Fund"
scheme_name: "ICICI Prudential Bluechip Fund"
plan_type: "Direct"
option_type: "Growth"
year: 2025
month: 1
isin: "INE002A01018"
company_name: "Reliance Industries Limited"
quantity: 125000
market_value_inr: 306250000.00  (₹30.625 crores)
percent_of_nav: 3.06
sector: "Energy"
industry: "Oil & Gas"
```

### Example: INVALID Holding Records

**Invalid 1: Missing required field**
```
❌ SKIP THIS ROW
Reason: percent_of_nav is missing

amc_name: "HDFC Mutual Fund"
scheme_name: "HDFC Equity Fund"
plan_type: "Direct"
option_type: "Growth"
year: 2025
month: 1
isin: "INE040A01034"
company_name: "HDFC Bank Limited"
quantity: 50000
market_value_inr: 75000000.00
percent_of_nav: null  ❌ MISSING
```

**Invalid 2: Non-equity ISIN**
```
❌ SKIP THIS ROW
Reason: ISIN is a debenture (security code "08")

amc_name: "Axis Mutual Fund"
scheme_name: "Axis Bluechip Fund"
plan_type: "Regular"
option_type: "Growth"
year: 2025
month: 1
isin: "INE040A08849"  ❌ Security code "08" (debenture)
company_name: "HDFC Bank Debenture"
quantity: 1000
market_value_inr: 10000000.00
percent_of_nav: 0.50
```

**Invalid 3: Ambiguous plan type**
```
❌ SKIP THIS SCHEME-MONTH
Reason: Cannot determine if Direct or Regular

amc_name: "Kotak Mahindra Mutual Fund"
scheme_name: "Kotak Equity Opportunities Fund"
plan_type: "Dir"  ❌ Ambiguous (should be "Direct" or "Regular")
option_type: "Growth"
year: 2025
month: 1
isin: "INE002A01018"
company_name: "Reliance Industries Limited"
quantity: 10000
market_value_inr: 24500000.00
percent_of_nav: 1.25
```

---

## 3️⃣ SCHEME NAME STANDARDISATION RULES

### Canonical Scheme Name Definition

**Rule**: The `scheme_name` field must contain ONLY the base scheme name, WITHOUT plan type or option type suffixes.

### What Must Be Removed

Remove ALL of the following from raw scheme names:

**Plan Type Suffixes**:
- "Direct Plan"
- "Direct"
- "Dir"
- "DP"
- "Regular Plan"
- "Regular"
- "Reg"
- "RP"

**Option Type Suffixes**:
- "Growth"
- "Growth Option"
- "Grw"
- "Gwth"
- "Dividend"
- "Dividend Option"
- "Div"
- "IDCW"
- "Income Distribution cum Capital Withdrawal"
- "Dividend Reinvestment"
- "Dividend Payout"

**Metadata Suffixes**:
- "An open ended..."
- "Portfolio Disclosure"
- "Monthly Report"
- Dates (e.g., "as on January 31, 2025")

### Extraction Priority Order

When extracting scheme name from Excel, use this priority:

**Priority 1**: Verbose descriptive name from first 10 rows
- Look for long text (>50 characters) containing "Fund" or "Scheme"
- Example: "ICICI Prudential Bluechip Fund - An open ended equity scheme predominantly investing in large cap stocks"
- Extract base name: "ICICI Prudential Bluechip Fund"

**Priority 2**: Index/mapping sheet
- If Excel has an "Index" or "Summary" sheet with scheme code → name mapping
- Use the full name from mapping

**Priority 3**: Sheet name
- If sheet name is descriptive (not just "Sheet1" or "AXS001")
- Use sheet name as base

**Priority 4**: Filename
- Last resort: extract from filename
- Example: "HDFC_Equity_Fund_Direct_Growth_Jan_2025.xlsx" → "HDFC Equity Fund"

### Normalization Rules

After extraction, apply these normalizations:

1. **Remove suffixes** (as listed above)
2. **Trim whitespace** (leading/trailing)
3. **Collapse multiple spaces** into single space
4. **Ensure AMC prefix**: If scheme name doesn't contain AMC name, prepend it
   - Example: "Bluechip Fund" → "ICICI Prudential Bluechip Fund"
5. **Remove special characters** at end (e.g., trailing hyphens, dashes)

### Examples

| Raw Scheme Name (from Excel) | Canonical `scheme_name` | `plan_type` | `option_type` |
|-------------------------------|-------------------------|-------------|---------------|
| "ICICI Prudential Bluechip Fund - Direct Plan - Growth" | "ICICI Prudential Bluechip Fund" | "Direct" | "Growth" |
| "HDFC Equity Fund - Regular Plan - Dividend Option" | "HDFC Equity Fund" | "Regular" | "Dividend" |
| "Axis Bluechip Fund - An open ended equity scheme..." | "Axis Bluechip Fund" | (extract separately) | (extract separately) |
| "Kotak_Equity_Opportunities_Fund_Dir_Grw" | "Kotak Equity Opportunities Fund" | "Direct" | "Growth" |
| "PPFAS Long Term Equity Fund - Direct - IDCW" | "PPFAS Long Term Equity Fund" | "Direct" | "IDCW" |

### What Must NEVER Appear in `scheme_name`

❌ Plan type indicators ("Direct", "Regular", "Dir", "Reg")
❌ Option type indicators ("Growth", "Dividend", "IDCW", "Grw", "Div")
❌ Metadata ("Portfolio Disclosure", "Monthly Report")
❌ Dates ("as on January 31, 2025")
❌ Underscores (replace with spaces)
❌ Multiple consecutive spaces

---

## 4️⃣ PERIOD RULES

### How Period is Determined

**STRICT RULE**: The period (year/month) MUST be provided by the pipeline orchestrator as input parameters.

**Extractors MUST NOT**:
- ❌ Infer period from Excel text (e.g., "Portfolio as on January 31, 2025")
- ❌ Infer period from filename (e.g., "HDFC_Jan_2025.xlsx")
- ❌ Infer period from file download date
- ❌ Infer period from any metadata

**Why**: Excel files may contain outdated text, filenames may be renamed, download dates are unreliable.

### Uniqueness Rules

**Rule**: A scheme can have ONLY ONE portfolio disclosure per period.

**Uniqueness Key**: `(amc_name, scheme_name, plan_type, option_type, year, month)`

**If Duplicate Detected**:
- ❌ SKIP the entire scheme-month
- 🚨 Send alert: "Duplicate portfolio found for [scheme] [period]"
- 📝 Log both file sources for manual investigation

### Period Validation

**Valid Periods**:
- Year: 2020 - (current year + 1)
- Month: 1 - 12

**Invalid Periods**:
- ❌ Future periods beyond next month
- ❌ Periods before 2020 (data quality concerns)
- ❌ Invalid month numbers (0, 13, etc.)

---

## 5️⃣ FAILURE & SKIP RULES

### Conditions That SKIP Entire Scheme-Month

These conditions cause the ENTIRE scheme-month to be skipped (no holdings inserted):

| Condition | Action | Alert Level |
|-----------|--------|-------------|
| Cannot determine `plan_type` (ambiguous or missing) | ❌ SKIP scheme-month | 🚨 ERROR |
| Cannot determine `option_type` (ambiguous or missing) | ❌ SKIP scheme-month | 🚨 ERROR |
| Cannot extract canonical `scheme_name` | ❌ SKIP scheme-month | 🚨 ERROR |
| No valid equity holdings found (all rows failed ISIN check) | ❌ SKIP scheme-month | ⚠️ WARNING |
| Duplicate scheme-month already exists in DB | ❌ SKIP scheme-month | 🚨 ERROR |
| Excel file is corrupted or unreadable | ❌ SKIP scheme-month | 🚨 ERROR |

### Conditions That SKIP Individual Rows

These conditions cause ONLY the specific row to be skipped (other rows in scheme-month are processed):

| Condition | Action | Alert Level |
|-----------|--------|-------------|
| ISIN fails equity check (not "INE", wrong length, security code != "10") | ❌ SKIP row | ℹ️ INFO |
| `company_name` is empty or null | ❌ SKIP row | ⚠️ WARNING |
| `quantity` is missing, null, or negative | ❌ SKIP row | ⚠️ WARNING |
| `market_value_inr` is missing, null, zero, or negative | ❌ SKIP row | ⚠️ WARNING |
| `percent_of_nav` is missing, null, zero, or negative | ❌ SKIP row | ⚠️ WARNING |
| Row is a subtotal or summary row (detected via heuristics) | ❌ SKIP row | ℹ️ INFO |

### Conditions That Generate WARNING (But Still Allowed)

These conditions allow the row to be inserted but generate a warning:

| Condition | Action | Alert Level |
|-----------|--------|-------------|
| `quantity` is 0 (valid for very small positions) | ✅ INSERT with warning | ⚠️ WARNING |
| `sector` or `industry` is missing | ✅ INSERT with null values | ℹ️ INFO |
| `company_name` looks unusual (e.g., all caps, special characters) | ✅ INSERT with warning | ⚠️ WARNING |

### Information Logged for Each Skip

When a scheme-month or row is skipped, log the following:

**For Scheme-Month Skips**:
```
timestamp: 2026-02-01 11:30:45
level: ERROR
amc_name: "HDFC Mutual Fund"
scheme_name: "HDFC Equity Fund" (if extractable)
year: 2025
month: 1
file_path: "data/input/HDFC_Equity_Fund_Jan_2025.xlsx"
skip_reason: "Cannot determine plan_type (ambiguous)"
raw_scheme_name: "HDFC Equity Fund - Dir Plan - Growth"
extracted_plan_type: "Dir" (invalid, must be "Direct" or "Regular")
```

**For Row Skips**:
```
timestamp: 2026-02-01 11:30:46
level: WARNING
amc_name: "ICICI Prudential Mutual Fund"
scheme_name: "ICICI Prudential Bluechip Fund"
plan_type: "Direct"
option_type: "Growth"
year: 2025
month: 1
row_number: 45
skip_reason: "market_value_inr is zero"
isin: "INE123A01012"
company_name: "ABC Limited"
quantity: 1000
market_value_inr: 0.00 (invalid)
percent_of_nav: 0.05
```

---

## 6️⃣ WHY THIS CONTRACT WILL NOT NEED REWRITING

### Future-Proof Design Principles

This contract is designed to support ALL future use cases without modification:

### ✅ Supports Portfolio Tracker

**Use Case**: Track individual investor portfolios across multiple schemes.

**How Contract Supports**:
- Unique identification: `(amc_name, scheme_name, plan_type, option_type)` allows precise scheme matching
- Period tracking: `(year, month)` enables month-over-month portfolio evolution
- Clean data: No partial holdings means accurate portfolio valuation

**Example Query** (conceptual):
```
Get all holdings for:
  - ICICI Prudential Bluechip Fund
  - Direct Plan
  - Growth Option
  - January 2025
```

### ✅ Supports Stock-Wise MF Holdings

**Use Case**: Show which mutual funds hold a specific stock (e.g., "Which funds hold Reliance?").

**How Contract Supports**:
- `isin` is the unique key for companies
- `company_name` provides human-readable name
- `quantity` and `market_value_inr` show exact positions
- `percent_of_nav` shows relative importance to each fund

**Example Query** (conceptual):
```
Get all schemes holding ISIN "INE002A01018" (Reliance)
For period: January 2025
Show: scheme_name, quantity, market_value_inr, percent_of_nav
Order by: market_value_inr DESC
```

### ✅ Supports Net Buying / Selling Analysis

**Use Case**: Detect if mutual funds are net buyers or sellers of a stock month-over-month.

**How Contract Supports**:
- `quantity` field enables precise share count comparison
- Period tracking enables month-over-month delta calculation
- Clean data ensures accurate trend detection

**Example Analysis** (conceptual):
```
For ISIN "INE002A01018" (Reliance):
  - January 2025 total quantity: 10,000,000 shares
  - February 2025 total quantity: 10,500,000 shares
  - Net buying: +500,000 shares (+5%)
```

### ✅ Supports Future Debt Funds

**Use Case**: Extend platform to include debt fund portfolios (bonds, debentures).

**How Contract Supports**:
- ISIN structure already supports debt instruments (just change security code validation)
- All other fields (`quantity`, `market_value_inr`, `percent_of_nav`) are asset-class agnostic
- Separate `asset_class` field can be added without breaking existing structure

**Future Extension** (no contract change needed):
```
Add new field: asset_class ("Equity" or "Debt")
Modify ISIN validation:
  - Equity: positions 9-10 = "10"
  - Debt: positions 9-10 = "07", "08", "09", etc.
All other fields remain unchanged.
```

### ✅ Handles New AMCs Without Changes

**Why**: Contract is AMC-agnostic.

**New AMC Checklist**:
1. Create new extractor module (e.g., `extractors/sbi_amc.py`)
2. Implement Excel parsing logic specific to SBI's format
3. Output holdings in canonical format (this contract)
4. No changes to database, validation, or loading logic

**Example**: Adding SBI Mutual Fund
- SBI provides Excel in different format (multi-sheet with codes)
- Extractor handles SBI-specific parsing
- Outputs canonical holdings with `amc_name: "SBI Mutual Fund"`
- Rest of pipeline works unchanged

### ✅ Handles Format Changes Without Breaking

**Why**: Contract separates "what data" from "how to extract it".

**Scenario**: HDFC changes Excel format in 2027
- Old format: One file per scheme
- New format: Single file with all schemes

**Impact**:
- Update ONLY `extractors/hdfc_amc.py` to handle new format
- Canonical holding output remains identical
- Database, validation, analytics unchanged

### ✅ Supports Multi-Currency (Future)

**Why**: `market_value_inr` is explicitly in INR, allowing future addition of other currencies.

**Future Extension** (no contract change needed):
```
Add new fields:
  - market_value_usd (for international funds)
  - currency_code ("INR", "USD", etc.)
Existing market_value_inr remains for backward compatibility.
```

### ✅ Supports Regulatory Changes

**Why**: Flexible optional fields and clear separation of plan/option types.

**Example**: SEBI mandates new disclosure field "Issuer Rating"
- Add as optional field: `issuer_rating`
- Existing holdings without rating remain valid (null)
- New holdings include rating if available
- No migration needed

---

## 🎯 SUMMARY

This canonical data contract is:

✅ **Strict**: No partial data, no ambiguity, no dirty data
✅ **AMC-Agnostic**: Works for any AMC, any format
✅ **Future-Proof**: Supports portfolio tracking, stock analysis, net buying/selling, debt funds
✅ **Stable**: Designed to remain unchanged for 10+ years
✅ **Extensible**: New fields can be added without breaking existing structure
✅ **Traceable**: Clear skip rules and logging for audit trail

**Golden Rule**: When in doubt, SKIP and ALERT. Never guess, never insert partial data.

---

**END OF CANONICAL DATA CONTRACT**

*This document is FINAL and should be treated as immutable. Any proposed changes must go through formal review process.*
