# Mutual Fund Portfolio Automation System - Technical Specification

## 1. System Overview

### Purpose
This system automates the extraction, standardization, and storage of monthly mutual fund portfolio disclosure data from multiple Asset Management Companies (AMCs) in India. Portfolio disclosures are published monthly by AMCs as Excel files containing holdings data for each scheme.

### Problem Statement
- AMCs publish portfolio data in heterogeneous Excel formats (different headers, layouts, sheet structures)
- Manual consolidation is error-prone and time-consuming
- No standardized schema exists across AMCs
- Data must be queryable for analysis, reporting, and API consumption

### Solution
A pipeline-based architecture that downloads, merges, extracts, standardizes, and loads portfolio data into a centralized database, exposing it via REST APIs.

### Why Merged Files?
AMCs publish one Excel file per scheme per month. A single AMC may have 50+ schemes, resulting in 50+ files monthly. The merger consolidates all scheme files for an AMC-month into a single merged Excel file, reducing extractor complexity and ensuring atomic processing per AMC-month.

---

## 2. High-Level Architecture

```
┌────────────┐    ┌─────────┐    ┌───────────┐    ┌────────┐    ┌──────────┐    ┌─────┐
│ Downloader │───▶│ Merger  │───▶│ Extractor │───▶│ Loader │───▶│ Database │───▶│ API │
└────────────┘    └─────────┘    └───────────┘    └────────┘    └──────────┘    └─────┘
```

### Component Responsibilities

| Component | Responsibility | NOT Responsible For |
|-----------|---------------|---------------------|
| **Downloader** | Download original Excel files from AMC websites, preserve original filenames, detect "Not Published" states | Parsing Excel content, renaming files, merging files |
| **Merger** | Consolidate scheme-wise Excel files into single monthly merged file per AMC | Data extraction, validation, standardization |
| **Extractor** | Parse merged Excel, detect tables, normalize headers, map columns, output canonical JSON | Database operations, file modification, downloading |
| **Loader** | Insert canonical JSON into database, handle duplicates, ensure transactional integrity | Excel parsing, data transformation, API logic |
| **Database** | Store standardized portfolio data, enforce constraints, serve as single source of truth | File I/O, Excel parsing, business logic |
| **API** | Expose database data via REST endpoints, handle authentication, caching | Direct file access, Excel parsing, database schema changes |

---

## 3. File Flow

### Directory Structure

```
data/
├── downloads/
│   ├── HDFC/
│   │   ├── 2025-11/
│   │   │   ├── HDFC_Scheme_A_Nov_2025.xlsx
│   │   │   ├── HDFC_Scheme_B_Nov_2025.xlsx
│   │   │   └── ...
│   │   └── 2025-12/
│   │       ├── HDFC_Scheme_A_Dec_2025.xlsx
│   │       └── ...
│   ├── SBI/
│   │   ├── 2025-11/
│   │   └── 2025-12/
│   └── ...
├── merged/
│   ├── HDFC/
│   │   ├── HDFC_2025-11_merged.xlsx
│   │   └── HDFC_2025-12_merged.xlsx
│   ├── SBI/
│   │   ├── SBI_2025-11_merged.xlsx
│   │   └── SBI_2025-12_merged.xlsx
│   └── ...
└── extracted/
    ├── HDFC/
    │   ├── HDFC_2025-11_extracted.json
    │   └── HDFC_2025-12_extracted.json
    └── ...
```

### Naming Conventions

- **Downloads**: `{AMC}_{Scheme}_{Month}_{Year}.xlsx` (original filename preserved)
- **Merged**: `{AMC}_{YYYY-MM}_merged.xlsx`
- **Extracted**: `{AMC}_{YYYY-MM}_extracted.json`

### Flow Rules

1. Downloader writes to `downloads/{AMC}/{YYYY-MM}/`
2. Merger reads from `downloads/{AMC}/{YYYY-MM}/`, writes to `merged/{AMC}/`
3. Extractor reads from `merged/{AMC}/`, writes to `extracted/{AMC}/`
4. Loader reads from `extracted/{AMC}/`, writes to database
5. **No component modifies files created by another component**

---

## 4. Extractor Layer (Contract-Based System)

### 4.1 Extractor Philosophy

The extractor is a **contract-based orchestration engine** that transforms heterogeneous Excel structures into a canonical data model. It operates on the principle of **scheme isolation**: failure in one scheme must not stop processing of other schemes.

### 4.2 Base Extractor Responsibilities

1. **File Reading**: Load merged Excel file using `openpyxl` or `pandas`
2. **Scheme Isolation**: Iterate through sheets, treating each as a potential scheme
3. **Sheet Detection**: Identify which sheets contain portfolio data vs metadata/summary sheets
4. **Header Normalization**: Standardize column headers (e.g., "Security Name" → "security_name")
5. **Column Mapping**: Map AMC-specific columns to canonical fields
6. **Table Detection**: Locate the start/end of portfolio tables within sheets
7. **Row Parsing**: Extract data rows, handle merged cells, skip subtotals/footers
8. **Data Validation**: Validate ISINs, numeric fields, dates
9. **Error Handling**: Log errors per scheme, continue processing remaining schemes
10. **Output Generation**: Produce canonical JSON array

### 4.3 Sheet Detection Logic

```
For each sheet in merged Excel:
    If sheet_name matches scheme pattern (e.g., contains "SCHEME", "FUND"):
        Attempt extraction
    Else if sheet_name in ["Summary", "Index", "Contents"]:
        Skip
    Else:
        Log warning, attempt extraction
```

### 4.4 Header Normalization

Headers are normalized using fuzzy matching and synonym dictionaries:

```python
HEADER_SYNONYMS = {
    "security_name": ["Security Name", "Name of Security", "Issuer", "Company Name"],
    "isin": ["ISIN", "ISIN Code", "ISIN No"],
    "instrument_type": ["Instrument", "Type", "Asset Class", "Security Type"],
    "market_value": ["Market Value", "Value", "Mkt Value (Rs)", "Amount"],
    "nav_percentage": ["% to NAV", "% of NAV", "Percentage to NAV", "NAV %"],
    ...
}
```

### 4.5 Column Mapping

Each AMC extractor defines a column map:

```python
COLUMN_MAP = {
    "A": "security_name",
    "B": "isin",
    "C": "instrument_type",
    "D": "quantity",
    "E": "market_value",
    "F": "nav_percentage",
    ...
}
```

### 4.6 Table Detection

Tables are detected by:
1. Scanning for header row (contains ≥3 canonical field names)
2. Marking table start as row immediately after header
3. Detecting table end by:
   - Empty rows (3+ consecutive)
   - Footer keywords ("Total", "Grand Total", "Net Assets")
   - Sheet end

### 4.7 Row Parsing Rules

- **Skip rows** where all cells are empty
- **Skip rows** containing footer keywords
- **Parse rows** where ISIN or security_name is present
- **Handle merged cells** by propagating value downward
- **Numeric parsing**: Strip currency symbols, commas; convert to float
- **Date parsing**: Detect DD-MM-YYYY, MM/DD/YYYY, YYYY-MM-DD formats

### 4.8 Error Handling

#### Scheme-Level Errors (Non-Fatal)
- Header not found → Log warning, skip scheme
- Table structure invalid → Log error, skip scheme
- Parsing error in row → Log error, skip row, continue scheme

#### File-Level Errors (Fatal)
- File not found → Raise exception, stop AMC processing
- File corrupted → Raise exception, stop AMC processing
- No schemes extracted → Raise exception, stop AMC processing

### 4.9 Logging Standards

```
[INFO] Processing AMC: HDFC, Month: 2025-12
[INFO] Found 45 sheets in merged file
[INFO] Processing sheet: HDFC Equity Fund
[DEBUG] Header detected at row 5
[DEBUG] Table spans rows 6-234
[INFO] Extracted 228 holdings from HDFC Equity Fund
[WARNING] Sheet "Summary" skipped (metadata sheet)
[ERROR] Sheet "HDFC Debt Fund": Header not found
[INFO] Successfully extracted 42/45 schemes
```

---

## 5. Canonical Data Model

### 5.1 Output Schema

Each extracted holding is represented as:

```json
{
    "scheme_name": "HDFC Equity Fund - Direct Plan - Growth",
    "amc_name": "HDFC",
    "isin": "INE002A01018",
    "security_name": "Reliance Industries Limited",
    "instrument_type": "Equity",
    "sector": "Energy",
    "rating": null,
    "quantity": 1250000,
    "market_value": 312500000.00,
    "nav_percentage": 8.45,
    "report_date": "2025-12-31"
}
```

### 5.2 Field Definitions

| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `scheme_name` | string | Yes | Full scheme name including plan type | Non-empty |
| `amc_name` | string | Yes | AMC identifier (uppercase) | Must match known AMC list |
| `isin` | string | Conditional | 12-character ISIN code | Regex: `^IN[A-Z0-9]{10}$` (if present) |
| `security_name` | string | Yes | Name of security/issuer | Non-empty |
| `instrument_type` | string | Yes | Asset class (Equity, Debt, etc.) | Must match canonical types |
| `sector` | string | No | Sector classification | Free text |
| `rating` | string | No | Credit rating (for debt) | Free text |
| `quantity` | float | No | Number of units held | ≥ 0 |
| `market_value` | float | Yes | Market value in INR | > 0 |
| `nav_percentage` | float | Yes | Percentage of NAV | 0 < x ≤ 100 |
| `report_date` | date | Yes | Portfolio date (last day of month) | Valid date, format: YYYY-MM-DD |

### 5.3 Why Canonical Schema?

- **Database Consistency**: Single table schema across all AMCs
- **API Simplicity**: Uniform response structure
- **Query Efficiency**: Standardized field names enable indexed queries
- **Future-Proofing**: New AMCs integrate without schema changes

---

## 6. HDFC AMC - Full Walkthrough

### 6.1 File Structure

- **Merged File**: `HDFC_2025-12_merged.xlsx`
- **Sheets**: One per scheme (e.g., "HDFC Equity Fund - Direct Plan - Growth")
- **Layout**: Header row followed by holdings table

### 6.2 Sheet Identification

```python
def is_valid_scheme_sheet(sheet_name: str) -> bool:
    skip_keywords = ["summary", "index", "contents", "disclaimer"]
    if any(kw in sheet_name.lower() for kw in skip_keywords):
        return False
    return "hdfc" in sheet_name.lower() or "fund" in sheet_name.lower()
```

### 6.3 Header Detection

HDFC headers typically appear in row 4-6. Detection logic:

```python
for row_idx in range(1, 10):
    row_values = [cell.value for cell in sheet[row_idx]]
    normalized = [normalize_header(v) for v in row_values if v]
    if len(set(normalized) & REQUIRED_HEADERS) >= 3:
        header_row = row_idx
        break
```

### 6.4 Column Mapping

HDFC standard columns:

| Excel Column | Header | Canonical Field |
|--------------|--------|-----------------|
| A | Name of the Instrument | security_name |
| B | ISIN | isin |
| C | Quantity | quantity |
| D | Market Value (Rs. In Lakhs) | market_value |
| E | % to NAV | nav_percentage |
| F | Sector | sector |

### 6.5 Row Parsing

```python
for row in sheet.iter_rows(min_row=header_row+1, max_row=sheet.max_row):
    security_name = row[0].value
    if not security_name or "total" in str(security_name).lower():
        continue
    
    holding = {
        "security_name": clean_text(security_name),
        "isin": clean_text(row[1].value),
        "quantity": parse_number(row[2].value),
        "market_value": parse_number(row[3].value) * 100000,  # Lakhs to INR
        "nav_percentage": parse_number(row[4].value),
        "sector": clean_text(row[5].value),
    }
    holdings.append(holding)
```

### 6.6 Special Rules

- **Market Value Conversion**: HDFC reports in Lakhs, multiply by 100,000
- **Scheme Name Extraction**: Use sheet name as scheme_name
- **Report Date**: Extract from filename or use last day of month
- **Footer Detection**: Stop at "Total", "Net Assets", or 3 consecutive empty rows

### 6.7 Output Example

```json
[
    {
        "scheme_name": "HDFC Equity Fund - Direct Plan - Growth",
        "amc_name": "HDFC",
        "isin": "INE002A01018",
        "security_name": "Reliance Industries Limited",
        "instrument_type": "Equity",
        "sector": "Energy",
        "rating": null,
        "quantity": 1250000,
        "market_value": 312500000.00,
        "nav_percentage": 8.45,
        "report_date": "2025-12-31"
    }
]
```

---

## 7. SBI AMC - Full Walkthrough

### 7.1 File Structure

- **Merged File**: `SBI_2025-12_merged.xlsx`
- **Sheets**: One per scheme, often with longer names
- **Layout**: Multi-row header with merged cells, followed by holdings

### 7.2 Differences from HDFC

1. **Multi-Row Headers**: SBI uses 2-3 rows for headers with merged cells
2. **Market Value Units**: Reported in Crores (not Lakhs)
3. **Instrument Type Column**: Explicitly present (HDFC infers from sector)
4. **Rating Column**: Present for debt schemes

### 7.3 Sheet Identification

```python
def is_valid_scheme_sheet(sheet_name: str) -> bool:
    skip_keywords = ["summary", "index", "nav", "disclaimer"]
    if any(kw in sheet_name.lower() for kw in skip_keywords):
        return False
    return "sbi" in sheet_name.lower() or len(sheet_name) > 10
```

### 7.4 Header Detection (Multi-Row)

```python
def detect_header_multirow(sheet, max_scan=15):
    for start_row in range(1, max_scan):
        # Check if next 2-3 rows form a complete header
        combined_headers = []
        for col_idx in range(sheet.max_column):
            cell_values = []
            for offset in range(3):  # Check 3 rows
                cell = sheet.cell(start_row + offset, col_idx + 1)
                if cell.value:
                    cell_values.append(str(cell.value).strip())
            combined_headers.append(" ".join(cell_values))
        
        normalized = [normalize_header(h) for h in combined_headers]
        if len(set(normalized) & REQUIRED_HEADERS) >= 4:
            return start_row + 2  # Data starts 2 rows after header start
    return None
```

### 7.5 Column Mapping

SBI standard columns:

| Excel Column | Header | Canonical Field |
|--------------|--------|-----------------|
| A | Name of the Issuer | security_name |
| B | ISIN Code | isin |
| C | Type of Instrument | instrument_type |
| D | Rating | rating |
| E | Quantity / No. of Shares | quantity |
| F | Market Value (Rs. Crores) | market_value |
| G | % to Net Assets | nav_percentage |
| H | Sector | sector |

### 7.6 Row Parsing

```python
for row in sheet.iter_rows(min_row=data_start_row, max_row=sheet.max_row):
    security_name = row[0].value
    if not security_name or "total" in str(security_name).lower():
        continue
    
    holding = {
        "security_name": clean_text(security_name),
        "isin": clean_text(row[1].value),
        "instrument_type": clean_text(row[2].value),
        "rating": clean_text(row[3].value),
        "quantity": parse_number(row[4].value),
        "market_value": parse_number(row[5].value) * 10000000,  # Crores to INR
        "nav_percentage": parse_number(row[6].value),
        "sector": clean_text(row[7].value),
    }
    holdings.append(holding)
```

### 7.7 Special Rules

- **Market Value Conversion**: SBI reports in Crores, multiply by 10,000,000
- **Merged Cell Handling**: Use `openpyxl`'s `merged_cells` to unmerge before parsing
- **Instrument Type Mapping**: Map SBI types to canonical types:
  - "Equity Shares" → "Equity"
  - "Debentures" → "Debt"
  - "Government Securities" → "Debt"
  - "TREPS" → "Cash Equivalent"
- **Rating Normalization**: Standardize rating formats (e.g., "AAA (SO)" → "AAA")

### 7.8 Output Example

```json
[
    {
        "scheme_name": "SBI Bluechip Fund - Direct Plan - Growth",
        "amc_name": "SBI",
        "isin": "INE002A01018",
        "security_name": "Reliance Industries Limited",
        "instrument_type": "Equity",
        "sector": "Energy",
        "rating": null,
        "quantity": 2500000,
        "market_value": 625000000.00,
        "nav_percentage": 7.32,
        "report_date": "2025-12-31"
    }
]
```

---

## 8. Loader Layer

### 8.1 Responsibilities

1. Read canonical JSON from `extracted/{AMC}/{AMC}_{YYYY-MM}_extracted.json`
2. Validate data against schema
3. Insert into database using batch operations
4. Handle duplicates using upsert logic
5. Ensure transactional integrity (all-or-nothing per AMC-month)
6. Log insertion statistics

### 8.2 Input Format

Loader expects an array of holdings in canonical format:

```json
[
    {"scheme_name": "...", "amc_name": "...", ...},
    {"scheme_name": "...", "amc_name": "...", ...}
]
```

### 8.3 Validation

Before insertion, validate each holding:

```python
def validate_holding(holding: dict) -> tuple[bool, str]:
    required_fields = ["scheme_name", "amc_name", "security_name", "market_value", "nav_percentage", "report_date"]
    for field in required_fields:
        if field not in holding or holding[field] is None:
            return False, f"Missing required field: {field}"
    
    if holding.get("isin") and not re.match(r"^IN[A-Z0-9]{10}$", holding["isin"]):
        return False, f"Invalid ISIN: {holding['isin']}"
    
    if holding["market_value"] <= 0:
        return False, "Market value must be positive"
    
    if not (0 < holding["nav_percentage"] <= 100):
        return False, "NAV percentage must be between 0 and 100"
    
    return True, ""
```

### 8.4 Batch Insert Logic

```python
def load_holdings(holdings: list[dict], batch_size: int = 1000):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for i in range(0, len(holdings), batch_size):
            batch = holdings[i:i+batch_size]
            cursor.executemany(INSERT_QUERY, batch)
        
        conn.commit()
        logger.info(f"Inserted {len(holdings)} holdings")
    except Exception as e:
        conn.rollback()
        logger.error(f"Insertion failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
```

### 8.5 Duplicate Handling (Upsert)

Primary key: `(amc_name, scheme_name, isin, report_date)`

```sql
INSERT INTO portfolio_holdings (
    scheme_name, amc_name, isin, security_name, instrument_type,
    sector, rating, quantity, market_value, nav_percentage, report_date
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (amc_name, scheme_name, isin, report_date)
DO UPDATE SET
    security_name = EXCLUDED.security_name,
    instrument_type = EXCLUDED.instrument_type,
    sector = EXCLUDED.sector,
    rating = EXCLUDED.rating,
    quantity = EXCLUDED.quantity,
    market_value = EXCLUDED.market_value,
    nav_percentage = EXCLUDED.nav_percentage,
    updated_at = CURRENT_TIMESTAMP;
```

### 8.6 Transaction Safety

- **Atomic per AMC-month**: All holdings for an AMC-month are inserted in a single transaction
- **Rollback on error**: If any holding fails validation or insertion, entire transaction is rolled back
- **Idempotency**: Re-running loader for same AMC-month updates existing records (upsert)

### 8.7 Rollback Rules

- **Validation failure**: Rollback, log error, do not proceed
- **Database constraint violation**: Rollback, log error, do not proceed
- **Partial insertion failure**: Rollback, log error, do not proceed

---

## 9. Database Layer

### 9.1 Database as System of Record

The database is the **single source of truth** for portfolio data. All downstream consumers (APIs, analytics, reports) query the database, never the Excel files.

### 9.2 Schema

```sql
CREATE TABLE portfolio_holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scheme_name TEXT NOT NULL,
    amc_name TEXT NOT NULL,
    isin TEXT,
    security_name TEXT NOT NULL,
    instrument_type TEXT NOT NULL,
    sector TEXT,
    rating TEXT,
    quantity REAL,
    market_value REAL NOT NULL CHECK (market_value > 0),
    nav_percentage REAL NOT NULL CHECK (nav_percentage > 0 AND nav_percentage <= 100),
    report_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (amc_name, scheme_name, isin, report_date)
);

CREATE INDEX idx_amc_date ON portfolio_holdings (amc_name, report_date);
CREATE INDEX idx_scheme_date ON portfolio_holdings (scheme_name, report_date);
CREATE INDEX idx_isin ON portfolio_holdings (isin);
CREATE INDEX idx_report_date ON portfolio_holdings (report_date);
```

### 9.3 Constraints

- **Primary Key**: Auto-incrementing `id`
- **Unique Constraint**: `(amc_name, scheme_name, isin, report_date)` prevents duplicate holdings
- **Check Constraints**:
  - `market_value > 0`
  - `0 < nav_percentage <= 100`
- **NOT NULL**: `scheme_name`, `amc_name`, `security_name`, `instrument_type`, `market_value`, `nav_percentage`, `report_date`

### 9.4 Why Loader Never Trusts Extractor Blindly

- **Validation Layer**: Loader re-validates all data before insertion
- **Constraint Enforcement**: Database enforces constraints (extractor may have bugs)
- **Audit Trail**: `created_at` and `updated_at` track data lineage
- **Idempotency**: Upsert logic ensures re-runs don't create duplicates

---

## 10. Orchestration & Automation

### 10.1 Run Order

```
1. Downloader (per AMC, per month)
   ↓
2. Merger (per AMC, per month)
   ↓
3. Extractor (per AMC, per month)
   ↓
4. Loader (per AMC, per month)
   ↓
5. Cache Refresh (API layer)
```

### 10.2 Scheduler

- **Frequency**: Daily at 2:00 AM IST
- **Logic**:
  1. Check if current month's data is published (day > 10)
  2. For each AMC, check if data already downloaded
  3. If not, trigger Downloader → Merger → Extractor → Loader
  4. Send Telegram notification on success/failure

### 10.3 Failure Alerts

- **Downloader failure**: Telegram alert with AMC name, month, error message
- **Merger failure**: Telegram alert with AMC name, month, file count
- **Extractor failure**: Telegram alert with AMC name, month, schemes extracted
- **Loader failure**: Telegram alert with AMC name, month, holdings count

### 10.4 Idempotency

Re-running the pipeline for the same AMC-month should:
- **Downloader**: Skip download if files already exist (check success marker)
- **Merger**: Overwrite merged file (deterministic output)
- **Extractor**: Overwrite extracted JSON (deterministic output)
- **Loader**: Upsert holdings (update existing records, no duplicates)

**Result**: No duplicate data, safe to re-run.

---

## 11. Adding a New AMC (Implementation Checklist)

### Step 1: Identify Structure

- [ ] Download 2-3 months of sample files for the AMC
- [ ] Inspect Excel structure:
  - Number of sheets per file
  - Header row location
  - Column order and names
  - Footer/total row patterns
  - Merged cells
  - Units for market value (Lakhs/Crores/INR)

### Step 2: Inspect Headers

- [ ] List all unique column headers across samples
- [ ] Map headers to canonical fields:
  - `security_name`
  - `isin`
  - `instrument_type`
  - `sector`
  - `rating`
  - `quantity`
  - `market_value`
  - `nav_percentage`
- [ ] Identify missing fields (mark as `None`)

### Step 3: Define Column Map

- [ ] Create `{AMC}_COLUMN_MAP` dictionary
- [ ] Define header normalization rules
- [ ] Define unit conversion rules (Lakhs/Crores → INR)

### Step 4: Implement Scheme Parser

- [ ] Create `{AMC}Extractor` class inheriting from `BaseExtractor`
- [ ] Implement `is_valid_scheme_sheet(sheet_name)` method
- [ ] Implement `detect_header_row(sheet)` method
- [ ] Implement `parse_scheme(sheet, scheme_name)` method
- [ ] Handle merged cells, multi-row headers, footer detection

### Step 5: Validate Output

- [ ] Run extractor on sample merged file
- [ ] Verify JSON output matches canonical schema
- [ ] Check field types, required fields, value ranges
- [ ] Validate ISIN format, market value > 0, nav_percentage in (0, 100]

### Step 6: Test in Isolation

- [ ] Create unit tests for extractor
- [ ] Test with edge cases:
  - Empty schemes
  - Missing headers
  - Invalid ISINs
  - Negative market values
  - Merged cells
- [ ] Verify error handling (scheme failures don't stop AMC processing)

### Step 7: Run Full Pipeline

- [ ] Trigger Downloader for AMC (1 month)
- [ ] Run Merger
- [ ] Run Extractor
- [ ] Run Loader
- [ ] Query database to verify data
- [ ] Check logs for errors/warnings

### Step 8: Backfill Historical Data

- [ ] Run pipeline for last 6-12 months
- [ ] Monitor for failures
- [ ] Fix extractor bugs, re-run failed months

### Step 9: Integrate into Scheduler

- [ ] Add AMC to scheduler configuration
- [ ] Test scheduled run
- [ ] Verify Telegram notifications

---

## 12. Error Handling Philosophy

### 12.1 Fail Scheme, Not AMC

If a single scheme fails extraction (e.g., header not found), log the error and continue processing remaining schemes. Do not stop the entire AMC extraction.

**Example**:
```
[ERROR] Scheme "XYZ Debt Fund": Header not found
[INFO] Continuing with remaining schemes
[INFO] Successfully extracted 42/45 schemes for AMC XYZ
```

### 12.2 Fail File, Not System

If a single AMC-month file is corrupted or missing, log the error and continue processing other AMCs. Do not stop the entire pipeline.

**Example**:
```
[ERROR] AMC ABC, Month 2025-12: Merged file not found
[INFO] Continuing with remaining AMCs
[INFO] Successfully processed 18/20 AMCs
```

### 12.3 Logging Expectations

- **INFO**: Normal operations (file loaded, scheme extracted, data inserted)
- **WARNING**: Non-critical issues (scheme skipped, field missing, default value used)
- **ERROR**: Critical issues (file not found, parsing failed, database error)
- **DEBUG**: Detailed diagnostics (header row detected, table bounds, column mapping)

### 12.4 Graceful Degradation

- **Extractor**: If optional field missing, set to `None` and continue
- **Loader**: If validation fails for one holding, log error and skip that holding (don't rollback entire batch)
- **API**: If database query fails, return cached data with staleness warning

---

## 13. Non-Negotiable Rules

### Rule 1: Extractor Never Edits Files
The extractor is **read-only**. It must never modify source Excel files, merged files, or downloaded files. All transformations happen in-memory and output to JSON.

### Rule 2: Loader Never Parses Excel
The loader only accepts canonical JSON. It must never read Excel files directly. Excel parsing is the extractor's responsibility.

### Rule 3: Database Never Stores Raw Excel
The database stores structured, validated data only. No binary Excel files, no raw cell values, no unvalidated data.

### Rule 4: API Never Accesses Filesystem
The API queries the database only. It must never read Excel files, JSON files, or any filesystem artifacts.

### Rule 5: Frontend Never Accesses Database
The frontend communicates with the backend API only. It must never connect to the database directly.

### Rule 6: Downloader Never Renames Files
The downloader preserves original filenames from AMC websites. Renaming happens in the merger (if needed) or extractor (via metadata).

### Rule 7: Components Are Decoupled
Each component has a clear input/output contract. Changes to one component must not require changes to others (except contract changes).

### Rule 8: Idempotency Is Mandatory
Re-running any component for the same input must produce the same output without side effects (e.g., duplicates, data corruption).

### Rule 9: Fail Fast on File Errors, Fail Gracefully on Data Errors
- **File errors** (missing, corrupted): Raise exception, stop processing that AMC-month
- **Data errors** (invalid ISIN, missing field): Log error, skip that row/scheme, continue processing

### Rule 10: Canonical Schema Is Immutable
The canonical schema is the contract between extractor and loader. Changes require versioning and migration plan.

---

## 14. Canonical Instrument Types

All AMC-specific instrument types must be mapped to one of the following canonical types:

- `Equity`
- `Debt`
- `Cash Equivalent`
- `Government Securities`
- `Mutual Fund Units`
- `Derivatives`
- `Other`

**Mapping Examples**:
- "Equity Shares" → `Equity`
- "Debentures" → `Debt`
- "Commercial Paper" → `Debt`
- "Treasury Bills" → `Government Securities`
- "TREPS" → `Cash Equivalent`
- "Futures" → `Derivatives`

---

## 15. Data Validation Rules

### 15.1 ISIN Validation
- Format: `^IN[A-Z0-9]{10}$`
- Example: `INE002A01018`
- If invalid or missing, set to `None` (allowed for certain instruments like government securities)

### 15.2 Market Value Validation
- Must be positive (`> 0`)
- Stored in INR (convert from Lakhs/Crores)
- Type: `float`

### 15.3 NAV Percentage Validation
- Must be in range `(0, 100]`
- Sum of all holdings in a scheme should be ≤ 100% (allow slight variance for rounding)
- Type: `float`

### 15.4 Report Date Validation
- Must be last day of month
- Format: `YYYY-MM-DD`
- Must not be in the future

### 15.5 Scheme Name Validation
- Must be non-empty
- Should include plan type (Direct/Regular) and option (Growth/Dividend)
- Example: `HDFC Equity Fund - Direct Plan - Growth`

---

## 16. Performance Considerations

### 16.1 Extractor Performance
- Use `openpyxl` in read-only mode: `load_workbook(filename, read_only=True, data_only=True)`
- Process sheets in parallel (if thread-safe)
- Limit row scanning to first 1000 rows for header detection

### 16.2 Loader Performance
- Use batch inserts (1000 holdings per batch)
- Use prepared statements to prevent SQL injection
- Create indexes on frequently queried columns

### 16.3 Database Performance
- Index on `(amc_name, report_date)` for AMC-wise queries
- Index on `(scheme_name, report_date)` for scheme-wise queries
- Index on `isin` for security-wise queries
- Partition table by `report_date` (if database supports partitioning)

---

## 17. Security Considerations

### 17.1 SQL Injection Prevention
- Use parameterized queries (never string concatenation)
- Validate all inputs before database operations

### 17.2 File Path Validation
- Validate file paths to prevent directory traversal attacks
- Use absolute paths, never user-provided relative paths

### 17.3 API Authentication
- Implement JWT-based authentication
- Rate limiting on API endpoints
- HTTPS only in production

---

## 18. Monitoring & Observability

### 18.1 Metrics to Track
- Holdings extracted per AMC per month
- Extraction success rate (schemes extracted / total schemes)
- Loader success rate (holdings inserted / holdings extracted)
- Pipeline execution time per AMC
- Database size growth

### 18.2 Alerts
- Downloader failure (Telegram)
- Extractor failure (Telegram)
- Loader failure (Telegram)
- Database disk space < 10% (Email)
- API response time > 2s (Email)

### 18.3 Logs Retention
- Keep logs for 90 days
- Archive to S3/cloud storage after 30 days

---

## 19. Testing Strategy

### 19.1 Unit Tests
- Test each extractor in isolation with sample files
- Test loader with sample JSON
- Test validation functions

### 19.2 Integration Tests
- Test full pipeline (Downloader → Merger → Extractor → Loader)
- Test with real AMC files (anonymized)

### 19.3 Regression Tests
- Maintain golden dataset (expected output for known inputs)
- Run regression tests on every extractor change

---

## 20. Deployment

### 20.1 Environment Variables
```
DB_PATH=/path/to/database.db
DATA_DIR=/path/to/data
TELEGRAM_BOT_TOKEN=xxx
TELEGRAM_CHAT_ID=xxx
LOG_LEVEL=INFO
```

### 20.2 Dependencies
```
openpyxl>=3.1.0
pandas>=2.0.0
python-telegram-bot>=20.0
schedule>=1.1.0
```

### 20.3 Deployment Checklist
- [ ] Set environment variables
- [ ] Create data directories
- [ ] Initialize database schema
- [ ] Test Telegram notifications
- [ ] Run backfill for 1 AMC
- [ ] Verify database contents
- [ ] Enable scheduler
- [ ] Monitor logs for 24 hours

---

**END OF SPECIFICATION**
