---
trigger: always_on
---

4. Extractor Layer (Contract-Based System)
4.1 Extractor Philosophy
The extractor is a contract-based orchestration engine that transforms heterogeneous Excel structures into a canonical data model. It operates on the principle of scheme isolation: failure in one scheme must not stop processing of other schemes.

4.2 Base Extractor Responsibilities
File Reading: Load merged Excel file using openpyxl or pandas
Scheme Isolation: Iterate through sheets, treating each as a potential scheme
Sheet Detection: Identify which sheets contain portfolio data vs metadata/summary sheets
Header Normalization: Standardize column headers (e.g., "Security Name" → "security_name")
Column Mapping: Map AMC-specific columns to canonical fields
Table Detection: Locate the start/end of portfolio tables within sheets
Row Parsing: Extract data rows, handle merged cells, skip subtotals/footers
Data Validation: Validate ISINs, numeric fields, dates
Error Handling: Log errors per scheme, continue processing remaining schemes
Output Generation: Produce canonical JSON array
4.3 Sheet Detection Logic
For each sheet in merged Excel:
    If sheet_name matches scheme pattern (e.g., contains "SCHEME", "FUND"):
        Attempt extraction
    Else if sheet_name in ["Summary", "Index", "Contents"]:
        Skip
    Else:
        Log warning, attempt extraction
4.4 Header Normalization
Headers are normalized using fuzzy matching and synonym dictionaries:

python
HEADER_SYNONYMS = {
    "security_name": ["Security Name", "Name of Security", "Issuer", "Company Name"],
    "isin": ["ISIN", "ISIN Code", "ISIN No"],
    "instrument_type": ["Instrument", "Type", "Asset Class", "Security Type"],
    "market_value": ["Market Value", "Value", "Mkt Value (Rs)", "Amount"],
    "nav_percentage": ["% to NAV", "% of NAV", "Percentage to NAV", "NAV %"],
    ...
}
4.5 Column Mapping
Each AMC extractor defines a column map:

python
COLUMN_MAP = {
    "A": "security_name",
    "B": "isin",
    "C": "instrument_type",
    "D": "quantity",
    "E": "market_value",
    "F": "nav_percentage",
    ...
}
4.6 Table Detection
Tables are detected by:

Scanning for header row (contains ≥3 canonical field names)
Marking table start as row immediately after header
Detecting table end by:
Empty rows (3+ consecutive)
Footer keywords ("Total", "Grand Total", "Net Assets")
Sheet end
4.7 Row Parsing Rules
Skip rows where all cells are empty
Skip rows containing footer keywords
Parse rows where ISIN or security_name is present
Handle merged cells by propagating value downward
Numeric parsing: Strip currency symbols, commas; convert to float
Date parsing: Detect DD-MM-YYYY, MM/DD/YYYY, YYYY-MM-DD formats
4.8 Error Handling
Scheme-Level Errors (Non-Fatal)
Header not found → Log warning, skip scheme
Table structure invalid → Log error, skip scheme
Parsing error in row → Log error, skip row, continue scheme
File-Level Errors (Fatal)
File not found → Raise exception, stop AMC processing
File corrupted → Raise exception, stop AMC processing
No schemes extracted → Raise exception, stop AMC processing
4.9 Logging Standards
[INFO] Processing AMC: HDFC, Month: 2025-12
[INFO] Found 45 sheets in merged file
[INFO] Processing sheet: HDFC Equity Fund
[DEBUG] Header detected at row 5
[DEBUG] Table spans rows 6-234
[INFO] Extracted 228 holdings from HDFC Equity Fund
[WARNING] Sheet "Summary" skipped (metadata sheet)
[ERROR] Sheet "HDFC Debt Fund": Header not found
[INFO] Successfully extracted 42/45 schemes
5. Canonical Data Model
5.1 Output Schema
Each extracted holding is represented as:

json
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
5.2 Field Definitions
Field	Type	Required	Description	Validation
scheme_name	string	Yes	Full scheme name including plan type	Non-empty
amc_name	string	Yes	AMC identifier (uppercase)	Must match known AMC list
isin	string	Conditional	12-character ISIN code	Regex: ^IN[A-Z0-9]{10}$ (if present)
security_name	string	Yes	Name of security/issuer	Non-empty
instrument_type	string	Yes	Asset class (Equity, Debt, etc.)	Must match canonical types
sector	string	No	Sector classification	Free text
rating	string	No	Credit rating (for debt)	Free text
quantity	float	No	Number of units held	≥ 0
market_value	float	Yes	Market value in INR	> 0
nav_percentage	float	Yes	Percentage of NAV	0 < x ≤ 100
report_date	date	Yes	Portfolio date (last day of month)	Valid date, format: YYYY-MM-DD
5.3 Why Canonical Schema?
Database Consistency: Single table schema across all AMCs
API Simplicity: Uniform response structure
Query Efficiency: Standardized field names enable indexed queries
Future-Proofing: New AMCs integrate without schema changes
6. HDFC AMC - Full Walkthrough
6.1 File Structure
Merged File: HDFC_2025-12_merged.xlsx
Sheets: One per scheme (e.g., "HDFC Equity Fund - Direct Plan - Growth")
Layout: Header row followed by holdings table
6.2 Sheet Identification
python
def is_valid_scheme_sheet(sheet_name: str) -> bool:
    skip_keywords = ["summary", "index", "contents", "disclaimer"]
    if any(kw in sheet_name.lower() for kw in skip_keywords):
        return False
    return "hdfc" in sheet_name.lower() or "fund" in sheet_name.lower()
6.3 Header Detection
HDFC headers typically appear in row 4-6. Detection logic:

python
for row_idx in range(1, 10):
    row_values = [cell.value for cell in sheet[row_idx]]
    normalized = [normalize_header(v) for v in row_values if v]
    if len(set(normalized) & REQUIRED_HEADERS) >= 3:
        header_row = row_idx
        break
6.4 Column Mapping
HDFC standard columns:

Excel Column	Header	Canonical Field
A	Name of the Instrument	security_name
B	ISIN	isin
C	Quantity	quantity
D	Market Value (Rs. In Lakhs)	market_value
E	% to NAV	nav_percentage
F	Sector	sector
6.5 Row Parsing
python
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
6.6 Special Rules
Market Value Conversion: HDFC reports in Lakhs, multiply by 100,000
Scheme Name Extraction: Use sheet name as scheme_name
Report Date: Extract from filename or use last day of month
Footer Detection: Stop at "Total", "Net Assets", or 3 consecutive empty rows
6.7 Output Example
json
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
7. SBI AMC - Full Walkthrough
7.1 File Structure
Merged File: SBI_2025-12_merged.xlsx
Sheets: One per scheme, often with longer names
Layout: Multi-row header with merged cells, followed by holdings
7.2 Differences from HDFC
Multi-Row Headers: SBI uses 2-3 rows for headers with merged cells
Market Value Units: Reported in Crores (not Lakhs)
Instrument Type Column: Explicitly present (HDFC infers from sector)
Rating Column: Present for debt schemes
7.3 Sheet Identification
python
def is_valid_scheme_sheet(sheet_name: str) -> bool:
    skip_keywords = ["summary", "index", "nav", "disclaimer"]
    if any(kw in sheet_name.lower() for kw in skip_keywords):
        return False
    return "sbi" in sheet_name.lower() or len(sheet_name) > 10
7.4 Header Detection (Multi-Row)
python
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
7.5 Column Mapping
SBI standard columns:

Excel Column	Header	Canonical Field
A	Name of the Issuer	security_name
B	ISIN Code	isin
C	Type of Instrument	instrument_type
D	Rating	rating
E	Quantity / No. of Shares	quantity
F	Market Value (Rs. Crores)	market_value
G	% to Net Assets	nav_percentage
H	Sector	sector
7.6 Row Parsing
python
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
7.7 Special Rules
Market Value Conversion: SBI reports in Crores, multiply by 10,000,000
Merged Cell Handling: Use openpyxl's merged_cells to unmerge before parsing
Instrument Type Mapping: Map SBI types to canonical types:
"Equity Shares" → "Equity"
"Debentures" → "Debt"
"Government Securities" → "Debt"
"TREPS" → "Cash Equivalent"
Rating Normalization: Standardize rating formats (e.g., "AAA (SO)" → "AAA")
7.8 Output Example
json
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
