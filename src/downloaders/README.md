# Downloaders Module

## Overview

This module contains downloaders for fetching monthly portfolio files from AMC websites.

**Scope**: Download files ONLY - no parsing, validation, or database operations.

---

## What Downloaders Do

✅ **Download** monthly portfolio Excel files from AMC websites  
✅ **Save** files to canonical raw data folder (`data/raw/{amc}/{YYYY_MM}/`)  
✅ **Return** metadata about the download (success/failure)  
✅ **Log** clearly with human-readable messages  

---

## What Downloaders Do NOT Do

❌ **Parse** Excel files  
❌ **Read** sheets or extract data  
❌ **Validate** data against Canonical Data Contract  
❌ **Normalize** data  
❌ **Insert** data into database  
❌ **Call** extractors or loaders  

---

## Folder Structure

```
src/downloaders/
├── __init__.py
├── base_downloader.py      # Abstract base class
├── hdfc_downloader.py       # HDFC Mutual Fund downloader
└── README.md                # This file

data/raw/
└── hdfc/
    └── 2025_01/
        └── hdfc_portfolio.xlsx
```

---

## Base Downloader

**`base_downloader.py`**: Abstract base class for all AMC downloaders.

All downloaders must implement:
- `download(year, month)` - Download file and return metadata

Provides shared utilities:
- `ensure_directory(path)` - Create directory if missing
- `log_start()`, `log_success()`, `log_failure()` - Clean logging
- `get_target_folder()`, `get_target_file_path()` - Path helpers

---

## HDFC Downloader

**`hdfc_downloader.py`**: HDFC Mutual Fund downloader using official API.

### Implementation

**Method**: Official HDFC API (no browser automation)

**API Endpoint**: `https://cms.hdfcfund.com/en/hdfc/api/v2/disclosures/monthforportfolio`

**Features**:
- Direct API calls (no Playwright)
- Financial year conversion (Jan-Mar → FY-1, Apr-Dec → FY)
- Multiple file downloads per period
- Preserves original filenames
- Robust error handling

### Usage

**Recommended** (via CLI wrapper):
```bash
python -m src.cli.run_hdfc_downloader --year 2025 --month 1
```

**Alternative** (direct module execution):
```bash
python -m src.downloaders.hdfc_downloader --year 2025 --month 1
```

**Note**: The CLI wrapper is recommended to avoid import warnings.

### Output

**On Success**:
```json
{
  "amc": "HDFC Mutual Fund",
  "year": 2025,
  "month": 1,
  "files_downloaded": 3,
  "files": ["data/raw/hdfc/2025_01/file1.xlsx", "..."],
  "status": "success"
}
```

**On Failure**:
```json
{
  "amc": "HDFC Mutual Fund",
  "year": 2025,
  "month": 1,
  "status": "failed",
  "reason": "API returned status 404"
}
```

### Logs

```
[INFO] ============================================================
[INFO] HDFC MUTUAL FUND DOWNLOADER STARTED
[INFO] Period: January 2025
[INFO] Target folder: data/raw/hdfc/2025_01
[INFO] ============================================================
[INFO] Financial year: 2024-2025
[INFO] Calling HDFC API
[INFO] Found 3 file(s)
[INFO] Downloading file 1/3: portfolio_jan_2025.xlsx
[SUCCESS] Saved: data/raw/hdfc/2025_01/portfolio_jan_2025.xlsx
[INFO] Downloading file 2/3: holdings_jan_2025.xlsx
[SUCCESS] Saved: data/raw/hdfc/2025_01/holdings_jan_2025.xlsx
[INFO] Downloading file 3/3: summary_jan_2025.xlsx
[SUCCESS] Saved: data/raw/hdfc/2025_01/summary_jan_2025.xlsx
[INFO] ============================================================
[SUCCESS] ✅ HDFC Mutual Fund download completed
[SUCCESS] Downloaded 3 file(s)
[INFO] ============================================================
```

---

## Integration with Extractor

Downloaders are **separate** from extractors:

1. **Downloader** downloads raw file → `data/raw/hdfc/2025_01/hdfc_portfolio.xlsx`
2. **Extractor** (future) reads file → parses → validates → normalizes
3. **Loader** (existing) inserts to database

**Workflow**:
```
Download → Extract → Validate → Normalize → Load
```

---

## Adding New AMC Downloaders

To add a new AMC downloader:

1. Create `{amc}_downloader.py` (e.g., `icici_downloader.py`)
2. Inherit from `BaseDownloader`
3. Implement `download(year, month)` method
4. Use Playwright to navigate and download
5. Save to `data/raw/{amc}/{YYYY_MM}/`
6. Return metadata dictionary

---

## Dependencies

- `requests` - HTTP client for API calls
- `src.config.logger` - Logging

**Note**: HDFC downloader uses official API (no browser automation required).

---

## Design Principles

1. **Single Responsibility**: Download files only
2. **No Coupling**: Independent of extractors/loaders
3. **Clean Separation**: Raw data → Processed data
4. **Idempotent**: Can re-run safely
5. **Clear Logging**: Human-readable progress

---

**Last Updated**: 2026-02-01  
**Status**: HDFC downloader implemented (skeleton)
