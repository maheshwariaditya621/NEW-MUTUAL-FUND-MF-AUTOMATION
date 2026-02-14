# New AMC Extraction + Loading Playbook (End-to-End)

This document is the full onboarding runbook to add a **new AMC** into this pipeline and make it production-ready.

It covers everything required:
- download flow
- merge flow
- extractor flow (including equity filtering rules)
- orchestrator flow
- loading flow
- verification and troubleshooting

---

## 1) Pipeline Architecture (What happens in order)

1. **Downloader** fetches raw AMC files to:
   - `data/raw/<amc_slug>/<YYYY_MM>/`
   - writes `_SUCCESS.json` marker when completed
2. **Merger** consolidates downloaded files into one merged workbook:
   - `data/output/merged excels/<amc_slug>/<YYYY>/CONSOLIDATED_<AMC>_<YYYY>_<MM>.xlsx`
3. **Extractor** reads merged workbook and creates canonical equity holdings rows
4. **Orchestrator** handles idempotency, extractor selection, drift checks, dry-run/load mode
5. **Loader** writes holdings into DB with per-scheme snapshot and dedupe rules
6. **Verification** confirms counts, NAV sanity, and scheme-level quality

---

## 2) Source-of-Truth Files in This Repo

Use these files while implementing:

- Downloader base patterns: `src/downloaders/base_downloader.py`
- Merge logic: `src/utils/excel_merger.py`
- Core extraction utilities + equity filter: `src/extractors/base_extractor.py`
- Default reusable extractor: `src/extractors/common_extractor_v1.py`
- Extractor registry: `src/extractors/extractor_factory.py`
- Orchestration (idempotency/dry-run/load): `src/extractors/orchestrator.py`
- Loader logic (dedupe/merge/snapshots): `src/loaders/portfolio_loader.py`
- Existing downloader guide (download-focused): `NEW_AMC_IMPLEMENTATION_GUIDE.md`
- Canonical contract references: `docs/CANONICAL_DATA_CONTRACT.md`, `docs/CANONICAL_DATA_CONTRACT_v1.0.md`

---

## 3) Onboarding Decision Tree

Before coding, decide this:

### A) Downloader type
- **API downloader preferred** (stable endpoint, no captcha/dynamic flow)
- **Playwright downloader** only if API is unavailable/unreliable

### B) Extractor type
- Use **CommonExtractorV1** if AMC follows standard sheet/header structure
- Build **custom extractor** if AMC has special layout, units, or parsing behavior

### C) Run mode
- First run must be **dry-run only**
- Enable DB loading only after dry-run quality passes

---

## 4) New AMC Downloader Implementation

Create:
- `src/downloaders/<amc_slug>_downloader.py`
- optional scheduler/backfill:
  - `src/scheduler/<amc_slug>_scheduler.py`
  - `src/scheduler/<amc_slug>_backfill.py`

### Mandatory Downloader Rules

1. **Idempotency check**
   - If folder exists and `_SUCCESS.json` exists → skip
   - If folder exists but marker missing → move to `_corrupt`

2. **Publish-safe behavior**
   - If target month not published → return `not_published`, do not hard-fail pipeline

3. **Strict month routing**
   - only save files that match requested `(year, month)`

4. **Preserve source file names**
   - do not rewrite names unless unavoidable

5. **Create `_SUCCESS.json` on completion**
   - include AMC, year, month, file count, timestamp

6. **Retry strategy**
   - retry network/transient failures
   - no blind retries for hard 4xx logic errors

7. **Always trigger consolidation step after successful download**

---

## 5) Merge/Consolidation Logic (Critical)

Merge entry point:
- `consolidate_amc_downloads(amc_slug, year, month)` in `src/utils/excel_merger.py`

### What merge does

1. Reads raw folder: `data/raw/<amc>/<YYYY_MM>`
2. Preprocess:
   - extracts zip archives
   - converts `.xls` → `.xlsx` when needed
3. Merges all sheets from all files into one consolidated workbook
4. Writes merged output file in `data/output/merged excels/...`

### Important merge behavior in current code

- Excludes temporary files (`~$...`)
- Supports COM merge on Windows, openpyxl fallback otherwise
- Handles sheet name cleaning + uniqueness
- For `.xls` conversion fallback:
  - sanitizes sheet names
  - strips illegal control characters from cell strings
  - writes formula-like strings safely as text where needed

### Freshness logic

If consolidated file already exists and is newer than raw files, merge is skipped as up-to-date.

---

## 6) Extractor Logic: Mandatory Rules

All extractors must return canonical rows, one row per equity holding.

### 6.1 Header detection

From `BaseExtractor.find_header_row`:
- scans first 25 rows
- header valid only if row has:
  - `ISIN`
  - AND one of: `INSTRUMENT` / `ISSUER` / `COMPANY` / `NAME OF THE`

### 6.2 ISIN cleaning

From `BaseExtractor.clean_isin`:
- strip spaces and `*`
- uppercase
- truncate to first 12 chars

### 6.3 Equity filter (TRIPLE FILTER) — must use

From `BaseExtractor.filter_equity_isins`:

A row is treated as equity only if all are true:
1. ISIN starts with `INE`
2. ISIN length is `12`
3. ISIN positions 9-10 (index `[8:10]`) equal `10`

### 6.4 Multi-table stop logic — must use

Extraction stops when post-data table markers appear:
- row contains `GRAND TOTAL`
- row contains `NET ASSETS`
- row contains `% TO NAV 100`
- row starts with `TOTAL` (except `SUB TOTAL`)

This prevents leakage into second/third tables.

### 6.5 Units + currency normalization

From `BaseExtractor` and `CommonExtractorV1`:
- detect `RUPEES` / `LAKHS` / `CRORES`
- normalize all values to INR rupees (`market_value_inr`)

### 6.6 Scheme parsing

From `BaseExtractor.parse_verbose_scheme_name`:
- derive:
  - `scheme_name`
  - `scheme_description`
  - `plan_type` (Direct/Regular)
  - `option_type` (Growth/IDCW)
  - `is_reinvest`

### 6.7 Post-extraction validation guards

From `BaseExtractor.validate_nav_completeness`:

- NAV total guard (currently 90-105% in code)
- holdings count sanity (warn <20, fail >200 unless scheme-type exceptions)
- duplicate ISIN guard (fails on duplicates)
- top-weight guard (warn for non-ETF if >15%)

> Note: Function comments may mention 95-105 but implementation currently enforces 90-105 in extractor.

---

## 7) Canonical Output Schema (Extractor Output)

Each holding row should include:

- `amc_name`
- `scheme_name`
- `scheme_description`
- `plan_type`
- `option_type`
- `is_reinvest`
- `isin`
- `company_name`
- `quantity`
- `market_value_inr`
- `percent_to_nav`
- `sector`

---

## 8) Registering the New AMC Extractor

Update `src/extractors/extractor_factory.py`.

### If using common extractor

1. Add AMC name in `ADDITIONAL_AMC_NAMES` dictionary.
2. Ensure slug maps to:
   - `CommonExtractorV1(amc_slug=..., amc_name=...)`

### If using custom extractor

1. Add `src/extractors/<amc_slug>_extractor_v1.py`
2. Import class in factory
3. Add branch in `get_extractor(...)`

---

## 9) Orchestrator Behavior (Processing Controls)

`ExtractionOrchestrator.process_amc_month(...)` handles:

- merged file existence checks
- period lock checks (load mode)
- extractor selection from factory
- file-hash idempotency (`check_file_already_extracted`)
- optional redo purge
- dry-run mode (`rows_read`, no DB writes)
- load mode (uses `PortfolioLoader`)
- run metadata recording (`record_extraction_run`)

### Dry-run first policy

For new AMCs, run multiple months in dry-run until stable.

---

## 10) Loader Behavior (DB Persistence Rules)

`PortfolioLoader.load_holdings(...)` groups by scheme and loads atomically.

### Loader logic details

1. **Scheme upsert** with granularity:
   - scheme_name + plan_type + option_type + reinvest
2. **Snapshot idempotency**:
   - skip if snapshot already exists for scheme-period
3. **ISIN-level dedupe/merge**:
   - exact duplicate dropped
   - split duplicates merged (quantity/value/%NAV summed)
4. **Company resolution**:
   - ISIN master lookup + refresh
   - canonical sector mapping
   - company upsert for FK
5. **Snapshot + holdings insert**
6. **NAV guard warning** during load if equity exposure unusual

---

## 11) Execution Commands (Recommended Order)

### 11.1 Download + merge for one month

```bash
.venv/bin/python -c "from src.downloaders.<amc_slug>_downloader import <ClassName>; print(<ClassName>().download(2025,11))"
```

### 11.2 Dry-run extraction for one month

```bash
.venv/bin/python -c "from src.extractors.orchestrator import ExtractionOrchestrator; o=ExtractionOrchestrator(); print(o.process_amc_month('<amc_slug>',2025,11,dry_run=True,redo=True))"
```

### 11.3 Dry-run extraction for range (example script pattern)

Use a batch script similar to existing `src/scripts/run_batch3_download_merge_dryrun.py`.

### 11.4 Load mode (only after dry-run quality pass)

```bash
.venv/bin/python -c "from src.extractors.orchestrator import ExtractionOrchestrator; o=ExtractionOrchestrator(); print(o.process_amc_month('<amc_slug>',2025,11,dry_run=False,redo=False))"
```

---

## 12) New AMC Quality Gate Checklist (Must Pass)

A new AMC should be considered ready only after all pass:

1. Download idempotency works (`_SUCCESS.json` skip behavior)
2. Merge output generated in correct merged path
3. Merged workbook has expected sheet count
4. Extractor returns non-zero equity rows for published months
5. Triple equity filter produces valid equity ISINs only
6. No multi-table leakage beyond total markers
7. NAV guard mostly within expected range for schemes
8. No duplicate ISINs inside scheme output
9. Dry-run results stable across at least 2-3 months
10. Load mode inserts snapshots and holdings without integrity errors

---

## 13) Common Failure Scenarios + Fixes

### A) Download success but merged file has only 1 sheet
- Check `.xls -> .xlsx` conversion behavior in `excel_merger.py`
- Validate raw `.xls` vs converted `.xlsx` sheet counts
- Ensure fallback conversion handles illegal characters and safe sheet names

### B) Dry-run rows = 0 for one month only
- Compare month-specific source workbook format/header shifts
- Validate header row detection and column mapping
- Confirm sheet naming doesn’t break scheme extraction

### C) `ModuleNotFoundError: src` in scripts
- Add project-root bootstrap (`sys.path.insert`) in standalone scripts

### D) High NAV guard failures for all schemes
- verify `% to NAV` parsing and scale conversion
- check if source is decimal vs percentage (parse logic)

### E) Loader skips everything
- check existing snapshot idempotency and hash-based run checks
- use `redo=True` only when you intentionally want reprocessing

---

## 14) Minimal New AMC Implementation Recipe (Fast Track)

If AMC follows standard portfolio workbook structure:

1. Build downloader using existing downloader template standards
2. Confirm raw file and merged file correctness for 3 months
3. Register AMC in `ADDITIONAL_AMC_NAMES` in extractor factory
4. Use `CommonExtractorV1` (no custom extractor initially)
5. Run dry-run across at least 3 months
6. Create per-scheme summary CSV and validate counts
7. Enable load mode after quality sign-off

---

## 15) Recommended Deliverables for Every New AMC

Create and keep these artifacts:

1. Downloader source file
2. Backfill/scheduler files (if required)
3. Factory registration update
4. Dry-run verification CSV (month-wise)
5. Scheme-level summary CSV
6. Short issue log (anomalies + resolutions)

---

## 16) Final Notes

- Always prioritize **data correctness over speed**.
- Keep extraction logic deterministic and month-stable.
- Use dry-run output and scheme-level reconciliation as the primary approval gate before load.
- When in doubt, compare row-level output with source sheet manually for 1-2 schemes.

