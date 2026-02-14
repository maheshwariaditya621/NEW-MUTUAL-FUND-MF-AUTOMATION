# Extractor → Loader Phase Detailed Guide (Post-Merger Only)

## 1) Scope and Boundaries

This document covers **only** the pipeline stages after merged Excel files are already created:

- **Stage 3: Extractor**
- **Stage 4: Loader**

Not covered here:
- Downloader logic
- Merger internals

Prerequisite input format expected by extractor orchestration:
- `data/output/merged excels/{amc_slug}/{year}/CONSOLIDATED_{AMC_SLUG_UPPER}_{year}_{month:02d}.xlsx`

---

## 2) Primary Runtime Entry Points

### 2.1 CLI used for extraction + loading
- File: `src/extractors/cli.py`
- Command pattern:
  - `python -m src.extractors.cli --amc hdfc --year 2025 --month 12`
  - Optional flags:
    - `--dry-run` (extract only, no DB writes)
    - `--redo` (purge existing data for that AMC+period and re-run)

### 2.2 Orchestrator (actual control plane)
- File: `src/extractors/orchestrator.py`
- Main method:
  - `ExtractionOrchestrator.process_amc_month(amc_slug, year, month, redo=False, dry_run=False)`

### 2.3 Backfill automation script examples
- `src/scripts/run_stage2_backfill.py`
- `src/scripts/run_stage3_full_backfill.py`

These scripts call the same orchestrator method repeatedly over a month range.

---

## 3) End-to-End Control Flow (Exact Current Behavior)

The orchestrator performs the following sequence:

1. **Resolve merged file path**
   - Builds path using slug/year/month naming convention.
   - If file does not exist → returns failed (`file_not_found`).

2. **Period lock guard**
   - If not `dry_run` and not `redo`, checks `periods.period_status`.
   - If status is `FINAL` for year+month, processing is skipped.

3. **Extractor selection**
   - Uses `ExtractorFactory.get_extractor(amc_slug, year, month)`.
   - If no extractor registered for slug → failed (`no_extractor`).

4. **Idempotency via SHA-256 hash**
   - Computes hash of merged file.
   - If `redo=True`: purges previous holdings/snapshots/extraction_run for AMC+period.
   - Else checks `extraction_runs` for same hash with `SUCCESS`; if found, skips (`already_extracted`).

5. **Drift check (layout fingerprint)**
   - Reads columns from sample sheet and computes fingerprint via `DriftDetector`.
   - Stores first-seen fingerprint in `data/config/fingerprints/{amc}_{version}.json`.
   - On mismatch logs schema drift (does not itself block processing).

6. **Extractor execution**
   - Calls extractor’s `extract(file_path)` and gets list of holding dictionaries.
   - `rows_read = len(holdings)`.

7. **Dry run branch**
   - If `dry_run=True`, returns success with `rows_read` and exits before DB persistence.

8. **Transactional DB load**
   - Starts `TransactionContext`.
   - Upserts AMC and period.
   - Calls `PortfolioLoader.load_holdings(amc_id, period_id, holdings)`.

9. **Lineage logging**
   - Writes extraction metadata into `extraction_runs`:
     - file/hash/version/fingerprint
     - rows read/inserted
     - total extracted value
     - processing duration
     - git commit hash

10. **Reconciliation report**
    - Generates CSV in `reports/reconciliation_{amc}_{year}_{month}.csv` grouped by scheme.

11. **Automated DB backup**
    - Calls backup and prunes old backups (keep latest 6).

12. **Error handling**
    - Any exception returns failed status with error message.
    - DB transaction is rolled back automatically by context manager.

---

## 4) Extractor Selection Matrix (Registered AMCs)

`ExtractorFactory` currently maps these slugs:

- `hdfc` → `HDFCExtractorV1`
- `sbi` → `SBIExtractorV1`
- `icici`, `icici_pru` → `ICICIExtractorV1`
- `hsbc` → `HSBCExtractorV1`
- `kotak` → `KotakExtractorV1`
- `ppfas` → `PPFASExtractorV1`
- `axis` → `AxisExtractorV1`
- `bajaj` → `BajajExtractorV1`
- `absl` → `ABSLExtractorV1`
- `angelone` → `AngelOneExtractorV1`

If a slug is not in this mapping, extraction cannot start for that AMC.

---

## 5) Shared Extraction Rules from BaseExtractor

All AMC extractors rely partly or heavily on `src/extractors/base_extractor.py`.

### 5.1 Header detection strategy
- Searches first 25 rows for:
  - presence of `ISIN`
  - and at least one secondary descriptor keyword (`INSTRUMENT` / `ISSUER` / `COMPANY` / `NAME OF THE`)
- Returns header index, else `-1`.

### 5.2 ISIN cleaning
- Uppercases, trims, removes spaces and `*`, truncates to first 12 chars.

### 5.3 Equity Triple Filter (critical)
A row is considered equity only if:
1. ISIN length == 12
2. ISIN starts with `INE`
3. ISIN positions 9-10 (0-based `[8:10]`) == `10`

### 5.4 Multi-table termination while scanning
`filter_equity_isins()` stops once data has started and a termination marker appears:
- `GRAND TOTAL`
- `NET ASSETS`
- `% TO NAV 100`
- or row starts with `TOTAL` (excluding `SUB TOTAL`)

### 5.5 % parsing
`parse_percentage()` behavior:
- if absolute value is in `(0, 1]` ⇒ multiplied by 100
- else value kept as-is

### 5.6 Currency unit detection and normalization
- Unit detection via text (`RUPEES`, `LAKHS`, `CRORES`) in headers/metadata.
- Data-driven fallback:
  - `max > 1,00,00,000` ⇒ Rupees
  - `median < 10,000` ⇒ Lakhs
- Conversion:
  - Lakhs × 100,000
  - Crores × 10,000,000

### 5.7 Scheme parsing helper
`parse_verbose_scheme_name()` infers:
- `scheme_name`
- `description`
- `plan_type` (Direct/Regular)
- `option_type` (Growth/IDCW)
- `is_reinvest`

### 5.8 Post-extraction guard (`validate_nav_completeness`)
Checks per scheme set (called by extractor-specific code where implemented):
- Total NAV roughly within 90–105
- Holdings count sanity (20–200 preferred; low count warning)
- No duplicate ISINs
- Top holding concentration warning if >15% (conditional relax for ETF/index-like names)

---

## 6) AMC-Specific Extractor Logic (Current Implementations)

## 6.1 HDFC (`hdfc_extractor_v1.py`)
- Iterates each sheet as a scheme.
- Detects units from metadata/header; maps `MARKET VALUE`, `FAIR VALUE` etc.
- Cleans sheet suffixes like `HDFC...` code tails.
- Uses triple ISIN filter.
- Emits `percent_to_nav` field.

## 6.2 ICICI (`icici_extractor_v1.py`)
- Reads first rows for metadata (scheme/date), finds header, then full sheet.
- Exact-style mapping for ICICI columns.
- Converts market value from **Lakhs → INR**.
- Converts NAV decimal to percent (`*100`).
- Uses triple ISIN filter to remove section headers/debt.
- Emits `percent_to_nav` field.

## 6.3 SBI (`sbi_extractor_v1.py`)
- Robust header detection + multiple NAV column aliases.
- Attempts scheme name extraction from top cells (C3 etc.).
- Custom parser keeps unique sub-series parts in scheme name when relevant.
- Uses triple ISIN filter.
- Emits `percent_to_nav` field.

## 6.4 HSBC (`hsbc_extractor_v1.py`)
- Uses top rows for scheme metadata.
- Fuzzy header mapping (newline-safe).
- Market value treated as Lakhs → INR.
- NAV scaled from decimal to percent.
- Uses triple ISIN filter.
- Emits `percent_to_nav` field.

## 6.5 Kotak (`kotak_extractor_v1.py`)
- Handles merged header cells via forward-fill.
- Scheme name extracted from `Portfolio of ... as on ...` pattern.
- Market value in Lakhs → INR.
- Includes merged-column resolver utility.
- Uses triple ISIN filter.
- Emits `percent_to_nav` field.

## 6.6 PPFAS (`ppfas_extractor_v1.py`)
- Custom row loop (does not use base filter directly for full loop control due to subtotal structure).
- Handles Equity + Arbitrage blocks with subtotals in between.
- Stops at net/grand total.
- Aggregates duplicate ISIN rows by summing quantity/value/nav.
- Market value Lakhs → INR.
- Emits `percent_to_nav` field.

## 6.7 Axis (`axis_extractor_v1.py`)
- Skips metadata sheets (summary/index/disclaimer/contents).
- Scheme name extracted from top row cell fallback to cleaned sheet name.
- Defaults value unit to Lakhs when not explicitly found.
- Uses triple ISIN filter.
- Emits `percent_of_nav` key in final holding dictionary.

## 6.8 ABSL (`absl_extractor_v1.py`)
- Reads sheet headers/metadata and maps column variants.
- Defaults value unit to Lakhs when unit not explicit.
- Uses triple ISIN filter.
- Emits `percent_of_nav` key.

## 6.9 Bajaj (`bajaj_extractor_v1.py`)
- Tracks section headers (`Equity`, `Debt`, `Cash`, etc.) while scanning rows.
- Uses ISIN validity to keep only equity lines.
- Defaults value unit to Lakhs.
- Emits `percent_of_nav` key.

## 6.10 Angel One (`angelone_extractor_v1.py`)
- Section-aware extraction with conservative sheet skip rules.
- Stops/pauses equity capture around total/subtotal and non-equity sections.
- Uses ISIN validity + positive data check.
- Treats market value as Lakhs.
- Emits `percent_of_nav` key.

---

## 7) Loader Internals (PortfolioLoader)

Implemented in `src/loaders/portfolio_loader.py` and called by orchestrator.

### 7.1 Input grouping
- Groups extracted holdings by `(scheme_name, plan_type, option_type)`.

### 7.2 Scheme-level idempotency
For each grouped scheme:
- upsert scheme
- check `scheme_snapshots` for existing `(scheme_id, period_id)`
- if exists, skip that scheme

### 7.3 ISIN deduplication/merge logic
Within one scheme bucket:
- If same ISIN appears twice with same quantity + value: drops exact duplicate.
- If same ISIN appears with different values: merges by summing quantity, market value, and NAV percentage.

### 7.4 Company/ISIN master resolution
For each unique ISIN (cached across runs in process memory):
1. `get_isin_details(isin)`
2. choose canonical company name:
   - prefer ISIN master canonical name if present and not `N/A`
   - else use extractor company name
3. `upsert_isin_master(...)`
4. sector canonicalization via `get_canonical_sector(...)`
5. `upsert_company(...)` to get `company_id`

### 7.5 Snapshot + holdings write
- Creates one row in `scheme_snapshots`.
- Bulk inserts rows in `equity_holdings` with:
  - `company_id`
  - `quantity`
  - `market_value_inr`
  - `percent_of_nav`

### 7.6 %NAV soft guard
- Logs warning if sum of equity `percent_of_nav` is outside 95–105.
- Does not hard fail on this guard.

---

## 8) Database Touchpoints in Extractor→Loader Phase

Main tables touched in this phase:

- `amcs`
- `periods`
- `schemes`
- `isin_master`
- `companies`
- `scheme_snapshots`
- `equity_holdings`
- `extraction_runs`

Auxiliary lookup:
- `sector_master` (for canonical sector resolution)

---

## 9) Current Field Contract Used in Practice

### 9.1 Orchestrator/PortfolioLoader expected keys
`PortfolioLoader` currently reads holdings with key names including:
- `scheme_name`
- `plan_type`
- `option_type`
- `isin`
- `company_name`
- `quantity`
- `market_value_inr`
- **`percent_to_nav`** (important)

### 9.2 DB insert column naming
- At DB insert stage this becomes `percent_of_nav` in `equity_holdings`.

### 9.3 Practical compatibility note
Some extractors currently emit `percent_of_nav` instead of `percent_to_nav`.
Because loader merge logic accesses `percent_to_nav`, PM/reviewer should verify field compatibility per AMC during onboarding/testing before production runs.

---

## 10) Artifacts Produced by this Phase

After successful extractor→loader execution, these artifacts are produced:

1. **Database rows** in the listed tables.
2. **Run lineage** in `extraction_runs` with hash + metrics.
3. **Reconciliation CSV**:
   - `reports/reconciliation_{amc}_{year}_{month}.csv`
4. **DB backup file** in backup directory managed by backup utility.
5. **Header fingerprint files**:
   - `data/config/fingerprints/{amc}_{version}.json`

---

## 11) Operational Runbook (Mac-friendly)

From project root:

1. Dry run (safe validation):
   - `python -m src.extractors.cli --amc icici --year 2025 --month 12 --dry-run`

2. Normal run (writes to DB):
   - `python -m src.extractors.cli --amc icici --year 2025 --month 12`

3. Redo run (purge + reload month):
   - `python -m src.extractors.cli --amc icici --year 2025 --month 12 --redo`

4. Full scripted backfill example:
   - `python src/scripts/run_stage3_full_backfill.py`

Environment prerequisites for DB write runs:
- `.env` must include DB credentials (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`).

---

## 12) Troubleshooting Checklist (Extractor→Loader Only)

### A) `file_not_found`
- Check merged file exists at exact orchestrator path format.
- Validate slug (`--amc`) matches factory mapping.

### B) `no_extractor`
- Slug not registered in `ExtractorFactory`.

### C) `already_extracted` skip
- Same file hash already marked `SUCCESS` in `extraction_runs`.
- Use `--redo` to force reprocess.

### D) `period_locked` skip
- Period has status `FINAL` in `periods` table.

### E) Low/zero rows extracted
- Check header not found in sheet.
- Check ISIN triple filter eliminating all rows (common when column mapping fails).
- Check section stop keywords causing early break.

### F) Load warnings on `%NAV`
- Indicates equity-only extraction total is outside expected range.
- Common if scheme has significant debt/cash allocation excluded by equity filter.

### G) Schema drift messages
- Excel layout likely changed.
- Inspect fingerprint diff and column mapping for affected extractor.

---

## 13) Governance Notes for New Project Managers

1. This phase is designed to be **idempotent** and **transaction-safe**.
2. `--dry-run` should be used first for any new/changed AMC month.
3. Always confirm:
   - extracted row count is reasonable
   - key scheme-level NAV sums are sensible
   - reconciliation CSV exists and matches expectations
4. For production reruns, use `--redo` deliberately because it triggers purge for that AMC+period.
5. Keep an eye on extractor output key compatibility (`percent_to_nav` vs `percent_of_nav`) when onboarding AMCs.

---

## 14) Quick Reference: Key Files

- Orchestration: `src/extractors/orchestrator.py`
- CLI entry: `src/extractors/cli.py`
- Factory: `src/extractors/extractor_factory.py`
- Base rules: `src/extractors/base_extractor.py`
- AMC extractors: `src/extractors/*_extractor_v1.py`
- Loader: `src/loaders/portfolio_loader.py`
- DB repositories: `src/db/repositories.py`
- DB transactions: `src/db/transactions.py`
- Canonical constants: `src/config/constants.py`

---

## 15) Summary

The post-merger pipeline currently works as:

**Merged File → ExtractorFactory-chosen AMC extractor → equity-only filtered holdings → PortfolioLoader grouping + dedup + master upserts → scheme snapshot + holdings insert → extraction lineage + reconciliation + backup**.

This is the exact functional path currently implemented in code for extractor and loader phases.

---

## 16) Execution Checklist Reference

For implementation sequencing, AMC-by-AMC validation, and OS-neutral rollout tasks, use:

- `docs/EXTRACTOR_LOADER_EXECUTION_CHECKLIST.md`