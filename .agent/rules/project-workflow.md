---
trigger: always_on
---

# Extractor to DB Loading Instructions (Only This Journey)

This guide covers only the flow from **merged Excel extraction** to **database loading**.

Scope starts at:
- `data/output/merged excels/<amc>/<year>/CONSOLIDATED_<AMC>_<YYYY>_<MM>.xlsx`

Scope ends at:
- snapshots + holdings inserted in PostgreSQL via loader.

---

## 1) Entry Point: Orchestrator

Main runtime entry:
- `src/extractors/orchestrator.py`
- method: `ExtractionOrchestrator.process_amc_month(amc_slug, year, month, redo=False, dry_run=False)`

What orchestrator does in order:
1. Builds merged file path and validates file exists.
2. Checks period lock (in load mode).
3. Selects extractor from `ExtractorFactory`.
4. Computes file hash for idempotency.
5. Handles `redo` purge or skip-if-already-extracted checks.
6. Executes extraction.
7. In dry-run mode: returns count only.
8. In load mode: calls `PortfolioLoader.load_holdings(...)`.
9. Records extraction run metadata.

---

## 2) Extractor Selection

Extractor mapping is controlled by:
- `src/extractors/extractor_factory.py`

Rules:
- Some AMCs use dedicated extractor files (e.g., HDFC/ICICI/SBI etc.).
- Many AMCs use `CommonExtractorV1`.
- If common logic fails for an AMC, create dedicated extractor and remap in factory.

---

## 3) Core Extraction Logic

Base utilities are in:
- `src/extractors/base_extractor.py`

Common implementation:
- `src/extractors/common_extractor_v1.py`

### 3.1 Header Detection Rule
Header row is valid only if it has:
- `ISIN`
- and at least one of: `INSTRUMENT`, `ISSUER`, `COMPANY`, `NAME OF THE`

### 3.2 Equity Filter (Mandatory Triple Filter)
Row is treated as equity only when all are true:
1. ISIN starts with `INE`
2. ISIN length is `12`
3. ISIN security code at `[8:10]` is `10`

### 3.3 Multi-table Stop Logic
Extraction stops after data starts when row matches markers like:
- `GRAND TOTAL`
- `NET ASSETS`
- `% TO NAV 100`
- row starts with `TOTAL` (excluding `SUB TOTAL`)

### 3.4 Units & Value Normalization
- Detects `RUPEES` / `LAKHS` / `CRORES`
- Normalizes to `market_value_inr` in base INR rupees.

### 3.5 Scheme Parsing
Parses sheet/scheme text into:
- `scheme_name`
- `scheme_description`
- `plan_type` (Direct/Regular)
- `option_type` (Growth/IDCW)
- `is_reinvest`

### 3.6 Validation Guards
`validate_nav_completeness` applies post-extraction checks per scheme:
- NAV range guard (code-enforced range)
- holdings count sanity
- duplicate ISIN guard
- top weight warning logic

---

## 4) Extractor Output Contract (Input to Loader)

Each extracted row should include:
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

## 5) Dry-Run vs Load Mode

### Dry-run
Use when validating extraction quality.

Command example:
```bash
.venv/bin/python -c "from src.extractors.orchestrator import ExtractionOrchestrator; o=ExtractionOrchestrator(); print(o.process_amc_month('nippon',2025,11,dry_run=True,redo=True))"
```

Expected return:
- status
- rows_read
- dry_run=True

### Load mode
Use only after dry-run quality is accepted.

Command example:
```bash
.venv/bin/python -c "from src.extractors.orchestrator import ExtractionOrchestrator; o=ExtractionOrchestrator(); print(o.process_amc_month('nippon',2025,11,dry_run=False,redo=False))"
```

Expected return:
- status
- rows_inserted
- duration

---

## 6) Loader Internals (DB Write Logic)

Loader file:
- `src/loaders/portfolio_loader.py`

Method:
- `PortfolioLoader.load_holdings(amc_id, period_id, holdings)`

What loader does:
1. Groups holdings by scheme key:
   - `(scheme_name, plan_type, option_type)`
2. Upserts scheme record.
3. Checks if snapshot for `(scheme_id, period_id)` already exists:
   - if yes, skips idempotently.
4. Resolves company IDs by ISIN using cache + ISIN master updates.
5. Applies duplicate handling inside each scheme:
   - exact duplicate rows dropped
   - split rows merged (quantity/value/% summed)
6. Creates snapshot with totals.
7. Inserts final holdings rows into DB.

---

## 7) Idempotency and Redo Behavior

### Hash-based run idempotency
Orchestrator can skip already-processed file hashes.

### Snapshot-level idempotency
Loader skips scheme-period when snapshot exists.

### Redo
Use `redo=True` to purge previous run data for that AMC+period and reprocess.

---

## 8) Recommended Execution Sequence (Per New AMC/Month)

1. Confirm merged file exists in `data/output/merged excels/...`
2. Run `dry_run=True, redo=True`
3. Review:
   - `rows_read`
   - scheme-level quality
   - NAV/duplicate warnings and errors
4. If quality pass, run `dry_run=False`
5. Validate inserted counts and reconciliation output

---

## 9) Troubleshooting (Extractor → DB only)

### `file_not_found`
- Merged workbook path missing or wrong naming convention.

### `no_extractor`
- AMC slug not mapped in `ExtractorFactory`.

### `rows_read = 0`
- header mismatch, unit mismatch, or equity filter excluding all rows.
- inspect header detection and ISIN format in merged file.

### Too many/few rows
- check multi-table stop conditions.
- check duplicate merge behavior.

### Load success but fewer inserts than extracted
- expected if duplicates were merged/dropped in loader.

### Snapshot skipped
- snapshot already exists for scheme+period.
- use `redo=True` only when intentional.

---

## 10) Minimal Checklist Before DB Load

- [ ] Extractor mapped and runs for target AMC/month
- [ ] Dry-run non-zero and reasonable row count
- [ ] No critical validation failures
- [ ] Scheme-level output looks consistent
- [ ] Then run load mode

