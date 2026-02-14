# Extractor + Loader Execution Checklist (AMC-Safe, OS-Neutral)

## Purpose
This is the **execution plan** to harden extractor + loader pipeline quality without changing AMC-specific extraction logic (sheet structure, column positions, section logic).

This plan is safe for both macOS and Windows because it uses:
- Python module/script entry points
- `pathlib`-compatible project-relative paths
- no OS-specific APIs (`win32com`, shell-specific features)

---

## Hard Rules (Must Follow)

1. **Do not modify AMC-specific parsing behavior** unless explicitly required for a confirmed bug.
2. **Do not change sheet-position assumptions per AMC** just to force uniformity.
3. **Only standardize cross-AMC contracts** (shared output keys, shared guards, status handling).
4. **Validate all already-implemented AMCs** before and after fixes.

---

## Implemented AMC Scope (Current)

From `ExtractorFactory`, these AMCs are in-scope:
- `hdfc`
- `sbi`
- `icici` / `icici_pru`
- `hsbc`
- `kotak`
- `ppfas`
- `axis`
- `bajaj`
- `absl`
- `angelone`

---

## Phase 0 — Pre-Execution Setup

### Task 0.1: Create working branch
- Create a dedicated branch for parity hardening.
- Keep commits grouped by phase (contract, loader safety, validation).

### Task 0.2: Confirm environment
- Python env activated
- DB reachable from `.env`
- `requirements.txt` installed

### Task 0.3: Confirm merged input availability
Expected path format:
- `data/output/merged excels/{amc_slug}/{year}/CONSOLIDATED_{AMC}_{YEAR}_{MONTH}.xlsx`

If missing, run download+merge first (outside this checklist) and then continue.

---

## Phase 1 — Cross-AMC Contract Freeze (No AMC Logic Changes)

## Goal
Make all extractors emit a shared loader-facing contract while preserving AMC-specific extraction internals.

### Task 1.1: Freeze canonical extractor output keys
Mandatory loader-facing keys:
- `amc_name`
- `scheme_name`
- `plan_type`
- `option_type`
- `isin`
- `company_name`
- `quantity`
- `market_value_inr`
- `percent_to_nav`  ← internal standard key for loader
- `sector` (optional)
- `industry` (optional)
- `is_reinvest` (optional)

### Task 1.2: Add compatibility shim (centralized)
- Add one normalization step before loader call to map aliases:
  - `percent_of_nav` → `percent_to_nav`
- Keep this centralized (orchestrator pre-loader), not AMC-by-AMC edits where possible.

### Task 1.3: Standardize shared header-not-found semantics
- `BaseExtractor.find_header_row()` returns `-1`.
- Any `is None` header checks in extractors must be corrected to `== -1`.
- This is cross-AMC consistency, not format logic change.

### Task 1.4: Standardize validation function invocation
- Ensure all calls to `validate_nav_completeness(holdings, scheme_name)` pass both args.
- Keep per-AMC pass/fail policy unchanged in this phase.

Deliverable:
- Contract parity for all AMCs with no AMC-specific parsing behavior changes.

---

## Phase 2 — Loader Reliability and Run Status Integrity

## Goal
Prevent silent partial loads from being marked full success.

### Task 2.1: Add explicit load outcome model
`PortfolioLoader.load_holdings()` should return:
- `rows_inserted`
- `schemes_total`
- `schemes_loaded`
- `schemes_failed`
- `failed_scheme_keys` (sample or full)

### Task 2.2: Define orchestrator status policy
- `SUCCESS`: no scheme failures
- `PARTIAL_SUCCESS`: at least one scheme failed, at least one loaded
- `FAILED`: none loaded or fatal exception

### Task 2.3: Persist truthful run status in `extraction_runs`
- Record above status instead of always `SUCCESS`.

### Task 2.4: Hash idempotency hardening
- Scope idempotency check by `(amc_id, period_id, file_hash)` to avoid accidental cross-month or cross-AMC skip.

Deliverable:
- Accurate operational status + safer idempotency.

---

## Phase 3 — Cross-AMC Validation Gate (Still AMC-Safe)

## Goal
Enforce shared non-AMC-specific guarantees only.

### Task 3.1: Add centralized pre-loader invariant validator
Checks only:
- required keys present
- numeric fields parse cleanly
- `isin` non-empty and cleaned
- `market_value_inr` numeric and non-negative
- `percent_to_nav` numeric

### Task 3.2: Keep AMC-specific rules untouched
- No edits to section detection, metadata extraction, or column fuzzy matching logic in this phase.

Deliverable:
- Bad contract rows caught centrally, not via AMC-specific rewrites.

---

## Phase 4 — Real AMC-by-AMC Verification (Must Run)

This section is mandatory to confirm current implemented AMCs are working in real runs.

## 4A. Dry-run extraction test (all AMCs)
Run for each AMC (use the month/year where merged file exists):

- `python -m src.extractors.cli --amc hdfc --year YYYY --month MM --dry-run`
- `python -m src.extractors.cli --amc sbi --year YYYY --month MM --dry-run`
- `python -m src.extractors.cli --amc icici --year YYYY --month MM --dry-run`
- `python -m src.extractors.cli --amc hsbc --year YYYY --month MM --dry-run`
- `python -m src.extractors.cli --amc kotak --year YYYY --month MM --dry-run`
- `python -m src.extractors.cli --amc ppfas --year YYYY --month MM --dry-run`
- `python -m src.extractors.cli --amc axis --year YYYY --month MM --dry-run`
- `python -m src.extractors.cli --amc bajaj --year YYYY --month MM --dry-run`
- `python -m src.extractors.cli --amc absl --year YYYY --month MM --dry-run`
- `python -m src.extractors.cli --amc angelone --year YYYY --month MM --dry-run`

Record for each AMC:
- status
- rows_read
- warnings/errors seen
- runtime

## 4B. Loader integration test (all AMCs)
Run non-dry for each AMC in a controlled month:
- `python -m src.extractors.cli --amc <amc> --year YYYY --month MM`

Then verify:
1. `extraction_runs.status`
2. `rows_inserted > 0` (for expected published months)
3. scheme snapshot count increase
4. reconciliation CSV generated in `reports/`

## 4C. Redo behavior test (sample AMCs)
For 2–3 AMCs:
- run once normally
- run again with `--redo`
- verify purge + clean reinsert path works

## 4D. Locked period test
- Mark a test period as `FINAL`
- confirm run is skipped without writes

---

## Phase 5 — Regression Matrix (Before/After Fix Comparison)

Build a matrix file (CSV or markdown) with one row per AMC:

Columns:
- AMC
- Test month
- Dry-run status (before)
- Loader status (before)
- Rows inserted (before)
- Dry-run status (after)
- Loader status (after)
- Rows inserted (after)
- Delta comment

Acceptance criteria:
- No AMC-specific extraction behavior regression
- No drop in successfully loaded schemes unless a real data error is exposed
- Status reporting is more accurate than before

---

## Phase 6 — Rollout Order (Safe Sequence)

1. Contract parity + header/validation signature fixes
2. Loader outcome/status accuracy
3. Idempotency scope hardening
4. Full AMC regression run
5. Production rollout

If any AMC fails after Phase 1:
- revert only that AMC-touching change
- keep centralized compatibility layer
- continue with other AMCs

---

## File Touch Order (Recommended)

1. `src/extractors/orchestrator.py` (central normalization gate + status handling)
2. `src/loaders/portfolio_loader.py` (structured outcome reporting)
3. `src/db/repositories.py` (idempotency query scope)
4. Extractors with clear shared-contract bugs only:
   - `src/extractors/hdfc_extractor_v1.py`
   - `src/extractors/kotak_extractor_v1.py`
   - `src/extractors/ppfas_extractor_v1.py`
   - `src/extractors/icici_extractor_v1.py`

Note: do not modify column mapping or sheet structure logic unless bug-proof evidence requires it.

---

## OS-Neutral Execution Notes (Mac now, Windows later)

- Use same Python commands on both OS.
- Keep paths project-relative and `pathlib` based.
- Avoid shell scripts that depend on bash/zsh/PowerShell syntax differences.
- If automation is needed, create Python driver scripts (not OS-specific shell loops).

---

## Final Definition of Done

Done means all are true:
1. All 10 implemented AMCs pass dry-run on at least one real merged month.
2. All 10 AMCs pass loader run with truthful run status persisted.
3. No contract mismatch remains between extractor output and loader input.
4. No false skips from global hash collision behavior.
5. No OS-specific logic introduced.
6. Regression matrix completed and archived.

---

## Auto-Fill Verification Report

You now have:

- Template: `docs/AMC_VERIFICATION_RESULTS_TEMPLATE.csv`
- Auto-generator: `src/scripts/generate_amc_verification_report.py`
- Summary dashboard generator: `src/scripts/generate_amc_verification_summary.py`

### Quick auto-fill (dry-run, safest)

- `python src/scripts/generate_amc_verification_report.py --mode dry-run`

Output:
- `docs/AMC_VERIFICATION_RESULTS_AUTO.csv`

### Auto-fill with loader validation

- `python src/scripts/generate_amc_verification_report.py --mode both`

Optional:
- fixed period for all AMCs:
  - `python src/scripts/generate_amc_verification_report.py --mode both --year 2025 --month 12`
- force reload path during loader mode:
  - `python src/scripts/generate_amc_verification_report.py --mode both --redo-load`

### Important behavior

- If merged files are missing for an AMC, row is auto-filled with:
  - `merged_file_found=NO`
  - note explaining file absence

### Generate manager summary dashboard

- `python3 src/scripts/generate_amc_verification_summary.py`

Output:
- `docs/AMC_VERIFICATION_SUMMARY.md`

---

## Immediate Next Action (Practical)

1. Ensure merged files exist locally for target test months.
2. Execute Phase 4A dry-run commands for all AMCs.
3. Share results matrix; then apply Phase 1 fixes in one controlled patch set.