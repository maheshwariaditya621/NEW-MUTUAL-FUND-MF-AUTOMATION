# Post-Download Portfolio Automation Pipeline Guide

This document details the end-to-end process followed after downloading monthly portfolio files, using **HDFC** and **SBI** as the established gold standards.

---

## 🏗️ Pipeline Architecture

The pipeline consists of four distinct stages:
**Downloader** → **Merger** → **Extractor** → **Loader** → **Database**

---

## 📂 Stage 2: The Merger (Consolidation)
**Responsibility**: Combine multiple scheme-wise files into a single master monthly AMC file.
**Key Component**: `src/utils/excel_merger.py`

### 1. File Preparation
- Automatically identifies folder: `data/raw/{amc}/{year}_{month}/`
- Extracts any `.zip` files found.
- Converts `.xls` (legacy) to `.xlsx` (modern) using `win32com` or `pandas`.

### 2. Merging Strategy
- **High-Fidelity (COM)**: For AMCs with complex formatting (like HDFC), it uses Microsoft Excel via COM (`win32com`). This preserves 100% of formatting, merged cells, and column widths.
- **OpenPyXL (Standard)**: Lightweight fallback for standard Excel files.

### 3. Sheet Name Normalization
- Merged sheets are renamed for clarity.
- **Stripping**: Removes generic noise like "Monthly Portfolio Disclosure", "November 2025", "HDFC", "SBI", etc.
- **Deduplication**: Automatically handles duplicate names by adding suffixes (e.g., `Scheme (1)`).
- **Limit**: Strictly adheres to the 31-character Excel sheet name limit.

**Output**: `data/output/merged excels/{amc}/{year}/CONSOLIDATED_{AMC}_{YEAR}_{MONTH}.xlsx`

---

## 🔍 Stage 3: The Extractor (Parsing)
**Responsibility**: Convert unstructured Excel sheets into structured JSON/Python objects.
**Key Component**: `src/extractors/orchestrator.py` & `src/extractors/base_extractor.py`

### 1. Orchestration & Idempotency
- Computes **SHA-256 hash** of the merged file.
- Checks `extraction_runs` table. If the hash matches, it skips processing unless `--redo` is passed.
- Selects the correct AMC version (e.g., `HDFCExtractorV1`) via `ExtractorFactory`.

### 2. Triple Filter (The Golden Rule)
- Only **Equity** instruments are extracted.
- Filter Criteria:
  1. ISIN starts with `INE`.
  2. ISIN length is exactly 12 characters.
  3. Security Type (Position 9-10) is `10`.
- All other instruments (Debt, Gold, Cash, Term Deposits) are filtered out.

### 3. Header Detection & Drift
- Uses a **Keyword-based Search** (ISIN, Company, Quantity) within the first 25 rows to find the header.
- Computes a **Header Fingerprint** to detect if the AMC changed their Excel layout (Drift Detection).

---

## 📥 Stage 4: The Loader (Persistence)
**Responsibility**: Clean, validate, and insert data into the PostgreSQL database.
**Key Component**: `src/loaders/portfolio_loader.py`

### 1. Scheme Resolution
- Upserts scheme data (name, plan, option) into the `schemes` table.
- Identifies Plan Type (Direct/Regular) and Option Type (Growth/IDCW) by parsing verbose names.

### 2. Master Data Mapping
- **ISIN Master**: Ensures the ISIN exists in `isin_master`.
- **Company Master**: Resolves the raw name to a `company_id`.
- **Sector Normalization**: Maps raw sector names to a canonical sector list.

### 3. Data Integrity Guards
- **Deduplication**: Merges split holdings (e.g., if a scheme holds the same ISIN in multiple sub-accounts).
- **%NAV Guard**: Validates that the total equity weight is between **95% and 105%**. If it falls outside this range, a warning is logged for manual audit.
- **Currency Normalization**: Ensures all values are stored in base **Rupees** (converting from Lakhs/Crores where applicable).

---

## ✅ Finalization & Reporting
- **Extraction Run Log**: Records metrics (rows read, rows inserted, processing time, git commit).
- **Reconciliation Report**: Generates a CSV in `reports/reconciliation_*.csv` showing summary totals per scheme for manual verification.
- **Atomic Backup**: Triggers a database backup after successful ingestion.

---

## 🚀 How to Onboard a New AMC
1. Use the **Merger** to consolidate your raw files.
2. Create a new extractor in `src/extractors/` inheriting from `BaseExtractor`.
3. Map columns in your new extractor based on keywords.
4. Logic for "Scheme Metadata" extraction (from rows 1-5).
5. Register in `ExtractorFactory`.
6. Run dry-run via `orchestrator.py` and verify against the **%NAV Guard**.
