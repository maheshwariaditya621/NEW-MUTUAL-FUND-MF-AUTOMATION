# Quantum Mutual Fund Downloader: Rules & Constraints

This document defines the behavior and constraints for the Quantum MF downloader to ensure 100% compliance with the HDFC Gold Reference.

## 1. File Count Expectations (Observability)
Quantum returns exactly **ONE** file per month (Combined portfolio for all funds).
- `FILE_COUNT_MIN = 1`
- `FILE_COUNT_MAX = 1`
- *Note: These limits are for observability and must never block `_SUCCESS.json` creation if the file is downloaded.*

## 2. Month Routing Rule (CRITICAL)
The target folder path **MUST** be derived strictly from the requested parameters:
- **Year**: `requested yearId`
- **Month**: `requested monthId`
- **Final Folder Format**: `data/raw/quantum/YYYY_MM/`
- **🚫 Constraint**: Do NOT infer month/year from filename text, `FSMonth`, or `FactSheetDate`.

## 3. Not-Published Detection (STRICT)
- **Condition**: `objProductPortfolioList == []` (API returns an empty array).
- **Required Behavior**:
  - Emit `NOT_PUBLISHED` event (via Telegram notifier).
  - **🚫 DO NOT** create `_SUCCESS.json`.
  - **🚫 DO NOT** leave an empty folder (delete if created).
  - **Return Status**: `"not_published"`.
  - *Must exactly match HDFC semantics.*

## 4. Idempotency & Corruption Handling
Before initiating any download:
- **Corrupt Folder**: If `YYYY_MM/` exists **WITHOUT** `_SUCCESS.json`, move the directory to `data/raw/quantum/_corrupt/YYYY_MM/`.
- **Skip Logic**: If `_SUCCESS.json` **EXISTS**, skip the month entirely.
- **🚫 Constraint**: Never merge, overwrite, or patch partial data.

## 5. Filename Handling (NON-NEGOTIABLE)
- **Source**: Use the `OriginalFileName` field from the API response.
- **Requirement**: Save the file using the **exact original filename**.
- **🚫 Constraint**: No renaming, normalization, prefixes, or suffixes.
- **Example Path**: `data/raw/quantum/2026_01/Combine-Monthly Portfolio-Jan2026.xlsx`

## 6. Retry Rules (HTTP GET)
- **Retry Conditions**: Only on **timeout** or **5xx** status codes.
- **🚫 NO Retry**: Do not retry on **4xx** status codes.
- **Implementation**: Use global retry configuration (2 retries, logic shared with HDFC).

---
**Implementation Note**: This design ensures the Quantum MF downloader can be implemented by duplicating the `hdfc_downloader.py` logic and making minimal adjustments to the API endpoint and response parsing.
