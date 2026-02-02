# New AMC Downloader Implementation Guide

## Complete End-to-End Checklist

This guide provides a step-by-step process for implementing a new AMC downloader that meets HDFC gold standard compliance (95%+).

---

## Phase 1: Discovery & Research

### 1.1 API Discovery
- [ ] Identify AMC's official website
- [ ] Locate monthly portfolio disclosure section
- [ ] Inspect network traffic (Chrome DevTools → Network tab)
- [ ] Identify API endpoint (or determine if Playwright needed)
- [ ] Document HTTP method (GET/POST)
- [ ] Document request headers
- [ ] Document request payload structure
- [ ] Test API with curl/Postman for **at least 2 different months**
- [ ] Verify no authentication/CAPTCHA required
- [ ] Document API response structure

### 1.2 Data Structure Analysis
- [ ] Identify file format (Excel, PDF, ZIP)
- [ ] Determine file naming convention
- [ ] Identify month identifier in response (timestamp, filename, metadata)
- [ ] Verify expected file count per month
- [ ] Check for multi-month responses (critical for routing)
- [ ] Document any edge cases or anomalies

### 1.3 Decision: API vs Playwright
- [ ] **Prefer API** if available and stable
- [ ] Use Playwright **only if unavoidable** (dynamic content, no API)
- [ ] Document decision and rationale

---

## Phase 2: Implementation

### 2.1 Create Downloader File

**File**: `src/downloaders/<amc_name>_downloader.py`

**Template Structure**:
```python
import requests
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

# Import downloader config
try:
    from src.config.downloader_config import (
        DRY_RUN, FILE_COUNT_MIN, FILE_COUNT_MAX,
        MAX_RETRIES, RETRY_BACKOFF
    )
except ImportError:
    DRY_RUN = False
    FILE_COUNT_MIN = 1
    FILE_COUNT_MAX = 1
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]


class <AMC>Downloader(BaseDownloader):
    API_URL = "..."  # AMC API endpoint
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    
    def __init__(self):
        super().__init__("<AMC Full Name>")
        self.notifier = get_notifier()
    
    # Helper methods
    def _check_file_count(self, file_count: int, year: int, month: int):
        """Soft check - warning only, never blocks."""
        pass
    
    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        """Create _SUCCESS.json marker."""
        pass
    
    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str):
        """Move incomplete folder to _corrupt/."""
        pass
    
    def _api_call_with_retry(self, ...):
        """API call with retry logic (2 retries, exponential backoff)."""
        pass
    
    def download(self, year: int, month: int) -> Dict:
        """Main download method."""
        pass
```

### 2.2 Implement Core Components

#### A. Identity Check (Idempotency)
```python
target_dir = Path(self.get_target_folder("<amc_name>", year, month))

if target_dir.exists():
    if not (target_dir / "_SUCCESS.json").exists():
        logger.warning(f"⚠️ Incomplete folder detected for {year}-{month:02d}")
        self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json")
    else:
        logger.info("⏭️  Month already downloaded — SKIPPING")
        return {"amc": "<AMC>", "year": year, "month": month, "status": "skipped"}

self.ensure_directory(str(target_dir))
```

#### B. API Call with Retry
```python
def _api_call_with_retry(self, url, headers, payload, year, month):
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.Timeout:
            if attempt < MAX_RETRIES:
                backoff = RETRY_BACKOFF[attempt]
                logger.warning(f"⏳ Timeout {attempt+1}/{MAX_RETRIES+1}, retrying in {backoff}s")
                time.sleep(backoff)
            else:
                raise
        except requests.HTTPError:
            raise  # Don't retry on HTTP errors
```

#### C. Month Matching (CRITICAL)
```python
# STRICT: Only match files for requested (year, month)
matched = []

for item in files:
    # Extract year/month from API response
    # Use timestamp, filename, or metadata
    
    if item_year == year and item_month == month:
        matched.append({
            "name": original_filename,  # PRESERVE EXACTLY
            "url": download_url,
            "matched_year": item_year,
            "matched_month": item_month
        })

if not matched:
    logger.warning(f"Month not yet published: {month_name} {year}")
    logger.warning("No matching files found in API response")
    
    # Emit not published event
    self.notifier.notify_not_published(amc="<AMC>", year=year, month=month)
    
    if target_dir.exists():
        shutil.rmtree(target_dir)
    
    return {"amc": "<AMC>", "year": year, "month": month, "status": "not_published"}
```

#### D. Download Files
```python
saved_files = []

for idx, f in enumerate(matched, start=1):
    logger.info(f"⬇️  Downloading {idx}/{len(matched)}: {f['name']}")
    r = requests.get(f["url"], timeout=60)
    r.raise_for_status()
    
    # Save to ACTUAL month folder (not requested month)
    actual_year = f["matched_year"]
    actual_month = f["matched_month"]
    actual_folder = Path(self.get_target_folder("<amc_name>", actual_year, actual_month))
    
    self.ensure_directory(str(actual_folder))
    
    path = actual_folder / f["name"]  # PRESERVE ORIGINAL FILENAME
    with open(path, "wb") as out:
        out.write(r.content)
    
    saved_files.append(str(path))
    logger.info(f"✅ Saved: {path.name} → {actual_year}_{actual_month:02d}/")
```

#### E. Finalize
```python
# Create _SUCCESS.json in actual folder
if len(saved_files) > 0:
    actual_folder = Path(saved_files[0]).parent
    actual_folder_name = actual_folder.name
    
    import re
    match = re.match(r'^(\d{4})_(\d{2})$', actual_folder_name)
    if match:
        folder_year = int(match.group(1))
        folder_month = int(match.group(2))
        
        self._check_file_count(len(saved_files), folder_year, folder_month)
        self._create_success_marker(actual_folder, folder_year, folder_month, len(saved_files))

# Clean up empty requested month folder
if target_dir.exists():
    zip_files = list(target_dir.glob("*.zip"))  # or *.xlsx, *.pdf
    if len(zip_files) == 0:
        shutil.rmtree(target_dir)
        logger.info(f"🗑️  Removed empty folder: {target_dir.name}")

duration = time.time() - start_time

# Emit success event
self.notifier.notify_success(
    amc="<AMC>",
    year=year,
    month=month,
    files_downloaded=len(saved_files),
    duration=duration
)

logger.info("=" * 60)
logger.info(f"🎉 SUCCESS | <AMC> {year}-{month:02d}")
logger.info(f"📄 Files downloaded: {len(saved_files)}")
logger.info(f"⏱️  Duration: {duration:.2f}s")
logger.info("=" * 60)
```

#### F. CLI Entry Point
```python
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="<AMC> Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    
    downloader = <AMC>Downloader()
    result = downloader.download(args.year, args.month)
    
    if result["status"] != "success":
        raise SystemExit(1)
```

### 2.3 Implement Logging Standards

**Use HDFC/ICICI logging format**:
```python
# Success
logger.info("✅ File count OK (1)")
logger.info("🎉 SUCCESS | <AMC> 2025-12")

# Warnings
logger.warning("Month not yet published: January 2026")
logger.warning("API returned empty files list")

# Errors
logger.error("❌ FAILED | <AMC> 2025-12")

# Info
logger.info("📥 <AMC> DOWNLOADER")
logger.info("🗓️  Period: 2025-12 (December)")
logger.info("📁 Target directory ready: data/raw/<amc>/2025_12")
logger.info("🌐 Calling <AMC> API…")
logger.info("📦 API returned 20 file(s)")
logger.info("⬇️  Downloading 1/1: filename.zip")
logger.info("⏭️  Month already downloaded — SKIPPING")
```

---

## Phase 3: Backfill Implementation

### 3.1 Create Backfill File

**File**: `src/scheduler/<amc_name>_backfill.py`

**Required Features**:
- [ ] Manual range mode (start_year, start_month, end_year, end_month)
- [ ] `_SUCCESS.json` checks to skip completed months
- [ ] DRY_RUN support
- [ ] Comprehensive logging
- [ ] CLI entry point

**Template**: Copy from `icici_backfill.py` and adapt

---

## Phase 4: Scheduler Implementation

### 4.1 Create Scheduler File

**File**: `src/scheduler/<amc_name>_scheduler.py`

**Required Features**:
- [ ] AUTO MODE only (previous calendar month)
- [ ] Startup guard (wait for next scheduled time)
- [ ] 3 scheduled times (07:00, 15:00, 23:45)
- [ ] Infinite loop with error handling
- [ ] Telegram scheduler summaries
- [ ] Crash-safe (exceptions don't crash loop)

**Template**: Copy from `icici_scheduler.py` and adapt

---

## Phase 5: Utilities

### 5.1 Folder Repair Utility (Optional)

**File**: `src/utils/<amc_name>_folder_repair.py`

**Features**:
- [ ] Scan all folders
- [ ] Detect incomplete folders (missing `_SUCCESS.json`)
- [ ] Move to `_corrupt/`
- [ ] Comprehensive reporting

**Template**: Copy from `icici_folder_repair.py` and adapt

---

## Phase 6: Testing

### 6.1 Downloader Tests
```bash
# Test 1: Successful download
python -m src.downloaders.<amc>_downloader --year 2025 --month 12

# Test 2: Idempotency (re-download)
python -m src.downloaders.<amc>_downloader --year 2025 --month 12

# Test 3: Not-published month
python -m src.downloaders.<amc>_downloader --year 2026 --month 2

# Test 4: Empty API response
python -m src.downloaders.<amc>_downloader --year 2020 --month 1
```

### 6.2 Verify Outputs
- [ ] Check folder structure: `data/raw/<amc>/YYYY_MM/`
- [ ] Verify `_SUCCESS.json` exists
- [ ] Verify original filenames preserved
- [ ] Check Telegram notifications sent
- [ ] Verify logs are standardized

### 6.3 Backfill Tests
```bash
# Test manual range
python -m src.scheduler.<amc>_backfill --start-year 2025 --start-month 1 --end-year 2025 --end-month 12

# Verify skips completed months
python -m src.scheduler.<amc>_backfill --start-year 2025 --start-month 11 --end-year 2025 --end-month 12
```

### 6.4 Corruption Tests
- [ ] Create incomplete folder (no `_SUCCESS.json`)
- [ ] Run downloader for that month
- [ ] Verify folder moved to `_corrupt/`
- [ ] Verify re-download successful

---

## Phase 7: Documentation

### 7.1 Create Compliance Checklist

**File**: `<amc>_compliance_checklist.md`

Use HDFC gold standard checklist and mark items as complete:
- [ ] API & Discovery (6/6)
- [ ] Downloader Contract (6/6)
- [ ] Folder & Data Integrity (5/5)
- [ ] Corruption & Safety (5/5)
- [ ] Retry & Error Control (6/6)
- [ ] File Count Sanity (4/4)
- [ ] Modes & CLI (5/5)
- [ ] Scheduler (6/6)
- [ ] Empty/Not-Published Handling (4/4)
- [ ] Telegram & Observability (5/5)
- [ ] Architectural Guarantees (8/8)

**Target**: 95%+ compliance (57/60 items minimum)

### 7.2 Create Test Report

**File**: `<amc>_validation_report.md`

Document:
- [ ] All test results with evidence
- [ ] Component validation
- [ ] Production readiness checklist
- [ ] Known limitations
- [ ] Deployment recommendation

---

## Phase 8: Production Deployment

### 8.1 Pre-Deployment Checklist
- [ ] All tests passed (100%)
- [ ] Compliance ≥ 95%
- [ ] Telegram notifications working
- [ ] Folder structure validated
- [ ] `_SUCCESS.json` contract enforced
- [ ] Corruption handling tested
- [ ] Scheduler tested (dry run)

### 8.2 Git Commit
```bash
git add .
git commit -m "downloader logic 100% implemented for <amc> mf"
git push origin main
```

### 8.3 Deploy Scheduler
- [ ] Copy scheduler to production server
- [ ] Configure as systemd service (Linux) or Task Scheduler (Windows)
- [ ] Verify startup guard working
- [ ] Monitor first scheduled run
- [ ] Verify Telegram summaries

---

## Critical Rules (NON-NEGOTIABLE)

### ❌ DO NOT:
1. Modify existing downloaders
2. Change folder structure
3. Modify `_SUCCESS.json` contract
4. Add schema validation in downloader
5. Mix extraction logic with download
6. Introduce Playwright unless unavoidable
7. Scan historical months in scheduler
8. Allow mass downloads via scheduler
9. Change retry logic
10. Rename downloaded files

### ✅ DO:
1. Use API if available
2. Preserve original filenames exactly
3. Route files to actual month folder
4. Create `_SUCCESS.json` only after 100% success
5. Move incomplete folders to `_corrupt/`
6. Implement retry logic (2 retries, exponential backoff)
7. Emit Telegram events (success, not_published)
8. Use standardized logging (emojis, format)
9. Implement idempotency (skip if `_SUCCESS.json` exists)
10. Make scheduler crash-safe

---

## Quick Reference: File Checklist

### Required Files (Minimum)
- [ ] `src/downloaders/<amc>_downloader.py`
- [ ] `src/scheduler/<amc>_backfill.py`
- [ ] `src/scheduler/<amc>_scheduler.py`

### Optional Files
- [ ] `src/utils/<amc>_folder_repair.py`

### Documentation Files
- [ ] `<amc>_compliance_checklist.md`
- [ ] `<amc>_validation_report.md`

---

## Estimated Timeline

| Phase | Duration | Complexity |
|-------|----------|------------|
| **Discovery** | 1-2 hours | Low-Medium |
| **Implementation** | 3-4 hours | Medium-High |
| **Backfill/Scheduler** | 1-2 hours | Low |
| **Testing** | 2-3 hours | Medium |
| **Documentation** | 1 hour | Low |
| **Deployment** | 1 hour | Low |
| **Total** | **9-13 hours** | **Medium** |

---

## Success Criteria

### Minimum Requirements
✅ Downloader passes all tests (100%)  
✅ HDFC compliance ≥ 95% (57/60 items)  
✅ Telegram notifications working  
✅ Folder structure correct  
✅ `_SUCCESS.json` contract enforced  
✅ Corruption handling working  
✅ Scheduler production-ready  

### Production Ready When:
✅ All critical components complete  
✅ All tests passed  
✅ Documentation complete  
✅ Git committed and pushed  
✅ Ready for set-and-forget operation  

---

## Common Pitfalls to Avoid

1. **Cross-Month Contamination**: API returns multiple months → must filter strictly
2. **Filename Modification**: Never rename files → preserve exactly
3. **Premature `_SUCCESS.json`**: Only create after 100% download complete
4. **Folder Routing**: Save to actual month, not requested month
5. **Retry on 4xx**: Never retry on HTTP 400/404 → only on timeout/5xx
6. **Blocking Telegram**: Telegram failures must never crash downloader
7. **Historical Scanning**: Scheduler must only download previous month
8. **Mass Downloads**: Scheduler must be impossible to trigger bulk downloads

---

## Reference Implementations

**Best Practices**: Study these files
- `src/downloaders/hdfc_downloader.py` (API-based, clean)
- `src/downloaders/icici_downloader.py` (API-based, fallback matching)
- `src/scheduler/hdfc_scheduler.py` (two-stage with range)
- `src/scheduler/icici_scheduler.py` (pure auto mode)
- `src/alerts/telegram_client.py` (retry logic)

---

## Final Checklist

Before declaring AMC "LOCKED":

- [ ] Downloader implemented and tested
- [ ] Backfill implemented and tested
- [ ] Scheduler implemented and tested
- [ ] Telegram notifications working
- [ ] Corruption handling working
- [ ] Folder routing correct
- [ ] Compliance ≥ 95%
- [ ] All tests passed (100%)
- [ ] Documentation complete
- [ ] Git committed and pushed
- [ ] Production deployment approved

**If all checked → AMC is LOCKED and PRODUCTION-READY** 🔒✅
