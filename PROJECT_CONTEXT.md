# PROJECT CONTEXT - Mutual Fund Data Ingestion Pipeline

## 🎯 What This System Is

**A production-grade, idempotent, scheduler-driven raw data ingestion pipeline** for mutual fund portfolio data.

**NOT** a web scraper. **NOT** a one-off download script.

This is a **data ingestion foundation** designed for:
- Automated, scheduled execution
- Atomic completion guarantees
- Automatic retry and recovery
- Clean extension to new AMCs
- Future extraction layer integration

---

## 🏗️ System Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    SCHEDULER (3x daily)                      │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ Stage 1: Scheduled Range Backfill (optional)         │   │
│  │ Stage 2: Auto Eligible Month Check (always)          │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    BACKFILL MODULE                           │
│  • Generates month ranges                                   │
│  • Checks completion markers                                │
│  • Orchestrates downloads                                   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    DOWNLOADER                                │
│  • API calls with retry logic                               │
│  • File downloads                                            │
│  • Atomic completion markers                                │
│  • Corruption recovery                                       │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    RAW DATA STORAGE                          │
│  data/raw/{amc}/YYYY_MM/                                     │
│    ├── *.xlsx (portfolio files)                             │
│    └── _SUCCESS.json (completion marker)                    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              TELEGRAM ALERTS (observability)                 │
│  • SUCCESS, WARNING, ERROR events                           │
│  • Scheduler summaries                                       │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
src/
├── alerts/                      # Telegram alerting (DONE)
│   ├── telegram_client.py       # Low-level HTTP
│   ├── telegram_templates.py    # Message formatting
│   └── telegram_notifier.py     # Event-based decisions
│
├── cli/                         # Command-line interfaces (DONE)
│   ├── run_hdfc_downloader.py   # Single month download
│   └── run_hdfc_bulk_downloader.py  # Range download
│
├── config/                      # Configuration (DONE)
│   ├── settings.py              # Global settings
│   ├── downloader_config.py     # Retry, file count thresholds
│   └── telegram_config.py       # Telegram credentials + flags
│
├── downloaders/                 # Downloader layer (HDFC DONE)
│   ├── base_downloader.py       # Abstract base class
│   └── hdfc_downloader.py       # HDFC implementation
│
├── scheduler/                   # Scheduling layer (DONE)
│   ├── hdfc_backfill.py         # Backfill logic
│   └── hdfc_scheduler.py        # Two-stage scheduler
│
├── extractors/                  # Extraction layer (NOT DONE)
│   └── (future: Excel → structured data)
│
├── validators/                  # Validation layer (NOT DONE)
│   └── (future: data quality checks)
│
└── database/                    # Database layer (NOT DONE)
    └── (future: load to PostgreSQL)

data/
└── raw/
    ├── hdfc/                    # HDFC raw data
    │   ├── 2025_01/
    │   │   ├── *.xlsx
    │   │   └── _SUCCESS.json
    │   └── _corrupt/            # Quarantine
    └── {other_amc}/             # Future AMCs
```

---

## 🔐 Critical Contracts (NEVER BREAK THESE)

### 1. Folder Contract

**Location**: `data/raw/{amc}/YYYY_MM/`

**Structure**:
```
YYYY_MM/
├── *.xlsx                    # Portfolio files
└── _SUCCESS.json             # Atomic completion marker
```

**Rules**:
- Folder without `_SUCCESS.json` = INCOMPLETE
- Incomplete folders moved to `_corrupt/`
- Never overwrite existing folders
- Never merge partial data

**_SUCCESS.json Format**:
```json
{
  "amc": "HDFC",
  "year": 2025,
  "month": 1,
  "files_downloaded": 103,
  "timestamp": "2026-02-01T23:59:30.597041"
}
```

### 2. Completion Check

```python
def is_month_complete(amc: str, year: int, month: int) -> bool:
    """Check if month download is complete."""
    marker = Path(f"data/raw/{amc}/{year}_{month:02d}/_SUCCESS.json")
    return marker.exists()
```

**This is the ONLY source of truth for completion.**

### 3. Downloader Return Contract

```python
{
    "amc": str,              # AMC name
    "year": int,             # Calendar year
    "month": int,            # Calendar month (1-12)
    "files_downloaded": int, # Number of files
    "status": str,           # "success" | "failed" | "skipped"
    "reason": str,           # Optional error reason
    "duration": float        # Optional duration in seconds
}
```

### 4. Idempotency Guarantee

**Running the same download twice MUST be safe:**
- Check completion marker first
- Skip if already complete
- Never re-download complete months
- Safe for scheduled execution

---

## ✅ What Is DONE

### HDFC Downloader (Complete)
- ✅ API-based download (no Playwright)
- ✅ Atomic completion markers
- ✅ Retry logic (max 2, backoff 5s/15s)
- ✅ File count sanity checks (80-120 files)
- ✅ Corruption recovery
- ✅ Empty files list handling
- ✅ Dry-run mode
- ✅ Duration tracking

### Backfill System (Complete)
- ✅ Manual range mode
- ✅ Auto mode (latest month only)
- ✅ Completion marker checks
- ✅ Skip existing months
- ✅ Continue on failure

### Scheduler (Complete)
- ✅ Two-stage execution
- ✅ Scheduled range backfill (optional)
- ✅ Auto eligible month check (always)
- ✅ Startup time guard
- ✅ 3x daily schedule (07:00, 15:00, 23:45)
- ✅ Duplicate prevention

### Telegram Alerts (Complete)
- ✅ Event-based architecture
- ✅ 5 alert types (SUCCESS, WARNING, ERROR, NOT_PUBLISHED, SCHEDULER)
- ✅ Configurable feature flags
- ✅ Clean separation from business logic

### CLI Tools (Complete)
- ✅ Single month downloader
- ✅ Bulk downloader (range + auto modes)

---

## ❌ What Is NOT DONE

### Extraction Layer
- ❌ Excel parsing
- ❌ Data structuring
- ❌ Scheme name standardization
- ❌ Portfolio data extraction

### Validation Layer
- ❌ Data quality checks
- ❌ Schema validation
- ❌ Completeness checks

### Database Layer
- ❌ PostgreSQL integration
- ❌ Data loading
- ❌ Deduplication

### Multi-AMC Support
- ✅ Architecture supports it
- ❌ Only HDFC implemented
- ❌ Need: ICICI, Axis, SBI, etc.

---

## 🚀 How to Add a New AMC Downloader

### Step 1: Create Downloader Class

```python
# src/downloaders/{amc}_downloader.py

from src.downloaders.base_downloader import BaseDownloader
from src.alerts.telegram_notifier import get_notifier

class {AMC}Downloader(BaseDownloader):
    API_URL = "https://..."  # AMC's API endpoint
    
    def __init__(self):
        super().__init__("{AMC} Mutual Fund")
        self.notifier = get_notifier()
    
    def download(self, year: int, month: int) -> Dict:
        """
        Download {AMC} monthly portfolio files.
        
        MUST:
        1. Check for incomplete month (folder without _SUCCESS.json)
        2. Move incomplete to _corrupt/
        3. Download files
        4. Create _SUCCESS.json ONLY on success
        5. Emit Telegram events
        6. Return standard dict
        """
        # Implementation here
        pass
```

### Step 2: Follow HDFC Pattern

**Reference**: `src/downloaders/hdfc_downloader.py`

**Required methods**:
- `download(year, month)` - Main download logic
- `_create_success_marker()` - Atomic marker creation
- `_move_to_corrupt()` - Corruption recovery
- `_check_file_count()` - Sanity check (optional)

**Required behaviors**:
- Check completion marker first
- Retry logic with exponential backoff
- Handle empty files list
- Emit Telegram events
- Clean up on failure

### Step 3: Create Backfill Module

```python
# src/scheduler/{amc}_backfill.py

from src.downloaders.{amc}_downloader import {AMC}Downloader

def is_month_complete(year: int, month: int) -> bool:
    """Check if month is complete."""
    marker = Path(f"data/raw/{amc}/{year}_{month:02d}/_SUCCESS.json")
    return marker.exists()

def run_{amc}_backfill(start_year=None, start_month=None, 
                       end_year=None, end_month=None) -> dict:
    """
    Run {AMC} backfill.
    
    Two modes:
    - Manual range (dates provided)
    - Auto (no dates, latest month only)
    """
    # Follow hdfc_backfill.py pattern
    pass
```

### Step 4: Create Scheduler

```python
# src/scheduler/{amc}_scheduler.py

from src.scheduler.{amc}_backfill import run_{amc}_backfill

SCHEDULE_RANGE = {
    "start": (2025, 1),
    "end": (2026, 1)
}

SCHEDULE_TIMES = [
    dt_time(7, 0),
    dt_time(15, 0),
    dt_time(23, 45)
]

def run_scheduler():
    """
    Run {AMC} scheduler.
    
    Two-stage execution:
    1. Scheduled range backfill (optional)
    2. Auto eligible month check (always)
    """
    # Follow hdfc_scheduler.py pattern
    pass
```

### Step 5: Create CLI Tools

```python
# src/cli/run_{amc}_downloader.py
# src/cli/run_{amc}_bulk_downloader.py
```

Follow HDFC CLI patterns exactly.

---

## 🛡️ Invariants (NEVER VIOLATE)

### 1. Atomic Completion
- `_SUCCESS.json` created ONLY when ALL files downloaded
- Never create marker on partial success
- Never create marker on failure

### 2. Idempotency
- Running same download twice = safe
- Check marker before download
- Skip if already complete

### 3. No Data Loss
- Never overwrite existing folders
- Never merge partial data
- Move incomplete to `_corrupt/`

### 4. Clean Separation
- Downloader = raw data only
- No Excel parsing in downloader
- No database operations in downloader
- No validation in downloader

### 5. Event-Based Alerts
- Downloader emits events
- Notifier decides when to send
- No tight coupling

---

## 📊 Configuration Files

### Global Config
**File**: `src/config/settings.py`
- Database credentials
- Logging configuration
- Global paths

### Downloader Config
**File**: `src/config/downloader_config.py`
```python
DRY_RUN = False           # Dry-run mode
FILE_COUNT_MIN = 80       # Min expected files
FILE_COUNT_MAX = 120      # Max expected files
MAX_RETRIES = 2           # Retry attempts
RETRY_BACKOFF = [5, 15]   # Backoff delays (seconds)
```

### Telegram Config
**File**: `src/config/telegram_config.py`
```python
TELEGRAM_BOT_TOKEN = "..."
TELEGRAM_CHAT_ID = "..."
TELEGRAM_ENABLED = True

ALERTS_ENABLED = {
    "SUCCESS": True,
    "WARNING": True,
    "ERROR": True,
    "NOT_PUBLISHED": False,
    "SCHEDULER": False,
}
```

---

## 🧪 Testing New AMC Downloader

### Test 1: Single Month Download
```bash
python -m src.cli.run_{amc}_downloader --year 2025 --month 1
```

**Verify**:
- Files downloaded to `data/raw/{amc}/2025_01/`
- `_SUCCESS.json` created
- Telegram SUCCESS alert sent

### Test 2: Incomplete Month Recovery
```bash
# Create incomplete folder
mkdir data/raw/{amc}/2025_02
echo "test" > data/raw/{amc}/2025_02/test.xlsx

# Run backfill
python -m src.scheduler.{amc}_backfill
```

**Verify**:
- Folder moved to `_corrupt/`
- Month re-downloaded
- `_SUCCESS.json` created
- Telegram WARNING alert sent

### Test 3: Idempotency
```bash
# Download same month twice
python -m src.cli.run_{amc}_downloader --year 2025 --month 1
python -m src.cli.run_{amc}_downloader --year 2025 --month 1
```

**Verify**:
- Second run skipped
- No re-download
- No duplicate alerts

### Test 4: Scheduler
```bash
python -m src.scheduler.{amc}_scheduler
```

**Verify**:
- Waits for scheduled time
- Runs two-stage backfill
- Sends summary alerts

---

## 🔄 Future Extraction Layer

**When implementing extraction**:

### Input Contract
- Read from `data/raw/{amc}/YYYY_MM/`
- Only process folders with `_SUCCESS.json`
- Never modify raw data

### Output Contract
- Write to `data/processed/{amc}/YYYY_MM/`
- Create own completion marker
- Structured data (CSV, JSON, or database)

### Separation
- Extraction is a SEPARATE layer
- Downloader never parses Excel
- Downloader never validates data
- Downloader never loads to database

---

## 📝 Key Design Decisions

### Why Atomic Markers?
- Folder existence alone is unreliable
- Network failures can leave partial data
- Marker = proof of complete download
- Enables automatic corruption recovery

### Why Two-Stage Scheduler?
- Stage 1: Heal historical gaps (optional)
- Stage 2: Download latest data (always)
- Prevents mass downloads on startup
- Safe for production

### Why Event-Based Alerts?
- No tight coupling to Telegram
- Easy to add other notification channels
- Downloader doesn't know about alerts
- Clean separation of concerns

### Why No Playwright?
- API-based downloads are faster
- More reliable
- Easier to debug
- Lower resource usage
- Better for scheduled execution

---

## 🚨 Common Pitfalls (AVOID THESE)

### ❌ Don't Parse Excel in Downloader
**Wrong**:
```python
def download(self, year, month):
    files = self._download_files()
    data = self._parse_excel(files)  # ❌ NO!
    return data
```

**Right**:
```python
def download(self, year, month):
    files = self._download_files()
    self._create_success_marker()  # ✅ YES
    return {"files_downloaded": len(files)}
```

### ❌ Don't Skip Completion Marker
**Wrong**:
```python
if folder.exists():
    return "skipped"  # ❌ NO!
```

**Right**:
```python
if is_month_complete(year, month):
    return "skipped"  # ✅ YES
```

### ❌ Don't Create Marker on Failure
**Wrong**:
```python
try:
    download_files()
except Exception:
    self._create_success_marker()  # ❌ NO!
```

**Right**:
```python
try:
    download_files()
    self._create_success_marker()  # ✅ YES
except Exception:
    cleanup()
```

### ❌ Don't Hardcode Dates
**Wrong**:
```python
for year in range(2010, 2026):  # ❌ NO!
    download(year, month)
```

**Right**:
```python
# Use backfill with range or auto mode
run_backfill(start_year, start_month, end_year, end_month)
```

---

## 📚 Reference Documentation

**Essential Reading**:
1. `folder_contract.md` - Folder structure specification
2. `telegram_guide.md` - Telegram alerting setup
3. `walkthrough.md` - Production hardening details
4. `src/downloaders/hdfc_downloader.py` - Reference implementation

**Code Examples**:
- HDFC downloader (complete reference)
- HDFC backfill (two-mode pattern)
- HDFC scheduler (two-stage pattern)

---

## 🎯 Success Criteria for New AMC

Your new AMC downloader is complete when:

✅ Single month download works  
✅ Completion marker created on success  
✅ Incomplete months moved to `_corrupt/`  
✅ Idempotent (safe to run twice)  
✅ Retry logic implemented  
✅ Telegram alerts emitted  
✅ Backfill module created  
✅ Scheduler created  
✅ CLI tools created  
✅ All tests pass  

---

## 🔮 Roadmap

### Phase 1: Raw Data Ingestion (DONE)
- ✅ HDFC downloader
- ✅ Atomic completion markers
- ✅ Scheduler
- ✅ Telegram alerts

### Phase 2: Multi-AMC Support (NEXT)
- ⏳ ICICI downloader
- ⏳ Axis downloader
- ⏳ SBI downloader
- ⏳ Other AMCs

### Phase 3: Extraction Layer (FUTURE)
- ⏳ Excel parsing
- ⏳ Data structuring
- ⏳ Scheme name standardization

### Phase 4: Database Layer (FUTURE)
- ⏳ PostgreSQL integration
- ⏳ Data loading
- ⏳ Deduplication

### Phase 5: Validation Layer (FUTURE)
- ⏳ Data quality checks
- ⏳ Completeness validation

---

**Status**: Production-Ready Foundation  
**Version**: 1.0  
**Date**: 2026-02-02  
**Next**: Implement additional AMC downloaders
