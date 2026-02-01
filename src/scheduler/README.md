# HDFC Scheduler

Automated scheduler for HDFC Mutual Fund portfolio downloads.

## Overview

Pure-Python scheduler that:
- Runs 3 times daily at fixed times
- Auto-backfills missing months from 2010
- Uses folder existence as source of truth
- Cross-platform (Windows, macOS, Linux)
- No external dependencies (cron, Task Scheduler)

## Components

### 1. `hdfc_backfill.py`

Auto-backfill module that:
- Generates month range from 2010-01 to previous completed month
- Checks folder existence: `data/raw/hdfc/YYYY_MM/`
- Downloads missing months using existing downloader
- Skips existing folders
- Returns comprehensive summary

**Usage**:
```python
from src.scheduler.hdfc_backfill import run_hdfc_auto_backfill

result = run_hdfc_auto_backfill()
```

### 2. `hdfc_scheduler.py`

Scheduler that runs backfill at:
- **07:00 AM**
- **03:00 PM** (15:00)
- **11:20 PM** (23:20)

Features:
- Infinite loop with 60-second check interval
- Prevents duplicate runs in same time window
- Graceful error handling
- Keyboard interrupt support

**Usage**:
```bash
python -m src.scheduler.hdfc_scheduler
```

## Configuration

### Start Date
```python
START_YEAR = 2010
START_MONTH = 1
```

### Schedule Times
```python
SCHEDULE_TIMES = [
    dt_time(7, 0),   # 07:00 AM
    dt_time(15, 0),  # 03:00 PM
    dt_time(23, 20)  # 11:20 PM
]
```

### Check Interval
```python
SLEEP_INTERVAL = 60  # seconds
```

## How It Works

### Backfill Logic

1. **Get current date** → Determine previous completed month
2. **Generate range** → All months from 2010-01 to previous month
3. **Check each month**:
   - Folder exists? → Skip
   - Folder missing? → Download
4. **Return summary** → Downloaded, skipped, failed counts

### Scheduler Logic

1. **Infinite loop** with 60-second sleep
2. **Check current time** against schedule
3. **If match**:
   - Check if already ran in this window
   - If not → Run backfill
   - Mark as executed
4. **Repeat**

### Duplicate Prevention

Tracks last execution as `(date, hour)`:
- Same date + same hour → Skip
- Different date or hour → Run

## Running the Scheduler

### Start Scheduler
```bash
python -m src.scheduler.hdfc_scheduler
```

### Stop Scheduler
Press `Ctrl+C`

### Run Backfill Once (Testing)
```bash
python -m src.scheduler.hdfc_backfill
```

## Expected Behavior

### Case 1: All Up to Date
```
[INFO] Total checked: 180
[INFO] Skipped (already exists): 180
[INFO] Downloaded: 0
[INFO] Failed: 0
[INFO] ℹ️  All months already downloaded
```

### Case 2: Missing Months
```
[INFO] Total checked: 180
[INFO] Skipped (already exists): 175
[INFO] Downloaded: 3
[INFO] Failed: 2
[INFO] Downloaded months:
[INFO]   ✅ 2024-10
[INFO]   ✅ 2024-11
[INFO]   ✅ 2024-12
```

### Case 3: Scheduled Run
```
======================================================================
SCHEDULED RUN TRIGGERED - 2026-02-01 07:00:00
======================================================================
[INFO] HDFC AUTO BACKFILL STARTED
...
[SUCCESS] ✅ Scheduled run completed - 1 month(s) downloaded
======================================================================
Next scheduled run at: 15:00
======================================================================
```

## Folder Structure

```
data/raw/hdfc/
├── 2010_01/
├── 2010_02/
├── ...
├── 2024_12/
└── 2025_01/
```

**Rule**: Folder exists = month downloaded successfully

## Error Handling

- **API failure** → Log warning, continue to next month
- **Network error** → Log error, continue to next month
- **Scheduler error** → Log error, retry after sleep interval
- **Keyboard interrupt** → Graceful shutdown

## Dependencies

- `src.downloaders.hdfc_downloader` - Existing downloader
- `src.config.logger` - Logging
- Standard library only (datetime, time, pathlib)

## What It Does NOT Do

❌ Parse Excel files  
❌ Write to database  
❌ Send Telegram alerts  
❌ Use Playwright/Selenium  
❌ Require cron or Task Scheduler  
❌ Re-download existing months  

## Production Usage

### Run on Server
```bash
# Start in background (Linux/macOS)
nohup python -m src.scheduler.hdfc_scheduler > scheduler.log 2>&1 &

# Start in background (Windows)
start /B python -m src.scheduler.hdfc_scheduler
```

### Run as Service
Use system service manager (systemd, Windows Service, etc.) to run:
```bash
python -m src.scheduler.hdfc_scheduler
```

## Testing

### Test Backfill Once
```bash
python -m src.scheduler.hdfc_backfill
```

### Test Scheduler (Short Run)
```bash
python -m src.scheduler.hdfc_scheduler
# Press Ctrl+C after observing behavior
```

## Logs

All operations logged via `src.config.logger`:
- `[INFO]` - Normal operations
- `[SUCCESS]` - Successful downloads
- `[WARNING]` - Failed downloads (data not available)
- `[ERROR]` - Unexpected errors
- `[DEBUG]` - Detailed debug info

## Exit Codes

Backfill:
- N/A (returns dict)

Scheduler:
- Runs indefinitely until interrupted
- Keyboard interrupt → Clean exit

---

**Status**: Production-ready  
**Platform**: Cross-platform (Windows, macOS, Linux)  
**Dependencies**: Python 3.7+ only
