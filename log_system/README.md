# Logging Module

## Responsibility
Centralized, beautified, colorized logging system for the entire application.

## Why This Exists
Consistent, readable logs are CRITICAL for:
- Debugging issues
- Monitoring pipeline progress
- Understanding what went wrong
- Communicating with non-technical stakeholders

## Features
✅ **Colorized output** - Different colors for different log levels  
✅ **Timestamps** - Know exactly when things happened  
✅ **Module names** - Know which part of the code logged the message  
✅ **Custom SUCCESS level** - Celebrate successful operations  
✅ **Non-coder friendly** - Logs are readable by anyone  

## Log Levels

### 🔵 INFO
**When to use**: General progress updates, starting processes, informational messages

**Examples**:
- "Starting monthly portfolio ingestion for HDFC MF"
- "Processing file: hdfc_jan_2026.xlsx"
- "Connecting to database..."

### ✅ SUCCESS
**When to use**: Successful completion of operations

**Examples**:
- "Successfully extracted 1,245 rows from Excel"
- "Data validation passed: 100% clean data"
- "Loaded 1,245 rows into PostgreSQL"

### ⚠️ WARNING
**When to use**: Non-critical issues that don't stop the pipeline

**Examples**:
- "3 rows have missing ISIN codes, flagged for review"
- "Retrying database connection (attempt 2/3)"
- "Using default value for missing configuration"

### ❌ ERROR
**When to use**: Critical failures that stop the pipeline

**Examples**:
- "Database connection failed: timeout exceeded"
- "Excel file not found: hdfc_jan_2026.xlsx"
- "Validation failed: 50 rows have invalid ISINs"

## Usage

```python
from logging.logger import get_logger

# Get a logger (usually at module level)
logger = get_logger(__name__)

# Log messages
logger.info("Starting process")
logger.success("Process completed successfully")
logger.warning("Non-critical issue detected")
logger.error("Critical failure occurred")
```

## Example Terminal Output

```
[2026-02-01 10:45:12] [INFO] 🔵 [ingestion.pipeline] Starting monthly portfolio ingestion for HDFC MF
[2026-02-01 10:45:15] [SUCCESS] ✅ [extractors.hdfc] Successfully extracted 1,245 rows from Excel
[2026-02-01 10:45:18] [WARNING] ⚠️ [validation.rules] 3 rows have missing ISIN codes, flagged for review
[2026-02-01 10:45:20] [ERROR] ❌ [loaders.postgres] Database connection failed: timeout exceeded
```

## Configuration

### Disable Emoji Symbols
If your terminal doesn't support emojis:
```python
logger = get_logger(__name__, use_symbols=False)
```

### Log to File
To also save logs to a file:
```python
logger = get_logger(__name__, log_to_file="logs/pipeline.log")
```

### Change Log Level
```python
import logging
logger = get_logger(__name__, level=logging.DEBUG)
```

## Testing the Logger

Run the logger module directly to see a demo:
```bash
python logging/logger.py
```
