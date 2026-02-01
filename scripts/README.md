# Scripts Module

## Responsibility
One-time utility scripts, debugging tools, and maintenance tasks.

## Why This Exists
Sometimes you need to:
- Run one-time data fixes
- Debug specific issues
- Perform manual data migrations
- Test individual components

These scripts are NOT part of the automated pipeline.

## What It Contains
- Ad-hoc debugging scripts
- One-time data migration scripts
- Testing utilities
- Manual data inspection tools

## What It Does NOT Contain
- Production pipeline code (that's in other modules)
- Automated scheduled tasks (that's `ingestion/scheduler.py`)

## Examples
- `test_extractor.py` - Test a single AMC extractor
- `fix_data.py` - One-time data correction script
- `inspect_excel.py` - Manually inspect Excel file structure
