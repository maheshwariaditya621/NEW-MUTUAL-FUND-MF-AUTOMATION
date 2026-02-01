# Validation Module

## Responsibility
Enforce strict data quality rules before database loading.

## Why This Exists
**CRITICAL RULE**: We NEVER save partial or dirty data to the database.

If validation fails, the ENTIRE batch is rejected. No exceptions.

## What It Does
- Checks for required fields (ISIN, scheme name, etc.)
- Validates data formats (dates, numbers, percentages)
- Checks for duplicates
- Validates business rules (e.g., portfolio percentages sum to ~100%)
- Flags data quality issues

## Validation Levels
1. **CRITICAL** - Must pass or entire batch is rejected
2. **WARNING** - Logged but doesn't block loading
3. **INFO** - Informational only

## What It Does NOT Do
- Does NOT transform data (that's `standardisation/`)
- Does NOT extract data (that's `extractors/`)
- Does NOT save to database (that's `loaders/`)

## Future Components
- `rules.py` - Validation rule definitions
- `validators.py` - Validation logic
- `reports.py` - Validation report generation
