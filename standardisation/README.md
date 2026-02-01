# Standardisation Module

## Responsibility
Transform raw extracted data into a consistent, standardized format.

## Why This Exists
Different AMCs use different:
- Column names (e.g., "ISIN Code" vs "ISIN" vs "Security Code")
- Date formats (DD/MM/YYYY vs MM/DD/YYYY)
- Number formats (1,234.56 vs 1234.56)
- Text casing (UPPERCASE vs Title Case)

We need ONE consistent format for the database.

## What It Does
- Renames columns to standard names
- Converts data types (strings to dates, numbers, etc.)
- Normalizes text (trim whitespace, fix casing)
- Handles missing/null values consistently

## What It Does NOT Do
- Does NOT validate data quality (that's `validation/`)
- Does NOT extract data (that's `extractors/`)
- Does NOT save to database (that's `loaders/`)

## Future Components
- `rules.py` - Standardization rules and mappings
- `transformers.py` - Data transformation functions
