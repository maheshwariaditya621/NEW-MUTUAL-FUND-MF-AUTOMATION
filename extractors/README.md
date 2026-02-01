# Extractors Module

## Responsibility
Extract raw data from AMC-specific Excel files.

## Why This Exists
Each AMC (HDFC, ICICI, Axis, etc.) publishes portfolio data in different Excel formats:
- Different column names
- Different sheet structures
- Different data layouts
- Different file naming conventions

## What It Does
- Reads Excel files for specific AMCs
- Extracts raw data into a common intermediate format
- Handles AMC-specific quirks and edge cases

## What It Does NOT Do
- Does NOT standardize data (that's `standardisation/`)
- Does NOT validate data quality (that's `validation/`)
- Does NOT save to database (that's `loaders/`)

## Future Structure
```
extractors/
├── base_extractor.py      # Common extraction interface
├── hdfc_extractor.py      # HDFC-specific logic
├── icici_extractor.py     # ICICI-specific logic
├── axis_extractor.py      # Axis-specific logic
└── ...                    # Other AMCs
```
