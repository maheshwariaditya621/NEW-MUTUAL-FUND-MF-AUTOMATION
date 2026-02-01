# Ingestion Module

## Responsibility
Orchestrates the complete data ingestion pipeline for monthly AMC portfolio data.

## What It Does
- Coordinates the flow: Excel → Extraction → Standardization → Validation → Loading
- Handles error recovery and retry logic
- Sends alerts on success/failure
- Logs all pipeline activities

## What It Does NOT Do
- Does NOT parse Excel files directly (that's `extractors/`)
- Does NOT transform data (that's `standardisation/`)
- Does NOT validate data (that's `validation/`)
- Does NOT connect to database (that's `loaders/`)

## Future Components
- `pipeline.py` - Main orchestration logic
- `scheduler.py` - Automated monthly runs
