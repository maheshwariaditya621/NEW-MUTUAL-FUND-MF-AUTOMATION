# Streamlit App Module

## Responsibility
Backend verification UI for data inspection and monitoring.

## Why This Exists
A web-based UI makes it easy to:
- View loaded portfolio data
- Check pipeline execution history
- Inspect validation failures
- Monitor data quality metrics
- Debug issues visually

## What It Does
- Provides a Streamlit-based web interface
- Displays data from PostgreSQL
- Shows pipeline logs and status
- Allows filtering and searching data

## What It Does NOT Do
- Does NOT load data (that's `loaders/`)
- Does NOT run the pipeline (that's `ingestion/`)

## Future Components
- `app.py` - Main Streamlit application
- `pages/` - Multi-page app structure
- `components/` - Reusable UI components
