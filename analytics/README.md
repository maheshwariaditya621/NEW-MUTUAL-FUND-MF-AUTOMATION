# Analytics Module

## Responsibility
Generate insights and reports from loaded portfolio data.

## Why This Exists
Once data is in PostgreSQL, we need to:
- Generate monthly reports
- Track portfolio changes over time
- Identify trends and patterns
- Support business intelligence queries

## What It Does
- Queries PostgreSQL for analytical insights
- Generates summary statistics
- Exports reports in various formats
- Supports custom analytics queries

## What It Does NOT Do
- Does NOT load data (that's `loaders/`)
- Does NOT validate data (that's `validation/`)

## Future Components
- `reports.py` - Report generation logic
- `queries.py` - Analytical SQL queries
- `exporters.py` - Export to Excel, CSV, etc.
