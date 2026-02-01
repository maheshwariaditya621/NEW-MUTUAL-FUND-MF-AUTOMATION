# Loaders Module

## Responsibility
Load validated data into PostgreSQL database.

## Why This Exists
After data passes validation, it needs to be efficiently and safely loaded into PostgreSQL.

## What It Does
- Manages database connections
- Handles bulk inserts (efficient loading of large datasets)
- Manages transactions (all-or-nothing loading)
- Handles duplicate detection and updates
- Logs loading statistics

## What It Does NOT Do
- Does NOT validate data (that's `validation/`)
- Does NOT transform data (that's `standardisation/`)
- Does NOT extract data (that's `extractors/`)

## Transaction Safety
**CRITICAL**: All loading happens in transactions.
- If ANY row fails to load, the ENTIRE batch is rolled back
- Database is never left in a partial state

## Future Components
- `postgres_loader.py` - PostgreSQL-specific loading logic
- `bulk_operations.py` - Efficient bulk insert/update operations
