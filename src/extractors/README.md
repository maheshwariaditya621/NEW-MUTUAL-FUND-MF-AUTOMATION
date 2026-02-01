# Extractors Module

## Overview

This module contains extractors for parsing mutual fund portfolio data from various AMC sources.

Each AMC has its own extractor class that inherits from `BaseExtractor` and implements AMC-specific parsing logic.

---

## Base Extractor

**`base_extractor.py`**: Abstract base class defining the extractor interface.

All AMC extractors must implement:
- `get_amc_name()` - Returns canonical AMC name
- `extract_scheme_metadata(file_path)` - Extracts scheme metadata
- `extract_holdings(file_path)` - Extracts equity holdings

---

## AMC-Specific Extractors

**Status**: NOT IMPLEMENTED YET

AMC-specific extractors will be created in future steps, for example:
- `hdfc_extractor.py` - HDFC Mutual Fund
- `icici_extractor.py` - ICICI Prudential Mutual Fund
- `axis_extractor.py` - Axis Mutual Fund
- etc.

Each extractor will handle:
- Excel file parsing (using openpyxl)
- AMC-specific data formats
- Column mapping
- Unit detection
- Data normalization

---

## Usage Example

```python
from src.extractors import HDFCExtractor  # Future
from src.loaders import EquityHoldingsLoader

# Create extractor
extractor = HDFCExtractor()

# Extract data
metadata, holdings = extractor.extract_all("hdfc_jan_2025.xlsx")

# Load to database
loader = EquityHoldingsLoader()
snapshot_id = loader.load_scheme_month(
    amc_name=extractor.get_amc_name(),
    scheme_name=metadata['scheme_name'],
    plan_type=metadata['plan_type'],
    option_type=metadata['option_type'],
    year=metadata['year'],
    month=metadata['month'],
    holdings_data=holdings
)
```

---

## Design Principles

1. **One extractor per AMC**: Each AMC has unique file formats
2. **Inherit from BaseExtractor**: Ensures consistent interface
3. **Validate early**: Use canonical validators before returning data
4. **Normalize in extractor**: Convert units, clean names, etc.
5. **Fail loudly**: Raise clear errors for ambiguous data

---

## Next Steps

AMC-specific extractors will be implemented in Step 5 (bulk data ingestion).
