"""
Check what ISINs are actually in the extracted data
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from src.extractors.axis_extractor_v1 import AxisExtractorV1
from src.utils.isin import is_valid_equity_isin

file_path = Path("data/output/merged excels/axis/2025/CONSOLIDATED_AXIS_2025_12.xlsx")

print("Extracting first scheme...")
extractor = AxisExtractorV1()
holdings = extractor.extract(str(file_path))

print(f"Total holdings: {len(holdings)}")
print()

# Check first 10 ISINs
print("First 10 ISINs from extraction:")
print("=" * 80)
for i, h in enumerate(holdings[:10]):
    isin = h.get('isin', '')
    is_valid = is_valid_equity_isin(isin)
    print(f"{i+1:2d}. ISIN: '{isin}' | Valid: {is_valid} | Company: {h.get('company_name', '')[:30]}")
    if not is_valid:
        print(f"    Type: {type(isin)} | Repr: {repr(isin)} | Len: {len(isin)}")
        if len(isin) == 12:
            print(f"    isin[8:10] = '{isin[8:10]}'")
