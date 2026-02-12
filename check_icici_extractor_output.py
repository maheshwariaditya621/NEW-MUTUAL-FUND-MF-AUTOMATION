"""
Check what actual data ICICI extractor is producing for company_name field.
"""
from src.extractors.icici_extractor_v1 import ICICIExtractorV1
from pathlib import Path

extractor = ICICIExtractorV1()
file_path = Path("data/output/merged excels/icici/2025/CONSOLIDATED_ICICI_2025_12.xlsx")

print("="*80)
print("ICICI EXTRACTOR OUTPUT ANALYSIS")
print("="*80)

# Extract first scheme
import pandas as pd
xls = pd.ExcelFile(file_path, engine='openpyxl')
sheet_name = xls.sheet_names[0]

print(f"\nExtracting from sheet: {sheet_name}")

holdings = extractor.extract(file_path)

print(f"\nTotal holdings extracted: {len(holdings)}")

# Check company_name field
print(f"\nSample holdings (first 10):")
for i, h in enumerate(holdings[:10]):
    print(f"\n  Holding {i+1}:")
    print(f"    ISIN: {h.get('isin')}")
    print(f"    company_name: '{h.get('company_name')}'")
    print(f"    security_name: '{h.get('security_name', 'N/A')}'")
    print(f"    sector: {h.get('sector')}")
    print(f"    market_value: {h.get('market_value_inr')}")

# Count N/A company names
na_count = sum(1 for h in holdings if h.get('company_name') == 'N/A')
print(f"\nHoldings with company_name='N/A': {na_count}/{len(holdings)}")

# Check specific ISINs
test_isins = ['INE034S01021', 'INE258B01022', 'INE955V01021']
print(f"\nChecking specific ISINs:")
for isin in test_isins:
    matches = [h for h in holdings if h.get('isin') == isin]
    if matches:
        h = matches[0]
        print(f"  {isin}: company_name='{h.get('company_name')}'")
    else:
        print(f"  {isin}: NOT FOUND in extracted holdings")

print("\n" + "="*80)
