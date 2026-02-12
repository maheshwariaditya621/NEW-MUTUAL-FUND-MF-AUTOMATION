"""
Test filtering logic for ICICI data.
"""
import pandas as pd
from pathlib import Path

file_path = Path("data/output/merged excels/icici/2025/CONSOLIDATED_ICICI_2025_12.xlsx")
xls = pd.ExcelFile(file_path, engine='openpyxl')

# Test with first sheet
sheet_name = xls.sheet_names[0]
print(f"Testing sheet: {sheet_name}\n")

# Read with header
df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=30)
header_idx = 3  # From previous test

df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)

# Apply column mapping
column_mapping = {
    "Company/Issuer/Instrument Name": "security_name",
    "ISIN": "isin",
    "Industry/Rating": "sector",
    "Quantity": "quantity",
    "Exposure/Market Value(Rs.Lakh)": "market_value",
    "% to Nav": "percent_to_nav"
}

mapped_cols = {}
for col in df.columns:
    for key, value in column_mapping.items():
        if key in str(col):
            mapped_cols[col] = value
            break

df = df.rename(columns=mapped_cols)

print(f"Total rows before filtering: {len(df)}")
print(f"\nFirst 15 rows:")
print(df[['security_name', 'isin', 'market_value']].head(15))

# Test filter_equity_isins logic
print(f"\n{'='*60}")
print("Testing filter_equity_isins logic:")
print(f"{'='*60}")

# Check ISIN pattern
import re
isin_pattern = re.compile(r'^IN[A-Z0-9]{10}$')

print(f"\nISIN validation:")
for idx, row in df.head(15).iterrows():
    isin = row.get('isin')
    is_valid = bool(isin and isinstance(isin, str) and isin_pattern.match(isin))
    print(f"  Row {idx}: ISIN='{isin}' → Valid={is_valid}")

# Apply filter
equity_df = df[df['isin'].notna()]
print(f"\nAfter notna() filter: {len(equity_df)} rows")

equity_df = equity_df[equity_df['isin'].astype(str).str.match(r'^IN[A-Z0-9]{10}$', na=False)]
print(f"After ISIN pattern filter: {len(equity_df)} rows")

print(f"\nFiltered data (first 10):")
print(equity_df[['security_name', 'isin', 'market_value']].head(10))
