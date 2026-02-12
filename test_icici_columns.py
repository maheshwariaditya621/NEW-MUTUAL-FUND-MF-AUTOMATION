"""
Test ICICI column mapping by reading a sample sheet.
"""
import pandas as pd
from pathlib import Path

file_path = Path("data/output/merged excels/icici/2025/CONSOLIDATED_ICICI_2025_12.xlsx")
xls = pd.ExcelFile(file_path, engine='openpyxl')

# Test with first sheet
sheet_name = xls.sheet_names[0]
print(f"Testing sheet: {sheet_name}\n")

# Read first 30 rows
df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=30)

# Find header row (look for "ISIN")
header_idx = -1
for idx, row in df_raw.iterrows():
    row_str = ' '.join([str(v) for v in row.values if pd.notna(v)]).upper()
    if 'ISIN' in row_str and ('COMPANY' in row_str or 'ISSUER' in row_str or 'INSTRUMENT' in row_str):
        header_idx = idx
        print(f"Header found at row {idx}")
        print(f"Header values: {row.values}")
        break

if header_idx == -1:
    print("No header found!")
else:
    # Read with proper header
    df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
    print(f"\nOriginal columns:")
    for i, col in enumerate(df.columns):
        print(f"  {i}: {col}")
    
    # Apply column mapping
    column_mapping = {
        "Company/Issuer/Instrument Name": "security_name",
        "ISIN": "isin",
        "Industry/Rating": "sector",
        "Quantity": "quantity",
        "Exposure/Market Value(Rs.Lakh)": "market_value",
        "% to Nav": "percent_to_nav"
    }
    
    # Map columns
    mapped_cols = {}
    for col in df.columns:
        for key, value in column_mapping.items():
            if key in str(col):
                mapped_cols[col] = value
                break
    
    df = df.rename(columns=mapped_cols)
    
    print(f"\nMapped columns:")
    for i, col in enumerate(df.columns):
        print(f"  {i}: {col}")
    
    # Check if security_name exists
    if 'security_name' in df.columns:
        print(f"\n✅ security_name column found!")
        print(f"\nFirst 5 security_name values:")
        for i, val in enumerate(df['security_name'].head(10)):
            print(f"  {i}: {val}")
    else:
        print(f"\n❌ security_name column NOT found!")
        print(f"Available columns: {df.columns.tolist()}")
