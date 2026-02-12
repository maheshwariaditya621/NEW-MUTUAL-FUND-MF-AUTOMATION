"""
Extract actual company names from ICICI Excel for ISINs with 'N/A' in isin_master.
"""
import pandas as pd
from pathlib import Path

file_path = Path("data/output/merged excels/icici/2025/CONSOLIDATED_ICICI_2025_12.xlsx")
xls = pd.ExcelFile(file_path, engine='openpyxl')

# ISINs to check
target_isins = [
    'INE034S01021',
    'INE258B01022',
    'INE955V01021',
    'INE163A01018',
    'INE910A01012'
]

print("Searching for company names in ICICI Excel:\n")

found_names = {}

for sheet_name in xls.sheet_names:
    try:
        # Read sheet
        df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=50)
        
        # Find header
        header_idx = -1
        for idx, row in df_raw.iterrows():
            row_str = ' '.join([str(v) for v in row.values if pd.notna(v)]).upper()
            if 'ISIN' in row_str and ('COMPANY' in row_str or 'ISSUER' in row_str):
                header_idx = idx
                break
        
        if header_idx == -1:
            continue
        
        # Read with header
        df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
        
        # Map columns
        column_mapping = {
            "Company/Issuer/Instrument Name": "security_name",
            "ISIN": "isin"
        }
        
        mapped_cols = {}
        for col in df.columns:
            for key, value in column_mapping.items():
                if key in str(col):
                    mapped_cols[col] = value
                    break
        
        df = df.rename(columns=mapped_cols)
        
        if 'isin' not in df.columns or 'security_name' not in df.columns:
            continue
        
        # Check for target ISINs
        for isin in target_isins:
            if isin in found_names:
                continue
            
            matches = df[df['isin'] == isin]
            if not matches.empty:
                company_name = matches.iloc[0]['security_name']
                if pd.notna(company_name) and str(company_name).strip() not in ['', 'N/A', 'nan']:
                    found_names[isin] = str(company_name).strip()
                    print(f"  ✅ {isin}: '{company_name}'")
    
    except Exception as e:
        continue

print(f"\nFound {len(found_names)}/{len(target_isins)} company names")

if len(found_names) < len(target_isins):
    print("\nMissing ISINs:")
    for isin in target_isins:
        if isin not in found_names:
            print(f"  ❌ {isin}")
