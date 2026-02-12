import pandas as pd
import re
from pathlib import Path

def audit_icici(file_path):
    print(f"Auditing file: {file_path}")
    xls = pd.ExcelFile(file_path, engine='openpyxl')
    
    mapping = {
        "COMPANY": "company_name",
        "ISSUER": "company_name",
        "INSTRUMENT": "company_name",
        "ISIN": "isin"
    }
    
    results = []
    
    for sheet_name in xls.sheet_names:
        print(f"\n--- Sheet: {sheet_name} ---")
        df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=20)
        
        # Header finding logic (replicated from BaseExtractor)
        header_idx = -1
        found_isin = False
        secondary_keywords = ["INSTRUMENT", "ISSUER", "COMPANY", "NAME OF THE"]
        
        for i in range(min(15, len(df_raw))):
            row_values = [str(val).upper().strip() for val in df_raw.iloc[i].values if not pd.isna(val)]
            has_isin = any("ISIN" in val for val in row_values)
            has_secondary = any(any(skw in val for skw in secondary_keywords) for val in row_values)
            
            if has_isin and has_secondary:
                header_idx = i
                found_isin = True
                break
        
        if header_idx == -1:
            print("  [ERROR] Header not found!")
            continue
            
        header_row = [str(c).strip() for c in df_raw.iloc[header_idx].values]
        print(f"  Header Row (idx {header_idx}): {header_row}")
        
        # Check mapping
        mapped_cols = {}
        for col in header_row:
            col_upper = col.upper()
            for pattern, canonical in mapping.items():
                if pattern in col_upper:
                    mapped_cols[canonical] = col
                    break
        
        print(f"  Mapped: {mapped_cols}")
        
        if "isin" not in mapped_cols:
             print("  [ERROR] ISIN mapping FAILED")
        if "company_name" not in mapped_cols:
             print("  [ERROR] Company mapping FAILED")
             
        # Look at first few rows of data
        df_data = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx, nrows=5)
        print("  Sample Data (First 5 rows):")
        print(df_data.to_string(index=False))

if __name__ == "__main__":
    icici_path = r"d:\CODING\NEW MUTUAL FUND MF AUTOMATION\data\output\merged excels\icici\2025\CONSOLIDATED_ICICI_2025_12.xlsx"
    audit_icici(icici_path)
