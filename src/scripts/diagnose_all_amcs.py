import pandas as pd
import os
import sys
from pathlib import Path

AMCS = {
    "icici": r"icici\2025\CONSOLIDATED_ICICI_2025_12.xlsx",
    "nippon": r"nippon\2025\CONSOLIDATED_NIPPON_2025_12.xlsx",
    "axis": r"axis\2025\CONSOLIDATED_AXIS_2025_12.xlsx",
    "kotak": r"kotak\2025\CONSOLIDATED_KOTAK_2025_12.xlsx",
    "absl": r"absl\2025\CONSOLIDATED_ABSL_2025_12.xlsx",
    "uti": r"uti\2025\CONSOLIDATED_UTI_2025_12.xlsx",
    "mirae_asset": r"mirae_asset\2025\CONSOLIDATED_MIRAE_ASSET_2025_12.xlsx"
}

BASE_PATH = Path(r"d:\CODING\NEW MUTUAL FUND MF AUTOMATION\data\output\merged excels")
DIAG_DIR = Path(r"d:\CODING\NEW MUTUAL FUND MF AUTOMATION\diagnostics")
DIAG_DIR.mkdir(exist_ok=True)

def find_header_row(df, keywords=["ISIN"]):
    for i in range(min(25, len(df))): # Increased to 25
        row_values = [str(val).upper().strip() for val in df.iloc[i].values if not pd.isna(val)]
        for val in row_values:
            if any(kw.upper() in val for kw in keywords):
                return i
    return -1

def diagnose_amc(amc_name, relative_path):
    file_path = BASE_PATH / relative_path
    output_file = DIAG_DIR / f"{amc_name}_diag.txt"
    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"{'='*80}\n")
        f.write(f"DIAGNOSING AMC: {amc_name.upper()}\n")
        f.write(f"File: {file_path}\n")
        
        if not file_path.exists():
            f.write(f"MISSING FILE: {file_path}\n")
            return

        try:
            xls = pd.ExcelFile(file_path)
            sheet_names = xls.sheet_names
            f.write(f"Total Sheets: {len(sheet_names)}\n")
            
            # For UTI, there is only one sheet, but let's check it thoroughly
            test_indices = [0, len(sheet_names)//2] if len(sheet_names) > 1 else [0]
            
            for idx in test_indices:
                sheet = sheet_names[idx]
                f.write(f"\n--- Sheet [{idx}]: {sheet} ---\n")
                df_raw = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=50) # Increased to 50
                
                h_idx = find_header_row(df_raw)
                if h_idx != -1:
                    f.write(f"Header found at row: {h_idx}\n")
                    header = df_raw.iloc[h_idx].tolist()
                    f.write(f"RAW HEADER: {header}\n")
                    data = df_raw.iloc[h_idx+1:h_idx+15]
                    f.write("SAMPLE DATA:\n")
                    # Use to_csv for more readable data structure analysis
                    f.write(data.to_string(index=False, header=False) + "\n")
                else:
                    f.write("HEADER NOT FOUND\n")
                    f.write(df_raw.head(30).to_string(index=False, header=False) + "\n")
                
        except Exception as e:
            f.write(f"ERROR: {e}\n")
    
    print(f"Generated diagnostics for {amc_name} -> {output_file}")

if __name__ == "__main__":
    for amc, rel_path in AMCS.items():
        diagnose_amc(amc, rel_path)
