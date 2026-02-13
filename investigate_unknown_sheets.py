"""
Investigate the "Unknown" sheets in detail
"""

import pandas as pd
from pathlib import Path

file_path = Path("data/output/merged excels/axis/2025/CONSOLIDATED_AXIS_2025_12.xlsx")

xls = pd.ExcelFile(file_path, engine='openpyxl')

unknown_sheets = [
    "%20Portfolio %% AXISASD",
    "%20Portfolio %% AXISEFOF",
    "%20Portfolio %% AXISETS",
    "%20Portfolio %% AXISFLO",
    "%20Portfolio %% AXISGETF",
    "%20Portfolio %% AXISGIF",
    "%20Portfolio %% AXISGLD",
    "%20Portfolio %% AXISGSP",
    "%20Portfolio %% AXISIAP",
    "%20Portfolio %% AXISMAF",
    "%20Portfolio %% AXISONF",
    "%20Portfolio %% AXISSDL",
    "%20Portfolio %% AXISSIL",
    "%20Portfolio %% AXISTAA",
    "%20Portfolio %% AXISUSF"
]

print("=" * 80)
print("INVESTIGATING UNKNOWN SHEETS")
print("=" * 80)
print()

for sheet_name in unknown_sheets:
    if sheet_name not in xls.sheet_names:
        continue
        
    print("=" * 80)
    print(f"SHEET: {sheet_name}")
    print("=" * 80)
    
    # Read first 10 rows
    df = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=10)
    
    # Show scheme name
    if len(df.columns) > 1 and len(df) > 0:
        scheme_name = df.iloc[0, 1]
        print(f"Scheme Name (Row 0, Col 1): {scheme_name}")
    
    print()
    print("First 10 rows:")
    for row_idx in range(min(10, len(df))):
        print(f"\nRow {row_idx}:")
        for col_idx in range(min(6, len(df.columns))):
            cell_value = df.iloc[row_idx, col_idx]
            if pd.notna(cell_value):
                print(f"  Col {col_idx}: {str(cell_value)[:70]}")
    
    print()
