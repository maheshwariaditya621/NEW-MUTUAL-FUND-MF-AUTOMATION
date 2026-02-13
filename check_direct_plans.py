"""
Check if there are Direct Plan sheets being missed
"""

import pandas as pd
from pathlib import Path

file_path = Path("data/output/merged excels/axis/2025/CONSOLIDATED_AXIS_2025_12.xlsx")

xls = pd.ExcelFile(file_path, engine='openpyxl')

print("=" * 80)
print("CHECKING FOR DIRECT PLAN SCHEMES")
print("=" * 80)
print(f"Total Sheets: {len(xls.sheet_names)}")
print()

direct_plan_sheets = []
regular_plan_sheets = []
unclear_sheets = []

for sheet_name in xls.sheet_names:
    # Skip index sheet
    if "index" in sheet_name.lower():
        continue
    
    # Read first row to get scheme name
    df = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=1)
    
    if len(df.columns) > 1:
        scheme_name = df.iloc[0, 1]
        if pd.notna(scheme_name):
            scheme_str = str(scheme_name).upper()
            
            if "DIRECT" in scheme_str:
                direct_plan_sheets.append((sheet_name, str(scheme_name)))
            elif "REGULAR" in scheme_str:
                regular_plan_sheets.append((sheet_name, str(scheme_name)))
            else:
                # Check if it contains "PLAN" but neither Direct nor Regular
                if "PLAN" in scheme_str or "FUND" in scheme_str:
                    unclear_sheets.append((sheet_name, str(scheme_name)))

print(f"Sheets with DIRECT PLAN: {len(direct_plan_sheets)}")
print(f"Sheets with REGULAR PLAN: {len(regular_plan_sheets)}")
print(f"Sheets with unclear plan type: {len(unclear_sheets)}")
print()

if direct_plan_sheets:
    print("=" * 80)
    print("DIRECT PLAN SHEETS")
    print("=" * 80)
    for sheet, scheme in direct_plan_sheets[:20]:
        print(f"  {sheet[:45]:45s} | {scheme[:60]}")
    if len(direct_plan_sheets) > 20:
        print(f"  ... and {len(direct_plan_sheets) - 20} more")

print()
if unclear_sheets:
    print("=" * 80)
    print("UNCLEAR PLAN TYPE (first 20)")
    print("=" * 80)
    for sheet, scheme in unclear_sheets[:20]:
        print(f"  {sheet[:45]:45s} | {scheme[:60]}")
    if len(unclear_sheets) > 20:
        print(f"  ... and {len(unclear_sheets) - 20} more")
