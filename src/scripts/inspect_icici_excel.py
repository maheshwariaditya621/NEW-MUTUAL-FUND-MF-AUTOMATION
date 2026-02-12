import pandas as pd
import os

file_path = r"d:\CODING\NEW MUTUAL FUND MF AUTOMATION\data\output\merged excels\icici\2025\CONSOLIDATED_ICICI_2025_12.xlsx"

if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
else:
    try:
        xls = pd.ExcelFile(file_path)
        print(f"Sheet Names ({len(xls.sheet_names)}):")
        print(xls.sheet_names[:10])  # Print first 10 sheets
        
        first_sheet = xls.sheet_names[0]
        print(f"\nInspeting Sheet: {first_sheet}")
        
        df = pd.read_excel(xls, sheet_name=first_sheet, header=None, nrows=20)
        print(df.to_string())
        
    except Exception as e:
        print(f"Error reading Excel: {e}")
