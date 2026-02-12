import pandas as pd
from pathlib import Path
from src.db.connection import get_cursor, close_connection
from src.config.constants import AMC_SBI

def map_full_sheets():
    file_path = Path("data/output/merged excels/sbi/2025/CONSOLIDATED_SBI_2025_12.xlsx")
    xls = pd.ExcelFile(file_path, engine='openpyxl')
    
    output_file = "sbi_full_mapping.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"SBI SHEET MAPPING (Total Sheets: {len(xls.sheet_names)})\n")
        f.write("==================================================\n\n")
        
        for sheet in xls.sheet_names:
            try:
                # Read header
                df = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=5)
                
                # Logic from SBI Extractor
                p1 = str(df.iloc[2, 2]).strip() if df.shape[0] > 2 and df.shape[1] > 2 else ""
                p2 = str(df.iloc[2, 3]).strip() if df.shape[0] > 2 and df.shape[1] > 3 else ""
                
                p1 = p1.replace("nan", "").replace("None", "").strip()
                p2 = p2.replace("nan", "").replace("None", "").strip()
                
                full_name = f"{p1} {p2}".strip()
                full_name = full_name.replace("SCHEME NAME", "").replace(":", "").strip()
                
                f.write(f"Sheet: {sheet:<35} | Name: {full_name}\n")
                
            except Exception as e:
                 f.write(f"Sheet: {sheet:<35} | Name: ERROR {e}\n")

    print(f"Full mapping written to {output_file}")

if __name__ == "__main__":
    map_full_sheets()
