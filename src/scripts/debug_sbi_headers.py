import pandas as pd
from pathlib import Path
from src.extractors.sbi_extractor_v1 import SBIExtractorV1

def debug_headers():
    file_path = Path("data/output/merged excels/sbi/2025/CONSOLIDATED_SBI_2025_12.xlsx")
    if not file_path.exists():
        print("File not found")
        return

    extractor = SBIExtractorV1()
    xls = pd.ExcelFile(file_path, engine='openpyxl')
    
    # Check specific sheets
    target_sheets = [s for s in xls.sheet_names if "SMEEF" in s or "SBLUECHI" in s]
    
    for sheet in target_sheets:
        print(f"\n--- Processing {sheet} ---")
        try:
            df = pd.read_excel(xls, sheet_name=sheet, header=None)
            header_idx = extractor.find_header_row(df, keywords=["ISIN", "INSTRUMENT"])
            
            if header_idx != -1:
                print(f"Header Row Index: {header_idx}")
                # Read actual headers
                df_headers = pd.read_excel(xls, sheet_name=sheet, skiprows=header_idx, nrows=0)
                print("Found Headers:", df_headers.columns.tolist())
            else:
                print("Header NOT FOUND")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    debug_headers()
