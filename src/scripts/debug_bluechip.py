import pandas as pd
from pathlib import Path
from src.extractors.sbi_extractor_v1 import SBIExtractorV1

def debug_bluechip():
    file_path = Path("data/output/merged excels/sbi/2025/CONSOLIDATED_SBI_2025_12.xlsx")
    extractor = SBIExtractorV1()
    xls = pd.ExcelFile(file_path, engine='openpyxl')
    
    # Try to find Bluechip sheet
    target = next((s for s in xls.sheet_names if "BLUE" in s.upper()), None)
    
    if target:
        print(f"Testing Sheet: {target}")
        
        # 1. Check Name Extraction
        print("\n--- Name Check (Row 2) ---")
        try:
            header_df = pd.read_excel(xls, sheet_name=target, header=None, nrows=5)
            # C3
            c3 = str(header_df.iloc[2, 2])
            print(f"C3: '{c3}'")
            # D3
            d3 = str(header_df.iloc[2, 3])
            print(f"D3: '{d3}'")
        except Exception as e:
            print(f"Name Error: {e}")
            
        # 2. Check Data Extraction
        print("\n--- Data Check ---")
        try:
            df = pd.read_excel(xls, sheet_name=target, header=None)
            header_idx = extractor.find_header_row(df, keywords=["ISIN", "INSTRUMENT"])
            print(f"Header Row Index: {header_idx}")
            
            if header_idx != -1:
                # Print found headers
                real_headers = pd.read_excel(xls, sheet_name=target, skiprows=header_idx, nrows=0).columns.tolist()
                print("Found Headers:", real_headers)
                
                # Check Mapping
                df_data = pd.read_excel(xls, sheet_name=target, skiprows=header_idx)
                # Mock mapping
                for col in df_data.columns:
                    col_upper = str(col).upper()
                    mapped_to = None
                    for pattern, can in extractor.column_mapping.items():
                        if pattern in col_upper:
                            mapped_to = can
                            break
                    if mapped_to:
                        mapped_cols.append(f"'{col}' -> {mapped_to}")
                
                print("Mapped Columns:")
                for m in mapped_cols:
                    print(m)
                    
                # Check for ISIN col
                df_mapped = extractor._map_columns(df_data)
                if "isin" in df_mapped.columns:
                    print("ISIN Column Found: YES")
                    # Check first row
                    print("First Row:", df_mapped.iloc[0].to_dict())
                else:
                    print("ISIN Column Found: NO")
            
        except Exception as e:
            print(f"Data Error: {e}")

    else:
        print("Bluechip sheet NOT FOUND in Excel")

if __name__ == "__main__":
    debug_bluechip()
