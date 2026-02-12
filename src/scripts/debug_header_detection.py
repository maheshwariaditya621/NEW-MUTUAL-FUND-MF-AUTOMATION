import pandas as pd
from pathlib import Path

def debug_headers():
    file_path = Path("data/output/merged excels/sbi/2025/CONSOLIDATED_SBI_2025_12.xlsx")
    xls = pd.ExcelFile(file_path, engine='openpyxl')
    
    # List of sheets that failed extraction but have equity
    problem_sheets = [
        "All Schemes as on 31st SBFS", # Banking & Financial
        "All Schemes as on 31st SLTAF II", # Long Term Advantage
        "All Schemes as on 31st SMCBF SP", # Children's Fund
        "All Schemes as on 31st SRBF AHP" # Retirement Benefit
    ]
    
    print(f"DEBUGGING HEADER DETECTION for {len(problem_sheets)} sheets...")
    print("="*60)
    
    for sheet in problem_sheets:
        print(f"\n--- Sheet: {sheet} ---")
        try:
            df = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=20)
            
            # Print first 10 rows to see structure
            print(df.head(15))
            
            # Simulate find_header_row logic
            keywords = ["ISIN"]
            found_at = -1
            for i in range(len(df)):
                row_values = [str(val).upper().strip() for val in df.iloc[i].values if not pd.isna(val)]
                # print(f"Row {i}: {row_values}")
                for val in row_values:
                    if any(kw.upper() in val for kw in keywords):
                        print(f"  [MATCH] Found keyword in Row {i}: {val}")
                        found_at = i
                        break
                if found_at != -1:
                    break
            
            if found_at == -1:
                print("  [FAIL] Header NOT detected.")
            else:
                print(f"  [PASS] Header detected at Row {found_at}")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    debug_headers()
