import pandas as pd
from pathlib import Path

def check():
    file_path = Path("data/output/merged excels/sbi/2025/CONSOLIDATED_SBI_2025_12.xlsx")
    xls = pd.ExcelFile(file_path, engine='openpyxl')
    
    targets = [
        "All Schemes as on 31st SIA US E", # US Specific
        "All Schemes as on 31st SETFGOLD", # Gold ETF
        "All Schemes as on 31st SGF",      # Gold Fund
        "All Schemes as on 31st SETFSILV", # Silver ETF
        "All Schemes as on 31st SETF (1)"  # Silver ETF FoF
    ]
    
    print(f"Checking {len(targets)} specific schemes for content...")
    print("="*60)
    
    for sheet in targets:
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
            print(f"\n--- Sheet: {sheet} ---")
            # Flatten text
            text = " ".join(df.astype(str).values.flatten())
            
            # Check for ISIN patterns
            has_ine = "INE" in text
            has_inf = "INF" in text
            print(f"Contains 'INE': {has_ine}")
            print(f"Contains 'INF': {has_inf}")
            
            # Print sample rows just in case
            print("Row Sample:")
            print(df.iloc[10:15])
            
        except Exception as e:
            print(f"Error reading {sheet}: {e}")

if __name__ == "__main__":
    check()
