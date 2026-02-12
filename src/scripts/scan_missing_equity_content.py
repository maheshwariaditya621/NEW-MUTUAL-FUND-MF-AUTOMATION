import pandas as pd
from pathlib import Path
from src.db.connection import get_cursor, close_connection
from src.config.constants import AMC_SBI

def scan_content():
    # 1. Get DB Schemes
    cursor = get_cursor()
    cursor.execute("""
        SELECT DISTINCT s.scheme_name
        FROM schemes s
        JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
        JOIN periods p ON ss.period_id = p.period_id
        JOIN amcs a ON s.amc_id = a.amc_id
        WHERE a.amc_name = %s
          AND p.year = 2025 AND p.month = 12
    """, (AMC_SBI,))
    db_schemes = set(r[0] for r in cursor.fetchall())
    
    file_path = Path("data/output/merged excels/sbi/2025/CONSOLIDATED_SBI_2025_12.xlsx")
    xls = pd.ExcelFile(file_path, engine='openpyxl')
    
    print(f"Scanning {len(xls.sheet_names)} sheets for un-extracted EQUITY...")
    print("="*60)
    
    missing_with_equity = []
    
    for sheet in xls.sheet_names:
        # Resolve Name
        try:
            df_head = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=5)
            p1 = str(df_head.iloc[2, 2]).strip() if df_head.shape[0] > 2 and df_head.shape[1] > 2 else ""
            p2 = str(df_head.iloc[2, 3]).strip() if df_head.shape[0] > 2 and df_head.shape[1] > 3 else ""
            full_name = f"{p1} {p2}".strip().replace("nan", "").replace("None", "").replace("SCHEME NAME", "").replace(":", "").strip()
        except:
            full_name = "Unknown"
            
        if full_name in db_schemes:
            continue
            
        # This sheet is MISSING from DB. Check content.
        # Read full sheet
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
            # Flatten to verify content
            all_text = " ".join(df.astype(str).values.flatten())
            
            # Check for INE
            has_ine = "INE" in all_text
            
            # Detailed check: Find valid ISINs in any column
            found_isins = []
            for col in df.columns:
                for val in df[col].astype(str):
                    val = val.strip().upper()
                    if len(val) == 12 and val.startswith("INE"):
                        # Check security code
                        if val[8:10] == "10":
                            found_isins.append(val)
            
            unique_isins = list(set(found_isins))
            
            if unique_isins:
                print(f"[FOUND EQUITY] Sheet: {sheet} | Name: {full_name}")
                print(f"  -> Found {len(unique_isins)} valid Equity ISINs (INE...10)")
                print(f"  -> Sample: {unique_isins[:3]}")
                missing_with_equity.append(full_name)
            else:
                 # Check for Foreign Equity identifiers or loose INE
                 count_loose_ine = all_text.count("INE")
                 if count_loose_ine > 0:
                     print(f"[WARN] Sheet: {sheet} | Name: {full_name} has 'INE' string but passed strict filter.")
        except Exception as e:
            print(f"Error reading {sheet}: {e}")

    print("\nSUMMARY:")
    print(f"Total Missing Sheets with Equity Content: {len(missing_with_equity)}")
    for s in missing_with_equity:
        print(f"  - {s}")
        
    close_connection()

if __name__ == "__main__":
    scan_content()
