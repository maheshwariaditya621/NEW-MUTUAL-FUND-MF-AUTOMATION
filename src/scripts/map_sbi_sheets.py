import pandas as pd
from pathlib import Path
from src.db.connection import get_cursor, close_connection
from src.config.constants import AMC_SBI

def map_sheets():
    file_path = Path("data/output/merged excels/sbi/2025/CONSOLIDATED_SBI_2025_12.xlsx")
    xls = pd.ExcelFile(file_path, engine='openpyxl')
    
    print(f"Scanning {len(xls.sheet_names)} sheets in {file_path.name}...")
    
    sheet_map = []
    
    for sheet in xls.sheet_names:
        # Read header only
        try:
            # SBI V1 logic: Name is usually in C3 (Row 2, Col 2) or D3 (Row 2, Col 3)
            # We read first 5 rows
            df = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=5)
            
            # Robust Extraction Logic from SBIExtractorV1
            scheme_name = "Unknown"
            
            # 1. Try C3 + D3
            p1 = str(df.iloc[2, 2]).strip() if df.shape[0] > 2 and df.shape[1] > 2 else ""
            p2 = str(df.iloc[2, 3]).strip() if df.shape[0] > 2 and df.shape[1] > 3 else ""
            
            # Clean
            p1 = p1.replace("nan", "").replace("None", "").strip()
            p2 = p2.replace("nan", "").replace("None", "").strip()
            
            full_name = f"{p1} {p2}".strip()
            
            # Remove "SCHEME NAME :" prefix if present
            full_name = full_name.replace("SCHEME NAME", "").replace(":", "").strip()
            
            if full_name:
                scheme_name = full_name
            else:
                # Fallback to Cell 0,0 or similar if different format
                pass
                
            sheet_map.append({"sheet": sheet, "name": scheme_name})
            
        except Exception as e:
            sheet_map.append({"sheet": sheet, "name": f"Error: {e}"})

    # Get DB Schemes
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
    
    print("\n--- ANALYSIS ---")
    print(f"DB Count: {len(db_schemes)}")
    print(f"File Sheet Count: {len(sheet_map)}")
    
    missing_equity_candidates = []
    
    print("\n--- SHEETS NOT IN DB ---")
    for item in sheet_map:
        real_name = item["name"]
        if real_name not in db_schemes:
            # Filter check: Is it likely Equity?
            # Keywords: Fund, ETF, Equity, Index, Hybrid
            # Exclude: Liquid, Overnight, Gold, Bond
            
            is_likely_equity = any(x in real_name.lower() for x in ["equity", "index", "etf", "growth", "mid", "small", "flexi", "bluechip", "opportunities", "balanced"])
            is_likely_debt = any(x in real_name.lower() for x in ["liquid", "overnight", "gold", "bond", "debt", "gilt", "fixed maturity", "fmp"])
            
            if is_likely_equity and not is_likely_debt:
                print(f"[MISSING EQUITY?] Sheet: {item['sheet']} -> Name: {real_name}")
                missing_equity_candidates.append(real_name)
            else:
                # print(f"[Skipped Debt/Other] {real_name}")
                pass
                
    print(f"\nPotential Missing Equity Schemes: {len(missing_equity_candidates)}")
    for s in missing_equity_candidates:
        print(f"  - {s}")

    close_connection()

if __name__ == "__main__":
    map_sheets()
