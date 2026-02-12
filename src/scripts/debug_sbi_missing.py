import pandas as pd
from pathlib import Path
from src.db.connection import get_cursor, close_connection
from src.config.constants import AMC_SBI

def debug_missing_schemes():
    # 1. Get DB Schemes for Dec 2025
    cursor = get_cursor()
    cursor.execute("""
        SELECT DISTINCT s.scheme_name
        FROM schemes s
        JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
        JOIN periods p ON ss.period_id = p.period_id
        JOIN amcs a ON s.amc_id = a.amc_id
        WHERE a.amc_name = %s
          AND p.year = 2025 AND p.month = 12
        ORDER BY s.scheme_name
    """, (AMC_SBI,))
    db_schemes = set(r[0] for r in cursor.fetchall())
    print(f"DB Schemes (Dec 2025): {len(db_schemes)}")

    # 2. Get Raw Excel Sheets
    file_path = Path("data/output/merged excels/sbi/2025/CONSOLIDATED_SBI_2025_12.xlsx")
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return

    print(f"Reading Excel: {file_path}")
    xls = pd.ExcelFile(file_path, engine='openpyxl')
    raw_sheets = xls.sheet_names
    print(f"Raw Excel Sheets: {len(raw_sheets)}")
    
    # 3. Analyze Discrepancy
    # Note: Raw sheets might be "All Schemes as on...", need simple matching or manual check
    print("\n--- MISSING / SKIPPED ANALYSIS ---")
    
    # Heuristic: Check if raw sheet (normalized) exists in DB
    # We'll just look for keyword matches since naming differs
    found_count = 0
    missed_sheets = []
    
    for sheet in raw_sheets:
        # Simple normalization for matching check
        norm_sheet = sheet.replace("All Schemes as on 31st", "").replace("All Schemes as on 30th", "").strip()
        
        # Fuzzy match attempt
        is_found = False
        for db_s in db_schemes:
            # If the DB name contains the sheet code or substantial part
            # This is tricky because DB names are "Real Names" (e.g. SBI Large Cap) 
            # and Sheet names are often codes (e.g. SBLUECHI) or partials.
            # But wait, the user says "67 schemes with EQUITY".
            pass
            
    # Better approach: List what IS in DB and what IS in File side-by-side
    print(f"\nDB List ({len(db_schemes)}):")
    for s in sorted(db_schemes):
        print(f"  [DB] {s}")
        
    print(f"\nRaw Sheets ({len(raw_sheets)}):")
    for s in sorted(raw_sheets):
        print(f"  [XLS] {s}")

    close_connection()

if __name__ == "__main__":
    debug_missing_schemes()
