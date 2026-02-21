import pandas as pd
import os
from src.db.connection import get_cursor
from src.extractors.abakkus_extractor_v1 import AbakkusExtractorV1

def final_audit():
    ext = AbakkusExtractorV1()
    cursor = get_cursor()
    
    # Paths
    raw_dec = "data/raw/abakkus/2025_12/Abakkus_MF_MONTHLY_PORTFOLIO_31_12_2025_562986311e.xlsx"
    raw_jan = "data/raw/abakkus/2026_01/Abakkus_MF_Portfolio_31_01_2026_9cd68f430c.xlsx"
    merged_dec = "data/output/merged excels/abakkus/2025/CONSOLIDATED_ABAKKUS_2025_12.xlsx"
    merged_jan = "data/output/merged excels/abakkus/2026/CONSOLIDATED_ABAKKUS_2026_01.xlsx"
    
    print("--- HOLDINGS COUNT SUMMARY ---")
    results = []
    for label, path in [("RAW_DEC", raw_dec), ("RAW_JAN", raw_jan), ("MERGED_DEC", merged_dec), ("MERGED_JAN", merged_jan)]:
        if os.path.exists(path):
            try:
                h = ext.extract(path)
                flexi = [x for x in h if "FLEXI" in x['scheme_name'].upper()]
                results.append((label, len(flexi)))
            except Exception as e:
                results.append((label, f"Error: {e}"))
        else:
            results.append((label, "Missing"))
            
    for r in results:
        print(f"{r[0]}: {r[1]}")
        
    print("\n--- DB SNAPSHOTS ---")
    cursor.execute("""
        SELECT p.year, p.month, sn.total_holdings, sn.total_value_inr, sn.snapshot_id
        FROM schemes s
        JOIN scheme_snapshots sn ON s.scheme_id = sn.scheme_id
        JOIN periods p ON sn.period_id = p.period_id
        WHERE s.amc_id = 277 AND s.scheme_name LIKE '%FLEXI%'
    """)
    for row in cursor.fetchall():
        print(row)

if __name__ == "__main__":
    final_audit()
