import os
from pathlib import Path
from src.db import get_cursor

def check_status():
    slugs = ['abakkus', 'boi', 'edelweiss', 'hsbc', 'iti', 'samco']
    with get_cursor() as cur:
        cur.execute("SELECT amc_id, amc_name FROM amcs WHERE amc_name ILIKE '%bank of india%' OR amc_name ILIKE '%boi%'")
        print("Bank of India in amcs:", cur.fetchall())

        cur.execute("""
            SELECT a.amc_id, a.amc_name, COUNT(DISTINCT s.snapshot_id) as snapshots, COUNT(h.snapshot_id) as holdings
            FROM amcs a
            LEFT JOIN schemes sc ON a.amc_id = sc.amc_id
            LEFT JOIN scheme_snapshots s ON sc.scheme_id = s.scheme_id AND s.period_id = 1414
            LEFT JOIN equity_holdings h ON s.snapshot_id = h.snapshot_id
            WHERE a.amc_name ILIKE ANY(ARRAY['%abakkus%', '%bank of india%', '%edelweiss%', '%hsbc%', '%iti%', '%samco%'])
            GROUP BY a.amc_id, a.amc_name
            ORDER BY a.amc_name;
        """)
        print("\n--- Current DB Snapshots (Period 1414) ---")
        for r in cur.fetchall():
            print(f"AMC {r[0]}: {r[1]} -> {r[2]} snapshots, {r[3]} holdings")

    print("\n--- Files on Server ---")
    base_merged = Path("data/output/merged excels")
    base_downloads = Path("data/downloads")
    for s in slugs:
        merged_file = base_merged / s / "2026" / f"CONSOLIDATED_{s.upper()}_2026_09.xlsx"
        # also check alternative naming
        downloads_dir = base_downloads / s / "2026" / "09"
        m_exists = merged_file.exists()
        d_count = len(list(downloads_dir.glob("*"))) if downloads_dir.exists() else 0
        print(f"[{s}] Merged file exists: {m_exists} ({merged_file}) | Downloads count: {d_count}")

if __name__ == "__main__":
    check_status()
