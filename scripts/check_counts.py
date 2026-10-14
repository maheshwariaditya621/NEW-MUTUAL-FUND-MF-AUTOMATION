from src.db import get_cursor

def check():
    with get_cursor() as cur:
        cur.execute("""
            SELECT a.amc_id, a.amc_name, COUNT(DISTINCT s.snapshot_id) as snapshots, COUNT(h.snapshot_id) as holdings
            FROM amcs a
            JOIN schemes sc ON a.amc_id = sc.amc_id
            JOIN scheme_snapshots s ON sc.scheme_id = s.scheme_id
            LEFT JOIN equity_holdings h ON s.snapshot_id = h.snapshot_id
            WHERE s.period_id = 1414 AND a.amc_id IN (277, 23, 43, 63, 75, 139)
            GROUP BY a.amc_id, a.amc_name
            ORDER BY a.amc_name;
        """)
        print("--- 6 REQUESTED AMCS STATUS (PERIOD 1414) ---")
        for r in cur.fetchall():
            print(f"AMC ID {r[0]:<4} | {r[1]:<35}: {r[2]} snapshots, {r[3]} equity holdings")
        
        cur.execute("""
            SELECT COUNT(DISTINCT a.amc_id) as amc_count, COUNT(DISTINCT s.snapshot_id) as total_snapshots, COUNT(h.snapshot_id) as total_holdings
            FROM amcs a
            JOIN schemes sc ON a.amc_id = sc.amc_id
            JOIN scheme_snapshots s ON sc.scheme_id = s.scheme_id
            LEFT JOIN equity_holdings h ON s.snapshot_id = h.snapshot_id
            WHERE s.period_id = 1414;
        """)
        overall = cur.fetchone()
        print("=" * 60)
        print(f"OVERALL SEPTEMBER 2026 STATUS: {overall[0]} AMCs loaded | {overall[1]} snapshots | {overall[2]} equity holdings")
        print("=" * 60)

if __name__ == "__main__":
    check()
