import pandas as pd
from src.db.connection import get_cursor, close_connection, get_connection
import sys

# Redirect stdout to a file for safe reading
sys.stdout = open('analytics_verification_output.txt', 'w', encoding='utf-8')

def run_checks():
    conn = get_connection()
    
    print("\n" + "="*50)
    print("CHECK 1: SECTOR DISTRIBUTION (Sample Schemes)")
    print("="*50)
    # HDFC names are cleaned (e.g., "Flexi Cap")
    # SBI names are full (e.g., "SBI Bluechip Fund")
    query_sector = """
    SELECT 
        s.scheme_name,
        c.sector,
        SUM(h.percent_of_nav) as total_exposure
    FROM equity_holdings h
    JOIN scheme_snapshots ss ON h.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN companies c ON h.company_id = c.company_id
    JOIN periods p ON ss.period_id = p.period_id
    WHERE p.year = 2025 AND p.month = 12
        AND s.scheme_name IN ('Flexi Cap', 'SBI Large Cap Fund', 'Mid-Cap Opportunities', 'SBI Midcap Fund', 'Large and Mid Cap', 'SBI Large and Midcap Fund')
    GROUP BY s.scheme_name, c.sector
    ORDER BY s.scheme_name, total_exposure DESC
    """
    df_sector = pd.read_sql(query_sector, conn)
    
    if df_sector.empty:
        print("No sector data found! Checking random scheme...")
        # fallback to check any scheme
        q_any = "SELECT s.scheme_name FROM scheme_snapshots ss JOIN schemes s ON ss.scheme_id = s.scheme_id LIMIT 5"
        print(pd.read_sql(q_any, conn))
    
    for scheme in df_sector['scheme_name'].unique():
        print(f"\n--- {scheme} ---")
        print(df_sector[df_sector['scheme_name'] == scheme].head(5).to_string(index=False))

    print("\n" + "="*50)
    print("CHECK 2: STOCK EXPOSURE (HDFC Bank Ltd.)")
    print("="*50)
    query_stock = """
    SELECT 
        a.amc_name,
        COUNT(DISTINCT s.scheme_id) as scheme_count,
        SUM(h.quantity) as total_shares,
        SUM(h.market_value_inr) / 10000000 as total_value_cr
    FROM equity_holdings h
    JOIN companies c ON h.company_id = c.company_id
    JOIN scheme_snapshots ss ON h.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    JOIN periods p ON ss.period_id = p.period_id
    WHERE c.company_name ILIKE '%HDFC Bank%' 
      AND p.year = 2025 AND p.month = 12
    GROUP BY a.amc_name
    """
    df_stock = pd.read_sql(query_stock, conn)
    print(df_stock.to_string(index=False))

    print("\n" + "="*50)
    print("CHECK 3: MONTH CONTINUITY (Nov vs Dec 2025)")
    print("="*50)
    # Pick a scheme that should exist in both months
    # HDFC name: "Balanced Advantage" or "Flexi Cap"
    scheme_check = "Flexi Cap" 
    query_continuity = """
    SELECT 
        p.year,
        p.month,
        ss.total_holdings,
        ss.total_value_inr / 10000000 as aum_cr,
        count(h.holding_id) as actual_rows
    FROM scheme_snapshots ss
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN periods p ON ss.period_id = p.period_id
    LEFT JOIN equity_holdings h ON ss.snapshot_id = h.snapshot_id
    WHERE s.scheme_name = %s
      AND p.year = 2025 AND p.month IN (11, 12)
    GROUP BY p.year, p.month, ss.total_holdings, ss.total_value_inr
    ORDER BY p.month
    """
    df_cont = pd.read_sql(query_continuity, conn, params=(scheme_check,))
    print(f"Scheme: {scheme_check}")
    print(df_cont.to_string(index=False))

    print("\n" + "="*50)
    print("CHECK 4: TOP 10 HOLDINGS (Random Verification)")
    print("="*50)
    
    schemes_to_check = ["SBI Large Cap Fund", "Large and Mid Cap"] # One SBI (Bluechip equivalent), One HDFC
    
    for scheme in schemes_to_check:
        print(f"\n--- {scheme} (Top 10) ---")
        query_top10 = """
        SELECT 
            c.company_name,
            h.quantity,
            h.market_value_inr / 10000000 as val_cr,
            h.percent_of_nav
        FROM equity_holdings h
        JOIN scheme_snapshots ss ON h.snapshot_id = ss.snapshot_id
        JOIN schemes s ON ss.scheme_id = s.scheme_id
        JOIN companies c ON h.company_id = c.company_id
        JOIN periods p ON ss.period_id = p.period_id
        WHERE s.scheme_name = %s
          AND p.year = 2025 AND p.month = 12
        ORDER BY h.market_value_inr DESC
        LIMIT 10
        """
        df_top10 = pd.read_sql(query_top10, conn, params=(scheme,))
        print(df_top10.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    run_checks()
