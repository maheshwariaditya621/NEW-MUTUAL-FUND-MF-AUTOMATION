from src.db.connection import get_cursor, close_connection

def main():
    try:
        cursor = get_cursor()

        # Define filters
        amc_name = 'SBI Mutual Fund'
        year = 2025
        month = 12

        print(f"--- SUMMARY FOR {amc_name} ({year}-{month:02d}) ---")

        # 1. Total Unique ISIN Found
        cursor.execute("""
            SELECT COUNT(DISTINCT c.isin)
            FROM equity_holdings eh
            JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            JOIN amcs a ON s.amc_id = a.amc_id
            JOIN periods p ON ss.period_id = p.period_id
            JOIN companies c ON eh.company_id = c.company_id
            WHERE a.amc_name = %s AND p.year = %s AND p.month = %s
        """, (amc_name, year, month))
        total_unique_isin = cursor.fetchone()[0]
        print(f"Total Unique ISIN Found: {total_unique_isin}")

        # 2. Total Equity Holdings (Records)
        cursor.execute("""
            SELECT COUNT(eh.holding_id)
            FROM equity_holdings eh
            JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            JOIN amcs a ON s.amc_id = a.amc_id
            JOIN periods p ON ss.period_id = p.period_id
            WHERE a.amc_name = %s AND p.year = %s AND p.month = %s
        """, (amc_name, year, month))
        total_holdings_count = cursor.fetchone()[0]
        print(f"Total Equity Holdings (Records): {total_holdings_count}")

        # 3. List of Schemes with Holding Count and Total Market Value
        print("\n--- SCHEME DETAILS ---")
        print(f"{'Scheme Name':<60} | {'Holdings':<10} | {'Total Value (INR)':<20}")
        print("-" * 95)

        cursor.execute("""
            SELECT s.scheme_name, COUNT(eh.holding_id) as holding_count, SUM(eh.market_value_inr) as total_value
            FROM schemes s
            JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
            JOIN periods p ON ss.period_id = p.period_id
            JOIN amcs a ON s.amc_id = a.amc_id
            LEFT JOIN equity_holdings eh ON ss.snapshot_id = eh.snapshot_id
            WHERE a.amc_name = %s AND p.year = %s AND p.month = %s
            GROUP BY s.scheme_name
            ORDER BY s.scheme_name
        """, (amc_name, year, month))

        schemes = cursor.fetchall()
        for scheme in schemes:
            name = scheme[0]
            count = scheme[1]
            value = scheme[2] if scheme[2] else 0
            # Simple number formatting for value
            formatted_value = f"{value:,.2f}"
            print(f"{name:<60} | {count:<10} | {formatted_value:<20}")
        
        print("-" * 95)
        print(f"Total Schemes Found: {len(schemes)}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        close_connection()

if __name__ == "__main__":
    main()
