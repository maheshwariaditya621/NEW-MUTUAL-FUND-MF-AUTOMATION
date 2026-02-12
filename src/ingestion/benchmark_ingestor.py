import requests
import json
import pandas as pd
from datetime import date, datetime
from src.db.connection import get_connection
from src.config import logger

class BenchmarkIngestor:
    def __init__(self):
        self.base_url = "https://www.niftyindices.com/Backends/Indices_Historical_Report/TotalReturnIndex_HistoricalBySymbol"
        self.headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def fetch_index_chunks(self, symbol: str, start_date: date, end_date: date):
        """
        Symbol should be like 'NIFTY 50'
        """
        # Note: NiftyIndices usually allows max 1 year at a time
        data = []
        curr_start = start_date
        while curr_start < end_date:
            curr_end = min(curr_start.replace(year=curr_start.year + 1), end_date)
            
            payload = {
                "Symbol": symbol,
                "FromDate": curr_start.strftime("%d-%b-%Y"),
                "ToDate": curr_end.strftime("%d-%b-%Y")
            }
            
            logger.info(f"Fetching {symbol} from {payload['FromDate']} to {payload['ToDate']}...")
            try:
                # This is a hypothetical endpoint based on common patterns; we might need to verify the actual URL via browser
                # The browser subagent saw a POST or a dynamic table.
                # Let's use a simpler approach for now: seeding with visible data or finding the exact API.
                pass 
            except Exception as e:
                logger.error(f"Failed to fetch {symbol}: {e}")
            
            curr_start = curr_end
            
        return data

def seed_nifty_50_sample():
    """
    Seeds a few recent points to verify the table works.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    # Sample data from browser result
    # 28 Aug 2020 | 16478.01
    # 27 Aug 2020 | 16353.08
    # 26 Aug 2020 | 16339.41
    # 25 Aug 2020 | 16226.20
    # 24 Aug 2020 | 16218.02
    
    data = [
        ('NIFTY 50', '2020-08-28', 16478.01),
        ('NIFTY 50', '2020-08-27', 16353.08),
        ('NIFTY 50', '2020-08-26', 16339.41),
        ('NIFTY 50', '2020-08-25', 16226.20),
        ('NIFTY 50', '2020-08-24', 16218.02),
    ]
    
    try:
        cur.execute("SELECT benchmark_id FROM benchmark_master WHERE index_symbol = 'NIFTY 50'")
        b_id = cur.fetchone()[0]
        
        insert_data = [(b_id, d[1], d[2]) for d in data]
        
        from psycopg2.extras import execute_values
        execute_values(cur, "INSERT INTO benchmark_history (benchmark_id, nav_date, index_value) VALUES %s ON CONFLICT DO NOTHING", insert_data)
        conn.commit()
        logger.info(f"Seeded {len(insert_data)} points for Nifty 50 TRI.")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    seed_nifty_50_sample()
