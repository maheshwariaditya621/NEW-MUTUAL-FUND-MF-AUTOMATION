
from src.db.connection import get_connection, close_connection
from src.config import logger

def diagnose_master():
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # 1. Total count
        cur.execute("SELECT count(*) FROM isin_master")
        logger.info(f"Total rows in isin_master: {cur.fetchone()[0]}")
        
        # 2. Sample 10 rows
        cur.execute("SELECT isin, nse_symbol, canonical_name FROM isin_master LIMIT 10")
        rows = cur.fetchall()
        logger.info("Sample master data:")
        for r in rows:
            logger.info(f"  {r}")
            
        # 3. Check regex match count
        regex = '^INE[A-Z0-9]{5}10[A-Z0-9]{2}$'
        cur.execute(f"SELECT count(*) FROM isin_master WHERE isin ~ '{regex}'")
        logger.info(f"Rows matching regex '{regex}': {cur.fetchone()[0]}")
        
        # 4. Check ISINs that SHOULD be there but aren't
        # Sample an NSE ISIN from nse_equity_l.csv
        sample_isin = 'INE144J01027' # 20MICRONS
        cur.execute("SELECT * FROM isin_master WHERE isin = %s", (sample_isin,))
        match = cur.fetchone()
        logger.info(f"Checking for {sample_isin}: {'FOUND' if match else 'NOT FOUND'}")
        
    finally:
        cur.close()
        close_connection()

if __name__ == "__main__":
    diagnose_master()
