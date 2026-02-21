
from src.db.connection import get_connection, close_connection
from src.config import logger

def cleanup_non_equity():
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # 1. Check companies table for non-equity
        cur.execute("SELECT isin, company_name FROM companies WHERE NOT (isin ~ '^INE[A-Z0-9]{5}10[A-Z0-9]{2}$')")
        non_equity_holdings = cur.fetchall()
        logger.info(f"Found {len(non_equity_holdings)} companies with non-equity ISINs in current holdings.")
        for isin, name in non_equity_holdings[:5]:
            logger.info(f"  Sample: {isin} - {name}")
            
        # 2. Cleanup isin_master: Delete if not equity AND not in companies
        cur.execute("""
            DELETE FROM isin_master 
            WHERE NOT (isin ~ '^INE[A-Z0-9]{5}10[A-Z0-9]{2}$')
            AND isin NOT IN (SELECT isin FROM companies)
        """)
        logger.info(f"Deleted {cur.rowcount} non-equity ISINs from isin_master.")
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Cleanup failed: {e}")
    finally:
        cur.close()
        close_connection()

if __name__ == "__main__":
    cleanup_non_equity()
