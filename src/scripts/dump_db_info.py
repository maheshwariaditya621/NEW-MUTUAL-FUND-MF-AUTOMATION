
from src.db.connection import get_connection, close_connection
from src.config import logger

def dump_info():
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT * FROM corporate_actions")
        rows = cur.fetchall()
        logger.info(f"corporate_actions rows: {len(rows)}")
        for r in rows:
            logger.info(f"  {r}")
            
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'isin_master'")
        cols = [c[0] for c in cur.fetchall()]
        logger.info(f"isin_master columns: {cols}")

        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'companies'")
        cols = [c[0] for c in cur.fetchall()]
        logger.info(f"companies columns: {cols}")
        
    finally:
        cur.close()
        close_connection()

if __name__ == "__main__":
    dump_info()
