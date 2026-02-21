
from src.config import logger
from src.db.connection import get_connection, close_connection

def fix_constraints():
    conn = get_connection()
    cur = conn.cursor()
    try:
        # 1. Fix isin_master constraint
        cur.execute("ALTER TABLE IF EXISTS isin_master DROP CONSTRAINT IF EXISTS chk_isin_master_format")
        cur.execute("ALTER TABLE isin_master ADD CONSTRAINT chk_isin_master_format CHECK (isin ~ '^INE[A-Z0-9]{5}10[A-Z0-9]{2}$')")
        
        # 2. Fix companies constraint
        cur.execute("ALTER TABLE IF EXISTS companies DROP CONSTRAINT IF EXISTS chk_isin_format")
        cur.execute("ALTER TABLE companies ADD CONSTRAINT chk_isin_format CHECK (isin ~ '^INE[A-Z0-9]{5}10[A-Z0-9]{2}$')")
        
        conn.commit()
        print("✓ Database ISIN constraints updated to match 9th-10th char '10' rule.")
    except Exception as e:
        conn.rollback()
        print(f"✗ Failed to update constraints: {e}")
    finally:
        cur.close()
        close_connection()

if __name__ == "__main__":
    fix_constraints()
