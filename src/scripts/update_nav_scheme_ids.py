
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.db.connection import get_connection, close_connection
from src.config import logger

def update_nav_scheme_ids():
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    UPDATE nav_history nh
    SET scheme_id = s.scheme_id
    FROM schemes s
    WHERE nh.scheme_code = s.amfi_code
    AND nh.scheme_id IS NULL;
    """
    
    try:
        cursor.execute(query)
        affected = cursor.rowcount
        conn.commit()
        logger.info(f"Updated {affected} rows in nav_history with scheme_id.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update nav_history scheme_ids: {e}")
    finally:
        cursor.close()

if __name__ == "__main__":
    update_nav_scheme_ids()
    close_connection()
