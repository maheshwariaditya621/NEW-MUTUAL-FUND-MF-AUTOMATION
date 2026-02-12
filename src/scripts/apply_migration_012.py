from src.db.connection import get_connection
from src.config import logger
import os

def apply_migration(file_path):
    if not os.path.exists(file_path):
        logger.error(f"Migration file not found: {file_path}")
        return

    conn = get_connection()
    cur = conn.cursor()
    
    try:
        with open(file_path, 'r') as f:
            sql = f.read()
            
        cur.execute(sql)
        conn.commit()
        logger.info(f"Successfully applied migration: {file_path}")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    apply_migration("database/migrations/012_benchmark_refinements.sql")
