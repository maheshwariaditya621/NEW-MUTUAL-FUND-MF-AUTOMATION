from src.db.connection import get_connection
from src.config import logger
import sys
import os

def apply_sql_file(file_path):
    if not os.path.exists(file_path):
        logger.error(f"SQL file not found: {file_path}")
        return

    conn = get_connection()
    cur = conn.cursor()
    
    try:
        with open(file_path, 'r') as f:
            sql = f.read()
            
        cur.execute(sql)
        conn.commit()
        logger.info(f"Successfully applied SQL: {file_path}")
        
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        apply_sql_file(sys.argv[1])
    else:
        print("Usage: python src/scripts/apply_sql_file.py <path_to_sql>")
