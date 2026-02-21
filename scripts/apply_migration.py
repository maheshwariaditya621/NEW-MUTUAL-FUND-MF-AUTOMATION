
import psycopg2
import sys
import os
from pathlib import Path
from src.config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from src.config import logger

def apply_migration(migration_file):
    migration_path = Path(migration_file)
    if not migration_path.exists():
        print(f"Error: Migration file {migration_file} not found.")
        return False

    print(f"Applying migration: {migration_path.name}...")
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        
        with open(migration_path, "r") as f:
            sql = f.read()
            
        cur.execute(sql)
        conn.commit()
        
        print(f"Successfully applied migration: {migration_path.name}")
        return True
        
    except Exception as e:
        print(f"Failed to apply migration: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python apply_migration.py <path_to_sql_file>")
        sys.exit(1)
    
    success = apply_migration(sys.argv[1])
    sys.exit(0 if success else 1)
