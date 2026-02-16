
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

try:
    from src.db.connection import get_cursor, close_connection
    
    cursor = get_cursor()
    
    cursor.execute("SELECT COUNT(*) FROM schemes WHERE amfi_code IS NOT NULL")
    count = cursor.fetchone()[0]
    print(f"Schemes with amfi_code: {count}")
    
    cursor.execute("SELECT COUNT(*) FROM schemes")
    total = cursor.fetchone()[0]
    print(f"Total schemes: {total}")
    
    close_connection()

except Exception as e:
    print(f"Error: {e}")
