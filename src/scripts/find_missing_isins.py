
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.db.connection import get_cursor, close_connection

def find_missing_isins():
    cursor = get_cursor()
    
    # ISINs missing BOTH NSE and BSE symbols
    cursor.execute("""
        SELECT im.isin, im.canonical_name 
        FROM isin_master im
        WHERE im.nse_symbol IS NULL AND im.bse_code IS NULL
        LIMIT 20;
    """)
    rows = cursor.fetchall()
    
    print("ISINs missing both symbols:")
    for isin, name in rows:
        print(f"{isin} | {name}")
    
    close_connection()

if __name__ == "__main__":
    find_missing_isins()
