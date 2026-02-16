
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.db.connection import get_cursor, close_connection

def verify_symbols():
    cursor = get_cursor()
    
    cursor.execute("SELECT COUNT(*) FROM isin_master WHERE nse_symbol IS NOT NULL")
    nse_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM isin_master WHERE bse_code IS NOT NULL")
    bse_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM companies WHERE exchange_symbol IS NOT NULL")
    sync_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM isin_master")
    total = cursor.fetchone()[0]
    
    print(f"Total ISINs: {total}")
    print(f"NSE Symbols: {nse_count}")
    print(f"BSE Codes: {bse_count}")
    print(f"Synced Companies: {sync_count}")
    
    close_connection()

if __name__ == "__main__":
    verify_symbols()
