
import pandas as pd
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.db.connection import get_cursor, close_connection

def find_mismatch_in_db():
    nse_path = "data/raw/exchange_masters/nse_equity_l.csv"
    nse_df = pd.read_csv(nse_path)
    nse_df.columns = [c.strip() for c in nse_df.columns]
    nse_isins = set(nse_df['ISIN NUMBER'].tolist())
    
    bse_path = "data/raw/exchange_masters/bse_scrip_master.csv"
    bse_df = pd.read_csv(bse_path)
    bse_df.columns = [c.strip() for c in bse_df.columns]
    bse_isins = set(bse_df['ISIN'].dropna().tolist())
    
    cursor = get_cursor()
    
    # Find ISINs in DB with NULL symbols
    cursor.execute("SELECT isin, canonical_name FROM isin_master WHERE nse_symbol IS NULL OR bse_code IS NULL")
    rows = cursor.fetchall()
    
    found_in_csv_but_null_in_db = []
    
    for isin, name in rows:
        in_nse = isin in nse_isins
        in_bse = isin in bse_isins
        if in_nse or in_bse:
            found_in_csv_but_null_in_db.append((isin, name, in_nse, in_bse))
    
    print(f"Found {len(found_in_csv_but_null_in_db)} ISINs present in CSV but missing one or both symbols in DB.")
    print("-" * 60)
    for isin, name, in_nse, in_bse in found_in_csv_but_null_in_db[:20]:
        print(f"{isin} | {name[:30]:<30} | NSE:{in_nse} | BSE:{in_bse}")
    
    close_connection()

if __name__ == "__main__":
    find_mismatch_in_db()
