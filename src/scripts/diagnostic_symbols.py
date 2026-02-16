
import pandas as pd
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.db.connection import get_cursor, close_connection

def diagnostic():
    nse_path = "data/raw/exchange_masters/nse_equity_l.csv"
    nse_df = pd.read_csv(nse_path)
    nse_df.columns = [c.strip() for c in nse_df.columns]
    
    # Sample 10 ISINs from the CSV
    sample_isins = nse_df['ISIN NUMBER'].head(20).tolist()
    
    cursor = get_cursor()
    
    print(f"{'ISIN':<15} | {'In DB?':<7} | {'Symbol in DB':<10} | {'Symbol in CSV'}")
    print("-" * 60)
    
    for isin in sample_isins:
        cursor.execute("SELECT nse_symbol FROM isin_master WHERE isin = %s", (isin,))
        row = cursor.fetchone()
        in_db = "Yes" if row else "No"
        symbol_db = row[0] if row else "N/A"
        
        csv_row = nse_df[nse_df['ISIN NUMBER'] == isin]
        symbol_csv = csv_row['SYMBOL'].values[0] if not csv_row.empty else "N/A"
        
        print(f"{isin:<15} | {in_db:<7} | {symbol_db if symbol_db else 'NULL':<10} | {symbol_csv}")
    
    close_connection()

if __name__ == "__main__":
    diagnostic()
