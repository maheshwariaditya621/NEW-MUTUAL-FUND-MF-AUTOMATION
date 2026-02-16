
import pandas as pd
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

def debug_mapping_v3():
    bse_path = "data/raw/exchange_masters/bse_scrip_master.csv"
    df = pd.read_csv(bse_path, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    
    # Check Reliance (NSE Equity)
    rel_isin = "INE002A01018"
    rel_rows = df[(df['ISIN'].astype(str).str.strip() == rel_isin) & (df['ExchType'] == 'C')]
    print(f"Reliance ({rel_isin}) - Cash Segment:")
    print(rel_rows[['Exch', 'Scripcode', 'Name', 'FullName']])
    
    # Check HDFC Bank
    hdfc_isin = "INE040A01034"
    hdfc_rows = df[(df['ISIN'].astype(str).str.strip() == hdfc_isin) & (df['ExchType'] == 'C')]
    print(f"\nHDFC Bank ({hdfc_isin}) - Cash Segment:")
    print(hdfc_rows[['Exch', 'Scripcode', 'Name', 'FullName']])

if __name__ == "__main__":
    debug_mapping_v3()
