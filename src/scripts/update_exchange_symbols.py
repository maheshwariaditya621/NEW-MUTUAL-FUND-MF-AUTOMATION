
import pandas as pd
import sys
import os
from src.db.connection import get_connection, close_connection
from src.config import logger

def update_symbols():
    nse_path = "data/raw/exchange_masters/nse_equity_l.csv"
    bse_path = "data/raw/exchange_masters/bse_scrip_master.csv"
    
    if not os.path.exists(nse_path) or not os.path.exists(bse_path):
        logger.error("Master files missing. Please run fetch_exchange_masters.py first.")
        return

    # 1. Load Primary NSE Master (for equity symbols)
    logger.info("Loading Primary NSE Master...")
    nse_df = pd.read_csv(nse_path)
    nse_df.columns = [c.strip() for c in nse_df.columns]
    nse_map = nse_df.set_index('ISIN NUMBER')['SYMBOL'].to_dict()
    
    # 2. Load Multi-Exchange Master (IIFL) for supplemental NSE and all BSE
    logger.info("Loading Supplemental Multi-Exchange Master...")
    iifl_df = pd.read_csv(bse_path, low_memory=False)
    iifl_df.columns = [c.strip() for c in iifl_df.columns]
    iifl_df['ISIN'] = iifl_df['ISIN'].astype(str).str.strip()
    
    # Sort for priority: Cash segment first
    iifl_df['priority'] = iifl_df['ExchType'].map({'C': 0, 'Y': 1, 'D': 2, 'U': 3}).fillna(9)
    iifl_df = iifl_df.sort_values(by=['ISIN', 'priority'])
    
    # Supplemental NSE from IIFL (if listed on NSE)
    nse_supp = iifl_df[iifl_df['Exch'] == 'N'].drop_duplicates(subset=['ISIN']).set_index('ISIN')['Name'].to_dict()
    
    # BSE from IIFL (Scripcode is the 6-digit code)
    bse_map = iifl_df[iifl_df['Exch'] == 'B'].drop_duplicates(subset=['ISIN']).set_index('ISIN')['Scripcode'].to_dict()
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # 3. Get all ISINs from database
        cursor.execute("SELECT isin FROM isin_master")
        isins = [row[0] for row in cursor.fetchall()]
        logger.info(f"Processing {len(isins)} ISINs from database...")
        
        nse_updates = []
        bse_updates = []
        
        for isin in isins:
            clean_isin = isin.strip()
            
            # NSE SYMBOL: Priority 1: Primary Master, Priority 2: Supplemental
            nse_sym = nse_map.get(clean_isin) or nse_supp.get(clean_isin)
            if nse_sym:
                nse_updates.append((nse_sym, isin))
            
            # BSE CODE: From IIFL Master
            bse_code = bse_map.get(clean_isin)
            if bse_code and str(bse_code) != 'nan':
                try:
                    val = str(int(float(bse_code)))
                    bse_updates.append((val, isin))
                except (ValueError, TypeError):
                    bse_updates.append((str(bse_code), isin))
        
        # 4. Perform Updates
        if nse_updates:
            logger.info(f"Updating {len(nse_updates)} NSE symbols in isin_master...")
            cursor.executemany("UPDATE isin_master SET nse_symbol = %s WHERE isin = %s", nse_updates)
            
        if bse_updates:
            logger.info(f"Updating {len(bse_updates)} BSE codes in isin_master...")
            cursor.executemany("UPDATE isin_master SET bse_code = %s WHERE isin = %s", bse_updates)

        # 5. Sync to companies table (Enrich exchange_symbol as well)
        logger.info("Syncing symbols to companies table...")
        cursor.execute("""
            UPDATE companies c
            SET nse_symbol = im.nse_symbol,
                bse_code = im.bse_code,
                exchange_symbol = COALESCE(im.nse_symbol, im.bse_code)
            FROM isin_master im
            WHERE c.isin = im.isin;
        """)
            
        conn.commit()
        logger.info("Database update completed successfully.")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update symbols: {e}")
    finally:
        cursor.close()
        close_connection()

if __name__ == "__main__":
    update_symbols()
