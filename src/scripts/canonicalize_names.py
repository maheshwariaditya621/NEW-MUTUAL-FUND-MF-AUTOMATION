import pandas as pd
import psycopg2
from src.db.connection import get_connection, close_connection
from src.config import logger
import os

def canonicalize_names():
    nse_path = "data/raw/exchange_masters/nse_equity_l.csv"
    bse_path = "data/raw/exchange_masters/bse_scrip_master.csv"
    
    if not os.path.exists(nse_path):
        logger.error("NSE master missing. Please run fetch_exchange_masters.py first.")
        return

    # 1. Load NSE Master (Priority)
    logger.info("Loading NSE Master for full names...")
    nse_df = pd.read_csv(nse_path)
    nse_df.columns = [c.strip() for c in nse_df.columns]
    # NSE Columns: SYMBOL, NAME OF COMPANY, ..., ISIN NUMBER
    # We want ISIN -> Full Name
    nse_name_map = nse_df.set_index('ISIN NUMBER')['NAME OF COMPANY'].to_dict()
    
    # 2. Load BSE/IIFL Master (Fallback)
    bse_name_map = {}
    if os.path.exists(bse_path):
        logger.info("Loading BSE Master for supplemental names...")
        # Use low_memory=False for large IIFL file
        # We need ISIN and FullName
        # Note: IIFL file is huge, using chunking or specific cols
        try:
            bse_iter = pd.read_csv(bse_path, usecols=['ISIN', 'FullName'], chunksize=50000)
            for chunk in bse_iter:
                chunk = chunk.dropna(subset=['ISIN', 'FullName'])
                chunk['ISIN'] = chunk['ISIN'].astype(str).str.strip()
                # Update map with new ISINs (NSE names already prioritized later)
                for _, row in chunk.iterrows():
                    if row['ISIN'] not in bse_name_map:
                         bse_name_map[row['ISIN']] = str(row['FullName']).strip()
        except Exception as e:
            logger.warning(f"Failed to fully parse BSE master: {e}")

    # Combine maps (NSE overwrites BSE)
    master_name_map = {**bse_name_map, **nse_name_map}
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # 3. Get all ISINs from isin_master
        cur.execute("SELECT isin FROM isin_master")
        isins = [row[0] for row in cur.fetchall()]
        
        isin_updates = []
        for isin in isins:
            full_name = master_name_map.get(isin)
            if full_name:
                isin_updates.append((full_name, isin))
        
        if isin_updates:
            logger.info(f"Updating {len(isin_updates)} names in isin_master...")
            cur.executemany("UPDATE isin_master SET canonical_name = %s WHERE isin = %s", isin_updates)
        
        # 4. Propagate to corporate_entities
        # We find entities that have at least one ISIN with a updated name
        logger.info("Propagating names to corporate_entities...")
        cur.execute("""
            UPDATE corporate_entities ce
            SET canonical_name = im.canonical_name
            FROM isin_master im
            WHERE ce.entity_id = im.entity_id
              AND im.canonical_name IS NOT NULL
              AND im.canonical_name != ce.canonical_name;
        """)
        
        # 5. Final sync to companies
        logger.info("Syncing names to companies table...")
        cur.execute("""
            UPDATE companies c
            SET company_name = COALESCE(ce.canonical_name, im.canonical_name, c.company_name)
            FROM isin_master im
            LEFT JOIN corporate_entities ce ON im.entity_id = ce.entity_id
            WHERE c.isin = im.isin;
        """)
        
        conn.commit()
        logger.info("Canonicalization complete.")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to canonicalize names: {e}")
    finally:
        cur.close()
        close_connection()

if __name__ == "__main__":
    canonicalize_names()
