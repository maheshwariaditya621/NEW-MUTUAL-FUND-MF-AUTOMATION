
import pandas as pd
import requests
import io
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from src.config import logger
from src.db.connection import get_connection, close_connection

# URLs for Exchange Masters
NSE_MASTER_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_SYMBOL_CHANGE_URL = "https://archives.nseindia.com/content/equities/symbolchange.csv"
NSE_ISIN_CHANGE_URL = "https://archives.nseindia.com/content/equities/isinchange.csv" # Try original again
BSE_MASTER_URL = "http://content.indiainfoline.com/IIFLTT/Scripmaster.csv"

RAW_DIR = Path("data/raw/exchange_masters")

def is_equity_isin(isin: Any) -> bool:
    """
    Mandatory Triple Filter for Equity ISINs:
    1. Starts with 'INE'
    2. Length is exactly 12
    3. Security code (chars 9-10) is '10'
    """
    if not isinstance(isin, str):
        return False
    # Remove quotes, whitespace and standardize
    isin = isin.strip().replace('"', '').replace("'", "").upper()
    if len(isin) != 12:
        return False
    is_equity = isin.startswith("INE") and isin[8:10] == "10"
    return is_equity

def download_file(url: str, filename: str) -> Optional[Path]:
    """Download a file with headers to avoid blocking."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RAW_DIR / filename
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }
    
    try:
        logger.info(f"Downloading from {url}...")
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        
        with open(output_path, "wb") as f:
            f.write(response.content)
        
        logger.info(f"  ✓ Saved to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"  ✗ Failed to download {url}: {e}")
        return None

def sync_isin_master():
    """Bulk load all ISINs from NSE and BSE masters into isin_master table."""
    nse_path = download_file(NSE_MASTER_URL, "nse_equity_l.csv")
    bse_path = download_file(BSE_MASTER_URL, "bse_scrip_master.csv")
    
    if not nse_path or not bse_path:
        logger.error("Required master files failed to download. Aborting sync.")
        return

    # 1. Load NSE Master
    logger.info("Processing NSE Master...")
    nse_df = pd.read_csv(nse_path)
    nse_df.columns = [c.strip() for c in nse_df.columns]
    
    # Debug: Check first 5 ISINs
    logger.info(f"NSE Sample ISINs: {nse_df['ISIN NUMBER'].head().tolist()}")
    
    # Required: SYMBOL, NAME OF COMPANY, ISIN NUMBER
    nse_data = nse_df[['ISIN NUMBER', 'NAME OF COMPANY', 'SYMBOL']].rename(columns={
        'ISIN NUMBER': 'isin',
        'NAME OF COMPANY': 'name',
        'SYMBOL': 'symbol'
    })

    # 2. Load BSE Master (IIFL)
    logger.info("Processing BSE Master...")
    bse_df = pd.read_csv(bse_path, low_memory=False)
    bse_df.columns = [c.strip() for c in bse_df.columns]
    bse_df['ISIN'] = bse_df['ISIN'].astype(str).str.strip()
    
    # Priority segment 'C' (Cash)
    bse_data = bse_df[bse_df['Exch'] == 'B'].drop_duplicates(subset=['ISIN'])
    bse_data = bse_data[['ISIN', 'Name', 'Scripcode']].rename(columns={
        'ISIN': 'isin',
        'Name': 'name',
        'Scripcode': 'symbol'
    })

    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Bulk Upsert into isin_master
        logger.info("Upserting NSE data into isin_master...")
        accepted = 0
        for _, row in nse_data.iterrows():
            isin = str(row['isin']).strip().upper()
            if not is_equity_isin(isin):
                if accepted < 5:
                    logger.debug(f"  Rejected NSE ISIN: {isin} (length={len(isin)}, code={isin[8:10] if len(isin)>=10 else 'N/A'})")
                continue
            accepted += 1
            cursor.execute("""
                INSERT INTO isin_master (isin, canonical_name, nse_symbol)
                VALUES (%s, %s, %s)
                ON CONFLICT (isin) DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name,
                    nse_symbol = EXCLUDED.nse_symbol,
                    updated_at = CURRENT_TIMESTAMP
            """, (isin, row['name'], row['symbol']))
        logger.info(f"  ✓ Accepted {accepted} NSE Equity ISINs")

        logger.info("Upserting BSE data into isin_master...")
        accepted = 0
        for _, row in bse_data.iterrows():
            isin = str(row['isin']).strip().upper()
            if not is_equity_isin(isin):
                if accepted < 5:
                    logger.debug(f"  Rejected BSE ISIN: {isin} (length={len(isin)}, code={isin[8:10] if len(isin)>=10 else 'N/A'})")
                continue
            accepted += 1
            b_sym = str(row['symbol']).split('.')[0] if '.' in str(row['symbol']) else str(row['symbol'])
            cursor.execute("""
                INSERT INTO isin_master (isin, canonical_name, bse_code)
                VALUES (%s, %s, %s)
                ON CONFLICT (isin) DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name,
                    bse_code = EXCLUDED.bse_code,
                    updated_at = CURRENT_TIMESTAMP
            """, (isin, row['name'], b_sym))
        logger.info(f"  ✓ Accepted {accepted} BSE Equity ISINs")
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to sync ISIN master: {e}")
    finally:
        cursor.close()
        close_connection()

def sync_corporate_actions():
    """Download and process NSE historical changes."""
    sym_path = download_file(NSE_SYMBOL_CHANGE_URL, "symbolchange.csv")
    isin_path = download_file(NSE_ISIN_CHANGE_URL, "isinchange.csv")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        if sym_path:
            logger.info("Processing NSE Symbol changes...")
            # CSV format: Company Name,Old Symbol,New Symbol,Change Date
            df = pd.read_csv(sym_path)
            df.columns = [c.strip() for c in df.columns]
            for _, row in df.iterrows():
                # We can record these in corporate_actions if we want ticker-level tracking
                # For now, we mainly need ISIN changes for decoupling.
                pass
        
        if isin_path:
            logger.info("Processing NSE ISIN changes...")
            # CSV format: SYMBOL,COMPANY NAME,OLD ISIN,NEW ISIN,EFFECTIVE DATE
            df = pd.read_csv(isin_path)
            df.columns = [c.strip() for c in df.columns]
            for _, row in df.iterrows():
                old_isin = str(row['OLD ISIN']).strip().upper()
                new_isin = str(row['NEW ISIN']).strip().upper()
                
                # STRICT EQUITY FILTER: Skip if either ISIN is non-equity
                if not is_equity_isin(old_isin) or not is_equity_isin(new_isin):
                    continue

                # Ensure ISINs exist in master first (or at least placeholders)
                for isin in [old_isin, new_isin]:
                    cursor.execute("""
                        INSERT INTO isin_master (isin, canonical_name, nse_symbol)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (isin) DO NOTHING
                    """, (isin, row['COMPANY NAME'], row['SYMBOL']))
                
                # Insert Bridge
                cursor.execute("""
                    INSERT INTO corporate_actions (old_isin, new_isin, effective_date, action_type, description)
                    VALUES (%s, %s, %s, 'ISIN_CHANGE', %s)
                    ON CONFLICT DO NOTHING
                """, (old_isin, new_isin, row['EFFECTIVE DATE'], f"NSE ISIN change for {row['SYMBOL']}"))

        if sym_path:
            logger.info("Processing NSE Ticker (Symbol) lineage...")
            # CSV format: Company Name,Old Symbol,New Symbol,Change Date
            df = pd.read_csv(sym_path)
            # Standardize columns: Strip whitespace and uppercase for reliable mapping
            df.columns = [c.strip().upper() for c in df.columns]
            
            # Map common columns to canonical names
            col_map = {
                'OLD SYMBOL': 'old_sym',
                'NEW SYMBOL': 'new_sym',
                'CHANGE DATE': 'date',
                'EFFECTIVE DATE': 'date'
            }
            available_cols = {col_map.get(c, c): c for c in df.columns}
            
            for _, row in df.iterrows():
                old_sym = row.get(available_cols.get('old_sym'))
                new_sym = row.get(available_cols.get('new_sym'))
                eff_date = row.get(available_cols.get('date'))
                
                if not old_sym or not new_sym:
                    continue

                cursor.execute("""
                    INSERT INTO corporate_actions (old_isin, new_isin, effective_date, action_type, description)
                    SELECT im1.isin, im2.isin, %s, 'SYMBOL_CHANGE', %s
                    FROM isin_master im1, isin_master im2
                    WHERE im1.nse_symbol = %s AND im2.nse_symbol = %s
                    ON CONFLICT DO NOTHING
                """, (eff_date, f"Symbol change from {old_sym} to {new_sym}", old_sym, new_sym))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to sync corporate actions: {e}")
    finally:
        cursor.close()
        close_connection()

def group_entities_by_ticker():
    """Group all ISINs with the same ticker under a single Corporate Entity."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Find all unique tickers
        cursor.execute("""
            SELECT DISTINCT nse_symbol FROM isin_master 
            WHERE nse_symbol IS NOT NULL AND nse_symbol != ''
        """)
        tickers = [r[0] for r in cursor.fetchall()]
        
        logger.info(f"Grouping {len(tickers)} entities by Ticker...")
        
        for ticker in tickers:
            # 1. Upsert Corporate Entity
            cursor.execute("""
                INSERT INTO corporate_entities (canonical_name, group_symbol)
                SELECT canonical_name, %s FROM isin_master 
                WHERE nse_symbol = %s 
                ORDER BY updated_at DESC LIMIT 1
                ON CONFLICT (group_symbol) DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name
                RETURNING entity_id
            """, (ticker, ticker))
            entity_id = cursor.fetchone()[0]
            
            # 2. Link all ISINs with this ticker
            cursor.execute("""
                UPDATE isin_master SET entity_id = %s 
                WHERE nse_symbol = %s
            """, (entity_id, ticker))
            
            # 3. Handle ISIN changes (Bridge link)
            # If an old ISIN doesn't have a ticker, we find it from corporate_actions
            # and link it to the same entity_id as the new ISIN.
            cursor.execute("""
                UPDATE isin_master target
                SET entity_id = source.entity_id,
                    nse_symbol = source.nse_symbol
                FROM isin_master source
                JOIN corporate_actions ca ON source.isin = ca.new_isin
                WHERE target.isin = ca.old_isin
                AND target.entity_id IS NULL
                AND source.entity_id IS NOT NULL;
            """)

        # 4. Final Sync to 'companies' table
        logger.info("Syncing entities to companies table...")
        cursor.execute("""
            UPDATE companies c
            SET entity_id = im.entity_id,
                nse_symbol = im.nse_symbol,
                bse_code = im.bse_code
            FROM isin_master im
            WHERE c.isin = im.isin;
        """)
        
        conn.commit()
        logger.info("Entity grouping completed.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to group entities: {e}")
    finally:
        cursor.close()
        close_connection()

if __name__ == "__main__":
    sync_isin_master()
    sync_corporate_actions()
    group_entities_by_ticker()
