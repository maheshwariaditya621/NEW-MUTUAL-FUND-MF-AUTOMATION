
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.db.connection import get_cursor, get_connection, close_connection
from src.config import logger

def seed_company_master():
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. Get data from isin_master
    cursor.execute("SELECT isin, canonical_name, sector, industry FROM isin_master;")
    isin_master_rows = cursor.fetchall()
    
    # 2. Get data from companies
    cursor.execute("SELECT isin, company_name, sector, industry FROM companies;")
    companies_rows = cursor.fetchall()
    
    company_data = {} # isin -> [name, sector, industry]
    
    # Load companies first (fallback)
    for isin, name, sector, industry in companies_rows:
        company_data[isin] = [name, sector, industry]
        
    # Overwrite with isin_master (preferred)
    for isin, name, sector, industry in isin_master_rows:
        company_data[isin] = [name, sector, industry]
    
    # 3. Sync isin_master first to avoid FK violations
    sync_isin_query = """
    INSERT INTO isin_master (isin, canonical_name, sector, industry)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (isin) DO UPDATE SET
        canonical_name = COALESCE(isin_master.canonical_name, EXCLUDED.canonical_name),
        sector = COALESCE(isin_master.sector, EXCLUDED.sector),
        industry = COALESCE(isin_master.industry, EXCLUDED.industry),
        updated_at = CURRENT_TIMESTAMP;
    """
    
    sync_batch = []
    for isin, details in company_data.items():
        sync_batch.append((isin, details[0], details[1], details[2]))
        
    try:
        if sync_batch:
            cursor.executemany(sync_isin_query, sync_batch)
            logger.info(f"Synced {len(sync_batch)} ISINs to isin_master.")
            
        insert_query = """
        INSERT INTO company_master (isin, canonical_name, sector, industry, first_seen_date, last_seen_date)
        VALUES (%s, %s, %s, %s, CURRENT_DATE, CURRENT_DATE)
        ON CONFLICT (isin) DO UPDATE SET
            canonical_name = EXCLUDED.canonical_name,
            sector = COALESCE(EXCLUDED.sector, company_master.sector),
            industry = COALESCE(EXCLUDED.industry, company_master.industry),
            last_seen_date = CURRENT_DATE,
            updated_at = CURRENT_TIMESTAMP;
        """
        
        insert_batch = []
        for isin, details in company_data.items():
            insert_batch.append((isin, details[0], details[1], details[2]))
        
        if insert_batch:
            cursor.executemany(insert_query, insert_batch)
            conn.commit()
            logger.info(f"Successfully seeded {len(insert_batch)} companies into company_master.")
        else:
            logger.warning("No company data found to seed.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to seed company_master: {e}")
    finally:
        cursor.close()

if __name__ == "__main__":
    seed_company_master()
    close_connection()
