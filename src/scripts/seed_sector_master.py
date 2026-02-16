
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.db.connection import get_cursor, get_connection, close_connection
from src.config import logger

def seed_sector_master():
    conn = get_connection()
    cursor = conn.cursor()
    
    sectors = [
        ('FINANCE', 'Financial Services'),
        ('FINANCIAL SERVICES', 'Financial Services'),
        ('BANKS', 'Financial Services'),
        ('IT', 'Information Technology'),
        ('INFORMATION TECHNOLOGY', 'Information Technology'),
        ('AUTO', 'Automobile and Auto Components'),
        ('AUTOMOBILE', 'Automobile and Auto Components'),
        ('PHARMA', 'Healthcare'),
        ('PHARMACEUTICALS', 'Healthcare'),
        ('HEALTHCARE', 'Healthcare'),
        ('CONSUMER GOODS', 'Fast Moving Consumer Goods'),
        ('FMCG', 'Fast Moving Consumer Goods'),
        ('OIL & GAS', 'Oil, Gas & Consumable Fuels'),
        ('ENERGY', 'Oil, Gas & Consumable Fuels'),
        ('CONSTRUCTION', 'Construction'),
        ('CHEMICALS', 'Chemicals'),
        ('METALS', 'Metals & Mining'),
        ('METALS & MINING', 'Metals & Mining')
    ]
    
    query = """
    INSERT INTO sector_master (raw_sector_name, canonical_sector)
    VALUES (%s, %s)
    ON CONFLICT (raw_sector_name) DO UPDATE 
    SET canonical_sector = EXCLUDED.canonical_sector;
    """
    
    try:
        cursor.executemany(query, sectors)
        conn.commit()
        logger.info(f"Successfully seeded {len(sectors)} sectors into sector_master.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to seed sector_master: {e}")
    finally:
        cursor.close()

if __name__ == "__main__":
    seed_sector_master()
    close_connection()
