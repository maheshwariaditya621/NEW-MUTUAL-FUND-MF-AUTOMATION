import csv
import os
from src.db.connection import get_connection
from src.config import logger

def map_amfi_categories(file_path: str):
    conn = get_connection()
    cur = conn.cursor()
    
    logger.info(f"Mapping categories from {file_path}...")
    
    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            # Skip potential BOM or weirdness
            content = f.read().lstrip('\ufeff')
            reader = csv.DictReader(content.splitlines())
            
            mappings = []
            for row in reader:
                amfi_code = row.get('Code', '').strip()
                full_cat = row.get('Scheme Category', '').strip()
                
                if not amfi_code or not full_cat:
                    continue
                
                # Split "Equity Scheme - Large Cap Fund" -> "Equity", "Large Cap Fund"
                parts = full_cat.split(' - ', 1)
                broad_cat = parts[0].replace(' Scheme', '').strip()
                scheme_cat = parts[1].strip() if len(parts) > 1 else broad_cat
                
                # Sub-category isn't explicitly in this file but we can derive or leave NULL
                sub_cat = None
                if ' & ' in scheme_cat:
                    sub_cat = scheme_cat # e.g. Large & Mid Cap
                
                mappings.append((amfi_code, broad_cat, scheme_cat, sub_cat))
            
            # Bulk upsert
            query = """
                INSERT INTO scheme_category_master (amfi_code, broad_category, scheme_category, sub_category)
                VALUES %s
                ON CONFLICT (amfi_code) DO UPDATE SET
                    broad_category = EXCLUDED.broad_category,
                    scheme_category = EXCLUDED.scheme_category,
                    sub_category = EXCLUDED.sub_category,
                    updated_at = now() at time zone 'utc';
            """
            
            logger.info(f"Upserting {len(mappings)} category mappings...")
            from psycopg2.extras import execute_values
            execute_values(cur, query, mappings)
            conn.commit()
            logger.info("Category mapping completed successfully.")
            
    except Exception as e:
        logger.error(f"Error mapping categories: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    file_path = "data/raw/amfi/scheme_master.txt"
    if os.path.exists(file_path):
        map_amfi_categories(file_path)
    else:
        logger.error(f"File not found: {file_path}")
