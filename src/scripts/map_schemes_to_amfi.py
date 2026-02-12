import re
from src.db.connection import get_connection
from src.config import logger

def normalize_name(name: str) -> str:
    """
    Cleans scheme names for maximum AMFI matching compatibility.
    """
    # 1. Remove internal codes in brackets: e.g. "SBI MNC Fund (SMGLF)" -> "SBI MNC Fund"
    name = re.sub(r'\(.*?\)', '', name)
    # 2. Standardize common terminology
    name = name.replace('Smallcap', 'Small Cap').replace('Midcap', 'Mid Cap')
    # 3. Handle specific suffixes like " - SLTEF" or " (FMP)"
    name = re.sub(r'\s-\s[A-Z0-9]+', '', name)
    # 4. Collapse spaces
    name = ' '.join(name.split())
    return name

def run_mapping():
    conn = get_connection()
    cur = conn.cursor()
    
    # Fetch all schemes that don't have an amfi_code yet
    cur.execute("SELECT scheme_id, scheme_name, plan_type, option_type FROM schemes WHERE amfi_code IS NULL")
    schemes = cur.fetchall()
    
    logger.info(f"Attempting to map {len(schemes)} schemes to AMFI...")
    
    mapped_count = 0
    for s_id, s_name, plan, opt in schemes:
        clean_name = normalize_name(s_name)
        
        # Search strategy: Try partial match on cleaned name + exact attributes
        cur.execute("""
            SELECT scheme_code, scheme_name
            FROM nav_history 
            WHERE (scheme_name ILIKE %s OR scheme_name ILIKE %s)
              AND plan_type = %s 
              AND option_type = %s
            LIMIT 1
        """, (f"%{clean_name}%", f"%{s_name}%", plan, opt))
        
        match = cur.fetchone()
        if match:
            amfi_code = match[0]
            cur.execute(
                "UPDATE schemes SET amfi_code = %s, updated_at = NOW() WHERE scheme_id = %s",
                (amfi_code, s_id)
            )
            mapped_count += 1
            logger.debug(f"Mapped: {s_name} -> {amfi_code}")
            
    conn.commit()
    logger.info(f"Successfully mapped {mapped_count}/{len(schemes)} schemes.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_mapping()
