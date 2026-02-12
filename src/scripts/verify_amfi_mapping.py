import re
from src.db.connection import get_connection

def normalize_name(name: str) -> str:
    """
    Strips internal codes in parentheses and suffixes like (SLTEF) or (Regular).
    """
    # Remove text in parentheses: e.g. "SBI MNC Fund (SMGLF)" -> "SBI MNC Fund"
    name = re.sub(r'\(.*?\)', '', name)
    # Remove extra spaces
    name = ' '.join(name.split())
    return name

def check_mappings():
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Get schemes from our masters
    cur.execute("SELECT scheme_id, scheme_name, plan_type, option_type FROM schemes LIMIT 50")
    schemes = cur.fetchall()
    
    print("Verifying Aggressive Name Normalization Mappings:")
    matched = 0
    total = len(schemes)
    
    for s in schemes:
        s_id, s_name, plan, opt = s
        clean_name = normalize_name(s_name)
        
        # Try to find a match in nav_history
        cur.execute("""
            SELECT scheme_name, nav_value, nav_date, scheme_code
            FROM nav_history 
            WHERE (scheme_name ILIKE %s OR scheme_name ILIKE %s)
              AND plan_type = %s 
              AND option_type = %s
            LIMIT 1
        """, (f"%{clean_name}%", f"%{s_name}%", plan, opt))
        
        match = cur.fetchone()
        if match:
            matched += 1
            print(f"Match Found: {s_name} -> {match[0]} (Code: {match[3]})")
        else:
            print(f"FAILED: {s_name} (Cleaned: {clean_name})")

    print(f"\nFinal Tally: {matched}/{total} mapped successfully.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_mappings()
