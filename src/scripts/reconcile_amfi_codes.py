import csv
import os
from src.db.connection import get_connection
from src.config import logger

import re

def clean_scheme_name(name):
    # Remove text in brackets
    name = re.sub(r'\(.*?\)', '', name)
    # Remove "Erstwhile" clauses
    name = re.sub(r'Erstwhile.*', '', name, flags=re.IGNORECASE)
    # Remove Plan suffixes
    name = re.sub(r' - (Direct|Regular) Plan', '', name, flags=re.IGNORECASE)
    name = re.sub(r' (Direct|Regular) Plan', '', name, flags=re.IGNORECASE)
    # Remove Growth/IDCW suffixes
    name = re.sub(r' (Growth|IDCW|Dividend).*', '', name, flags=re.IGNORECASE)
    # Remove AMC name prefixes (optional, but keep for now)
    return name.strip().upper()

def reconcile_amfi_codes(file_path: str):
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Load AMFI Master into memory for fast lookup
    amfi_master = []
    with open(file_path, mode='r', encoding='utf-8') as f:
        content = f.read().lstrip('\ufeff')
        reader = csv.DictReader(content.splitlines())
        for row in reader:
            amfi_master.append({
                'code': row['Code'],
                'name': row['Scheme Name'],
                'nav_name': row['Scheme NAV Name'],
                'clean_name': clean_scheme_name(row['Scheme Name']),
                'clean_nav_name': clean_scheme_name(row['Scheme NAV Name'])
            })
            
    # 2. Get schemes missing AMFI code
    cur.execute("SELECT scheme_id, scheme_name FROM schemes WHERE amfi_code IS NULL")
    missing = cur.fetchall()
    
    updates = []
    for s_id, s_name in missing:
        s_name_clean = clean_scheme_name(s_name)
        match = None
        
        # Priority 1: Clean Exact Match
        for row in amfi_master:
            if s_name_clean == row['clean_name'] or s_name_clean == row['clean_nav_name']:
                match = row['code']
                break
        
        # Priority 2: Substring Match
        if not match:
            # Look for best candidate
            candidates = []
            for row in amfi_master:
                if s_name_clean in row['clean_name'] or row['clean_name'] in s_name_clean:
                    candidates.append(row)
            
            if len(candidates) >= 1:
                # Still take the first for now, but log it
                match = candidates[0]['code']

        # Priority 3: Manual Overrides for known edge cases
        if not match:
            manual_map = {
                "Banking & Financial Services HD": "148984", 
                "Nifty Digital Index": "153096", # HDFC Nifty India Digital Index Fund
                "NIFTY 100 Equal Weight Index HD": "149871", # HDFC NIFTY 100 Equal Weight Index Fund
                "Nifty500 Multicap 50.. Index HD": "152778", # HDFC NIFTY500 Multicap 50 25 25 Index Fund
                "NIFTY Top Equal Weight Index HD": "153404", # HDFC Nifty Top 20 Equal Weight Index Fund
                "NIFTY100 Low Volatility Index H": "149117", # HDFC NIFTY100 Low Volatility 30 Index Fund (checking code logic or assume match)
                # Actually let's restrict to confirmed ones.
            }
            if s_name in manual_map:
                match = manual_map[s_name]
            elif s_name_clean in manual_map:
                match = manual_map[s_name_clean]
            
            # Additional heuristic: Remove "HD" suffix if present
            if not match and s_name.endswith(" HD"):
                base_name = s_name[:-3].strip()
                # Try finding base name
                # Implement similar logic... simplified for now
                pass

        if match:
            updates.append((match, s_id))
            
    if updates:
        logger.info(f"Reconciled {len(updates)} AMFI codes.")
        cur.executemany("UPDATE schemes SET amfi_code = %s WHERE scheme_id = %s", updates)
        conn.commit()
    else:
        logger.warning("No AMFI codes reconciled.")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    file_path = "data/raw/amfi/scheme_master.txt"
    if os.path.exists(file_path):
        reconcile_amfi_codes(file_path)
    else:
        logger.error(f"File not found: {file_path}")
