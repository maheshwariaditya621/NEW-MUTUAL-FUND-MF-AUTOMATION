"""
Script to retroactively link all ISINs in the companies table to their logical entity_id.
Uses the Tiered Resolver logic: Exact Name -> Partial Word containment -> Symbol -> Fuzzy Match.
"""
import sys
import os
sys.path.append(os.getcwd())

from src.db.connection import get_connection, get_cursor
from src.db.repositories import find_entity_by_name, find_entity_by_symbol, fuzzy_search_entity
from src.config import logger

def reconcile_all():
    logger.info("Starting global entity reconciliation...")
    conn = get_connection()
    cur = conn.cursor()
    
    # Get all companies missing entity_id
    cur.execute("SELECT company_id, company_name, isin FROM companies WHERE entity_id IS NULL")
    missing = cur.fetchall()
    logger.info(f"Found {len(missing)} companies without entity_id.")
    
    updates = 0
    for cid, name, isin in missing:
        # 1. Exact Name / Containment
        entity_id = find_entity_by_name(name)
        tier = "NAME_MATCH"
        
        # 2. Symbol
        if not entity_id:
            entity_id = find_entity_by_symbol(name)
            tier = "SYMBOL_MATCH"
            
        # 3. Fuzzy
        if not entity_id:
            entity_id = fuzzy_search_entity(name, threshold=0.7)
            tier = "FUZZY_MATCH"
            
        if entity_id:
            cur.execute(
                "UPDATE companies SET entity_id = %s WHERE company_id = %s",
                (entity_id, cid)
            )
            logger.info(f"Linked '{name}' ({isin}) -> entity_id {entity_id} [via {tier}]")
            updates += 1
            
    conn.commit()
    logger.info(f"Reconciliation complete. Updated {updates} records.")
    cur.close()

if __name__ == "__main__":
    reconcile_all()
