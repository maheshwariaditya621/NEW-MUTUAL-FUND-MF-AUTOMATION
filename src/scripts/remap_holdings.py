import argparse
from src.config import logger
from src.db.connection import TransactionContext, get_cursor
from src.db.repositories import (
    get_isin_details, upsert_company_master, get_canonical_sector
)

def run_remapping():
    """
    Historical backfill & Migration:
    1. Reads from old 'companies' table.
    2. Resolves canonical sectors via Sector Master.
    3. Upserts into 'company_master' (Analytical Entity).
    4. Updates 'equity_holdings' to point to 'company_master'.
    """
    logger.info("Starting Historical ISIN Remapping & Master-Data Migration...")
    
    cursor = get_cursor()
    
    # 1. Fetch all unique ISINs from current holdings/companies
    cursor.execute("SELECT id, isin, company_name, sector, industry FROM companies")
    old_companies = cursor.fetchall()
    
    logger.info(f"Found {len(old_companies)} legacy company records.")
    
    update_count = 0
    with TransactionContext():
        for old_id, isin, raw_name, raw_sector, raw_industry in old_companies:
            # 2. Lookup best available metadata (prioritizes isin_master)
            meta = get_isin_details(isin)
            
            canonical_name = meta['canonical_name'] if meta else raw_name
            target_sector = meta.get('sector') if meta and meta.get('sector') else raw_sector
            
            # 3. Canonicalize Sector
            canonical_sector = get_canonical_sector(target_sector)
            
            # 4. Upsert into Company Master
            new_company_id = upsert_company_master(
                isin=isin,
                canonical_name=canonical_name,
                sector=canonical_sector,
                industry=meta.get('industry') if meta else raw_industry
            )
            
            # 5. Update Legacy Holdings (Redirect FK)
            # Note: We should ideally have ALREADY changed the FK constraint in DB
            # but this script ensures the IDs match.
            cursor.execute(
                "UPDATE equity_holdings SET company_id = %s WHERE company_id = %s",
                (new_company_id, old_id)
            )
            
            if raw_name != canonical_name or raw_sector != canonical_sector:
                logger.debug(f"Remapped {isin}: '{raw_name}' -> '{canonical_name}' | '{raw_sector}' -> '{canonical_sector}'")
                update_count += 1

    logger.info(f"Migration complete. Resolved {update_count} entities to canonical masters.")

if __name__ == "__main__":
    run_remapping()
