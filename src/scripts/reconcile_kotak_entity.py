from src.db.connection import get_connection
from src.config import logger

def reconcile_kotak_entities():
    """
    Links both Kotak Mahindra Bank ISINs to the same entity_id.
    INE237A01036 (Latest) and INE237A01028 (Old)
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # 1. Update existing companies to use the same entity_id
        # We'll use 685 as the target entity_id based on previous debug output
        target_entity_id = 685
        isins = ('INE237A01036', 'INE237A01028')
        
        logger.info(f"Linking ISINs {isins} to entity_group_id {target_entity_id}...")
        
        cur.execute("""
            UPDATE companies 
            SET entity_id = %s 
            WHERE isin IN %s
        """, (target_entity_id, isins))
        
        updated_count = cur.rowcount
        logger.info(f"Updated {updated_count} records in companies table.")
        
        # 2. Ensure both have the correct metadata (NSE symbol, Sector)
        # Often the newer ISIN has better metadata, or we want consistency
        cur.execute("""
            UPDATE companies
            SET 
                company_name = 'Kotak Mahindra Bank Ltd.',
                nse_symbol = 'KOTAKBANK',
                sector = 'Financial Services'
            WHERE entity_id = %s
        """, (target_entity_id,))
        
        # 3. Update isin_master for consistent resolution
        logger.info(f"Syncing isin_master entity_id for {isins}...")
        cur.execute("""
            UPDATE isin_master
            SET entity_id = %s
            WHERE isin IN %s
        """, (target_entity_id, isins))

        # 4. Patch Jan 2026 quantities (Divide by 5 for Kotak specifically)
        # Confirmed 50,610 (Jan) vs 11,400 (Dec) -> 4.4x jump.
        # Kotak Face Value is 5. Many AMCs reported FV * Quantity in Jan.
        logger.info("Applying quantity patch for Kotak Jan 2026 (dividing by 5 where inflated)...")
        # We only apply this if quantity is substantially higher than market value based shares
        # Price is ~1850. So expected Qty is MarketValue / 1850.
        # If stored Qty is ~5x that, it's definitely the FV error.
        cur.execute("""
            UPDATE equity_holdings
            SET quantity = quantity / 5.0
            FROM scheme_snapshots ss
            JOIN periods p ON ss.period_id = p.period_id
            WHERE equity_holdings.snapshot_id = ss.snapshot_id
              AND equity_holdings.company_id IN (
                  SELECT company_id FROM companies WHERE entity_id = %s
              )
              AND p.year = 2026 AND p.month = 1
              AND equity_holdings.quantity > (market_value_inr / 400.0) -- Guard: Only if Qty is > 4.5x what price (1850/400) suggests
        """, (target_entity_id,))
        
        patched_rows = cur.rowcount
        logger.info(f"Patched {patched_rows} holdings records for Kotak Jan 2026.")

        conn.commit()
        logger.info("Kotak entity reconciliation and data patch completed successfully.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Reconciliation failed: {e}")
        raise
    finally:
        cur.close()

if __name__ == "__main__":
    reconcile_kotak_entities()
