from src.db.connection import get_connection
from src.config import logger

def cleanup_empty_holdings():
    """
    Identifies and deletes equity holdings with 0 quantity or 0 market value.
    Then cleans up snapshots and schemes that no longer have any holdings or values.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # 1. Delete 0 holdings
        logger.info("Step 1: Deleting holdings with quantity <= 0 or market_value <= 0...")
        cur.execute("""
            DELETE FROM equity_holdings 
            WHERE quantity <= 0 OR market_value_inr <= 0;
        """)
        holdings_deleted = cur.rowcount
        logger.info(f"Deleted {holdings_deleted} rogue holding records.")

        # 2. Identify and delete snapshots with no holdings
        logger.info("Step 2: Cleaning up empty snapshots...")
        cur.execute("""
            DELETE FROM scheme_snapshots 
            WHERE snapshot_id NOT IN (SELECT DISTINCT snapshot_id FROM equity_holdings)
            OR total_value_inr <= 0;
        """)
        snapshots_deleted = cur.rowcount
        logger.info(f"Deleted {snapshots_deleted} empty snapshot records.")

        # 3. Identify and delete schemes with no snapshots
        logger.info("Step 3: Cleaning up empty schemes...")
        # Get list of orphan schemes
        cur.execute("SELECT scheme_id FROM schemes WHERE scheme_id NOT IN (SELECT DISTINCT scheme_id FROM scheme_snapshots)")
        orphan_scheme_ids = [r[0] for r in cur.fetchall()]
        
        if orphan_scheme_ids:
            # Delete from nav_history first
            cur.execute("DELETE FROM nav_history WHERE scheme_id IN %s", (tuple(orphan_scheme_ids),))
            nav_deleted = cur.rowcount
            logger.info(f"Deleted {nav_deleted} NAV history records for orphan schemes.")
            
            # Delete from schemes
            cur.execute("DELETE FROM schemes WHERE scheme_id IN %s", (tuple(orphan_scheme_ids),))
            schemes_deleted = cur.rowcount
            logger.info(f"Deleted {schemes_deleted} empty scheme records.")
        else:
            schemes_deleted = 0
            logger.info("No empty schemes found.")

        conn.commit()
        logger.info("Cleanup completed successfully.")
        
        return {
            "holdings_deleted": holdings_deleted,
            "snapshots_deleted": snapshots_deleted,
            "schemes_deleted": schemes_deleted
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"Cleanup failed: {e}")
        raise
    finally:
        cur.close()
        # conn is managed globally

if __name__ == "__main__":
    results = cleanup_empty_holdings()
    print("\nSummary of Cleanup:")
    for k, v in results.items():
        print(f"  {k}: {v}")
