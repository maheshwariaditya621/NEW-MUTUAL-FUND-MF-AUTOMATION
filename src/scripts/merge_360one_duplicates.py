from src.db.connection import get_connection
from src.config import logger

def merge_360one_duplicates():
    """
    Finds and merges duplicate 360 ONE schemes.
    Verbose names (with legal descriptions) are merged into canonical (short) names.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # 1. Identify pairs
        # We look for schemes in '360 ONE MUTUAL FUND' where one name is a prefix of another
        # and has a longer description.
        logger.info("Identifying duplicate scheme pairs in 360 ONE...")
        query = """
        SELECT 
            s1.scheme_id as canonical_id, 
            s1.scheme_name as canonical_name,
            s2.scheme_id as verbose_id, 
            s2.scheme_name as verbose_name
        FROM schemes s1
        JOIN schemes s2 ON s2.scheme_name LIKE s1.scheme_name || ' - %'
        JOIN amcs a ON s1.amc_id = a.amc_id
        WHERE a.amc_name = '360 ONE MUTUAL FUND'
        AND s1.amc_id = s2.amc_id
        AND s1.plan_type = s2.plan_type
        AND s1.option_type = s2.option_type;
        """
        cur.execute(query)
        pairs = cur.fetchall()
        
        if not pairs:
            logger.info("No duplicate pairs found to merge.")
            return

        logger.info(f"Found {len(pairs)} pairs to merge.")
        
        for canonical_id, c_name, verbose_id, v_name in pairs:
            logger.info(f"Merging: '{v_name}' (ID: {verbose_id}) -> '{c_name}' (ID: {canonical_id})")
            
            # Update snapshots
            cur.execute("UPDATE scheme_snapshots SET scheme_id = %s WHERE scheme_id = %s", (canonical_id, verbose_id))
            snapshots_moved = cur.rowcount
            
            # Update nav_history
            cur.execute("UPDATE nav_history SET scheme_id = %s WHERE scheme_id = %s", (canonical_id, verbose_id))
            nav_moved = cur.rowcount
            
            # Delete verbose scheme
            cur.execute("DELETE FROM schemes WHERE scheme_id = %s", (verbose_id,))
            
            logger.info(f"  Moved {snapshots_moved} snapshots and {nav_moved} NAV records. Deleted verbose scheme.")

        conn.commit()
        logger.info("Merge migration completed successfully.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Merge failed: {e}")
        raise
    finally:
        cur.close()

if __name__ == "__main__":
    merge_360one_duplicates()
