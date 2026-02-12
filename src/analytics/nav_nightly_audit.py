from src.db.connection import get_connection
from src.config import logger
from datetime import date, timedelta

class NAVNightlyAudit:
    """
    Automated checks for NAV data integrity and financial sanity.
    """

    def __init__(self):
        self.conn = get_connection()

    def check_return_spikes(self, threshold: float = -20.0):
        """
        Detects anomalous 1-day return drops.
        """
        cur = self.conn.cursor()
        cur.execute("""
            SELECT s.scheme_name, sr.return_1d, sr.latest_nav_date
            FROM scheme_returns sr
            JOIN schemes s ON sr.scheme_id = s.scheme_id
            WHERE sr.return_1d < %s
        """, (threshold,))
        
        spikes = cur.fetchall()
        if spikes:
            logger.warning(f"CRITICAL: Detected {len(spikes)} schemes with returns < {threshold}%!")
            for name, ret, d in spikes:
                logger.warning(f"  [SPIKE] {name}: {ret:.2f}% on {d}")
        else:
            logger.info(f"No return spikes below {threshold}% detected.")
        cur.close()

    def check_variant_isolation(self, collision_days: int = 30):
        """
        Checks if different variants (Plan/Option) share identical NAVs for too long.
        Uses the view_nav_collisions created in migration 010.
        """
        cur = self.conn.cursor()
        cur.execute("""
            SELECT scheme_1, scheme_2, COUNT(*) as collision_count
            FROM view_nav_collisions
            GROUP BY scheme_1, scheme_2
            HAVING COUNT(*) > %s
        """, (collision_days,))
        
        collisions = cur.fetchall()
        if collisions:
            logger.error(f"MAPPING ERROR: {len(collisions)} pairs of variants share identical NAVs for > {collision_days} days!")
            for s1, s2, count in collisions:
                logger.error(f"  [COLLISION] Scheme {s1} and Scheme {s2}: {count} days")
        else:
            logger.info("Variant isolation check passed.")
        cur.close()

    def check_trading_day_alignment(self):
        """
        Ensures NAV dates align with NSE trading calendar.
        """
        cur = self.conn.cursor()
        cur.execute("""
            SELECT COUNT(*) 
            FROM nav_history nh
            LEFT JOIN trading_calendar tc ON nh.nav_date = tc.trading_date
            WHERE tc.is_trading_day = FALSE
        """)
        misaligned = cur.fetchone()[0]
        
        if misaligned > 0:
            logger.warning(f"ALIGNMENT ISSUE: {misaligned} NAV entries recorded on non-trading days.")
        else:
            logger.info("NAV alignment with trading calendar verified.")
        cur.close()

    def run_all_checks(self):
        logger.info("Starting Nightly NAV Audit...")
        self.check_return_spikes()
        self.check_variant_isolation()
        self.check_trading_day_alignment()
        logger.info("Nightly NAV Audit completed.")

if __name__ == "__main__":
    audit = NAVNightlyAudit()
    audit.run_all_checks()
    from src.db import close_connection
    close_connection()
