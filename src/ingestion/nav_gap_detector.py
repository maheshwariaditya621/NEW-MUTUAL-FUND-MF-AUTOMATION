from src.db.connection import get_connection
from src.config import logger
from datetime import date, timedelta

class NAVGapDetector:
    """
    Checks for missing data gaps in nav_history.
    """

    @staticmethod
    def check_gaps(threshold_days: int = 7):
        """
        Identifies schemes with gaps larger than threshold_days.
        """
        conn = get_connection()
        cur = conn.cursor()
        
        # We check gaps for schemes that have at least 2 data points
        cur.execute("""
            WITH SortedNav AS (
                SELECT scheme_id, nav_date,
                       LAG(nav_date) OVER (PARTITION BY scheme_id ORDER BY nav_date) as prev_date
                FROM nav_history
                WHERE scheme_id IS NOT NULL
            )
            SELECT scheme_id, nav_date, prev_date, (nav_date - prev_date) as gap
            FROM SortedNav
            WHERE prev_date IS NOT NULL AND (nav_date - prev_date) > %s
        """, (threshold_days,))
        
        gaps = cur.fetchall()
        
        if gaps:
            logger.warning(f"Detected {len(gaps)} data gaps larger than {threshold_days} days.")
            for g in gaps:
                s_id, n_date, p_date, gap = g
                logger.warning(f"  Scheme ID {s_id}: Gap of {gap} days between {p_date} and {n_date}")
        else:
            logger.info(f"No data gaps larger than {threshold_days} days detected.")
            
        cur.close()
        conn.close()
        return gaps

if __name__ == "__main__":
    detector = NAVGapDetector()
    detector.check_gaps()
