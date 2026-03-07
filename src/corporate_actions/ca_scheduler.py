"""
Corporate Actions Daily Scheduler
====================================
Runs the NSEFetcher daily at a configured time, then drains the
reprocessing queue to apply adj_quantity adjustments.

Schedule:
  - 08:15 IST — fetch corporate actions from NSE for last 7 days
  - 08:30 IST — drain reprocessing_queue (adj_quantity updates)
  - 21:00 IST — second daily run as safety net

Usage:
    python -m src.corporate_actions.ca_scheduler
    OR
    from src.corporate_actions.ca_scheduler import run_ca_scheduler
    run_ca_scheduler()
"""

import time
from datetime import datetime

from src.config import logger
from src.corporate_actions.nse_fetcher import NSEFetcher
from src.corporate_actions.reprocessing_worker import ReprocessingWorker

# Daily schedule times in HH:MM (24h IST)
FETCH_TIMES    = ["08:15", "21:00"]
REPROCESS_TIME = ["08:35", "21:15"]

# How many days back to fetch corporate actions each run
LOOKBACK_DAYS = 14


def run_ca_fetch() -> dict:
    """Single run of the CA fetch + drain cycle."""
    logger.info("[CA Scheduler] === Starting Corporate Actions Fetch ===")

    fetcher = NSEFetcher()
    fetch_result = fetcher.run(lookback_days=LOOKBACK_DAYS)

    logger.info(
        f"[CA Scheduler] Fetch complete: "
        f"fetched={fetch_result.get('fetched_nse', 0)}, "
        f"inserted={fetch_result.get('inserted', 0)}, "
        f"updated={fetch_result.get('updated', 0)}, "
        f"enqueued={fetch_result.get('enqueued_for_reprocessing', 0)}, "
        f"errors={fetch_result.get('errors', 0)}"
    )
    return fetch_result


def run_ca_reprocess() -> dict:
    """Single run of the reprocessing queue drain."""
    logger.info("[CA Scheduler] === Draining Reprocessing Queue ===")

    worker = ReprocessingWorker()
    result = worker.drain_queue(max_items=200)

    logger.info(
        f"[CA Scheduler] Reprocess complete: "
        f"processed={result['processed']}, "
        f"succeeded={result['succeeded']}, "
        f"failed={result['failed']}"
    )
    return result


def run_ca_scheduler():
    """
    Long-running scheduler loop. Checks every 30 seconds and fires
    the fetch/reprocess jobs at their scheduled times.
    """
    logger.info("[CA Scheduler] Corporate Actions Scheduler started.")
    logger.info(f"[CA Scheduler] Fetch times: {FETCH_TIMES}")
    logger.info(f"[CA Scheduler] Reprocess times: {REPROCESS_TIME}")

    while True:
        now          = datetime.now()
        current_time = now.strftime("%H:%M")

        if current_time in FETCH_TIMES:
            logger.info(f"[CA Scheduler] Fetch time reached: {current_time}")
            try:
                run_ca_fetch()
            except Exception as e:
                logger.error(f"[CA Scheduler] Fetch job failed: {e}")
            time.sleep(61)   # Avoid double-trigger within same minute

        elif current_time in REPROCESS_TIME:
            logger.info(f"[CA Scheduler] Reprocess time reached: {current_time}")
            try:
                run_ca_reprocess()
            except Exception as e:
                logger.error(f"[CA Scheduler] Reprocess job failed: {e}")
            time.sleep(61)

        time.sleep(30)


if __name__ == "__main__":
    run_ca_scheduler()
