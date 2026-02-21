"""
Abakkus Mutual Fund Daily Scheduler.
Checked daily to see if the previous month's data is published.
"""

from src.downloaders.abakkus_downloader import AbakkusDownloader
from src.config import logger
from datetime import datetime


def run_daily():
    """
    Check for previous month's data availability.
    """
    now = datetime.now()
    if now.month == 1:
        year, month = now.year - 1, 12
    else:
        year, month = now.year, now.month - 1

    logger.info(f"SCHEDULER: Checking Abakkus for {year}-{month:02d}")
    
    downloader = AbakkusDownloader()
    result = downloader.download(year=year, month=month)
    
    if result["status"] == "success":
        logger.success(f"ABAKKUS: Successfully downloaded data for {year}-{month:02d}")
    elif result["status"] == "skipped":
        logger.info(f"ABAKKUS: Data for {year}-{month:02d} already present.")
    elif result["status"] == "not_published":
        logger.info(f"ABAKKUS: Data for {year}-{month:02d} not yet published.")
    else:
        logger.error(f"ABAKKUS: Scheduler check failed for {year}-{month:02d}")


if __name__ == "__main__":
    run_daily()
