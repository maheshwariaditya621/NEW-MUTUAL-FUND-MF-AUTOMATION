# src/scheduler/taurus_scheduler.py

from datetime import datetime, timedelta
from src.downloaders.taurus_downloader import TaurusDownloader
from src.config import logger

def run_daily():
    # Targeted for previous month to ensure data is published
    today = datetime.now()
    first_day_current_month = today.replace(day=1)
    last_day_prev_month = first_day_current_month - timedelta(days=1)
    
    year = last_day_prev_month.year
    month = last_day_prev_month.month
    
    logger.info(f"Scheduled Taurus run for {year}-{month:02d}")
    
    downloader = TaurusDownloader()
    try:
        # For daily runs, we don't necessarily need persistent session across months,
        # but the downloader handles session management internally if not already open.
        downloader.download(year, month)
    except Exception as e:
        logger.error(f"Scheduled Taurus run failed: {e}")

if __name__ == "__main__":
    run_daily()
