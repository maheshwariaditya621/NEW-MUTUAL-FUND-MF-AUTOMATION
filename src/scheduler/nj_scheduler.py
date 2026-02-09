# src/scheduler/nj_scheduler.py

from datetime import datetime, timedelta
from src.downloaders.nj_downloader import NJDownloader
from src.config import logger

def run_daily():
    # Targeted for previous month to ensure data is published
    today = datetime.now()
    first_day_current_month = today.replace(day=1)
    last_day_prev_month = first_day_current_month - timedelta(days=1)
    
    year = last_day_prev_month.year
    month = last_day_prev_month.month
    
    logger.info(f"Scheduled NJ run for {year}-{month:02d}")
    
    downloader = NJDownloader()
    try:
        # For single month runs, the downloader handles session management
        downloader.download(year, month)
    except Exception as e:
        logger.error(f"Scheduled NJ run failed: {e}")

if __name__ == "__main__":
    run_daily()
