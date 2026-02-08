# src/scheduler/whiteoak_scheduler.py

from datetime import datetime, timedelta
from src.downloaders.whiteoak_downloader import WhiteOakDownloader
from src.config import logger

def run_daily():
    """Run daily check for previous month's data."""
    today = datetime.now()
    first_of_month = today.replace(day=1)
    prev_month_date = first_of_month - timedelta(days=1)
    
    year = prev_month_date.year
    month = prev_month_date.month
    
    logger.info(f"WHITEOAK Scheduler: Checking data for {year}-{month:02d}")
    
    downloader = WhiteOakDownloader()
    try:
        downloader.download(year, month)
    except Exception as e:
        logger.error(f"WHITEOAK Scheduler Error: {e}")

if __name__ == "__main__":
    run_daily()
