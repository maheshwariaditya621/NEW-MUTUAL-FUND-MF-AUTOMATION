# src/scheduler/old_bridge_scheduler.py

from datetime import datetime, timedelta
from src.downloaders.old_bridge_downloader import OldBridgeDownloader
from src.config import logger

def run_daily():
    """Download previous month's portfolio for Old Bridge MF."""
    # Run for previous month
    today = datetime.now()
    first_of_month = today.replace(day=1)
    last_month = first_of_month - timedelta(days=1)
    
    year = last_month.year
    month = last_month.month
    
    downloader = OldBridgeDownloader()
    try:
        downloader.open_session()
        downloader.download(year, month)
    finally:
        downloader.close_session()

if __name__ == "__main__":
    run_daily()
