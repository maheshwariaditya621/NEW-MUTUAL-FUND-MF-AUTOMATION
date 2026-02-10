# src/scheduler/mirae_asset_backfill.py

import argparse
from src.downloaders.mirae_asset_downloader import MiraeAssetDownloader
from src.config import logger

def run_backfill(start_year: int, start_month: int, end_year: int, end_month: int):
    downloader = MiraeAssetDownloader()
    
    logger.info(f"Starting Mirae Asset backfill from {start_year}-{start_month} to {end_year}-{end_month}")
    
    # Persistent Session: Open once for the entire backfill
    # downloader.open_session()
    
    try:
        # User wants to go for Dec 2025 first if range is Nov-Dec
        # We'll just reverse the order if end > start
        current_year, current_month = end_year, end_month
        
        while (current_year > start_year) or (current_year == start_year and current_month >= start_month):
            try:
                downloader.download(current_year, current_month)
            except Exception as e:
                logger.error(f"Backfill failed for {current_year}-{current_month}: {e}")
            
            # Decrement month
            current_month -= 1
            if current_month < 1:
                current_month = 12
                current_year -= 1
                
    finally:
        # downloader.close_session()
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mirae Asset Mutual Fund Backfill")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--start-month", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--end-month", type=int, required=True)
    
    args = parser.parse_args()
    run_backfill(args.start_year, args.start_month, args.end_year, args.end_month)
