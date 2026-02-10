# src/scheduler/edelweiss_backfill.py

import time
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional
from src.downloaders.edelweiss_downloader import EdelweissDownloader
from src.config import logger

try:
    from src.config.downloader_config import DRY_RUN
except ImportError:
    DRY_RUN = False

def generate_month_range(start_year: int, start_month: int, end_year: int, end_month: int) -> List[Tuple[int, int]]:
    months = []
    year, month = start_year, start_month
    while (year < end_year) or (year == end_year and month <= end_month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months

def get_latest_eligible_months(count: int = 3) -> List[Tuple[int, int]]:
    months = []
    now = datetime.now()
    curr_year, curr_month = now.year, now.month
    for _ in range(count):
        if curr_month == 1:
            curr_month, curr_year = 12, curr_year - 1
        else:
            curr_month -= 1
        months.append((curr_year, curr_month))
    return months[::-1]

def run_edelweiss_backfill(
    start_year: Optional[int] = None,
    start_month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None
) -> dict:
    start_time = time.time()
    logger.info("=" * 70)
    logger.info("EDELWEISS BACKFILL STARTED")
    if DRY_RUN: logger.info("MODE: DRY RUN")
    logger.info("=" * 70)
    
    if all(v is not None for v in [start_year, start_month, end_year, end_month]):
        mode = "MANUAL"
        months = generate_month_range(start_year, start_month, end_year, end_month)
    else:
        mode = "AUTO"
        months = get_latest_eligible_months(3)
    
    logger.info(f"Mode: {mode} | Target: {len(months)} months")
    
    downloader = EdelweissDownloader()
    # downloader.open_session()
    
    downloaded, skipped, failed, not_published = 0, 0, 0, 0
    try:
        for year, month in months:
            result = downloader.download(year, month)
            status = result["status"]
            if status == "success": downloaded += 1
            elif status == "skipped": skipped += 1
            elif status == "not_published": not_published += 1
            else: failed += 1
            time.sleep(1)
    finally:
        # downloader.close_session()
        pass
        
    duration = time.time() - start_time
    logger.info("=" * 70)
    logger.info(f"EDELWEISS BACKFILL SUMMARY | Duration: {duration:.2f}s")
    logger.info(f"Downloaded Months: {downloaded} | Skipped: {skipped} | Failed: {failed} | Not Published: {not_published}")
    logger.info("=" * 70)
    
    return {"amc": "EDELWEISS", "months_downloaded": downloaded, "failed": failed, "duration": duration}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--start-month", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--end-month", type=int)
    args = parser.parse_args()
    
    if args.start_year:
        run_edelweiss_backfill(args.start_year, args.start_month, args.end_year, args.end_month)
    else:
        run_edelweiss_backfill()
