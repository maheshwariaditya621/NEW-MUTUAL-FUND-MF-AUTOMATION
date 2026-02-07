# src/scheduler/angelone_backfill.py

import time
import json
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional
from src.downloaders.angelone_downloader import AngelOneDownloader
from src.config import logger

try:
    from src.config.downloader_config import DRY_RUN
except ImportError:
    DRY_RUN = False


def generate_month_range(start_year: int, start_month: int, end_year: int, end_month: int) -> List[Tuple[int, int]]:
    months = []
    year = start_year
    month = start_month
    
    while (year < end_year) or (year == end_year and month <= end_month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


def get_latest_eligible_months(count: int = 3) -> List[Tuple[int, int]]:
    """Get the last N months for automated checking."""
    months = []
    now = datetime.now()
    curr_year = now.year
    curr_month = now.month
    
    for _ in range(count):
        if curr_month == 1:
            curr_month = 12
            curr_year -= 1
        else:
            curr_month -= 1
        months.append((curr_year, curr_month))
    
    return months[::-1]


def is_month_complete(year: int, month: int) -> bool:
    folder_path = Path(f"data/raw/angelone/{year}_{month:02d}")
    success_marker = folder_path / "_SUCCESS.json"
    return success_marker.exists()


def run_angelone_backfill(
    start_year: Optional[int] = None,
    start_month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None
) -> dict:
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("ANGEL ONE BACKFILL STARTED")
    if DRY_RUN:
        logger.info("MODE: DRY RUN (no network calls)")
    logger.info("=" * 70)
    
    # Calculate target months
    if all(v is not None for v in [start_year, start_month, end_year, end_month]):
        mode = "MANUAL"
        logger.info(f"Mode: {mode}")
        logger.info(f"Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        months = generate_month_range(start_year, start_month, end_year, end_month)
    else:
        mode = "AUTO"
        logger.info(f"Mode: {mode}")
        months = get_latest_eligible_months(3)
        logger.info(f"Checking last 3 months: {months[0][0]}-{months[0][1]:02d} to {months[-1][0]}-{months[-1][1]:02d}")
    
    downloader = AngelOneDownloader()
    
    # Use persistent session for backfills
    downloader.open_session()
    
    try:
        skipped = 0
        downloaded_months = []
        failed_months = []
        not_published_count = 0
        
        for year, month in months:
            if is_month_complete(year, month):
                logger.info(f"[SKIP] {year}-{month:02d} - Complete (_SUCCESS.json exists)")
                skipped += 1
            else:
                logger.info(f"[MISSING] {year}-{month:02d} - Attempting download...")
                try:
                    result = downloader.download(year=year, month=month)
                    status = result["status"]
                    
                    if status == "success":
                        downloaded_months.append((year, month))
                    elif status == "skipped":
                        skipped += 1
                    elif status == "not_published":
                        not_published_count += 1
                    else:
                        reason = result.get("reason", "Unknown error")
                        failed_months.append((year, month, reason))
                except Exception as e:
                    failed_months.append((year, month, str(e)))
                
                # Small gap for stability even on direct links
                time.sleep(1)
    finally:
        downloader.close_session()
    
    total_duration = time.time() - start_time
    logger.info("=" * 70)
    logger.info("BACKFILL SUMMARY - ANGEL ONE")
    logger.info("=" * 70)
    logger.info(f"Mode: {mode}")
    logger.info(f"Total checked: {len(months)}")
    logger.info(f"Skipped: {skipped}")
    logger.info(f"Downloaded: {len(downloaded_months)}")
    logger.info(f"Not published: {not_published_count}")
    logger.info(f"Failed: {len(failed_months)}")
    logger.info(f"Total duration: {total_duration:.2f}s")
    logger.info("=" * 70)
    
    return {
        "amc": "ANGELONE",
        "mode": mode,
        "total_checked": len(months),
        "skipped": skipped,
        "downloaded": len(downloaded_months),
        "not_published": not_published_count,
        "failed": len(failed_months),
        "downloaded_months": downloaded_months,
        "duration": total_duration
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Angel One Mutual Fund Backfill")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--start-month", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--end-month", type=int)
    
    args = parser.parse_args()
    
    if all(v is not None for v in [args.start_year, args.start_month, args.end_year, args.end_month]):
        run_angelone_backfill(args.start_year, args.start_month, args.end_year, args.end_month)
    else:
        run_angelone_backfill()
