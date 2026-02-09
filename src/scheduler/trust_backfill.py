# src/scheduler/trust_backfill.py

import time
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional
from src.downloaders.trust_downloader import TrustDownloader
from src.config import logger

# Import dry-run config
try:
    from src.config.downloader_config import DRY_RUN
except ImportError:
    DRY_RUN = False


def generate_month_range(start_year: int, start_month: int, end_year: int, end_month: int) -> List[Tuple[int, int]]:
    """Generate list of (year, month) tuples from start to end."""
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


def get_latest_eligible_month() -> Tuple[int, int]:
    """Get the latest eligible month (previous completed month)."""
    now = datetime.now()
    if now.month == 1:
        return (now.year - 1, 12)
    else:
        return (now.year, now.month - 1)


def is_month_complete(year: int, month: int) -> bool:
    """Check if month is complete (has _SUCCESS.json marker)."""
    folder_path = Path(f"data/raw/trust/{year}_{month:02d}")
    success_marker = folder_path / "_SUCCESS.json"
    return success_marker.exists()


def run_trust_backfill(
    start_year: Optional[int] = None,
    start_month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None
) -> dict:
    """
    Run Trust backfill.
    
    Modes:
    1. MANUAL_RANGE: User defined range.
    2. AUTO: Latest eligible month.
    """
    start_time = time.time()
    logger.info("=" * 70)
    logger.info("TRUST BACKFILL STARTED")
    if DRY_RUN: logger.info("MODE: DRY RUN")
    logger.info("=" * 70)
    
    downloader = TrustDownloader()
    
    skipped = 0
    downloaded_months = []
    failed_months = []
    not_published_count = 0
    
    try:
        # Open persistent session for efficiency
        downloader.open_session()
        
        if all([start_year, start_month, end_year, end_month]):
            # MODE 1: MANUAL RANGE
            mode = "MANUAL_RANGE"
            logger.info(f"Mode: {mode}")
            logger.info(f"Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
            months = generate_month_range(start_year, start_month, end_year, end_month)
            
            for year, month in months:
                if is_month_complete(year, month):
                    logger.info(f"[SKIP] {year}-{month:02d} - Complete")
                    skipped += 1
                    continue
                
                logger.info(f"[MISSING] {year}-{month:02d} - Attempting download...")
                if DRY_RUN:
                    downloaded_months.append((year, month))
                    continue
                    
                try:
                    result = downloader.download(year, month)
                    status = result.get("status")
                    if status == "success":
                        downloaded_months.append((year, month))
                        logger.success(f"[SUCCESS] {year}-{month:02d}")
                    elif status == "not_published":
                        not_published_count += 1
                        logger.info(f"[NOT PUBLISHED] {year}-{month:02d}")
                    elif status == "skipped":
                        skipped += 1
                    else:
                        reason = result.get("reason", "Unknown")
                        failed_months.append((year, month, reason))
                        logger.warning(f"[FAILED] {year}-{month:02d} - {reason}")
                except Exception as e:
                    failed_months.append((year, month, str(e)))
                    logger.error(f"[ERROR] {year}-{month:02d} - {e}")
        
        else:
            # MODE 2: AUTO
            mode = "AUTO"
            logger.info(f"Mode: {mode}")
            year, month = get_latest_eligible_month()
            logger.info(f"Latest eligible: {year}-{month:02d}")
            
            if is_month_complete(year, month):
                logger.info(f"[SKIP] {year}-{month:02d} - Complete")
                skipped = 1
            else:
                if DRY_RUN:
                    downloaded_months.append((year, month))
                else:
                    try:
                        result = downloader.download(year, month)
                        status = result.get("status")
                        if status == "success":
                            downloaded_months.append((year, month))
                            logger.success(f"[SUCCESS] {year}-{month:02d}")
                        elif status == "not_published":
                            not_published_count = 1
                        elif status == "skipped":
                            skipped = 1
                        else:
                            failed_months.append((year, month, result.get("reason")))
                    except Exception as e:
                        failed_months.append((year, month, str(e)))

    finally:
        downloader.close_session()

    total_duration = time.time() - start_time
    total_checked = 1 if mode == "AUTO" else len(months)
    
    logger.info("=" * 70)
    logger.info("BACKFILL SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Mode: {mode}")
    logger.info(f"Downloaded: {len(downloaded_months)}")
    logger.info(f"Skipped: {skipped}")
    logger.info(f"Failed: {len(failed_months)}")
    logger.info(f"Duration: {total_duration:.2f}s")
    logger.info("=" * 70)
    
    return {
        "mode": mode,
        "total_checked": total_checked,
        "skipped": skipped,
        "downloaded": len(downloaded_months),
        "failed": len(failed_months),
        "not_published": not_published_count,
        "downloaded_months": downloaded_months,
        "failed_months": failed_months,
        "duration": total_duration
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--start-month", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--end-month", type=int)
    args = parser.parse_args()
    
    run_trust_backfill(args.start_year, args.start_month, args.end_year, args.end_month)
