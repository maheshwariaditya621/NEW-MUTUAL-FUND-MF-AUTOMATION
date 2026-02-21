"""
Abakkus Mutual Fund Auto Backfill Module.

Supports two modes:
1. Manual range mode: User-defined date range
2. Auto mode: Latest eligible month only

Uses _SUCCESS.json marker as source of truth for completion.
"""

import time
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional
from src.downloaders.abakkus_downloader import AbakkusDownloader
from src.config import logger

# Import dry-run config
try:
    from src.config.downloader_config import DRY_RUN
except ImportError:
    DRY_RUN = False


def generate_month_range(start_year: int, start_month: int, end_year: int, end_month: int) -> List[Tuple[int, int]]:
    """
    Generate list of (year, month) tuples from start to end.
    """
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
    """
    Get the latest eligible month (previous completed month).
    """
    now = datetime.now()
    if now.month == 1:
        return (now.year - 1, 12)
    else:
        return (now.year, now.month - 1)


def run_backfill(
    start_year: Optional[int] = None,
    start_month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None
) -> dict:
    """
    Run Abakkus backfill.
    """
    start_time = time.time()

    logger.info("=" * 70)
    logger.info("ABAKKUS BACKFILL STARTED")
    if DRY_RUN:
        logger.info("MODE: DRY RUN (no network calls)")
    logger.info("=" * 70)

    downloader = AbakkusDownloader()
    skipped = 0
    downloaded_months = []
    failed_months = []
    not_published_count = 0

    if all([start_year, start_month, end_year, end_month]):
        mode = "MANUAL_RANGE"
        logger.info(f"Mode: {mode}")
        logger.info(f"Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        months = generate_month_range(start_year, start_month, end_year, end_month)
    else:
        mode = "AUTO"
        logger.info(f"Mode: {mode}")
        months = [get_latest_eligible_month()]

    logger.info(f"Total months to check: {len(months)}")

    for year, month in months:
        logger.info(f"[CHECK] {year}-{month:02d}")
        month_start_time = time.time()
        try:
            result = downloader.download(year=year, month=month)
            status = result["status"]

            if status == "success":
                downloaded_months.append((year, month))
                logger.success(f"[SUCCESS] {year}-{month:02d} in {time.time() - month_start_time:.2f}s")
            elif status == "skipped":
                skipped += 1
                logger.success(f"[OK] {year}-{month:02d} - Already complete")
            elif status == "not_published":
                not_published_count += 1
                logger.info(f"[NOT PUBLISHED] {year}-{month:02d}")
            else:
                reason = result.get("reason", "Unknown error")
                failed_months.append((year, month, reason))
                logger.warning(f"[FAILED] {year}-{month:02d} - {reason}")
        except Exception as e:
            error_msg = str(e)
            failed_months.append((year, month, error_msg))
            logger.error(f"[ERROR] {year}-{month:02d} - {error_msg}")

    total_duration = time.time() - start_time
    logger.info("=" * 70)
    logger.info("BACKFILL SUMMARY")
    logger.info(f"Skipped: {skipped} | Downloaded: {len(downloaded_months)} | Failed: {len(failed_months)}")
    logger.info(f"Total duration: {total_duration:.2f}s")
    logger.info("=" * 70)

    return {
        "mode": mode,
        "skipped": skipped,
        "downloaded": len(downloaded_months),
        "failed": len(failed_months),
        "not_published": not_published_count,
        "downloaded_months": downloaded_months,
        "failed_months": failed_months,
        "duration": total_duration
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Abakkus Mutual Fund Backfill")
    parser.add_argument("--start-year", type=int, help="Start year (YYYY)")
    parser.add_argument("--start-month", type=int, help="Start month (1-12)")
    parser.add_argument("--end-year", type=int, help="End year (YYYY)")
    parser.add_argument("--end-month", type=int, help="End month (1-12)")
    parser.add_argument("--auto", action="store_true", help="Run auto mode")

    args = parser.parse_args()

    if args.auto:
        result = run_backfill()
    elif all([args.start_year, args.start_month, args.end_year, args.end_month]):
        result = run_backfill(args.start_year, args.start_month, args.end_year, args.end_month)
    else:
        logger.error("Usage: Provide all 4 date args OR --auto")
        exit(1)

    if result["failed"] > 0:
        exit(1)
    exit(0)
