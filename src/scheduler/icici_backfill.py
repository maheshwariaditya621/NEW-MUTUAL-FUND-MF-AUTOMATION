"""
ICICI Auto Backfill Module.

Supports manual range mode only.
Uses _SUCCESS.json marker as source of truth for completion.
"""

import time
from pathlib import Path
from typing import List, Tuple, Optional
from src.downloaders.icici_downloader import ICICIDownloader
from src.config import logger

# Import dry-run config
try:
    from src.config.downloader_config import DRY_RUN
except ImportError:
    DRY_RUN = False


def generate_month_range(start_year: int, start_month: int, end_year: int, end_month: int) -> List[Tuple[int, int]]:
    """
    Generate list of (year, month) tuples from start to end.
    
    Args:
        start_year: Starting year
        start_month: Starting month (1-12)
        end_year: Ending year
        end_month: Ending month (1-12)
        
    Returns:
        List of (year, month) tuples
    """
    months = []
    year = start_year
    month = start_month
    
    while (year < end_year) or (year == end_year and month <= end_month):
        months.append((year, month))
        
        # Move to next month
        month += 1
        if month > 12:
            month = 1
            year += 1
    
    return months


def is_month_complete(year: int, month: int) -> bool:
    """
    Check if month is complete (has _SUCCESS.json marker).
    
    Args:
        year: Year
        month: Month (1-12)
        
    Returns:
        True if _SUCCESS.json exists, False otherwise
    """
    folder_path = Path(f"data/raw/icici/{year}_{month:02d}")
    success_marker = folder_path / "_SUCCESS.json"
    
    return success_marker.exists()


def run_icici_backfill(
    start_year: Optional[int] = None,
    start_month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None
) -> dict:
    """
    Run ICICI backfill.
    
    Manual Range Mode (REQUIRED):
        Downloads missing months in user-defined range
    
    Args:
        start_year: Start year (REQUIRED)
        start_month: Start month 1-12 (REQUIRED)
        end_year: End year (REQUIRED)
        end_month: End month 1-12 (REQUIRED)
        
    Returns:
        Dictionary with summary:
            - mode: str ("range")
            - total_checked: int
            - skipped: int
            - downloaded: int
            - failed: int
            - downloaded_months: List[Tuple[int, int]]
            - failed_months: List[Tuple[int, int, str]]
    """
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("ICICI BACKFILL STARTED")
    if DRY_RUN:
        logger.info("MODE: DRY RUN (no network calls)")
    logger.info("=" * 70)
    
    # Validate that all parameters are provided
    if not all([start_year, start_month, end_year, end_month]):
        logger.error("ERROR: All date parameters are required (start_year, start_month, end_year, end_month)")
        logger.error("ICICI backfill requires explicit date range - no auto mode available")
        logger.info("=" * 70)
        return {
            "mode": "MANUAL_RANGE",
            "total_checked": 0,
            "skipped": 0,
            "downloaded": 0,
            "failed": 0,
            "downloaded_months": [],
            "failed_months": [],
            "duration": 0
        }
    
    # Manual range mode
    mode = "MANUAL_RANGE"
    logger.info(f"Mode: {mode}")
    logger.info(f"Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
    
    months = generate_month_range(start_year, start_month, end_year, end_month)
    logger.info(f"Total months to check: {len(months)}")
    
    # Initialize downloader
    downloader = ICICIDownloader()
    
    # Track results
    skipped = 0
    downloaded_months = []
    failed_months = []
    
    # Process each month
    for year, month in months:
        month_start_time = time.time()
        
        if is_month_complete(year, month):
            logger.info(f"[SKIP] {year}-{month:02d} - Complete (_SUCCESS.json exists)")
            skipped += 1
        else:
            logger.info(f"[MISSING] {year}-{month:02d} - Attempting download...")
            
            if DRY_RUN:
                logger.info(f"[DRY RUN] Would download {year}-{month:02d}")
                continue
            
            try:
                result = downloader.download(year=year, month=month)
                
                if result["status"] == "success":
                    downloaded_months.append((year, month))
                    month_duration = time.time() - month_start_time
                    logger.success(f"[SUCCESS] {year}-{month:02d} - Downloaded {result['files_downloaded']} file(s) in {month_duration:.2f}s")
                elif result["status"] == "not_published":
                    # Not published is VALID - some months are not available from AMC
                    logger.info(f"[NOT PUBLISHED] {year}-{month:02d} - Month not available from AMC (expected)")
                    # Do NOT count as failed - this is normal
                else:
                    reason = result.get("reason", "Unknown error")
                    failed_months.append((year, month, reason))
                    logger.warning(f"[FAILED] {year}-{month:02d} - {reason}")
            except Exception as e:
                error_msg = str(e)
                failed_months.append((year, month, error_msg))
                logger.error(f"[ERROR] {year}-{month:02d} - {error_msg}")
    
    # Summary
    total_duration = time.time() - start_time
    
    logger.info("=" * 70)
    logger.info("BACKFILL SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Mode: {mode}")
    logger.info(f"Total checked: {len(months)}")
    logger.info(f"Skipped (already complete): {skipped}")
    logger.info(f"Downloaded: {len(downloaded_months)}")
    logger.info(f"Failed: {len(failed_months)}")
    logger.info(f"Total duration: {total_duration:.2f}s")
    
    if downloaded_months:
        logger.info("Downloaded months:")
        for year, month in downloaded_months:
            logger.info(f"  ✅ {year}-{month:02d}")
    
    if failed_months:
        logger.warning("Failed months:")
        for year, month, reason in failed_months:
            logger.warning(f"  ❌ {year}-{month:02d} - {reason}")
    
    logger.info("=" * 70)
    
    return {
        "mode": mode,
        "total_checked": len(months),
        "skipped": skipped,
        "downloaded": len(downloaded_months),
        "failed": len(failed_months),
        "downloaded_months": downloaded_months,
        "failed_months": failed_months,
        "duration": total_duration
    }


if __name__ == "__main__":
    # For testing - requires explicit date range
    import argparse
    
    parser = argparse.ArgumentParser(description="ICICI Backfill - Manual Range Mode")
    parser.add_argument("--start-year", type=int, required=True, help="Start year (YYYY)")
    parser.add_argument("--start-month", type=int, required=True, help="Start month (1-12)")
    parser.add_argument("--end-year", type=int, required=True, help="End year (YYYY)")
    parser.add_argument("--end-month", type=int, required=True, help="End month (1-12)")
    
    args = parser.parse_args()
    
    result = run_icici_backfill(
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month
    )
    
    if result["downloaded"] > 0:
        logger.success(f"✅ Backfill completed - {result['downloaded']} month(s) downloaded")
    elif result["skipped"] == result["total_checked"]:
        logger.info("ℹ️  All months already downloaded")
    else:
        logger.warning(f"⚠️  Backfill completed with {result['failed']} failure(s)")
