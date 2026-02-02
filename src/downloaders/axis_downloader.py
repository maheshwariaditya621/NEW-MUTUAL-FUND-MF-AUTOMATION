# src/downloaders/axis_downloader.py

import requests
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from src.downloaders.base_downloader import BaseDownloader
from src.config import logger

# Import downloader config
try:
    from src.config.downloader_config import (
        DRY_RUN, FILE_COUNT_MIN, FILE_COUNT_MAX, 
        MAX_RETRIES, RETRY_BACKOFF
    )
except ImportError:
    # Fallback defaults if config not found
    DRY_RUN = False
    FILE_COUNT_MIN = 80
    FILE_COUNT_MAX = 120
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]


class AxisDownloader(BaseDownloader):
    # Axis API configuration
    AMC_NAME = "axis"
    BASE_URL = "https://www.axismf.com"
    API_ENDPOINT = "/cms/api/statutory-disclosures-scheme"
    CATEGORY = "Monthly Scheme Portfolios"
    
    # Month name mapping
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    
    def __init__(self):
        """Initialize Axis downloader."""
        super().__init__("Axis Mutual Fund")

    def _fetch_axis_metadata(self) -> List[dict]:
        """
        Fetch metadata from Axis API with retry logic.
        
        Returns:
            List of metadata items
            
        Raises:
            requests.HTTPError: On non-retryable errors or max retries exceeded
        """
        url = f"{self.BASE_URL}{self.API_ENDPOINT}"
        params = {"cat": self.CATEGORY}
        
        logger.info(f"Fetching metadata from Axis API: {url}")
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                logger.info(f"Successfully fetched metadata from Axis API")
                return resp.json()
            
            except requests.Timeout as e:
                if attempt < MAX_RETRIES:
                    backoff = RETRY_BACKOFF[attempt]
                    logger.warning(f"Timeout on attempt {attempt + 1}/{MAX_RETRIES + 1}, retrying in {backoff}s...")
                    time.sleep(backoff)
                else:
                    logger.error(f"Max retries exceeded")
                    raise
            
            except requests.HTTPError as e:
                status_code = e.response.status_code
                
                # Never retry on 4xx errors
                if 400 <= status_code < 500:
                    logger.error(f"HTTP {status_code} error (non-retryable)")
                    raise
                
                # Retry on 5xx errors
                if 500 <= status_code < 600:
                    if attempt < MAX_RETRIES:
                        backoff = RETRY_BACKOFF[attempt]
                        logger.warning(f"HTTP {status_code} on attempt {attempt + 1}/{MAX_RETRIES + 1}, retrying in {backoff}s...")
                        time.sleep(backoff)
                    else:
                        logger.error(f"Max retries exceeded")
                        raise
                else:
                    raise

    def _filter_month_items(self, items: List[dict], year: int, month: int) -> List[dict]:
        """
        Filter API response items for target year/month.
        Only selects MONTHLY CONSOLIDATED portfolio files.
        
        Args:
            items: List of API response items
            year: Target year
            month: Target month (1-12)
            
        Returns:
            Filtered list of matching items
        """
        month_name = self.MONTH_NAMES.get(month)
        if not month_name:
            return []
        
        matched = []
        for item in items:
            try:
                # Check category
                if item.get("field_statutory_disclosures_cate") != self.CATEGORY:
                    continue
                
                # Check scheme code (must be Consolidated)
                if item.get("field_aboutus_scheme_code") != "Consolidated":
                    continue
                
                # Check year
                if str(item.get("field_year", "")).strip() != str(year):
                    continue
                
                # Check month
                if item.get("field_months", "").strip() != month_name:
                    continue
                
                # Check file path contains MONTHLY and excludes WEEKLY/DAILY
                file_path = item.get("field_related_file", "").upper()
                if "MONTHLY" not in file_path:
                    continue
                if "WEEKLY" in file_path or "DAILY" in file_path:
                    continue
                
                matched.append(item)
            except (AttributeError, KeyError, TypeError):
                # Skip items with missing/invalid fields
                continue
        
        return matched

    def _check_file_count(self, file_count: int, year: int, month: int):
        """
        Sanity check file count (logging only, never fails).
        
        Args:
            file_count: Number of files downloaded
            year: Year
            month: Month
        """
        if file_count < FILE_COUNT_MIN:
            logger.warning(f"File count ({file_count}) below expected minimum ({FILE_COUNT_MIN}) for {year}-{month:02d}")
        elif file_count > FILE_COUNT_MAX:
            logger.warning(f"File count ({file_count}) above expected maximum ({FILE_COUNT_MAX}) for {year}-{month:02d}")
        else:
            logger.info(f"File count ({file_count}) within normal range ({FILE_COUNT_MIN}-{FILE_COUNT_MAX})")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        """
        Create atomic completion marker.
        
        Args:
            target_dir: Target directory
            year: Year
            month: Month
            file_count: Number of files downloaded
        """
        marker_path = target_dir / "_SUCCESS.json"
        tmp_marker_path = target_dir / "_SUCCESS.json.tmp"
        
        marker_data = {
            "amc": "axis",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        
        # Atomic write: write to tmp, then rename
        with open(tmp_marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        
        tmp_marker_path.rename(marker_path)
        
        logger.info(f"Created completion marker: {marker_path.name}")

    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str):
        """
        Move incomplete/corrupt folder to quarantine.
        
        Args:
            source_dir: Source directory to move
            year: Year
            month: Month
            reason: Reason for corruption
        """
        corrupt_base = Path("data/raw/axis/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)
        
        corrupt_target = corrupt_base / f"{year}_{month:02d}"
        
        # If corrupt target already exists, append timestamp to preserve it
        if corrupt_target.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            corrupt_target = corrupt_base / f"{year}_{month:02d}_{timestamp}"
        
        # Move to corrupt
        shutil.move(str(source_dir), str(corrupt_target))
        logger.warning(f"Moved incomplete folder to _corrupt: {corrupt_target.name} (Reason: {reason})")

    def download(self, year: int, month: int) -> Dict:
        """
        Download Axis monthly portfolio files using official API.
        
        Args:
            year: Calendar year (e.g., 2025)
            month: Month (1-12)
            
        Returns:
            Download metadata dictionary
        """
        start_time = time.time()
        
        logger.info("=" * 60)
        logger.info("AXIS MUTUAL FUND API DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Check for incomplete month (folder exists but no _SUCCESS.json)
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if not success_marker.exists():
                logger.warning(f"Incomplete month detected: {year}-{month:02d}")
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json marker")
            else:
                # Month already complete
                duration = time.time() - start_time
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: Axis")
                logger.info(f"Mode: SKIPPED")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Status: ALREADY COMPLETE")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info("=" * 60)
                
                return {
                    "amc": "axis",
                    "year": year,
                    "month": month,
                    "files_downloaded": 0,
                    "status": "skipped"
                }
        
        # Create directory
        self.ensure_directory(str(target_dir))

        # DRY RUN MODE
        if DRY_RUN:
            logger.info("[DRY RUN] Would call API and download files")
            logger.info("[DRY RUN] Skipping actual network calls")
            duration = time.time() - start_time
            
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: Axis")
            logger.info(f"Mode: DRY RUN")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Status: SIMULATED")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info("=" * 60)
            
            return {
                "amc": "axis",
                "year": year,
                "month": month,
                "files_downloaded": 0,
                "status": "dry_run"
            }

        try:
            # Step 1: Fetch full metadata
            all_items = self._fetch_axis_metadata()
            
            # Step 2: Filter for target month
            filtered_items = self._filter_month_items(all_items, year, month)
            
            # Step 3: Handle not published
            if not filtered_items:
                logger.warning(f"No matching items found for {year}-{month:02d}")
                logger.warning("Month not yet published")
                
                duration = time.time() - start_time
                
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: Axis")
                logger.info(f"Mode: AUTO")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: 0")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: NOT PUBLISHED")
                logger.info("=" * 60)
                
                return {
                    "amc": "axis",
                    "year": year,
                    "month": month,
                    "files_downloaded": 0,
                    "status": "not_published"
                }
            
            logger.info(f"Found {len(filtered_items)} matching item(s)")
            
            # Step 4: Download files
            saved_files: List[str] = []
            
            for i, item in enumerate(filtered_items, 1):
                file_path = item.get("field_related_file", "")
                file_url = f"{self.BASE_URL}{file_path}"
                filename = file_path.split("/")[-1]
                file_dest = target_dir / filename
                
                logger.info(f"Downloading {i}/{len(filtered_items)}: {filename}")
                
                # Stream download
                r = requests.get(file_url, stream=True, timeout=60)
                r.raise_for_status()
                
                # Save with original filename using streaming
                with open(file_dest, "wb") as fp:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            fp.write(chunk)
                
                saved_files.append(str(file_dest))
                logger.success(f"Saved: {filename}")
            
            # File count sanity check (logging only)
            self._check_file_count(len(saved_files), year, month)
            
            # Create atomic completion marker
            self._create_success_marker(target_dir, year, month, len(saved_files))
            
            duration = time.time() - start_time
            
            logger.success("✅ AXIS download completed")
            logger.info("=" * 60)
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: Axis")
            logger.info(f"Mode: AUTO")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Files downloaded: {len(saved_files)}")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Status: SUCCESS")
            logger.info("=" * 60)

            return {
                "amc": "axis",
                "year": year,
                "month": month,
                "files_downloaded": len(saved_files),
                "status": "success",
                "duration": duration
            }

        except requests.HTTPError as e:
            error_msg = f"API request failed: HTTP {e.response.status_code}"
            logger.error(error_msg)
            
            duration = time.time() - start_time
            
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: Axis")
            logger.info(f"Mode: AUTO")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Files downloaded: 0")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Status: FAILED")
            logger.info("=" * 60)
            
            return {
                "amc": "axis",
                "year": year,
                "month": month,
                "status": "failed",
                "reason": error_msg,
                "duration": duration
            }
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Download failed: {error_msg}")
            
            duration = time.time() - start_time
            
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: Axis")
            logger.info(f"Mode: AUTO")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Files downloaded: 0")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Status: FAILED")
            logger.info("=" * 60)
            
            return {
                "amc": "axis",
                "year": year,
                "month": month,
                "status": "failed",
                "reason": error_msg,
                "duration": duration
            }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Axis Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Calendar year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    
    args = parser.parse_args()
    
    # Validate month
    if args.month < 1 or args.month > 12:
        logger.error("Invalid month. Must be between 1 and 12.")
        exit(1)
    
    downloader = AxisDownloader()
    result = downloader.download(year=args.year, month=args.month)
    
    if result["status"] != "success":
        exit(1)
