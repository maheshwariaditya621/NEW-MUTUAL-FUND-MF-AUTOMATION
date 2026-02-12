# src/downloaders/hdfc_downloader.py

import requests
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

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


class HDFCDownloader(BaseDownloader):
    # HDFC API configuration
    AMC_NAME = "hdfc"
    
    # CRITICAL: Correct endpoint with 'fort' (not 'for')
    API_URL = "https://cms.hdfcfund.com/en/hdfc/api/v2/disclosures/monthfortportfolio"
    
    # Month name mapping
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    
    def __init__(self):
        """Initialize HDFC downloader."""
        super().__init__(AMC_HDFC)
        self.notifier = get_notifier()

    def _financial_year(self, year: int, month: int) -> int:
        """
        Convert calendar year/month to financial year.
        
        Financial year in India: April to March
        - Jan-Mar → FY = year - 1
        - Apr-Dec → FY = year
        """
        return year - 1 if month <= 3 else year

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
        
        marker_data = {
            "amc": "HDFC",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        
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
        corrupt_base = Path("data/raw/hdfc/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)
        
        corrupt_target = corrupt_base / f"{year}_{month:02d}"
        
        # If corrupt target already exists, remove it first
        if corrupt_target.exists():
            shutil.rmtree(corrupt_target)
        
        # Move to corrupt
        shutil.move(str(source_dir), str(corrupt_target))
        logger.warning(f"Moved incomplete folder to _corrupt: {year}_{month:02d} (Reason: {reason})")
        
        # Emit warning event
        self.notifier.notify_warning(
            amc="HDFC",
            year=year,
            month=month,
            warning_type="Corruption Recovery",
            message=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )

    def _api_call_with_retry(self, url: str, headers: dict, data: dict, year: int, month: int) -> requests.Response:
        """
        Make API call with retry logic.
        
        Args:
            url: API URL
            headers: Request headers
            data: Request payload
            year: Year (for logging)
            month: Month (for logging)
            
        Returns:
            Response object
            
        Raises:
            requests.HTTPError: On non-retryable errors or max retries exceeded
        """
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = requests.post(url, headers=headers, data=data, timeout=30)
                resp.raise_for_status()
                return resp
            
            except requests.Timeout as e:
                if attempt < MAX_RETRIES:
                    backoff = RETRY_BACKOFF[attempt]
                    logger.warning(f"Timeout on attempt {attempt + 1}/{MAX_RETRIES + 1} for {year}-{month:02d}, retrying in {backoff}s...")
                    time.sleep(backoff)
                else:
                    logger.error(f"Max retries exceeded for {year}-{month:02d}")
                    raise
            
            except requests.HTTPError as e:
                status_code = e.response.status_code
                
                # Never retry on 4xx errors
                if 400 <= status_code < 500:
                    logger.error(f"HTTP {status_code} error (non-retryable) for {year}-{month:02d}")
                    raise
                
                # Retry on 5xx errors
                if 500 <= status_code < 600:
                    if attempt < MAX_RETRIES:
                        backoff = RETRY_BACKOFF[attempt]
                        logger.warning(f"HTTP {status_code} on attempt {attempt + 1}/{MAX_RETRIES + 1} for {year}-{month:02d}, retrying in {backoff}s...")
                        time.sleep(backoff)
                    else:
                        logger.error(f"Max retries exceeded for {year}-{month:02d}")
                        raise
                else:
                    raise

    def download(self, year: int, month: int) -> Dict:
        """
        Download HDFC monthly portfolio files using official API.
        
        Args:
            year: Calendar year (e.g., 2025)
            month: Month (1-12)
            
        Returns:
            Download metadata dictionary
        """
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("HDFC MUTUAL FUND API DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        fy = self._financial_year(year, month)
        target_dir = Path(self.get_target_folder("hdfc", year, month))
        
        # Check for incomplete month (folder exists but no _SUCCESS.json)
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if not success_marker.exists():
                logger.warning(f"Incomplete month detected: {year}-{month:02d}")
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json marker")
            else:
                # Month already complete - check for missing consolidation
                logger.info(f"HDFC: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
                
                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                # Month already complete
                duration = time.time() - start_time
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: HDFC")
                logger.info(f"Mode: SKIPPED")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Status: COMPLETE")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info("=" * 60)
                
                return {
                    "amc": "HDFC Mutual Fund",
                    "year": year,
                    "month": month,
                    "files_downloaded": 0,
                    "files": [],
                    "status": "skipped",
                    "reason": "Already complete"
                }
        
        # Create directory
        self.ensure_directory(str(target_dir))

        # API payload: MUST include all three fields
        data = {
            "year": fy,
            "type": "monthly",
            "month": month
        }

        # Minimal headers (exact as specified)
        headers = {
            "Accept": "*/*",
            "Origin": "https://www.hdfcfund.com",
            "Referer": "https://www.hdfcfund.com/",
            "User-Agent": "Mozilla/5.0",
        }

        logger.info(f"Calling HDFC API (FY={fy}, month={month})")

        # DRY RUN MODE
        if DRY_RUN:
            logger.info("[DRY RUN] Would call API and download files")
            logger.info("[DRY RUN] Skipping actual network calls")
            duration = time.time() - start_time
            
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: HDFC")
            logger.info(f"Mode: DRY RUN")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Status: SIMULATED")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info("=" * 60)
            
            return {
                "amc": "HDFC Mutual Fund",
                "year": year,
                "month": month,
                "files_downloaded": 0,
                "files": [],
                "status": "dry_run"
            }

        try:
            # Use retry logic for API call
            resp = self._api_call_with_retry(self.API_URL, headers, data, year, month)

            # Parse JSON response
            response_data = resp.json()
            files = response_data.get("data", {}).get("files", [])

            # Handle empty files list (month not yet published)
            if not files:
                logger.warning(f"Month not yet published: {month_name} {year}")
                logger.warning("API returned empty files list")
                
                # Emit not published event
                self.notifier.notify_not_published(
                    amc="HDFC",
                    year=year,
                    month=month
                )
                
                # Remove empty directory
                if target_dir.exists():
                    shutil.rmtree(target_dir)
                
                duration = time.time() - start_time
                
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: HDFC")
                logger.info(f"Mode: AUTO")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: 0")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: FAILED (not yet published)")
                logger.info("=" * 60)
                
                return {
                    "amc": "HDFC Mutual Fund",
                    "year": year,
                    "month": month,
                    "files_downloaded": 0,
                    "files": [],
                    "status": "failed",
                    "reason": f"No files returned by API for {month_name} {year}"
                }

            logger.info(f"API returned {len(files)} file(s)")

            # Download all files returned by API
            saved_files: List[str] = []

            for i, file_item in enumerate(files, 1):
                if "file" not in file_item:
                    logger.warning(f"Skipping item without 'file' field")
                    continue
                
                file_obj = file_item["file"]
                
                if "url" not in file_obj or "filename" not in file_obj:
                    logger.warning(f"Skipping file without url/filename")
                    continue
                
                url = file_obj["url"]
                name = file_obj["filename"]
                path = target_dir / name

                logger.info(f"Downloading {i}/{len(files)}: {name}")

                # Normal GET request (no authentication)
                r = requests.get(url, timeout=60)
                r.raise_for_status()

                # Save file with original filename
                with open(path, "wb") as fp:
                    fp.write(r.content)

                saved_files.append(str(path))
                logger.success(f"Saved: {path.name}")

            # File count sanity check (logging only)
            self._check_file_count(len(saved_files), year, month)

            # Create atomic completion marker
            self._create_success_marker(target_dir, year, month, len(saved_files))

            # Consolidate downloads
            self.consolidate_downloads(year, month)

            duration = time.time() - start_time
            
            # Emit success event
            self.notifier.notify_success(
                amc="HDFC",
                year=year,
                month=month,
                files_downloaded=len(saved_files),
                duration=duration
            )

            logger.success("✅ HDFC download completed")
            logger.info("=" * 60)
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: HDFC")
            logger.info(f"Mode: AUTO")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Files downloaded: {len(saved_files)}")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Status: SUCCESS")
            logger.info("=" * 60)

            return {
                "amc": "HDFC Mutual Fund",
                "year": year,
                "month": month,
                "files_downloaded": len(saved_files),
                "files": saved_files,
                "status": "success",
                "duration": duration
            }

        except requests.HTTPError as e:
            error_msg = f"API request failed: HTTP {e.response.status_code}"
            logger.error(error_msg)
            
            # Emit error event
            self.notifier.notify_error(
                amc="HDFC",
                year=year,
                month=month,
                error_type="API Error",
                reason=f"HTTP {e.response.status_code}"
            )
            
            # Remove incomplete directory
            if target_dir.exists():
                shutil.rmtree(target_dir)
            
            duration = time.time() - start_time
            
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: HDFC")
            logger.info(f"Mode: AUTO")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Files downloaded: 0")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Status: FAILED")
            logger.info("=" * 60)
            
            return {
                "amc": "HDFC Mutual Fund",
                "year": year,
                "month": month,
                "status": "failed",
                "reason": error_msg,
                "duration": duration
            }
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Download failed: {error_msg}")
            
            # Emit error event
            self.notifier.notify_error(
                amc="HDFC",
                year=year,
                month=month,
                error_type="Download Error",
                reason=error_msg[:100]  # Truncate long errors
            )
            
            # Remove incomplete directory
            if target_dir.exists():
                shutil.rmtree(target_dir)
            
            duration = time.time() - start_time
            
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: HDFC")
            logger.info(f"Mode: AUTO")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Files downloaded: 0")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Status: FAILED")
            logger.info("=" * 60)
            
            return {
                "amc": "HDFC Mutual Fund",
                "year": year,
                "month": month,
                "status": "failed",
                "reason": error_msg,
                "duration": duration
            }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="HDFC Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Calendar year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    
    args = parser.parse_args()
    
    # Validate month
    if args.month < 1 or args.month > 12:
        logger.error("Invalid month. Must be between 1 and 12.")
        exit(1)
    
    downloader = HDFCDownloader()
    result = downloader.download(year=args.year, month=args.month)
    
    status = result["status"]
    if status == "success":
        logger.success(f"✅ Success: Downloaded {result['files_downloaded']} file(s)")
    elif status == "skipped":
        logger.success(f"✅ Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"ℹ️  Info: Month not yet published")
    else:
        logger.error(f"❌ Failed: {result.get('reason', 'Unknown error')}")
        exit(1)
