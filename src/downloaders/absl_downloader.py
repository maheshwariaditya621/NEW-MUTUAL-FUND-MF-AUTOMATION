# src/downloaders/absl_downloader.py

import requests
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.utils.file_validator import validate_and_fix_extension

# Import downloader config
try:
    from src.config.downloader_config import (
        DRY_RUN, MAX_RETRIES, RETRY_BACKOFF
    )
except ImportError:
    # Fallback defaults if config not found
    DRY_RUN = False
    MAX_RETRIES = 3
    RETRY_BACKOFF = [5, 15, 30]


class ABSLDownloader(BaseDownloader):
    # ABSL API configuration
    AMC_NAME = "absl"
    API_URL = "https://mutualfund.adityabirlacapital.com/postlogin/CustomApi/Resources/FactsheetAccordionById"
    API_PARAMS = {
        "id": "3ccab227-9de5-4494-b78d-2b4f7c0c054a",
        "ctype": "/sitecore/content/Root/BSL/Library/Lists/FAQ/Customer Types/Individual",
        "month": "",  # Fetch all months
        "year": 0     # Fetch all years
    }
    
    # ABSL-specific: single consolidated ZIP per month
    EXPECTED_FILE_COUNT = 1
    
    # Month name mapping
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    
    # Exclude keywords (case-insensitive)
    EXCLUDE_KEYWORDS = [
        "debt", "close ended", "fmp", "fmps", "interval",
        "fund of fund", "open ended", "update"
    ]
    
    def __init__(self):
        """Initialize ABSL downloader."""
        super().__init__("Aditya Birla Sun Life Mutual Fund")

    def _fetch_absl_metadata(self) -> dict:
        """
        Fetch metadata from ABSL API with retry logic.
        
        Returns:
            API response dict with AccordionList
            
        Raises:
            Exception: If all retries fail
        """
        url = self.API_URL
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"[DRY RUN] Would fetch metadata from: {url}")
                    return {"ReturnCode": "1", "AccordionList": []}
                
                logger.info(f"Fetching metadata from ABSL API (attempt {attempt + 1}/{MAX_RETRIES + 1})")
                
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/json"
                }
                
                response = requests.get(
                    url,
                    params=self.API_PARAMS,
                    headers=headers,
                    timeout=30
                )
                
                response.raise_for_status()
                
                data = response.json()
                
                if str(data.get("ReturnCode")) != "1":
                    raise Exception(f"API returned error code: {data.get('ReturnCode')}")
                
                accordion_list = data.get("AccordionList", [])
                logger.info(f"Fetched {len(accordion_list)} items from API")
                
                return data
                
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt < MAX_RETRIES:
                    wait_time = RETRY_BACKOFF[attempt]
                    logger.warning(f"Network error: {str(e)}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed after {MAX_RETRIES + 1} attempts: {str(e)}")
                    
            except requests.HTTPError as e:
                # Don't retry on 4xx errors
                if 400 <= e.response.status_code < 500:
                    raise Exception(f"Client error {e.response.status_code}: {str(e)}")
                # Retry on 5xx errors
                elif attempt < MAX_RETRIES:
                    wait_time = RETRY_BACKOFF[attempt]
                    logger.warning(f"Server error: {str(e)}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed after {MAX_RETRIES + 1} attempts: {str(e)}")
                    
            except Exception as e:
                raise Exception(f"Unexpected error fetching metadata: {str(e)}")

    def _filter_month_items(self, accordion_list: List[dict], year: int, month: int) -> List[dict]:
        """
        Filter for consolidated monthly portfolio ONLY.
        
        Filtering rules:
        1. ResourceLink starts with "Monthly Portfolio" or "Monthly Portfolios"
        2. Extract month/year from title
        3. Exclude: debt, close ended, fmp, interval, fund of fund, open ended, update
        
        Args:
            accordion_list: List of items from API
            year: Target year
            month: Target month (1-12)
            
        Returns:
            List of matching items (MUST be 0 or 1)
        """
        month_name = self.MONTH_NAMES[month]
        matched = []
        
        logger.info(f"Filtering for: {month_name} {year}")
        
        for item in accordion_list:
            resource_link = item.get("ResourceLink", "")
            
            # Check if it starts with "Monthly Portfolio" variants
            if not (resource_link.startswith("Monthly Portfolio as on") or 
                    resource_link.startswith("Monthly Portfolios as on")):
                continue
            
            # Exclude unwanted categories (case-insensitive)
            resource_link_lower = resource_link.lower()
            if any(keyword in resource_link_lower for keyword in self.EXCLUDE_KEYWORDS):
                logger.debug(f"Excluded (keyword match): {resource_link}")
                continue
            
            # Extract month and year from title
            # Format: "Monthly Portfolios as on December 31, 2025"
            # or "Monthly Portfolio as on December 31, 2017"
            if month_name in resource_link and str(year) in resource_link:
                matched.append(item)
                logger.info(f"Matched: {resource_link}")
        
        logger.info(f"Found {len(matched)} matching item(s) for {year}-{month:02d}")
        return matched

    def _validate_single_file(self, filtered_items: List[dict], year: int, month: int):
        """
        Enforce ABSL contract: exactly 0 or 1 file per month.
        
        Args:
            filtered_items: Filtered items
            year: Target year
            month: Target month
            
        Raises:
            ValueError: If >1 file found (contract violation)
        """
        if len(filtered_items) > 1:
            titles = [item.get("ResourceLink", "Unknown") for item in filtered_items]
            raise ValueError(
                f"ABSL contract violation: Found {len(filtered_items)} files for {year}-{month:02d}. "
                f"Expected exactly 1. Titles: {titles}"
            )

    def _check_file_count(self, file_count: int, year: int, month: int):
        """
        ABSL-specific sanity check (warning only, never blocks).
        
        Expected: file_count == 1
        
        Args:
            file_count: Number of files downloaded
            year: Year
            month: Month
        """
        if file_count != self.EXPECTED_FILE_COUNT:
            logger.warning(
                f"File count ({file_count}) does not match expected ({self.EXPECTED_FILE_COUNT}) "
                f"for {year}-{month:02d}"
            )
        else:
            logger.info(f"File count ({file_count}) matches expected")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        """
        Create atomic _SUCCESS.json marker.
        
        Args:
            target_dir: Target directory
            year: Year
            month: Month
            file_count: Number of files downloaded
        """
        marker_path = target_dir / "_SUCCESS.json"
        tmp_marker_path = target_dir / "_SUCCESS.json.tmp"
        
        marker_data = {
            "amc": self.AMC_NAME,
            "year": year,
            "month": month,
            "file_count": file_count,
            "timestamp": datetime.now().isoformat()
        }
        
        # Atomic write: write to tmp, then rename
        with open(tmp_marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        
        tmp_marker_path.rename(marker_path)
        
        logger.info(f"Created success marker: {marker_path.name}")

    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str):
        """
        Move incomplete folder to _corrupt with timestamp.
        
        Args:
            source_dir: Source directory to move
            year: Year
            month: Month
            reason: Reason for corruption
        """
        corrupt_base = Path(f"data/raw/{self.AMC_NAME}/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        corrupt_dir = corrupt_base / f"{year}_{month:02d}_{timestamp}"
        
        logger.warning(f"Moving incomplete folder to: {corrupt_dir}")
        logger.warning(f"Reason: {reason}")
        
        shutil.move(str(source_dir), str(corrupt_dir))
        
        logger.info(f"Corruption recovery complete")

    def download(self, year: int, month: int) -> Dict:
        """
        Download ABSL monthly portfolio.
        
        Process:
        1. Check corruption (folder without _SUCCESS.json)
        2. Fetch metadata (month="", year=0)
        3. Filter for target month
        4. VALIDATE: len(filtered_items) must be 0 or 1
           - If 0: return status="not_published"
           - If >1: raise ValueError (contract violation)
        5. Download single ZIP with streaming
        6. Sanity check (expect file_count == 1)
        7. Create _SUCCESS.json on complete success
        8. Return status dict
        
        Args:
            year: Calendar year
            month: Month (1-12)
            
        Returns:
            {
                "amc": "absl",
                "year": year,
                "month": month,
                "status": "success" | "skipped" | "not_published" | "failed",
                "files_downloaded": 0 or 1,
                "duration": float
            }
        """
        start_time = time.time()
        
        logger.info("=" * 60)
        logger.info("ABSL MUTUAL FUND API DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        logger.info("=" * 60)
        
        if DRY_RUN:
            logger.info("[DRY RUN MODE] No actual downloads will be performed")
        
        target_dir = Path(f"data/raw/{self.AMC_NAME}/{year}_{month:02d}")
        success_marker = target_dir / "_SUCCESS.json"
        
        try:
            # Check if already complete
            if success_marker.exists():
                # Month already complete - check for missing consolidation
                logger.info(f"ABSL: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/erroed previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info(f"Month {year}-{month:02d} already complete - UPDATED")
                logger.info("=" * 60)
                
                return {
                    "amc": self.AMC_NAME,
                    "year": year,
                    "month": month,
                    "status": "skipped",
                    "files_downloaded": 0,
                    "duration": duration
                }
            
            # Check for corruption (folder exists without _SUCCESS.json)
            if target_dir.exists():
                logger.warning(f"Found incomplete folder: {target_dir}")
                self._move_to_corrupt(target_dir, year, month, "Incomplete download detected")
            
            # Create target directory
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # Fetch metadata
            logger.info("Fetching metadata from ABSL API...")
            api_response = self._fetch_absl_metadata()
            accordion_list = api_response.get("AccordionList", [])
            
            # Filter for target month
            filtered_items = self._filter_month_items(accordion_list, year, month)
            
            # VALIDATE: Must be 0 or 1 file
            self._validate_single_file(filtered_items, year, month)
            
            # Handle not published
            if not filtered_items:
                duration = time.time() - start_time
                
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: ABSL")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: 0")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: NOT PUBLISHED")
                logger.info("=" * 60)
                
                # Remove empty folder
                if target_dir.exists():
                    shutil.rmtree(target_dir)
                
                return {
                    "amc": self.AMC_NAME,
                    "year": year,
                    "month": month,
                    "status": "not_published",
                    "files_downloaded": 0,
                    "duration": duration
                }
            
            # Download single file
            item = filtered_items[0]
            pdf_url = item.get("pdfUrl", "")
            resource_link = item.get("ResourceLink", "Unknown")
            
            if not pdf_url:
                raise Exception(f"No pdfUrl found for: {resource_link}")
            
            # FIX: Replace outdated domain with current domain
            # ABSL API still returns old azureedge.net domain, but files are now hosted on mutualfund.adityabirlacapital.com
            if "abcscprod.azureedge.net" in pdf_url:
                pdf_url = pdf_url.replace("abcscprod.azureedge.net", "mutualfund.adityabirlacapital.com")
                logger.info(f"Corrected URL domain: azureedge.net → adityabirlacapital.com")
            
            # Extract filename from URL
            filename = pdf_url.split("/")[-1]
            file_path = target_dir / filename
            
            logger.info(f"Downloading: {resource_link}")
            logger.info(f"URL: {pdf_url}")
            logger.info(f"Filename: {filename}")
            
            if DRY_RUN:
                logger.info(f"[DRY RUN] Would download to: {file_path}")
            else:
                # Download with streaming and retry logic
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "*/*"
                }
                
                for attempt in range(MAX_RETRIES + 1):
                    try:
                        response = requests.get(pdf_url, stream=True, headers=headers, timeout=60)
                        response.raise_for_status()
                        
                        with open(file_path, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        # Validate file format and fix extension if needed
                        validated_path = validate_and_fix_extension(file_path)
                        if validated_path != file_path:
                            file_path = validated_path
                        
                        file_size = file_path.stat().st_size
                        logger.info(f"Downloaded: {file_path.name} ({file_size:,} bytes)")
                        break
                        
                    except (requests.Timeout, requests.ConnectionError) as e:
                        if attempt < MAX_RETRIES:
                            wait_time = RETRY_BACKOFF[attempt]
                            logger.warning(f"Download error: {str(e)}. Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                        else:
                            raise Exception(f"Failed to download ZIP after {MAX_RETRIES + 1} attempts: {str(e)}")
                            
                    except requests.HTTPError as e:
                        # Don't retry on 4xx errors
                        if 400 <= e.response.status_code < 500:
                            raise Exception(f"Client error {e.response.status_code}: {str(e)}")
                        # Retry on 5xx errors
                        elif attempt < MAX_RETRIES:
                            wait_time = RETRY_BACKOFF[attempt]
                            logger.warning(f"Server error: {str(e)}. Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                        else:
                            raise Exception(f"Failed to download ZIP after {MAX_RETRIES + 1} attempts: {str(e)}")
            
            # Sanity check
            file_count = 1
            self._check_file_count(file_count, year, month)
            
            # Create success marker
            self._create_success_marker(target_dir, year, month, 1)
            
            # Consolidate downloads
            self.consolidate_downloads(year, month)
            
            duration = time.time() - start_time
            
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: ABSL")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Files downloaded: {file_count}")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Status: SUCCESS")
            logger.info("=" * 60)
            
            return {
                "amc": self.AMC_NAME,
                "year": year,
                "month": month,
                "status": "success",
                "files_downloaded": file_count,
                "duration": duration
            }
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Download failed: {error_msg}")
            
            # Move partial data to _corrupt if folder exists
            if target_dir.exists():
                try:
                    self._move_to_corrupt(target_dir, year, month, f"Download failed: {error_msg}")
                except Exception as cleanup_error:
                    logger.error(f"Failed to move corrupt folder: {str(cleanup_error)}")
            
            duration = time.time() - start_time
            
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: ABSL")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Files downloaded: 0")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Status: FAILED")
            logger.info("=" * 60)
            
            return {
                "amc": self.AMC_NAME,
                "year": year,
                "month": month,
                "status": "failed",
                "reason": error_msg,
                "duration": duration
            }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="ABSL Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Calendar year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    
    args = parser.parse_args()
    
    # Validate month
    if args.month < 1 or args.month > 12:
        logger.error("Invalid month. Must be between 1 and 12.")
        exit(1)
    
    downloader = ABSLDownloader()
    result = downloader.download(year=args.year, month=args.month)
    
    status = result["status"]
    if status == "success":
        logger.success(f"✅ Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success(f"✅ Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"ℹ️  Info: Month not yet published")
    else:
        logger.error(f"❌ Failed: {result.get('reason', 'Unknown error')}")
        exit(1)
