"""
HSBC Mutual Fund Downloader.

Month-based downloader following HDFC gold standard.
"""

import requests
import re
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
from urllib.parse import urljoin
from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier
from src.utils.file_validator import validate_and_fix_extension

try:
    from src.config.downloader_config import DRY_RUN, MAX_RETRIES, RETRY_BACKOFF
except ImportError:
    DRY_RUN = False
    MAX_RETRIES = 3
    RETRY_BACKOFF = [5, 15, 30]


class HSBCDownloader(BaseDownloader):
    """
    HSBC Mutual Fund Downloader.
    
    Month-based downloader (HDFC gold standard compliant).
    """
    
    AMC_NAME = "hsbc"
    LIBRARY_URL = "https://www.assetmanagement.hsbc.co.in/en/mutual-funds/investor-resources/information-library"
    BASE_URL = "https://www.assetmanagement.hsbc.co.in"
    
    # Two filename patterns:
    # Pattern 1: Full date - hsbc-fund-name-31-may-2024.xlsx
    # Pattern 2: Month-year only - hsbc-fund-name-mar-2024.xlsx
    FILENAME_PATTERN_FULL = re.compile(
        r"/(?P<slug>hsbc-[a-z0-9\-]+)-(?P<day>\d{2})-(?P<mon>[a-z]+)-(?P<year>\d{4})\.xlsx$",
        re.IGNORECASE
    )
    FILENAME_PATTERN_SHORT = re.compile(
        r"/(?P<slug>hsbc-[a-z0-9\-]+)-(?P<mon>[a-z]+)-(?P<year>\d{4})\.xlsx$",
        re.IGNORECASE
    )
    
    EXPECTED_FILE_COUNT_MIN = 20
    EXPECTED_FILE_COUNT_MAX = 80
    
    def __init__(self):
        super().__init__(self.AMC_NAME)
        self.notifier = get_notifier()
    
    def _fetch_html(self) -> str:
        """Fetch HTML from Information Library page."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5"
        }
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                logger.info(f"Fetching HTML from HSBC (attempt {attempt + 1}/{MAX_RETRIES + 1})")
                response = requests.get(self.LIBRARY_URL, headers=headers, timeout=60)
                response.raise_for_status()
                return response.text
                
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt < MAX_RETRIES:
                    wait_time = RETRY_BACKOFF[attempt]
                    logger.warning(f"Network error: {str(e)}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed to fetch HTML after {MAX_RETRIES + 1} attempts: {str(e)}")
                    
            except requests.HTTPError as e:
                if 400 <= e.response.status_code < 500:
                    raise Exception(f"Client error {e.response.status_code}: {str(e)}")
                elif attempt < MAX_RETRIES:
                    wait_time = RETRY_BACKOFF[attempt]
                    logger.warning(f"Server error: {str(e)}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed to fetch HTML after {MAX_RETRIES + 1} attempts: {str(e)}")
    
    def _extract_portfolio_links(self, html: str) -> List[str]:
        """Extract portfolio XLSX links from HTML."""
        all_xlsx = re.findall(r'href="([^"]+\.xlsx)"', html, re.IGNORECASE)
        portfolio_links = []
        
        for link in all_xlsx:
            if "/mutual-funds/portfolios/" in link.lower():
                portfolio_links.append(urljoin(self.BASE_URL, link))
        
        logger.info(f"Found {len(portfolio_links)} portfolio links")
        return portfolio_links
    
    def _parse_link(self, url: str):
        """
        Parse portfolio link to extract fund slug and date FROM FOLDER PATH.
        
        HSBC has inconsistent filename patterns, but the folder path is reliable:
        - /documents-31012024/ → Jan 31, 2024
        - /document-29022024/ → Feb 29, 2024
        - /document-06062024/ → Jun 6, 2024
        
        We extract the date from the folder path, not the filename.
        """
        # Pattern to match folder date: document(s)-DDMMYYYY
        folder_pattern = re.compile(r'/documents?-(\d{8})/', re.IGNORECASE)
        folder_match = folder_pattern.search(url)
        
        if not folder_match:
            return None
        
        try:
            # Extract DDMMYYYY
            date_str = folder_match.group(1)
            day = date_str[0:2]
            month = date_str[2:4]
            year = date_str[4:8]
            
            # Parse date
            date_obj = datetime.strptime(f"{day}{month}{year}", "%d%m%Y")
            
            # Extract fund slug from filename
            # Try to get a clean fund name from the URL
            filename = url.split('/')[-1].replace('.xlsx', '').replace('.XLSX', '')
            
            # Remove common suffixes to get fund slug
            fund_slug = filename.lower()
            # Remove date patterns from filename if present
            fund_slug = re.sub(r'-\d{2}-[a-z]+-\d{4}$', '', fund_slug)
            fund_slug = re.sub(r'-[a-z]+-\d{4}$', '', fund_slug)
            fund_slug = re.sub(r'-\d{4}$', '', fund_slug)
            
            # If fund_slug doesn't start with 'hsbc-', prepend it
            if not fund_slug.startswith('hsbc-'):
                fund_slug = f"hsbc-{fund_slug}"
            
            return fund_slug, date_obj, url
            
        except Exception as e:
            logger.debug(f"Failed to parse link {url}: {str(e)}")
            return None
    
    
    def _filter_by_month(self, portfolio_links: List[str], year: int, month: int) -> Dict[str, str]:
        """
        Filter links by month with 10-day grace window.
        
        HSBC publishes month-end files in early next month (e.g., Jan 31 data on Feb 5).
        Include files dated in target month OR first 10 days of next month.
        """
        from calendar import monthrange
        
        filtered = {}
        
        # Calculate next month
        next_month = month + 1 if month < 12 else 1
        next_year = year if month < 12 else year + 1
        
        for url in portfolio_links:
            parsed = self._parse_link(url)
            if not parsed:
                continue
            
            fund_slug, date_obj, url = parsed
            
            # Include if:
            # 1. File date is in target month
            # 2. File date is in first 10 days of next month (grace window)
            if (date_obj.year == year and date_obj.month == month) or \
               (date_obj.year == next_year and date_obj.month == next_month and date_obj.day <= 10):
                filename = f"{fund_slug}_{date_obj.strftime('%Y-%m-%d')}.xlsx"
                filtered[filename] = url
        
        logger.info(f"Filtered to {len(filtered)} files for {year}-{month:02d} (with 10-day grace window)")
        return filtered
    
    def _download_file(self, url: str, file_path: Path) -> bool:
        """Download single file with retry logic."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*"
        }
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = requests.get(url, headers=headers, stream=True, timeout=120)
                response.raise_for_status()
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Validate file format and fix extension if needed
                validated_path = validate_and_fix_extension(file_path)
                if validated_path != file_path:
                    # File was renamed, update the path
                    file_path = validated_path
                
                return True
                
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt < MAX_RETRIES:
                    wait_time = RETRY_BACKOFF[attempt]
                    logger.warning(f"Download error: {str(e)}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to download after {MAX_RETRIES + 1} attempts")
                    return False
                    
            except requests.HTTPError as e:
                if 400 <= e.response.status_code < 500:
                    logger.error(f"Client error {e.response.status_code}")
                    return False
                elif attempt < MAX_RETRIES:
                    wait_time = RETRY_BACKOFF[attempt]
                    logger.warning(f"Server error: {str(e)}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to download after {MAX_RETRIES + 1} attempts")
                    return False
        
        return False
    
    def _move_to_corrupt(self, target_dir: Path, year: int, month: int):
        """Move incomplete folder to _corrupt/."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        corrupt_dir = Path(f"data/raw/{self.AMC_NAME}/_corrupt/{target_dir.name}_{timestamp}")
        corrupt_dir.parent.mkdir(parents=True, exist_ok=True)
        
        logger.warning(f"Moving incomplete folder to: {corrupt_dir}")
        shutil.move(str(target_dir), str(corrupt_dir))
        
        # Emit warning event
        self.notifier.notify_warning(
            amc="HSBC",
            year=year,
            month=month,
            warning_type="Corruption Recovery",
            message=f"Incomplete download detected and moved to quarantine"
        )
    
    def _check_file_count(self, file_count: int):
        """Check file count sanity (soft check)."""
        if file_count < self.EXPECTED_FILE_COUNT_MIN or file_count > self.EXPECTED_FILE_COUNT_MAX:
            logger.warning(f"File count ({file_count}) outside expected range [{self.EXPECTED_FILE_COUNT_MIN}-{self.EXPECTED_FILE_COUNT_MAX}]")
        else:
            logger.info(f"File count ({file_count}) within expected range")
    
    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        """Create _SUCCESS.json marker atomically."""
        marker_data = {
            "amc": self.AMC_NAME,
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        
        marker_path = target_dir / "_SUCCESS.json"
        tmp_marker_path = target_dir / "_SUCCESS.json.tmp"
        
        with open(tmp_marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        
        tmp_marker_path.rename(marker_path)
        logger.info(f"Created success marker: {marker_path.name}")
    
    def download(self, year: int, month: int) -> Dict:
        """
        Download HSBC portfolio files for specified month.
        
        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)
            
        Returns:
            Dictionary with status, files_downloaded, etc.
        """
        start_time = time.time()
        
        logger.info("=" * 70)
        logger.info("HSBC MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        logger.info("=" * 70)
        
        if DRY_RUN:
            logger.info("[DRY RUN MODE] No actual downloads")
            return {
                "amc": self.AMC_NAME,
                "year": year,
                "month": month,
                "status": "success",
                "files_downloaded": 0,
                "duration": time.time() - start_time
            }
        
        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        success_marker = target_dir / "_SUCCESS.json"
        
        # Check if already complete
        if success_marker.exists():
            logger.info(f"Month {year}-{month:02d} already complete (_SUCCESS.json exists)")
            return {
                "amc": self.AMC_NAME,
                "year": year,
                "month": month,
                "status": "skipped",
                "reason": "Already complete",
                "duration": time.time() - start_time
            }
        
        # Check for corruption
        if target_dir.exists() and not success_marker.exists():
            logger.warning(f"Incomplete folder detected: {target_dir}")
            self._move_to_corrupt(target_dir, year, month)
        
        # Create target directory
        target_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Fetch HTML
            html = self._fetch_html()
            
            # Extract portfolio links
            portfolio_links = self._extract_portfolio_links(html)
            
            # Filter by month
            files_to_download = self._filter_by_month(portfolio_links, year, month)
            
            # Handle not published
            if not files_to_download:
                logger.info(f"No files found for {year}-{month:02d} (not yet published)")
                shutil.rmtree(target_dir)
                
                # Emit not published event
                self.notifier.notify_not_published(
                    amc="HSBC",
                    year=year,
                    month=month
                )
                
                return {
                    "amc": self.AMC_NAME,
                    "year": year,
                    "month": month,
                    "status": "not_published",
                    "duration": time.time() - start_time
                }
            
            # Download files
            files_downloaded = 0
            
            for filename, url in files_to_download.items():
                file_path = target_dir / filename
                
                logger.info(f"Downloading: {filename}")
                if self._download_file(url, file_path):
                    files_downloaded += 1
                    file_size = file_path.stat().st_size
                    logger.info(f"Downloaded: {filename} ({file_size:,} bytes)")
                else:
                    raise Exception(f"Failed to download: {filename}")
            
            # File count sanity check
            self._check_file_count(files_downloaded)
            
            # Create success marker
            self._create_success_marker(target_dir, year, month, files_downloaded)
            
            duration = time.time() - start_time
            
            # Emit success event
            self.notifier.notify_success(
                amc="HSBC",
                year=year,
                month=month,
                files_downloaded=files_downloaded,
                duration=duration
            )
            
            logger.info("=" * 70)
            logger.success(f"✅ Downloaded {files_downloaded} files in {duration:.2f}s")
            logger.info("=" * 70)
            
            return {
                "amc": self.AMC_NAME,
                "year": year,
                "month": month,
                "status": "success",
                "files_downloaded": files_downloaded,
                "duration": duration
            }
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Download failed: {str(e)}")
            
            # Emit error event
            self.notifier.notify_error(
                amc="HSBC",
                year=year,
                month=month,
                error_type="Download Error",
                reason=str(e)[:100]
            )
            
            # Clean up on failure
            if target_dir.exists():
                shutil.rmtree(target_dir)
            
            return {
                "amc": self.AMC_NAME,
                "year": year,
                "month": month,
                "status": "failed",
                "reason": str(e),
                "duration": duration
            }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="HSBC Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    
    args = parser.parse_args()
    
    if args.month < 1 or args.month > 12:
        logger.error(f"Invalid month: {args.month}. Must be between 1 and 12.")
        exit(1)
    
    downloader = HSBCDownloader()
    result = downloader.download(year=args.year, month=args.month)
    
    if result["status"] == "success":
        logger.success(f"✅ Successfully downloaded {result['files_downloaded']} files")
    elif result["status"] == "skipped":
        logger.info(f"ℹ️  Month already complete")
    elif result["status"] == "not_published":
        logger.info(f"ℹ️  Data not yet published for {args.year}-{args.month:02d}")
    else:
        logger.error(f"❌ Download failed: {result.get('reason', 'Unknown error')}")
        exit(1)
