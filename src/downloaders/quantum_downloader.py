# src/downloaders/quantum_downloader.py

import requests
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict

from src.downloaders.base_downloader import BaseDownloader
import time
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

# Import downloader config
try:
    from src.config.downloader_config import (
        MAX_RETRIES, RETRY_BACKOFF
    )
except ImportError:
    # Fallback defaults if config not found
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]


class QuantumDownloader(BaseDownloader):
    AMC_NAME = "quantum"
    """
    Quantum Mutual Fund monthly portfolio downloader.

    Behavior is strictly defined in:
    quantum_downloader_rules.md

    This class MUST conform to the HDFC Gold Reference.
    """
    
    API_URL = "https://www.quantumamc.com/ProductPortfolio/GetProductPortfolioPaginatedList"
    
    # Rules: FILE_COUNT_MIN = 1, FILE_COUNT_MAX = 1 (Observability only)
    FILE_COUNT_MIN = 1
    FILE_COUNT_MAX = 1
    
    def __init__(self):
        """Initialize Quantum downloader."""
        super().__init__("Quantum Mutual Fund")
        self.notifier = get_notifier()

    def _check_file_count(self, file_count: int, year: int, month: int):
        """
        Sanity check file count (logging only, never fails).
        
        Args:
            file_count: Number of files downloaded
            year: Year
            month: Month
        """
        if file_count < self.FILE_COUNT_MIN:
            logger.warning(f"File count ({file_count}) below expected minimum ({self.FILE_COUNT_MIN}) for {year}-{month:02d}")
        elif file_count > self.FILE_COUNT_MAX:
            logger.warning(f"File count ({file_count}) above expected maximum ({self.FILE_COUNT_MAX}) for {year}-{month:02d}")
        else:
            logger.info(f"File count ({file_count}) within normal range ({self.FILE_COUNT_MIN}-{self.FILE_COUNT_MAX})")

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
            "amc": "Quantum",
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
        corrupt_base = Path("data/raw/quantum/_corrupt")
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
            amc="Quantum",
            year=year,
            month=month,
            warning_type="Corruption Recovery",
            message=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.quantumamc.com/portfolio/combined/-1/1/0/0"
    }
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    MONTH_SHORT_NAMES = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    def _api_call_with_retry(self, year: int, month: int) -> list:
        """
        Make API call with retry logic and pagination handling.
        
        Args:
            year: Year
            month: Month
            
        Returns:
            List of all portfolio records matching criteria
        """
        all_records = []
        page_index = 1
        total_pages = 1
        
        while page_index <= total_pages:
            params = {
                "productSchemeId": -1,
                "yearId": year,
                "monthId": month,
                "Frequency": 1,
                "pageIndex": page_index
            }
            
            logger.info(f"Calling Quantum API: {year}-{month:02d} (page {page_index})")
            
            page_data = None
            for attempt in range(MAX_RETRIES + 1):
                try:
                    resp = requests.get(self.API_URL, params=params, headers=self.HEADERS, timeout=30)
                    resp.raise_for_status()
                    
                    response_data = resp.json()
                    if "objProductPortfolioList" not in response_data:
                        logger.error(f"Malformed API response: missing 'objProductPortfolioList'")
                        raise ValueError("API response missing required data key")
                    
                    page_data = response_data
                    break
                
                except requests.Timeout:
                    if attempt < MAX_RETRIES:
                        backoff = RETRY_BACKOFF[attempt]
                        logger.warning(f"Timeout on attempt {attempt + 1}/{MAX_RETRIES + 1} for {year}-{month:02d}, retrying in {backoff}s...")
                        time.sleep(backoff)
                    else:
                        logger.error(f"Max retries exceeded for {year}-{month:02d}")
                        raise
                
                except requests.HTTPError as e:
                    status_code = e.response.status_code
                    if 400 <= status_code < 500:
                        logger.error(f"HTTP {status_code} error (non-retryable) for {year}-{month:02d}")
                        raise
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

            if page_data:
                total_pages = page_data.get("totalPageCount", 1)
                records = page_data.get("objProductPortfolioList", [])
                all_records.extend(records)
                page_index += 1
            else:
                break
                
        return all_records

    def _select_combined_record(self, records: list, year: int, month: int) -> dict:
        """
        Selects strictly the COMBINED / ALL FUNDS portfolio record.
        """
        month_name = self.MONTH_NAMES.get(month, "").lower()
        month_short = self.MONTH_SHORT_NAMES.get(month, "").lower()
        
        candidates = []
        for r in records:
            if r.get("SchemeId") != -1 or r.get("FactSheetFreq") != 1:
                continue
            combined_str = f"{r.get('OriginalFileName', '')} {r.get('SchemeName', '')} {r.get('FSMonth', '')}".lower()
            if str(year) in combined_str and (month_name in combined_str or month_short in combined_str):
                candidates.append(r)
                
        if candidates:
            return candidates[0]
            
        fallback = [r for r in records if r.get("SchemeId") == -1 and r.get("FactSheetFreq") == 1]
        if fallback:
            return fallback[0]
            
        return None

    def download(self, year: int, month: int) -> Dict:
        """
        Download Quantum monthly portfolio files using official API.
        
        Args:
            year: Calendar year
            month: Month (1-12)
            
        Returns:
            Download metadata dictionary
        """
        start_time = time.time()
        target_dir = Path(self.get_target_folder("quantum", year, month))
        
        # Check for incomplete month
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if not success_marker.exists():
                logger.warning(f"Incomplete month detected: {year}-{month:02d}")
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json marker")
            else:
                logger.info(f"Quantum: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"🕒 Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {
                    "amc": self.amc_name,
                    "year": year,
                    "month": month,
                    "files_downloaded": 0,
                    "files": [],
                    "status": "skipped",
                    "reason": "already_downloaded",
                    "duration": duration
                }

        try:
            # API Call
            portfolio_list = self._api_call_with_retry(year, month)

            # Select combined record
            file_item = self._select_combined_record(portfolio_list, year, month)

            # Handle empty/unmatched records (not yet published)
            if not file_item:
                logger.warning(f"Combined portfolio not yet published: {year}-{month:02d}")
                self.notifier.notify_not_published(amc="Quantum", year=year, month=month)
                if target_dir.exists():
                    shutil.rmtree(target_dir)
                return {
                    "amc": self.amc_name,
                    "year": year,
                    "month": month,
                    "files_downloaded": 0,
                    "files": [],
                    "status": "not_published"
                }

            # Create directory
            self.ensure_directory(str(target_dir))

            # Download single combined file
            url = file_item["FileUrl"]
            name = file_item["OriginalFileName"]
            path = target_dir / name

            logger.info(f"Downloading combined portfolio: {name}")
            with requests.get(url, headers={"User-Agent": self.HEADERS["User-Agent"]}, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(path, "wb") as fp:
                    for chunk in r.iter_content(chunk_size=65536):
                        if chunk:
                            fp.write(chunk)

            logger.info(f"Saved: {path.name} ({path.stat().st_size:,} bytes)")

            # Validate XLSX structure
            import openpyxl
            wb = openpyxl.load_workbook(path, read_only=True)
            sheet_count = len(wb.sheetnames)
            wb.close()
            logger.info(f"Validated Excel workbook: {sheet_count} sheet(s)")

            # File count sanity check
            self._check_file_count(1, year, month)

            # Create atomic completion marker
            self._create_success_marker(target_dir, year, month, 1)
            
            # Consolidate downloads
            self.consolidate_downloads(year, month)

            duration = time.time() - start_time
            
            # Emit success event
            self.notifier.notify_success(
                amc="Quantum",
                year=year,
                month=month,
                files_downloaded=1,
                duration=duration
            )

            return {
                "amc": self.amc_name,
                "year": year,
                "month": month,
                "files_downloaded": 1,
                "files": [str(path)],
                "status": "success",
                "duration": duration
            }

        except Exception as e:
            logger.error(f"Download failed: {str(e)}")
            
            # Emit error event
            self.notifier.notify_error(
                amc="Quantum",
                year=year,
                month=month,
                error_type="Download Error",
                reason=str(e)[:100]
            )
            
            # Move to corrupt rather than silent deletion
            if target_dir.exists():
                self._move_to_corrupt(target_dir, year, month, f"Download failure: {str(e)}")
            
            raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Quantum Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Calendar year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    
    args = parser.parse_args()
    
    # Validate month
    if args.month < 1 or args.month > 12:
        logger.error("Invalid month. Must be between 1 and 12.")
        exit(1)
    
    downloader = QuantumDownloader()
    result = downloader.download(year=args.year, month=args.month)
    
    status = result.get("status")

    if status == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
        exit(0)

    elif status == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete (Consolidation refreshed)")
        exit(0)

    elif status == "not_published":
        logger.info(f"[INFO]  Info: Month not yet published")
        exit(0)

    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        exit(1)
