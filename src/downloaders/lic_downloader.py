# src/downloaders/lic_downloader.py

import os
import time
import json
import shutil
import re
import calendar
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import requests
import openpyxl
from bs4 import BeautifulSoup

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

# Import downloader config
try:
    from src.config.downloader_config import (
        DRY_RUN, MAX_RETRIES, RETRY_BACKOFF
    )
except ImportError:
    DRY_RUN = False
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]


POST_URL = "https://www.licmf.com/downloads/consolidated-portfolio-files"
BASE_URL = "https://www.licmf.com"
PAGE_URL = "https://www.licmf.com/downloads/monthly-portfolio"
CATEGORY_ID = "639"


class LICDownloader(BaseDownloader):
    """
    LIC Mutual Fund - Portfolio Downloader
    
    Direct requests-based scraper using the consolidated portfolio endpoint:
    POST https://www.licmf.com/downloads/consolidated-portfolio-files
    
    CRITICAL REQUIREMENT:
    Both 'Monthly Portfolio Debt' AND 'Monthly Portfolio Equity' files must be published
    and validated for the target month before the download is marked complete or merged.
    If either is pending (asynchronous publication), the run exits cleanly without merging.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("LIC Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "lic"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Referer": PAGE_URL,
        })
        logger.info("LICDownloader initialized (Requests + BeautifulSoup Version)")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "LIC",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        with open(marker_path, "w", encoding="utf-8") as f:
            json.dump(marker_data, f, indent=2)
        logger.info(f"Created completion marker: {marker_path.name}")

    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str):
        corrupt_base = Path(f"data/raw/{self.AMC_NAME}/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)
        
        corrupt_target = corrupt_base / f"{year}_{month:02d}"
        if corrupt_target.exists():
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            corrupt_target = corrupt_target.parent / f"{corrupt_target.name}__{ts}"
        
        logger.warning(f"LIC: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))

        self.notifier.notify_error(
            amc="LIC",
            year=year,
            month=month,
            error_type="Corruption Recovery",
            reason=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )

    @staticmethod
    def get_expected_date_str(year: int, month: int) -> Tuple[str, str]:
        """Returns (e.g. 'August 31, 2026', '31-Aug-2026') for the last day of the month."""
        month_name = calendar.month_name[month]
        last_day = calendar.monthrange(year, month)[1]
        formatted_full = f"{month_name} {last_day}, {year}"
        short_month = calendar.month_abbr[month]
        formatted_short = f"{last_day:02d}-{short_month}-{year}"
        return formatted_full, formatted_short

    def _validate_excel_file(self, file_path: Path) -> bool:
        """Validate ZIP signature and openpyxl readable workbook."""
        if not file_path.exists() or file_path.stat().st_size == 0:
            return False

        with open(file_path, "rb") as f:
            magic = f.read(4)
        if magic != b"PK\x03\x04" and magic != b"\xd0\xcf\x11\xe0":
            logger.error(f"LIC: Invalid magic bytes for {file_path.name}: {magic.hex()}")
            return False

        if magic == b"PK\x03\x04":
            try:
                wb = openpyxl.load_workbook(str(file_path), read_only=True, data_only=True)
                _ = wb.sheetnames
                wb.close()
                return True
            except Exception as e:
                logger.error(f"LIC: openpyxl validation failed for {file_path.name}: {e}")
                return False

        return True

    def _download_file(self, url: str, target_path: Path) -> bool:
        """Download file from url to target_path with validation."""
        try:
            resp = self.session.get(url, stream=True, timeout=60)
            if resp.status_code != 200:
                logger.error(f"LIC: Download failed for {url} with status {resp.status_code}")
                return False

            target_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = target_path.with_name(target_path.stem + ".tmp.xlsx")

            with open(temp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=16384):
                    if chunk:
                        f.write(chunk)

            if self._validate_excel_file(temp_path):
                if target_path.exists():
                    target_path.unlink()
                temp_path.rename(target_path)
                logger.info(f"  [OK] Saved and validated: {target_path.name} ({target_path.stat().st_size:,} bytes)")
                return True
            else:
                if temp_path.exists():
                    temp_path.unlink()
                return False
        except Exception as e:
            logger.error(f"LIC: Exception during download of {url}: {e}")
            return False

    def _run_download_flow(self, target_year: int, target_month: int, download_folder: Path) -> Optional[List[Path]]:
        """
        Queries endpoint, verifies presence of BOTH Debt and Equity files,
        and downloads both. Returns None if either is missing.
        """
        expected_full, expected_short = self.get_expected_date_str(target_year, target_month)
        month_name = calendar.month_name[target_month]

        data = {
            "id": CATEGORY_ID,
            "month": str(target_month),
            "year": str(target_year)
        }

        logger.info(f"LIC: Requesting consolidated portfolio files for {month_name} {target_year}...")
        resp = self.session.post(POST_URL, data=data, timeout=30)
        if resp.status_code != 200:
            logger.error(f"LIC: POST failed with status {resp.status_code}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.find_all("a")

        debt_url = None
        equity_url = None

        for a in links:
            href = a.get("href", "").strip()
            caption_el = a.find("span", class_="caption")
            caption = caption_el.get_text(strip=True) if caption_el else a.get_text(strip=True)

            if re.search(rf"Monthly\s+Portfolio\s+Debt\s+as\s+on\s+{re.escape(expected_full)}", caption, re.IGNORECASE):
                debt_url = urllib.parse.urljoin(BASE_URL, urllib.parse.quote(href))
                logger.info(f"LIC: Discovered Debt file: '{caption}'")
            elif re.search(rf"Monthly\s+Portfolio\s+Equity\s+as\s+on\s+{re.escape(expected_full)}", caption, re.IGNORECASE):
                equity_url = urllib.parse.urljoin(BASE_URL, urllib.parse.quote(href))
                logger.info(f"LIC: Discovered Equity file: '{caption}'")

        # Strict Gatekeeping: Both files required
        if not debt_url and not equity_url:
            logger.warning(f"LIC: Neither Debt nor Equity file is published yet for {month_name} {target_year}.")
            return None
        elif debt_url and not equity_url:
            logger.warning(f"LIC: [ASYNC PUBLICATION] Debt file is available, but Equity file is NOT YET PUBLISHED for {month_name} {target_year}.")
            logger.warning("LIC: Halting execution. Month remains incomplete until Equity file is published.")
            return None
        elif not debt_url and equity_url:
            logger.warning(f"LIC: [ASYNC PUBLICATION] Equity file is available, but Debt file is NOT YET PUBLISHED for {month_name} {target_year}.")
            logger.warning("LIC: Halting execution. Month remains incomplete until Debt file is published.")
            return None

        # Both files are present!
        logger.info(f"LIC: Both Debt and Equity files discovered for {month_name} {target_year}. Downloading...")
        debt_target = download_folder / f"LIC_MF_Monthly_Debt_Portfolio_{target_month:02d}_{target_year}.xlsx"
        equity_target = download_folder / f"LIC_MF_Monthly_Equity_Portfolio_{target_month:02d}_{target_year}.xlsx"

        debt_ok = self._download_file(debt_url, debt_target)
        equity_ok = self._download_file(equity_url, equity_target)

        if debt_ok and equity_ok:
            return [debt_target, equity_target]
        else:
            logger.error("LIC: Failed to download and validate both files.")
            return None

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("LIC MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # 1) Idempotency Check
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                logger.info(f"LIC: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
                self.consolidate_downloads(year, month)
                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {
                    "status": "skipped", 
                    "reason": "already_downloaded",
                    "duration": duration
                }
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json marker")

        self.ensure_directory(str(target_dir))

        # 2) Download Logic with Retry
        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"LIC: [DRY RUN] Would download {month_name} {year}")
                    return {"amc": "LIC", "year": year, "month": month, "status": "success", "dry_run": True}

                downloaded_files = self._run_download_flow(year, month, target_dir)
                
                if not downloaded_files:
                    duration = time.time() - start_time
                    logger.warning(f"LIC: {year}-{month:02d} not complete/published yet.")
                    self.notifier.notify_not_published("LIC", year, month)
                    
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                        
                    return {"amc": "LIC", "year": year, "month": month, "status": "not_published"}

                # Success: Both files downloaded and validated
                self._create_success_marker(target_dir, year, month, len(downloaded_files))
                
                # Consolidate downloads into merged excels
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("LIC", year, month, files_downloaded=len(downloaded_files), duration=duration)
                
                logger.success(f"[SUCCESS] LIC download completed: {len(downloaded_files)} files")
                return {
                    "amc": "LIC",
                    "year": year,
                    "month": month,
                    "status": "success",
                    "files_downloaded": len(downloaded_files),
                    "duration": duration
                }

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])
                continue

        # 3) Final Failure
        if target_dir.exists() and not (target_dir / "_SUCCESS.json").exists():
            shutil.rmtree(target_dir, ignore_errors=True)
            
        duration = time.time() - start_time
        self.notifier.notify_error("LIC", year, month, error_type="Download Failure", reason=last_error[:100])
        return {
            "amc": "LIC",
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration
        }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="LIC Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    args = parser.parse_args()

    downloader = LICDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"[INFO] Info: Month not yet published / waiting for both Debt and Equity")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
