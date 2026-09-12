# src/downloaders/navi_downloader.py

import os
import time
import json
import shutil
import re
import html
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
import requests
import openpyxl

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


BASE_PAGE_URL = "https://navi.com/mutual-fund/downloads/portfolio"
API_URL = "https://navi.com/wp-json/nv/v1/documents"
CATEGORY_ID = "884"


class NaviDownloader(BaseDownloader):
    """
    Navi Mutual Fund - Portfolio Downloader
    
    Direct requests-based scraper using the REST API:
    POST https://navi.com/wp-json/nv/v1/documents
    
    Dynamically extracts the WP-NONCE from the portfolio landing page
    and downloads all scheme-level monthly portfolio workbooks.
    """
    
    MONTH_ABBR = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    MONTH_FULL = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Navi Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "navi"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
        })
        self._nonce: Optional[str] = None
        logger.info("NaviDownloader initialized (Requests + REST API Version)")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Navi",
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
        
        logger.warning(f"{self.AMC_NAME}: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("Navi", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _get_financial_year(self, month: int, year: int) -> str:
        """
        Convert calendar month/year to Financial Year.
        FY runs from April to March.
        - April 2026 to December 2026 -> 2026-2027
        - January 2026 to March 2026 -> 2025-2026
        """
        if month >= 4:
            fy_start = year
            fy_end = year + 1
        else:
            fy_start = year - 1
            fy_end = year
        return f"{fy_start}-{fy_end}"

    def get_nonce(self) -> str:
        """Dynamically extract the WP-NONCE from the portfolio landing page."""
        if self._nonce:
            return self._nonce

        logger.info(f"Navi: Extracting WP-NONCE from {BASE_PAGE_URL}...")
        resp = self.session.get(BASE_PAGE_URL, timeout=20)
        resp.raise_for_status()

        match = re.search(r'navi_property\s*=\s*({[^}]+})', resp.text)
        if not match:
            raise RuntimeError("Could not find 'navi_property' in HTML page")

        props = json.loads(match.group(1))
        nonce = props.get("nonce")
        if not nonce:
            raise RuntimeError(f"Nonce missing in navi_property: {props}")

        self._nonce = nonce
        logger.info(f"Navi: Discovered dynamic WP-NONCE: {self._nonce}")
        return self._nonce

    def _validate_excel_file(self, file_path: Path) -> bool:
        """Validate ZIP signature and openpyxl readable workbook."""
        if not file_path.exists() or file_path.stat().st_size == 0:
            return False

        with open(file_path, "rb") as f:
            magic = f.read(4)
        if magic != b"PK\x03\x04" and magic != b"\xd0\xcf\x11\xe0":
            logger.error(f"Navi: Invalid magic bytes for {file_path.name}: {magic.hex()}")
            return False

        if magic == b"PK\x03\x04":
            try:
                wb = openpyxl.load_workbook(str(file_path), read_only=True, data_only=True)
                _ = wb.sheetnames
                wb.close()
                return True
            except Exception as e:
                logger.error(f"Navi: openpyxl validation failed for {file_path.name}: {e}")
                return False

        return True

    def _run_download_flow(
        self,
        target_year: int,
        target_month: int,
        month_abbr: str,
        month_full_name: str,
        download_folder: Path
    ) -> int:
        nonce = self.get_nonce()
        fy_year = self._get_financial_year(target_month, target_year)

        headers = {
            "WP-NONCE": nonce,
            "Referer": BASE_PAGE_URL,
            "Origin": "https://navi.com",
            "Accept": "application/json, text/plain, */*",
        }

        data = {
            "financial_year": fy_year,
            "value": month_full_name,
            "category": CATEGORY_ID,
            "type": "Monthly",
            "order": "DESC",
        }

        logger.info(f"Navi: Requesting documents API for {month_full_name} {target_year} ({fy_year})...")
        resp = self.session.post(API_URL, data=data, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.error(f"Navi: Documents API returned HTTP {resp.status_code}")
            return 0

        res_json = resp.json()
        if not res_json.get("success"):
            logger.warning(f"Navi: API returned success=False: {res_json}")
            return 0

        raw_records = res_json.get("data", [])
        logger.info(f"Navi: Received {len(raw_records)} documents from API.")

        if not raw_records:
            return 0

        seen_urls = set()
        success_count = 0

        for idx, doc in enumerate(raw_records, 1):
            raw_title = doc.get("title", "")
            clean_title = html.unescape(raw_title).strip()
            clean_title = clean_title.replace("\u2013", "-").replace("\u2014", "-")
            file_url = doc.get("url", "").strip()

            if not file_url or not clean_title:
                continue

            # Deduplication
            if file_url in seen_urls:
                continue
            seen_urls.add(file_url)

            # Date guard: Verify target month and year in title
            if not (month_full_name.lower() in clean_title.lower() and str(target_year) in clean_title):
                logger.info(f"Navi: Skipping document with mismatching period title: '{clean_title}'")
                continue

            # Derive clean filename
            url_filename = file_url.split("/")[-1].split("?")[0]
            if not url_filename.endswith(".xlsx") and not url_filename.endswith(".xls"):
                url_filename = f"navi_scheme_{idx:02d}.xlsx"

            target_path = download_folder / url_filename
            temp_path = target_path.with_name(target_path.stem + ".tmp.xlsx")

            try:
                dl_resp = self.session.get(file_url, stream=True, timeout=60)
                if dl_resp.status_code != 200:
                    logger.error(f"Navi: Download failed for {clean_title} with status {dl_resp.status_code}")
                    continue

                with open(temp_path, "wb") as f:
                    for chunk in dl_resp.iter_content(chunk_size=16384):
                        if chunk:
                            f.write(chunk)

                if self._validate_excel_file(temp_path):
                    if target_path.exists():
                        target_path.unlink()
                    temp_path.rename(target_path)
                    logger.info(f"  [OK] Saved: {target_path.name} ({target_path.stat().st_size:,} bytes)")
                    success_count += 1
                else:
                    if temp_path.exists():
                        temp_path.unlink()
                    logger.error(f"Navi: Validation failed for {clean_title}")
            except Exception as e:
                if temp_path.exists():
                    temp_path.unlink()
                logger.error(f"Navi: Exception downloading {clean_title}: {e}")

        logger.info(f"Navi: Successfully downloaded and validated {success_count} file(s).")
        return success_count

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        month_abbr = self.MONTH_ABBR[month]
        month_full_name = self.MONTH_FULL[month]
        
        logger.info("=" * 60)
        logger.info(f"NAVI MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_abbr})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Navi: {year}-{month:02d} files already downloaded.")
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
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"{self.AMC_NAME}: [DRY RUN] Would download {month_abbr} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_abbr, month_full_name, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"{self.AMC_NAME}: No portfolios found for {month_abbr} {year}")
                    self.notifier.notify_not_published("Navi", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads into merged excels
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("Navi", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Navi", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Navi Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    args = parser.parse_args()

    downloader = NaviDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"[INFO] Info: Month not yet published")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
