# src/downloaders/bajaj_downloader.py

import os
import time
import json
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import requests
from bs4 import BeautifulSoup

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

# Import downloader config
try:
    from src.config.downloader_config import (
        DRY_RUN, MAX_RETRIES, RETRY_BACKOFF, HEADLESS
    )
except ImportError:
    DRY_RUN = False
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]


class BajajDownloader(BaseDownloader):
    """
    Bajaj Finserv Mutual Fund - Portfolio Downloader
    
    Extracts dynamic WordPress nonce from the downloads page and queries
    the admin-ajax.php endpoint to download monthly consolidated portfolios.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Bajaj Finserv Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "bajaj"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        
        marker_data = {
            "amc": "BAJAJ",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        
        logger.info(f"Created completion marker: {marker_path.name}")

    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str):
        corrupt_base = Path(f"data/raw/{self.AMC_NAME}/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)
        
        corrupt_target = corrupt_base / f"{year}_{month:02d}"
        if corrupt_target.exists():
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            corrupt_target = corrupt_target.parent / f"{corrupt_target.name}__{ts}"
        
        logger.warning(f"BAJAJ: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))

        self.notifier.notify_error(
            amc="BAJAJ",
            year=year,
            month=month,
            error_type="Corruption Recovery",
            reason=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )

    def _get_fy_string(self, year: int, month: int) -> str:
        """Map calendar month/year to Bajaj's FY format (e.g., 2025-26)."""
        if month in [1, 2, 3]:
            return f"{year-1}-{str(year)[-2:]}"
        else:
            return f"{year}-{str(year+1)[-2:]}"


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("BAJAJ FINSERV MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # 1) Idempotency Check
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Bajaj: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"🕒 Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {
                    "amc": "Bajaj", 
                    "year": year, 
                    "month": month, 
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
                    logger.info(f"BAJAJ: [DRY RUN] Would download {month_name} {year}")
                    duration = time.time() - start_time
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: Bajaj")
                    logger.info(f"Mode: DRY RUN")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: SIMULATED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "Bajaj", "year": year, "month": month, "status": "success", "dry_run": True}

                file_path = self._run_download_flow(year, month, target_dir)
                
                if not file_path:
                    # Not Published Handling
                    duration = time.time() - start_time
                    logger.warning(f"BAJAJ: {year}-{month:02d} not yet published or found.")
                    self.notifier.notify_not_published("BAJAJ", year, month)
                    
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                        
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: Bajaj")
                    logger.info(f"Mode: AUTO")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: NOT PUBLISHED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "Bajaj", "year": year, "month": month, "status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("BAJAJ", year, month, files_downloaded=1, duration=duration)
                
                logger.success(f"[SUCCESS] Bajaj download completed")
                logger.info("=" * 60)
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: Bajaj")
                logger.info(f"Mode: AUTO")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: 1")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: SUCCESS")
                logger.info("=" * 60)
                
                return {
                    "amc": "Bajaj",
                    "year": year,
                    "month": month,
                    "status": "success",
                    "files_downloaded": 1,
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
        self.notifier.notify_error("BAJAJ", year, month, error_type="Download Failure", reason=last_error[:100])
        
        logger.info(f"[SUMMARY]")
        logger.info(f"AMC: Bajaj")
        logger.info(f"Mode: AUTO")
        logger.info(f"Month: {year}-{month:02d}")
        logger.info(f"Status: FAILED")
        logger.info(f"Duration: {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "amc": "Bajaj",
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration
        }

    def _run_download_flow(self, target_year: int, target_month: int, download_folder: Path) -> Optional[Path]:
        """Internal flow using requests and WordPress admin-ajax.php to download monthly portfolio."""
        month_name = self.MONTH_NAMES[target_month]
        fy_str = self._get_fy_string(target_year, target_month)

        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
        })

        # 1. Fetch downloads page to obtain current dynamic nonce and ajaxUrl
        page_url = "https://www.bajajamc.com/downloads?portfolio="
        logger.info(f"Fetching {page_url} for dynamic nonce...")
        resp = session.get(page_url, timeout=30)
        resp.raise_for_status()

        match = re.search(r"var\s+bajajDownloads\s*=\s*(\{.*?\});", resp.text)
        if not match:
            match = re.search(
                r'id=["\']bajaj-downloads-js-js-extra["\'][^>]*>\s*var\s+bajajDownloads\s*=\s*(\{.*?\});',
                resp.text
            )

        if not match:
            logger.error("Could not find 'bajajDownloads' configuration object in page HTML.")
            return None

        cfg = json.loads(match.group(1))
        ajax_url = cfg.get("ajaxUrl", "https://www.bajajamc.com/wp-admin/admin-ajax.php")
        nonce = cfg.get("nonce")

        if not nonce:
            logger.error("Extracted nonce is empty.")
            return None

        # 2. Query admin-ajax.php for monthly portfolio (section_id=757)
        payload = {
            "action": "bajaj_get_downloads",
            "nonce": nonce,
            "section_id": "757",
            "year": fy_str,
            "month": month_name,
        }

        logger.info(f"Querying portfolio disclosures for {month_name} {target_year} (FY {fy_str})...")
        ajax_resp = session.post(ajax_url, data=payload, timeout=30)
        if ajax_resp.status_code != 200:
            logger.error(f"AJAX request failed with status HTTP {ajax_resp.status_code}")
            return None

        result = ajax_resp.json()
        if not result.get("success"):
            logger.warning(f"AJAX response returned success=False for {month_name} {target_year}")
            return None

        data = result.get("data", {})
        count = data.get("count", 0)
        html_fragment = data.get("html", "")

        if count == 0 or not html_fragment:
            logger.warning(f"No documents published for {month_name} {target_year} (count={count})")
            return None

        # 3. Parse HTML fragment to extract document link
        soup = BeautifulSoup(html_fragment, "html.parser")
        a_tag = soup.find("a", href=True)
        if not a_tag:
            logger.warning("No <a> link found in response HTML fragment.")
            return None

        dl_url = a_tag["href"].strip()
        filename = a_tag.get("download") or dl_url.split("/")[-1]
        final_path = download_folder / filename

        # 4. Download file
        logger.info(f"Downloading: {filename} from {dl_url}...")
        dl_resp = session.get(dl_url, stream=True, timeout=60)
        if dl_resp.status_code != 200:
            logger.error(f"Failed to download file from {dl_url}: HTTP {dl_resp.status_code}")
            return None

        with open(final_path, "wb") as f:
            for chunk in dl_resp.iter_content(chunk_size=8192):
                f.write(chunk)

        # 5. Validate magic bytes
        with open(final_path, "rb") as f:
            magic = f.read(8)

        is_xlsx = magic.startswith(b"PK\x03\x04")
        is_xls = magic.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1")
        if not (is_xlsx or is_xls):
            logger.error(f"Invalid file signature for {filename}, deleting...")
            final_path.unlink(missing_ok=True)
            return None

        logger.info(f"Downloaded and verified successfully: {filename} ({final_path.stat().st_size:,} bytes)")
        return final_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Bajaj Finserv Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = BajajDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"[INFO]  Info: Month not yet published")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
