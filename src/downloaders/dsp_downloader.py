# src/downloaders/dsp_downloader.py

import os
import time
import json
import shutil
import zipfile
import calendar
import urllib.parse
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

import requests
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


class DSPDownloader(BaseDownloader):
    """
    DSP Mutual Fund - Portfolio Downloader
    
    URL: https://www.dspim.com/mandatory-disclosures/portfolio-disclosures
    Downloads consolidated ZIP file via requests/BeautifulSoup and extracts contents
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    DISCLOSURES_URL = "https://www.dspim.com/mandatory-disclosures/portfolio-disclosures"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    ZIP_MAGIC = b"PK\x03\x04"

    def __init__(self):
        super().__init__("DSP Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "dsp"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "DSP",
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
        
        logger.warning(f"DSP: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("DSP", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("DSP MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"DSP: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/errored previously
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
                    logger.info(f"DSP: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_extracted = self._run_download_flow(year, month, month_name, target_dir)
                
                if files_extracted == 0:
                    logger.warning(f"DSP: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("DSP", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_extracted)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("DSP", year, month, files_downloaded=files_extracted, duration=duration)
                logger.success(f"[SUCCESS] DSP download completed: {files_extracted} files extracted")
                return {"status": "success", "files_downloaded": files_extracted, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("DSP", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> int:
        """Fetch disclosures page via requests, locate Month End ZIP link, download and extract."""
        session = requests.Session()
        logger.info(f"Fetching disclosures page: {self.DISCLOSURES_URL}...")
        resp = session.get(self.DISCLOSURES_URL, headers=self.HEADERS, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Locate Month End Portfolio Disclosures <details> section
        month_end_details = None
        for det in soup.find_all("details", class_="pd-section-details"):
            summary = det.find("summary")
            if summary and "month end portfolio disclosures" in summary.get_text(strip=True).lower():
                month_end_details = det
                break

        if not month_end_details:
            logger.error("Could not find 'Month End Portfolio Disclosures' section in page")
            return 0

        # Pattern for target month: e.g. August or Aug and Year
        month_abbr = calendar.month_abbr[target_month]
        pattern = rf'\b({month_name}|{month_abbr})\b.*\b{target_year}\b'

        matching_link = None
        for a in month_end_details.find_all("a"):
            href = a.get("href", "")
            if not href or not href.endswith(".zip"):
                continue
            text = a.get_text(strip=True)
            if re.search(pattern, text, re.IGNORECASE) or re.search(pattern, href, re.IGNORECASE):
                matching_link = (text, href)
                break

        if not matching_link:
            logger.warning(f"  [FAIL] Month End ZIP link not found for {month_name} {target_year}")
            return 0

        link_text, link_href = matching_link
        zip_url = urllib.parse.urljoin(self.DISCLOSURES_URL, link_href)
        logger.info(f"  [OK] Found record: '{link_text}'")
        logger.info(f"  Downloading ZIP from: {zip_url}...")

        zip_name = zip_url.split("/")[-1]
        zip_path = download_folder / zip_name

        try:
            with session.get(zip_url, headers=self.HEADERS, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(zip_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)

            file_size = zip_path.stat().st_size
            if file_size < 1000:
                zip_path.unlink(missing_ok=True)
                logger.error(f"    [FAIL] Downloaded ZIP file too small ({file_size} bytes)")
                return 0

            # Magic bytes validation
            with open(zip_path, "rb") as f_check:
                magic = f_check.read(4)

            if magic != self.ZIP_MAGIC:
                zip_path.unlink(missing_ok=True)
                logger.error(f"    [FAIL] Invalid ZIP signature: {magic.hex()}")
                return 0

            # Extract ZIP contents
            logger.info(f"  Extracting ZIP ({file_size:,} bytes)...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                bad_file = zip_ref.testzip()
                if bad_file is not None:
                    zip_path.unlink(missing_ok=True)
                    logger.error(f"    [FAIL] Corrupted file in archive: {bad_file}")
                    return 0

                zip_ref.extractall(download_folder)
                file_count = len(zip_ref.namelist())

            logger.info(f"  [OK] Extracted {file_count} files")

            # Remove ZIP after successful extraction
            zip_path.unlink(missing_ok=True)
            logger.info("  [OK] Cleaned up ZIP file")

            return file_count

        except Exception as e:
            logger.error(f"  [FAIL] Download or extraction failed: {e}")
            if zip_path.exists():
                zip_path.unlink(missing_ok=True)
            return 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="DSP Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g. 2026)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    args = parser.parse_args()

    downloader = DSPDownloader()
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
