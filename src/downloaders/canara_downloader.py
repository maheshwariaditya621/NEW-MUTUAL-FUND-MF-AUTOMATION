# src/downloaders/canara_downloader.py

import os
import time
import json
import shutil
import re
import urllib.parse
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


class CanaraDownloader(BaseDownloader):
    """
    Canara Robeco Mutual Fund - Portfolio Downloader
    
    URL: https://www.canararobeco.com/documents/statutory-disclosures/scheme-dashboard/scheme-monthly-portfolio/
    Downloads all scheme-wise monthly portfolio files via direct HTTP GET requests
    using portal query-parameter pagination without browser automation.
    """
    
    BASE_URL = "https://www.canararobeco.com/documents/statutory-disclosures/scheme-dashboard/scheme-monthly-portfolio/"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.canararobeco.com/documents/statutory-disclosures/scheme-dashboard/scheme-monthly-portfolio/",
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Canara Robeco Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "canara"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "CANARA",
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
        
        logger.warning(f"CANARA: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("CANARA", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        month_idx = f"{month:02d}"
        
        logger.info("=" * 60)
        logger.info("CANARA ROBECO MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Canara: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"CANARA: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_idx, month_name, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"CANARA: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("CANARA", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("CANARA", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] CANARA download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("CANARA", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_idx: str, month_name: str, download_folder: Path) -> int:
        """
        Scrapes and downloads all scheme files across paginated results via direct HTTP requests.
        """
        session = requests.Session()
        session.headers.update(self.DEFAULT_HEADERS)

        all_discovered = {}  # url -> title
        page = 1
        
        while True:
            page_url = (
                f"{self.BASE_URL}?filteryear={target_year}&filtermonth={month_idx}&pagination={page}"
            )
            logger.info(f"CANARA: Querying page {page} for {target_year}-{month_idx}...")
            
            resp = session.get(page_url, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"CANARA: Page {page} returned status {resp.status_code}")
                break
                
            soup = BeautifulSoup(resp.text, "html.parser")
            new_files_on_page = 0
            
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if re.search(r"\.(xlsx?|zip|csv)", href, re.I):
                    text = a.text.strip()
                    if href not in all_discovered:
                        all_discovered[href] = text
                        new_files_on_page += 1
                    elif text and text.lower() != "download":
                        all_discovered[href] = text
            
            logger.info(f"  → Found {new_files_on_page} new file links on page {page}")
            
            # Check pagination block
            pag_block = soup.find(class_="custom-pagination")
            if not pag_block or new_files_on_page == 0:
                break
                
            next_link = pag_block.find("a", class_="next")
            if not next_link:
                break
                
            page += 1
            time.sleep(1)

        if not all_discovered:
            return 0

        logger.info(f"CANARA: Discovered total {len(all_discovered)} scheme workbooks. Starting downloads...")
        downloaded_count = 0

        for idx, (file_url, title) in enumerate(all_discovered.items(), 1):
            parsed_path = urllib.parse.unquote(urllib.parse.urlparse(file_url).path)
            orig_name = Path(parsed_path).name
            ext = Path(orig_name).suffix or ".xlsx"
            
            if title and title.lower() != "download":
                clean_title = title.replace("–", "-").replace("—", "-").replace(":", "-")
                clean_title = re.sub(r'[\\/*?:"<>|]', "", clean_title).strip()
                dest_filename = f"{clean_title}{ext}" if not clean_title.lower().endswith(ext.lower()) else clean_title
            else:
                dest_filename = orig_name
                
            dest_filename = re.sub(r'[\\/*?:"<>|]', "", dest_filename)
            dest_path = download_folder / dest_filename
            
            logger.info(f"  [{idx}/{len(all_discovered)}] Downloading: {dest_filename[:60]}...")
            
            try:
                r = session.get(file_url, timeout=60, stream=True)
                if r.status_code == 200:
                    content = r.content
                    # Validate magic bytes for Excel/Zip
                    if content.startswith(b"PK\x03\x04") or content.startswith(b"\xd0\xcf\x11\xe0"):
                        with open(dest_path, "wb") as f:
                            f.write(content)
                        downloaded_count += 1
                        logger.info(f"    [OK] Saved ({len(content):,} bytes)")
                    else:
                        logger.warning(f"    [SKIP] Invalid magic bytes for {dest_filename}")
                else:
                    logger.warning(f"    [FAIL] HTTP {r.status_code} for {file_url}")
            except Exception as e:
                logger.warning(f"    [FAIL] Download error: {e}")
                
            time.sleep(0.5)

        return downloaded_count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = CanaraDownloader()
    result = downloader.download(args.year, args.month)
    print(json.dumps(result))

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
