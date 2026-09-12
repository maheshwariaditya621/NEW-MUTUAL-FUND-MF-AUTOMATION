# src/downloaders/angelone_downloader.py

import os
import time
import json
import shutil
import re
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import calendar
from bs4 import BeautifulSoup

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

try:
    from src.config.downloader_config import (
        DRY_RUN, MAX_RETRIES, RETRY_BACKOFF, HEADLESS
    )
except ImportError:
    DRY_RUN = False
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]
    HEADLESS = True


class AngelOneDownloader(BaseDownloader):
    """
    Angel One Mutual Fund - Portfolio Downloader
    
    Extracts direct monthly portfolio XLSX links from https://www.angelonemf.com/downloads
    using requests and BeautifulSoup.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Angel One Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "angelone"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        
        marker_data = {
            "amc": "ANGELONE",
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
        
        logger.warning(f"ANGELONE: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))

        self.notifier.notify_error(
            amc="ANGELONE",
            year=year,
            month=month,
            error_type="Corruption Recovery",
            reason=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("ANGEL ONE MUTUAL FUND DOWNLOADER STARTED")
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
                logger.info(f"Angel One: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"🕒 Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {
                    "amc": "Angel One", 
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
                    logger.info(f"ANGELONE: [DRY RUN] Would download {month_name} {year}")
                    duration = time.time() - start_time
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: Angel One")
                    logger.info(f"Mode: DRY RUN")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: SIMULATED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "Angel One", "year": year, "month": month, "status": "success", "dry_run": True}

                downloaded_paths = self._run_download_flow(year, month, target_dir)
                
                if not downloaded_paths:
                    # Not Published Handling
                    duration = time.time() - start_time
                    logger.warning(f"ANGELONE: {year}-{month:02d} not yet published or no portfolio links found.")
                    self.notifier.notify_not_published("ANGELONE", year, month)
                    
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                        
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: Angel One")
                    logger.info(f"Mode: AUTO")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: NOT PUBLISHED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "Angel One", "year": year, "month": month, "status": "not_published"}

                # Success
                file_count = len(downloaded_paths)
                self._create_success_marker(target_dir, year, month, file_count)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("ANGELONE", year, month, files_downloaded=file_count, duration=duration)
                
                logger.success(f"[SUCCESS] Angel One download completed: {file_count} files")
                logger.info("=" * 60)
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: Angel One")
                logger.info(f"Mode: AUTO")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: {file_count}")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: SUCCESS")
                logger.info("=" * 60)
                
                return {
                    "amc": "Angel One",
                    "year": year,
                    "month": month,
                    "status": "success",
                    "files_downloaded": file_count,
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
        self.notifier.notify_error("ANGELONE", year, month, error_type="Download Failure", reason=last_error[:100])
        
        logger.info(f"[SUMMARY]")
        logger.info(f"AMC: Angel One")
        logger.info(f"Mode: AUTO")
        logger.info(f"Month: {year}-{month:02d}")
        logger.info(f"Status: FAILED")
        logger.info(f"Duration: {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "amc": "Angel One",
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration
        }

    def _run_download_flow(self, target_year: int, target_month: int, download_folder: Path) -> List[Path]:
        """Internal flow using requests and BeautifulSoup to extract and download direct XLSX files."""
        month_name = self.MONTH_NAMES[target_month]
        target_period_str = f"{month_name} {target_year}".lower()
        target_period_file_str = f"{month_name.lower()}-{target_year}"

        url = "https://www.angelonemf.com/downloads"
        logger.info(f"Fetching downloads page from {url}...")

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        session = requests.Session()
        session.headers.update(headers)

        resp = session.get(url, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        all_links = []
        seen_urls = set()

        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href.lower().endswith(".xlsx"):
                continue

            parent = a.find_parent(["tr", "div", "li"])
            context_text = parent.get_text(" ", strip=True) if parent else ""
            combined_text = f"{context_text} {href}".lower()

            if "monthly portfolio" not in combined_text and "monthly-portfolio" not in combined_text:
                continue

            if any(term in combined_text for term in ["aaum", "tracking-error", "tracking error", "distributor", "fortnightly", "half-yearly"]):
                continue

            if target_period_str not in combined_text and target_period_file_str not in combined_text:
                continue

            if href in seen_urls:
                continue
            seen_urls.add(href)
            all_links.append(href)

        if not all_links:
            logger.warning(f"No matching monthly portfolio XLSX links found for {month_name} {target_year}")
            return []

        # SMART DEDUPLICATION: Keep highest version (e.g. -1, -2)
        dedup_map = {}
        for dl_url in all_links:
            filename = dl_url.split("/")[-1]
            match = re.search(r'^(.*?)(?:-(\d+))?\.xlsx$', filename, re.I)
            if match:
                base_part = match.group(1)
                version = int(match.group(2)) if match.group(2) else 0
                if base_part not in dedup_map or version > dedup_map[base_part][1]:
                    dedup_map[base_part] = (dl_url, version)
            else:
                dedup_map[filename] = (dl_url, 0)

        final_links = [v[0] for v in dedup_map.values()]
        logger.info(f"Found {len(all_links)} candidate links. Decided on {len(final_links)} unique fund files.")

        downloaded_paths = []
        for idx, dl_url in enumerate(final_links):
            try:
                clean_filename = dl_url.split("/")[-1]
                save_path = download_folder / clean_filename

                logger.info(f"Downloading ({idx+1}/{len(final_links)}): {clean_filename}")

                dl_resp = session.get(dl_url, stream=True, timeout=60)
                if dl_resp.status_code == 200:
                    with open(save_path, 'wb') as f:
                        for chunk in dl_resp.iter_content(chunk_size=8192):
                            f.write(chunk)

                    # Validate file signature
                    with open(save_path, 'rb') as f:
                        magic = f.read(4)

                    if magic == b"PK\x03\x04":
                        downloaded_paths.append(save_path)
                    else:
                        logger.error(f"Invalid XLSX signature for {clean_filename}, deleting...")
                        save_path.unlink(missing_ok=True)
                else:
                    logger.error(f"Failed to download {dl_url}: HTTP {dl_resp.status_code}")
            except Exception as e:
                logger.error(f"Error downloading {dl_url}: {e}")

        return downloaded_paths


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Angel One Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = AngelOneDownloader()
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
