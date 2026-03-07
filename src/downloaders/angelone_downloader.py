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
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

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
    
    Uses Playwright to extract Next.js state data and identify direct XLSX download links.
    Supports persistent browser sessions and "no-refresh" multi-month logic.
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
        """Internal flow using Playwright to extract direct links from page state."""
        month_name = self.MONTH_NAMES[target_month]
        month_abbr = month_name[:3]
        
        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=HEADLESS,
                args=["--window-size=1920,1080", "--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                ignore_https_errors=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            url = "https://www.angelonemf.com/downloads"
            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="load", timeout=120000)
            
            # Wait for content or a bit of delay for Next.js to populate window object
            time.sleep(5)

            # Extract Next.js state or search in script tags
            logger.info("Extracting website internal state...")
            scripts = page.locator("script").all_inner_texts()
            full_content = "".join(scripts)
            
            # Fallback to window state if script tags are fragmented
            if not full_content or "disclosuresData" not in full_content:
                logger.debug("Falling back to window state evaluation...")
                eval_res = page.evaluate("() => { try { return JSON.stringify(window.__next_f); } catch(e) { return null; } }")
                if eval_res:
                    full_content = eval_res

            # Search for XLSX links matching the period
            # Use stricter patterns with delimiters and word boundaries to avoid "Mar" vs "Market" collision
            patterns = [
                rf'https://cms\.angelonemf\.com/[^\s"]+[-/_]{month_abbr}\b[^\s"]*?{target_year}[^\s"]*\.xlsx',
                rf'https://cms\.angelonemf\.com/[^\s"]+{target_year}[-/_]{month_abbr}\b[^\s"]*\.xlsx',
                rf'https://cms\.angelonemf\.com/[^\s"]+[-/_]{month_name}\b[^\s"]*?{target_year}[^\s"]*\.xlsx',
                rf'https://cms\.angelonemf\.com/[^\s"]+{target_year}[-/_]{month_name}\b[^\s"]*\.xlsx'
            ]
            
            all_links = []
            for p_str in patterns:
                found = re.findall(p_str, full_content, re.IGNORECASE)
                all_links.extend(found)
            
            # Clean and deduplicate by URL first
            unique_links = []
            seen_urls = set()
            for link in all_links:
                clean_link = link.replace('\\/', '/').replace('\\', '').replace('"', '').replace('u0026', '&')
                if clean_link not in seen_urls:
                    # Filter for Portfolio related files
                    if any(x in clean_link for x in ["Portfolio", "Monthly", "Scheme"]):
                        unique_links.append(clean_link)
                        seen_urls.add(clean_link)
            
            if not unique_links:
                logger.warning(f"No matching XLSX links found for {month_name} {target_year}")
                return []
            
            # SMART DEDUPLICATION: Many links from CMS are just versions (e.g. -1, -2)
            # We normalize the fund name and keep the "latest" one.
            dedup_map = {} # normalized_name -> (dl_url, version_num)
            
            for dl_url in unique_links:
                filename = dl_url.split("/")[-1]
                # Extract fund name part and possible version suffix
                # Example Match: ...Nifty-Total-Market-ETF-1.xlsx -> Name: ...ETF, Version: 1
                match = re.search(r'^(.*?)(?:-(\d+))?\.xlsx$', filename, re.I)
                if match:
                    base_part = match.group(1)
                    version = int(match.group(2)) if match.group(2) else 0
                    
                    # Store if new or if version is higher
                    if base_part not in dedup_map or version > dedup_map[base_part][1]:
                        dedup_map[base_part] = (dl_url, version)
                else:
                    # Non-standard name, keep it as is
                    dedup_map[filename] = (dl_url, 0)

            final_links = [v[0] for v in dedup_map.values()]
            logger.info(f"Found {len(unique_links)} potential links. Decided on {len(final_links)} unique fund files.")
            
            downloaded_paths = []
            for idx, dl_url in enumerate(final_links):
                try:
                    # Preserve original filename from URL
                    clean_filename = dl_url.split("/")[-1]
                    save_path = download_folder / clean_filename
                    
                    logger.info(f"Downloading ({idx+1}/{len(final_links)}): {clean_filename}")
                    
                    # Use requests for faster direct downloads
                    response = requests.get(dl_url, stream=True, timeout=60)
                    if response.status_code == 200:
                        with open(save_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        downloaded_paths.append(save_path)
                    else:
                        logger.error(f"Failed to download {dl_url}: HTTP {response.status_code}")
                except Exception as e:
                    logger.error(f"Error downloading {dl_url}: {e}")

            return downloaded_paths

        finally:
            if browser: browser.close()
            if pw: pw.stop()


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
