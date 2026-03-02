# src/downloaders/lic_downloader.py

import os
import time
import json
import shutil
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

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


class LICDownloader(BaseDownloader):
    """
    LIC Mutual Fund - Portfolio Downloader
    
    Uses Playwright to navigate forms and requests for efficient file downloads.
    Supports persistent browser sessions.
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
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        
        marker_data = {
            "amc": "LIC",
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
        
        logger.warning(f"LIC: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))

        self.notifier.notify_error(
            amc="LIC",
            year=year,
            month=month,
            error_type="Corruption Recovery",
            reason=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )

    def open_session(self):
        """Open a persistent browser session."""
        if self._page:
            return
            
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=False,
            args=["--window-size=1920,1080", "--start-maximized", "--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            slow_mo=500
        )
        self._context = self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            ignore_https_errors=True
        )
        self._page = self._context.new_page()
        Stealth().apply_stealth_sync(self._page)
        logger.info("Persistent Chrome session opened for LIC.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page:
            self._page.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()
            
        self._page = None
        self._context = None
        self._browser = None
        if self._playwright:
            self._playwright.stop()
        self._playwright = None
        logger.info("Persistent Chrome session closed for LIC.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("LIC MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # 1) Idempotency Check
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                # Month already complete - check for missing consolidation
                logger.info(f"LIC: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("✅ Month already complete — UPDATED")
                logger.info(f"🕒 Duration: {duration:.2f}s")
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
                    duration = time.time() - start_time
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: LIC")
                    logger.info(f"Mode: DRY RUN")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: SIMULATED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "LIC", "year": year, "month": month, "status": "success", "dry_run": True}

                downloaded_files = self._run_download_flow(year, month_name, target_dir)
                
                if not downloaded_files:
                    # Not Published Handling
                    duration = time.time() - start_time
                    logger.warning(f"LIC: {year}-{month:02d} not yet published.")
                    self.notifier.notify_not_published("LIC", year, month)
                    
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                        
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: LIC")
                    logger.info(f"Mode: AUTO")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: NOT PUBLISHED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "LIC", "year": year, "month": month, "status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, len(downloaded_files))
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("LIC", year, month, files_downloaded=len(downloaded_files), duration=duration)
                
                logger.success(f"✅ LIC download completed: {len(downloaded_files)} files")
                logger.info("=" * 60)
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: LIC")
                logger.info(f"Mode: AUTO")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: {len(downloaded_files)}")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: SUCCESS")
                logger.info("=" * 60)
                
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
        
        logger.info(f"[SUMMARY]")
        logger.info(f"AMC: LIC")
        logger.info(f"Mode: AUTO")
        logger.info(f"Month: {year}-{month:02d}")
        logger.info(f"Status: FAILED")
        logger.info(f"Duration: {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "amc": "LIC",
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration
        }

    def _run_download_flow(self, target_year: int, target_month_name: str, download_folder: Path) -> List[Path]:
        """Internal flow using Playwright to extract links or handle session."""
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        try:
            url = "https://www.licmf.com/downloads/monthly-portfolio"
            
            # Optimization: Skip navigation and wait if already on the page
            if page.url == url:
                logger.info(f"Already on {url}. Skipping navigation.")
                # Brief wait to ensure any previous dynamic content is cleared if necessary
                # but we'll mostly rely on the select_option and click triggers
            else:
                logger.info(f"Navigating to {url}...")
                page.goto(url, wait_until="domcontentloaded", timeout=120000)
                time.sleep(5)

            # 1. Click Consolidated Portfolio tab
            logger.info("Clicking 'Consolidated Portfolio' tab...")
            page.click("text=Consolidated Portfolio", timeout=30000)
            time.sleep(3)
            
            # 2. Select Portfolio Type: Monthly Portfolio
            logger.info("Selecting Portfolio Type: Monthly Portfolio...")
            page.select_option(".consolidated_type", label="Monthly Portfolio")
            time.sleep(3)
            
            # 3. Select Year
            logger.info(f"Selecting Year: {target_year}...")
            year_selected = False
            year_options = page.locator(".consolidated_year option").all()
            for opt in year_options:
                text = opt.inner_text().strip()
                if str(target_year) in text:
                    val = opt.get_attribute("value")
                    if val:
                        page.select_option(".consolidated_year", value=val)
                        year_selected = True
                        break
            
            if not year_selected:
                logger.error(f"Could not find year '{target_year}' in options.")
                return []
                
            time.sleep(3)
            
            # 4. Select Month
            logger.info(f"Selecting Month: {target_month_name}...")
            month_selected = False
            month_options = page.locator(".consolidated_month option").all()
            for opt in month_options:
                text = opt.inner_text().strip()
                if target_month_name.lower() in text.lower():
                    val = opt.get_attribute("value")
                    if val:
                        page.select_option(".consolidated_month", value=val)
                        month_selected = True
                        break
            
            if not month_selected:
                logger.error(f"Could not find month '{target_month_name}' in options.")
                return []
                
            time.sleep(2)
            
            # 5. Click Submit
            logger.info("Clicking Submit...")
            page.click(".consolidated-submit-btn")
            time.sleep(5)
            
            # 6. Extract links
            links = page.locator(".cportfolio-files a").all()
            if not links:
                logger.warning("No download links found for the selected period.")
                return []
                
            logger.info(f"Found {len(links)} links. Downloading...")
            
            # Get cookies for requests if needed, but we'll try direct download via requests first as per provided script
            headers = {
                'User-Agent': page.evaluate("navigator.userAgent")
            }
            
            downloaded_paths = []
            for link in links:
                title = link.inner_text().strip()
                href = link.get_attribute("href")
                
                if not href: continue
                    
                if not href.startswith("http"):
                    href = f"https://www.licmf.com{href if href.startswith('/') else '/' + href}"
                
                # Clean URL
                full_url = href.replace(" ", "%20")
                
                # Use original filename from URL
                filename = full_url.split("/")[-1].replace("%20", " ")
                if not filename or "." not in filename:
                    # Fallback to sanitized title if URL doesn't have a clear filename
                    safe_title = "".join([c for c in title if c.isalnum() or c in (" ", "-", "_")]).strip()
                    ext = ".xlsx" if ".xlsx" in full_url.lower() else ".xls"
                    if ".pdf" in full_url.lower(): ext = ".pdf"
                    filename = f"{safe_title}{ext}"
                
                filepath = download_folder / filename
                
                logger.info(f"Downloading: {title} ...")
                try:
                    # Use requests to download, following the user's logic
                    # verify=False handles potential cert issues on LIC site
                    response = requests.get(full_url, headers=headers, stream=True, timeout=30, verify=False)
                    if response.status_code == 200:
                        with open(filepath, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        logger.info(f"  Saved to: {filename}")
                        downloaded_paths.append(filepath)
                    else:
                        logger.warning(f"  Failed: Status {response.status_code}")
                except Exception as e:
                    logger.error(f"  Error downloading {full_url}: {e}")

            return downloaded_paths

        finally:
            if close_needed:
                self.close_session()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="LIC Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = LICDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"✅ Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success(f"✅ Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"ℹ️  Info: Month not yet published")
    else:
        logger.error(f"❌ Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
