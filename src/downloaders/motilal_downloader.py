# src/downloaders/motilal_downloader.py

import os
import time
import json
import shutil
import zipfile
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
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
    HEADLESS = True


class MotilalDownloader(BaseDownloader):
    """
    Motilal Oswal Mutual Fund - Portfolio Downloader
    
    URL: https://www.motilaloswalmf.com/download/scheme-portfolio-details
    Special Rule: Data for month N appears under month N+1 selection
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Motilal Oswal Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "motilal"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "MOTILAL",
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
        
        logger.warning(f"MOTILAL: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("MOTILAL", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _get_next_month(self, month: int, year: int):
        """Get the next month and year (for Motilal's offset quirk)."""
        if month == 12:
            return 1, year + 1
        else:
            return month + 1, year

    def open_session(self):
        """Open a persistent browser session."""
        if self._page:
            return
            
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        self._context = self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            accept_downloads=True
        )
        self._page = self._context.new_page()
        Stealth().apply_stealth_sync(self._page)
        logger.info("Persistent Chrome session opened for Motilal.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page: self._page.close()
        if self._browser: self._browser.close()
        if self._playwright: self._playwright.stop()
        self._page = self._context = self._browser = self._playwright = None
        logger.info("Persistent Chrome session closed for Motilal.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("MOTILAL OSWAL MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"MOTILAL: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"MOTILAL: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"MOTILAL: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("MOTILAL", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                duration = time.time() - start_time
                self.notifier.notify_success("MOTILAL", year, month, files_downloaded=1, duration=duration)
                logger.success(f"✅ MOTILAL download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("MOTILAL", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        url = "https://www.motilaloswalmf.com/download/scheme-portfolio-details"

        try:
            # Motilal quirk: Data for month N appears under month N+1 selection
            selection_month, selection_year = self._get_next_month(target_month, target_year)
            selection_month_name = self.MONTH_NAMES[selection_month]
            
            logger.info(f"Month offset: Selecting {selection_month_name} {selection_year} to get {month_name} {target_year} data")

            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="load", timeout=60000)
            time.sleep(3)
            logger.info("  ✓ Page loaded")

            # Select Year
            logger.info(f"Selecting year: {selection_year}...")
            page.locator(".css-19bb58m").first.click()
            time.sleep(1)
            page.get_by_role("option", name=str(selection_year)).click()
            time.sleep(2)
            logger.info(f"  ✓ Year {selection_year} selected")

            # Select Month
            logger.info(f"Selecting month: {selection_month_name}...")
            page.locator(".css-13cymwt-control > .css-hlgwow > .css-19bb58m").click()
            time.sleep(1)
            page.get_by_role("option", name=selection_month_name).click()
            time.sleep(2)
            logger.info(f"  ✓ Month {selection_month_name} selected")

            # Click document icon
            logger.info("Clicking document icon...")
            page.locator("div:nth-child(2) > .downloadWithPagination_rightSide__E_PDE > .downloadWithPagination_documentIcon__ph7xA > img").click()
            time.sleep(2)
            logger.info("  ✓ Document icon clicked")

            # Download file
            logger.info("Downloading file...")
            with page.expect_download(timeout=60000) as download_info:
                page.get_by_role("link", name="Download cloud").click()

            download = download_info.value
            suggested = download.suggested_filename
            ext = os.path.splitext(suggested)[1] if suggested else ".xlsx"

            # Save to temp location first
            temp_path = download_folder / f"temp_{suggested}"
            download.save_as(temp_path)
            logger.info(f"  ✓ Downloaded: {suggested}")

            # Process the file (extract if ZIP, rename)
            final_path = self._process_downloaded_file(temp_path, month_name, target_year, download_folder)
            
            return final_path

        finally:
            if close_needed: self.close_session()

    def _process_downloaded_file(self, temp_path: Path, month_name: str, year: int, download_folder: Path) -> Optional[Path]:
        """Process the downloaded file - extract if ZIP, rename appropriately."""
        try:
            # Check if it's a ZIP file
            if temp_path.suffix.lower() == '.zip':
                logger.info("ZIP file detected, extracting...")
                
                temp_extract_dir = download_folder / "temp_extract"
                temp_extract_dir.mkdir(exist_ok=True)
                
                with zipfile.ZipFile(temp_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_extract_dir)
                
                # Look for Excel file in extracted contents
                found_file = None
                for file in temp_extract_dir.rglob('*'):
                    if file.is_file() and file.suffix.lower() in ['.xlsx', '.xls']:
                        found_file = file
                        break
                
                if found_file:
                    logger.info(f"  ✓ Extracted: {found_file.name}")
                    final_filename = f"MotilalOswal_{month_name}_{year}.xlsx"
                    final_path = download_folder / final_filename
                    
                    shutil.move(str(found_file), str(final_path))
                    
                    # Cleanup
                    temp_path.unlink()
                    shutil.rmtree(temp_extract_dir, ignore_errors=True)
                    
                    logger.info(f"  ✓ Saved: {final_filename}")
                    return final_path
                else:
                    logger.warning("  ✗ No Excel file found in ZIP")
                    # Cleanup
                    temp_path.unlink()
                    shutil.rmtree(temp_extract_dir, ignore_errors=True)
                    return None
            else:
                # Direct Excel file
                final_filename = f"MotilalOswal_{month_name}_{year}{temp_path.suffix}"
                final_path = download_folder / final_filename
                
                shutil.move(str(temp_path), str(final_path))
                logger.info(f"  ✓ Saved: {final_filename}")
                
                return final_path
                
        except Exception as e:
            logger.error(f"Error processing file: {e}")
            if temp_path.exists():
                temp_path.unlink()
            return None


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = MotilalDownloader()
    downloader.download(args.year, args.month)
