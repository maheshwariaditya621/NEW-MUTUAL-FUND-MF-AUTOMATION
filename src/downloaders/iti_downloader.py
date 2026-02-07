# src/downloaders/iti_downloader.py

import os
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
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
    HEADLESS = True


class ITIDownloader(BaseDownloader):
    """
    ITI Mutual Fund - Portfolio Downloader
    
    URL: https://www.itiamc.com/statuory-disclosure?type=Portfolio%20Disclosures
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("ITI Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "iti"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "ITI",
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
        
        logger.warning(f"ITI: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("ITI", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

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
        logger.info("Persistent Chrome session opened for ITI.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page: self._page.close()
        if self._browser: self._browser.close()
        if self._playwright: self._playwright.stop()
        self._page = self._context = self._browser = self._playwright = None
        logger.info("Persistent Chrome session closed for ITI.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("ITI MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"ITI: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"ITI: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"ITI: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("ITI", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                duration = time.time() - start_time
                self.notifier.notify_success("ITI", year, month, files_downloaded=1, duration=duration)
                logger.success(f"✅ ITI download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("ITI", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        url = "https://www.itiamc.com/statuory-disclosure?type=Portfolio%20Disclosures"

        try:
            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)
            logger.info("  ✓ Page loaded")

            # 1) Collapse Half Yearly and Fortnightly sections
            logger.info("Managing accordion sections...")
            
            # Try to collapse Half Yearly
            half_yearly_variations = ["Half Yearly", "Half-Yearly", "Half yearly", "HALF YEARLY"]
            for variation in half_yearly_variations:
                try:
                    half_yearly = page.get_by_text(variation, exact=True)
                    if half_yearly.count() > 0 and half_yearly.is_visible(timeout=2000):
                        half_yearly.click()
                        time.sleep(1)
                        logger.info("  ✓ Collapsed Half Yearly")
                        break
                except: pass
            
            # Try to collapse Fortnightly
            try:
                fortnightly = page.get_by_text("Fortnightly", exact=True)
                if fortnightly.count() > 0 and fortnightly.is_visible(timeout=2000):
                    fortnightly.click()
                    time.sleep(1)
                    logger.info("  ✓ Collapsed Fortnightly")
            except: pass

            # 2) Ensure Monthly section is expanded
            logger.info("Ensuring Monthly section is expanded...")
            monthly_items_visible = page.locator("text=/Monthly Portfolio -/").count() > 0
            
            if not monthly_items_visible:
                try:
                    monthly_heading = page.get_by_text("Monthly", exact=True)
                    if monthly_heading.is_visible(timeout=2000):
                        monthly_heading.click()
                        time.sleep(2)
                        logger.info("  ✓ Expanded Monthly")
                except:
                    raise Exception("Could not expand Monthly section")

            # 3) Scroll to load all data
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(1)

            # 4) Find the specific month/year heading
            target_text = f"Monthly Portfolio - {month_name} {target_year}"
            logger.info(f"Looking for '{target_text}'...")
            
            month_heading = page.get_by_text(target_text, exact=True)
            
            if month_heading.count() == 0:
                logger.warning(f"  ✗ Not found: {target_text}")
                return None
            
            logger.info("  ✓ Found heading")
            
            # 5) Click to expand this specific month
            month_heading.scroll_into_view_if_needed()
            time.sleep(1)
            month_heading.click()
            time.sleep(3)
            
            # 6) Find the download button using DOM traversal
            logger.info("Finding download button...")
            
            download_button = None
            
            # Try next sibling
            try:
                next_sibling = month_heading.locator("xpath=following-sibling::*[1]")
                if next_sibling.count() > 0:
                    download_in_sibling = next_sibling.locator(".file-download-link")
                    if download_in_sibling.count() > 0:
                        download_button = download_in_sibling.first
                        logger.info("  ✓ Found in next sibling")
            except: pass
            
            # Try parent's next sibling
            if not download_button:
                try:
                    month_parent = month_heading.locator("xpath=..")
                    parent_next_sibling = month_parent.locator("xpath=following-sibling::*[1]")
                    if parent_next_sibling.count() > 0:
                        download_in_parent = parent_next_sibling.locator(".file-download-link")
                        if download_in_parent.count() > 0:
                            download_button = download_in_parent.first
                            logger.info("  ✓ Found in parent's sibling")
                except: pass
            
            if not download_button:
                raise Exception("Could not locate download button")
            
            # 7) Download the file
            logger.info("Starting download...")
            with page.expect_download(timeout=60000) as download_info:
                download_button.click()
            
            download = download_info.value
            
            # Save with standardized filename
            suggested = download.suggested_filename
            ext = os.path.splitext(suggested)[1] if suggested else ".xls"
            final_filename = f"ITI_{month_name}_{target_year}{ext}"
            save_path = download_folder / final_filename
            
            download.save_as(save_path)
            logger.info(f"  ✓ Saved: {final_filename}")
            
            return save_path

        finally:
            if close_needed: self.close_session()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = ITIDownloader()
    downloader.download(args.year, args.month)
