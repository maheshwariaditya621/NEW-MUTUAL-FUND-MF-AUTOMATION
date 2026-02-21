# src/downloaders/abakkus_downloader.py

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


class AbakkusDownloader(BaseDownloader):
    """
    Abakkus Mutual Fund - Portfolio Downloader
    
    Uses Playwright to navigate statutory disclosures and extract month-end portfolios.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Abakkus Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "abakkus"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        
        marker_data = {
            "amc": "ABAKKUS",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        
        logger.info(f"Created completion marker: {marker_path.name}")

    def open_session(self):
        """Open a persistent browser session."""
        if self._page:
            return
            
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=HEADLESS,
            args=[
                "--window-size=1920,1080",
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
            slow_mo=500
        )
        self._context = self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            ignore_https_errors=True
        )
        self._page = self._context.new_page()
        Stealth().apply_stealth_sync(self._page)
        logger.info("Persistent Chrome session opened for Abakkus.")

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
        self._playwright = None
        logger.info("Persistent Chrome session closed for Abakkus.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        
        logger.info("=" * 60)
        logger.info("ABAKKUS MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # 1) Idempotency Check
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                logger.info(f"Abakkus: {year}-{month:02d} files already downloaded.")
                self.consolidate_downloads(year, month)
                duration = time.time() - start_time
                return {
                    "status": "skipped", 
                    "reason": "already_downloaded",
                    "duration": duration
                }

        self.ensure_directory(str(target_dir))

        # 2) Download Logic with Retry
        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"Abakkus: [DRY RUN] Would download {month} {year}")
                    return {"amc": "ABAKKUS", "year": year, "month": month, "status": "success", "dry_run": True}

                downloaded_files = self._run_download_flow(year, month, target_dir)
                
                if not downloaded_files:
                    duration = time.time() - start_time
                    logger.warning(f"Abakkus: {year}-{month:02d} month-end portfolio not found or not yet published.")
                    
                    if target_dir.exists() and not any(target_dir.iterdir()):
                        shutil.rmtree(target_dir, ignore_errors=True)
                        
                    return {"amc": "ABAKKUS", "year": year, "month": month, "status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, len(downloaded_files))
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("Abakkus", year, month, files_downloaded=len(downloaded_files), duration=duration)
                
                return {
                    "amc": "ABAKKUS",
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
        duration = time.time() - start_time
        self.notifier.notify_error("Abakkus", year, month, error_type="Download Failure", reason=last_error[:100])
        
        return {
            "amc": "ABAKKUS",
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration
        }

    def _run_download_flow(self, target_year: int, target_month: int, download_folder: Path) -> List[Path]:
        """Internal flow using Playwright to extract links or handle session."""
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        try:
            url = "https://www.abakkusmf.com/statutory-disclosures.html#"
            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="networkidle", timeout=60000)
            
            # Handle Modal
            logger.info("Handling declaration modal...")
            try:
                # Based on user's codegen: await page.getByRole('button', { name: 'I AM NOT A US PERSON/' }).click();
                modal_button = page.get_by_role("button", name="I AM NOT A US PERSON/")
                modal_button.wait_for(state="visible", timeout=10000)
                modal_button.click()
                logger.info("Modal dismissed.")
                time.sleep(2)
            except Exception:
                logger.info("Modal not found or already dismissed.")
            
            # Click "Monthly Portfolio Disclosures" tab
            logger.info("Accessing Monthly Portfolio Disclosures tab...")
            # Based on user's codegen: await page.getByRole('tab', { name: 'Monthly Portfolio Disclosures' }).click();
            try:
                tab = page.get_by_role("tab", name="Monthly Portfolio Disclosures")
                tab.wait_for(state="visible", timeout=10000)
                tab.click()
                logger.info("Tab clicked.")
                time.sleep(3)
            except Exception as e:
                logger.error(f"Could not find or click 'Monthly Portfolio Disclosures' tab: {e}")
                return []
            
            # Extract link for specific month and year
            target_month_name = self.MONTH_NAMES[target_month]
            
            # Potential month-end dates to search for
            potential_dates = [31, 30, 29, 28] 
            downloaded_paths = []
            
            for day in potential_dates:
                # Based on user's codegen: await page.getByRole('heading', { name: 'January 31,' }).click();
                date_label = f"{target_month_name} {day},"
                logger.info(f"Searching for month-end portfolio: {date_label}")
                
                try:
                    # Check if the heading for this date exists
                    date_heading = page.get_by_role("heading", name=date_label)
                    if date_heading.is_visible():
                        logger.info(f"Found heading for {date_label}. Locating associated Download link...")
                        
                        # Use XPath to find the 'Download' link following the specific heading
                        # The user's codegen shows they clicked 'Download' nth(2) etc. 
                        # A relative XPath is more stable.
                        download_link_xpath = f'//h4[contains(text(), "{date_label}")]/following::a[contains(text(), "Download")][1]'
                        # Fallback if it's not strictly h4
                        if not page.locator(f"xpath={download_link_xpath}").is_visible():
                             download_link_xpath = f'//*[contains(text(), "{date_label}")]/following::a[contains(text(), "Download")][1]'

                        link = page.locator(f"xpath={download_link_xpath}")
                        
                        if link.is_visible():
                            href = link.get_attribute("href")
                            if not href: continue
                            
                            if not href.startswith("http"):
                                href = f"https://www.abakkusmf.com{href if href.startswith('/') else '/' + href}"
                            
                            # Clean URL
                            full_url = href.split("?")[0]
                            if not (full_url.endswith(".xls") or full_url.endswith(".xlsx")):
                                logger.debug(f"Skipping non-Excel file matching date {date_label}: {full_url}")
                                continue

                            filename = full_url.split("/")[-1]
                            filepath = download_folder / filename
                            
                            logger.info(f"Found match: {date_label} -> {filename}")
                            
                            # Handle download event as per user's nudge
                            with page.expect_download() as download_info:
                                link.click()
                            download = download_info.value
                            download.save_as(filepath)
                            
                            downloaded_paths.append(filepath)
                            logger.info(f"Successfully downloaded: {filename}")
                            return downloaded_paths # Exit after first match
                except Exception as e:
                    logger.debug(f"Error checking for {date_label}: {e}")
                    continue

            logger.warning(f"No matching month-end portfolio found for {target_month_name} {target_year}.")
            return []

        finally:
            if close_needed:
                self.close_session()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Abakkus Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = AbakkusDownloader()
    result = downloader.download(args.year, args.month)
    print(json.dumps(result, indent=2))
