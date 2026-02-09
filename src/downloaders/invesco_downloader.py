# src/downloaders/invesco_downloader.py

import os
import time
import json
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List
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


class InvescoDownloader(BaseDownloader):
    """
    Invesco Mutual Fund - Portfolio Downloader
    
    URL: https://invescomutualfund.com/literature-and-form?tab=Complete
    Features:
    - Persistent Session for efficiency.
    - Iterates through 6 product categories.
    - Column-based download link extraction (Jan=Col 2, Dec=Col 13).
    - Gold Standard compliance.
    """
    
    CATEGORIES = [
        "Equity",
        "Fixed income",
        "Fund of funds",
        "Exchange traded fund",
        "Hybrid",
        "Fixed maturity plans"
    ]
    
    MONTH_TO_COLUMN = {
        1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7,
        7: 8, 8: 9, 9: 10, 10: 11, 11: 12, 12: 13
    }
    
    MONTH_ABBR = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    def __init__(self):
        super().__init__("Invesco Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "invesco"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Invesco",
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
        
        logger.warning(f"{self.AMC_NAME}: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("Invesco", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def open_session(self):
        """Open a persistent browser session for Invesco."""
        if self._page: return
            
        self._playwright = sync_playwright().start()
        # Launch non-headless if requested, but default to HEADLESS config
        # The user script used headless=False, we will respect the config or default to True for prod
        self._browser = self._playwright.chromium.launch(
            headless=HEADLESS,
            channel="chrome",
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-infobars"]
        )

        self._context = self._browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            accept_downloads=True
        )
        self._page = self._context.new_page()
        Stealth().apply_stealth_sync(self._page)
        logger.info(f"Persistent browser session opened for {self.AMC_NAME}.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page: self._page.close()
        if self._browser: self._browser.close()
        if self._playwright: self._playwright.stop()
        self._page = self._context = self._browser = self._playwright = None
        logger.info(f"Persistent browser session closed for {self.AMC_NAME}.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_abbr = self.MONTH_ABBR[month]
        
        logger.info("=" * 60)
        logger.info(f"INVESCO MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_abbr})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"{self.AMC_NAME}: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"{self.AMC_NAME}: [DRY RUN] Would download {month_abbr} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_abbr, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"{self.AMC_NAME}: No portfolios found for {month_abbr} {year}")
                    self.notifier.notify_not_published("Invesco", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                duration = time.time() - start_time
                self.notifier.notify_success("Invesco", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Invesco", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_abbr: str, download_folder: Path) -> int:
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        url = "https://invescomutualfund.com/literature-and-form?tab=Complete"
        col_idx = self.MONTH_TO_COLUMN[target_month]
        files_downloaded = 0
        processed_urls = set()

        try:
            logger.info(f"Navigating to Invesco Holdings page...")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)

            for category in self.CATEGORIES:
                logger.info(f"Processing Category: {category}")
                try:
                    # Click category logic from user script
                    cat_loc = page.locator("#ClassificationCompleteMonthlyHoldings").get_by_text(category, exact=True)
                    if category == "Equity":
                        cat_loc.click() # First click
                        time.sleep(1)
                        cat_loc.click() # Second click (sometimes needed for Equity)
                    else:
                        cat_loc.click()
                    
                    time.sleep(3)

                    # Select Year
                    page.locator("#ddlYearCompleteMonthlyHoldings").get_by_text(str(target_year), exact=True).click()
                    time.sleep(4)

                    # Find rows
                    rows = page.locator("tbody tr").all()
                    if not rows:
                        logger.info(f"  No data for {category}")
                        continue

                    cat_files = 0
                    for row in rows:
                        try:
                            # Check for download link in specific month column
                            dl_link = row.locator(f"td:nth-child({col_idx}) > a")
                            if dl_link.count() == 0: continue

                            scheme_name = row.locator("td").first.text_content().strip()
                            clean_scheme = scheme_name.replace("Invesco India ", "").replace("Invesco ", "").strip()
                            clean_scheme = clean_scheme.replace(" ", "_").replace("/", "_").replace("&", "and")
                            clean_category = category.replace(" ", "_").replace("/", "_")
                            
                            logger.info(f"    Downloading: {scheme_name[:40]}...")

                            try:
                                with page.expect_download(timeout=30000) as download_info:
                                    # Handle potential popup
                                    with page.expect_popup(timeout=5000) as popup_info:
                                        dl_link.click()
                                    try:
                                        popup = popup_info.value
                                        popup.close()
                                    except: pass # Popup might not open or close instantly
                                
                                dl = download_info.value
                                ext = os.path.splitext(dl.suggested_filename)[1] or ".xlsx"
                                
                                # Standardized Filename
                                fname = f"INVESCO_{clean_scheme}_{clean_category}_{month_abbr}_{target_year}{ext}"
                                save_path = download_folder / fname
                                
                                dl.save_as(save_path)
                                logger.info(f"      ✓ Saved: {fname}")
                                files_downloaded += 1
                                cat_files += 1
                                time.sleep(0.5)
                                
                            except Exception as e:
                                logger.error(f"      ✗ Failed to download: {e}")

                        except Exception as e:
                            continue
                    
                    logger.info(f"  Category Summary: {cat_files} files from {category}")

                except Exception as e:
                    logger.error(f"Error processing category {category}: {e}")
                    continue
            
            return files_downloaded

        finally:
            if close_needed: self.close_session()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = InvescoDownloader()
    downloader.download(args.year, args.month)
