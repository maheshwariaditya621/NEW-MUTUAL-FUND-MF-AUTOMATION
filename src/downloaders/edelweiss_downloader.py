# src/downloaders/edelweiss_downloader.py

import os
import time
import json
import shutil
import re
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


class EdelweissDownloader(BaseDownloader):
    """
    Edelweiss Mutual Fund - Portfolio Downloader
    
    URL: https://www.edelweissmf.com/statutory/portfolio-of-schemes
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    
    MONTH_ABBR = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    def __init__(self):
        super().__init__("Edelweiss Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "edelweiss"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "EDELWEISS",
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
        
        logger.warning(f"EDELWEISS: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("EDELWEISS", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

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
            accept_downloads=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        self._page = self._context.new_page()
        Stealth().apply_stealth_sync(self._page)
        logger.info("Persistent Chrome session opened for Edelweiss.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page: self._page.close()
        if self._browser: self._browser.close()
        if self._playwright: self._playwright.stop()
        self._page = self._context = self._browser = self._playwright = None
        logger.info("Persistent Chrome session closed for Edelweiss.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        month_abbr = self.MONTH_ABBR[month]
        
        logger.info("=" * 60)
        logger.info("EDELWEISS MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"EDELWEISS: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"EDELWEISS: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_abbr, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"EDELWEISS: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("EDELWEISS", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                duration = time.time() - start_time
                self.notifier.notify_success("EDELWEISS", year, month, files_downloaded=1, duration=duration)
                logger.success(f"✅ EDELWEISS download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("EDELWEISS", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_abbr: str, download_folder: Path) -> Optional[Path]:
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        url = "https://www.edelweissmf.com/statutory/portfolio-of-schemes"

        try:
            # Visit home page first to get cookies
            if page.url != "https://www.edelweissmf.com/":
                try:
                    logger.info("Visiting home page to establish session...")
                    page.goto("https://www.edelweissmf.com/", wait_until="networkidle", timeout=30000)
                    time.sleep(2)
                except: pass

            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="networkidle", timeout=60000)
            time.sleep(3)

            # 1) Click "Monthly Portfolio and Risk-o-Meter" tab
            logger.info("Selecting 'Monthly Portfolio and Risk-o-Meter' tab...")
            try:
                tab = page.get_by_text("Monthly Portfolio and Risk-o-", exact=False)
                if tab.is_visible(timeout=5000):
                    tab.click()
                    logger.info("  ✓ Clicked the tab.")
                    time.sleep(2)
            except:
                logger.info("  ⚠ Tab not found or already active.")

            # 2) Select Year and Month from dropdowns
            logger.info(f"Selecting Year: {target_year}, Month: {month_abbr}...")
            
            # Find visible dropdowns
            visible_dropdowns = page.locator("mat-select:visible")
            dropdown_count = visible_dropdowns.count()
            logger.info(f"  Found {dropdown_count} visible dropdowns.")
            
            if dropdown_count >= 2:
                # Select Year
                year_dropdown = visible_dropdowns.nth(0)
                year_dropdown.scroll_into_view_if_needed()
                year_dropdown.click(force=True)
                time.sleep(1)
                page.get_by_role("option", name=str(target_year), exact=True).click(force=True)
                time.sleep(1)
                
                # Select Month
                month_dropdown = visible_dropdowns.nth(1)
                month_dropdown.scroll_into_view_if_needed()
                month_dropdown.click(force=True)
                time.sleep(1)
                page.get_by_role("option", name=month_abbr, exact=True).click(force=True)
                time.sleep(3)  # Wait for link to update
            else:
                raise Exception(f"Could not find both dropdowns (found {dropdown_count})")

            # 3) Find and download the portfolio link
            logger.info("Finding download link...")
            download_links = page.get_by_role("link").filter(has_text=re.compile(r"Monthly Portfolio", re.IGNORECASE))
            
            if download_links.count() == 0:
                logger.warning("No download link found!")
                return None

            target_link = download_links.first
            link_text = target_link.inner_text().strip()
            logger.info(f"  Found link: {link_text}")
            
            # Download the file
            logger.info("Starting download...")
            with page.expect_download(timeout=45000) as download_info:
                target_link.click()
            
            download = download_info.value
            
            # Save with standardized filename
            suggested = download.suggested_filename
            ext = os.path.splitext(suggested)[1] if suggested else ".xlsx"
            final_filename = f"EDELWEISS_{self.MONTH_NAMES[target_month]}_{target_year}{ext}"
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

    downloader = EdelweissDownloader()
    downloader.download(args.year, args.month)
