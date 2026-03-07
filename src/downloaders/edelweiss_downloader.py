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
                # Month already complete - check for missing consolidation
                logger.info(f"Edelweiss: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"🕒 Duration: {duration:.2f}s")
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
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("EDELWEISS", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] EDELWEISS download completed: {downloaded_path.name}")
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
        url = "https://www.edelweissmf.com/statutory/portfolio-of-schemes"
        month_name = self.MONTH_NAMES[target_month]

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                accept_downloads=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                extra_http_headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                }
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            # Visit home page first to get cookies
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
                    logger.info("  [OK] Clicked the tab.")
                    time.sleep(2)
            except:
                logger.info("  ⚠ Tab not found or already active.")

            # 2) Select Year
            logger.info(f"Selecting Year: {target_year}...")
            
            # Locator that works for both mat-select and standard select
            year_locator = page.locator("mat-select, select, [role='combobox'], ng-select").filter(has_text=re.compile(r"Year|Select Year", re.IGNORECASE)).first
            year_locator.wait_for(state="visible", timeout=10000)
            
            is_select_tag = year_locator.evaluate("el => el.tagName.toLowerCase() === 'select'")
            if is_select_tag:
                year_locator.select_option(label=str(target_year))
            else:
                year_locator.click(force=True)
                time.sleep(1)
                page.get_by_role("option", name=str(target_year), exact=True).click(force=True)
            
            logger.info(f"  [OK] Selected Year: {target_year}")
            time.sleep(3)  # Wait for links to update/load

            # 3) Find and download the portfolio link for the specific month
            logger.info(f"Finding download link for {month_name} {target_year}...")
            
            # The list of links should now be visible below the dropdown
            # Link text examples: "Monthly Portfolio - January 31, 2026", "Monthly Portfolio - December 31, 2025"
            link_pattern = re.compile(rf"Monthly Portfolio.*{month_name}.*{target_year}", re.IGNORECASE)
            
            # Wait for some links to be visible
            page.wait_for_selector("a", state="visible", timeout=10000)
            
            all_links = page.get_by_role("link").all()
            target_link = None
            for link in all_links:
                text = link.inner_text().strip()
                if link_pattern.search(text):
                    target_link = link
                    logger.info(f"  Found matching link: {text}")
                    break
            
            if not target_link:
                logger.warning(f"No download link found for {month_name} {target_year}")
                # Fallback: check for month abbreviation if full name fails
                month_abbr = self.MONTH_ABBR[target_month]
                abbr_pattern = re.compile(rf"Monthly Portfolio.*{month_abbr}.*{target_year}", re.IGNORECASE)
                for link in all_links:
                    text = link.inner_text().strip()
                    if abbr_pattern.search(text):
                        target_link = link
                        logger.info(f"  Found matching link (abbr): {text}")
                        break
            
            if not target_link:
                return None
            link_text = target_link.inner_text().strip()
            logger.info(f"  Found link: {link_text}")
            
            # Download the file
            logger.info("Starting download...")
            with page.expect_download(timeout=45000) as download_info:
                target_link.click()
            
            download = download_info.value
            final_filename = download.suggested_filename
            save_path = download_folder / final_filename
            
            download.save_as(save_path)
            logger.info(f"  [OK] Saved: {final_filename}")
            
            return save_path

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = EdelweissDownloader()
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
