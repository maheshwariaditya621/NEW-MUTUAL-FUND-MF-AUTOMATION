# src/downloaders/choice_downloader.py

import os
import time
import json
import shutil
import re
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


class ChoiceDownloader(BaseDownloader):
    """
    Choice Mutual Fund - Portfolio Downloader
    
    URL: https://choicemf.com/disclosures/monthly-portfolio
    Downloads all scheme-wise monthly portfolio files
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Choice Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "choice"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "CHOICE",
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
        
        logger.warning(f"CHOICE: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("CHOICE", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("CHOICE MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Choice: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"CHOICE: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"CHOICE: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("CHOICE", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("CHOICE", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] CHOICE download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("CHOICE", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> int:
        url = "https://choicemf.com/disclosures/monthly-portfolio"

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
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="networkidle", timeout=60000)
            time.sleep(2)
            logger.info("  [OK] Page loaded")

            # Open year accordion section - check if it exists first
            logger.info(f"Looking for year section: {target_year}...")
            year_button = page.locator("button").filter(has_text=str(target_year)).filter(has_text="–").first
            
            if year_button.count() == 0:
                year_button = page.get_by_role("button", name=re.compile(f".*{target_year}.*")).first
            
            # Check if year section exists with a short timeout
            try:
                if not year_button.is_visible(timeout=5000):
                    logger.warning(f"  [FAIL] Year section {target_year} not found - data not published")
                    return 0
            except:
                logger.warning(f"  [FAIL] Year section {target_year} not found - data not published")
                return 0
            
            year_button.click()
            time.sleep(3)
            logger.info(f"  [OK] Year section opened")

            # Open month accordion - check if it exists first
            logger.info(f"Looking for month: {month_name}...")
            month_button = page.get_by_role("button", name=month_name, exact=True)
            if month_button.count() == 0:
                month_button = page.get_by_text(month_name, exact=True)
            
            # Check if month section exists with a short timeout
            try:
                if not month_button.is_visible(timeout=5000):
                    logger.warning(f"  [FAIL] Month {month_name} not found - data not published")
                    return 0
            except:
                logger.warning(f"  [FAIL] Month {month_name} not found - data not published")
                return 0
            
            month_button.click()
            time.sleep(3)
            logger.info(f"  [OK] Month {month_name} opened")

            # Find all scheme files for this month
            search_text = f"{month_name} {target_year}"
            scheme_element_locator = page.locator(f"p:has-text('{search_text}')")
            
            scheme_count = scheme_element_locator.count()
            
            if scheme_count == 0:
                logger.warning(f"  [FAIL] No schemes found for {month_name} {target_year}")
                return 0
            
            logger.info(f"  [OK] Found {scheme_count} scheme(s)")

            downloaded_count = 0
            
            for i in range(scheme_count):
                try:
                    scheme_text = scheme_element_locator.nth(i)
                    scheme_name = scheme_text.text_content().strip()
                    logger.info(f"  [{i+1}/{scheme_count}] Downloading: {scheme_name[:60]}...")
                    
                    # Find the download button
                    download_btn = scheme_text.locator("xpath=ancestor::div[1]/following-sibling::button").first
                    
                    if download_btn.count() == 0:
                        parent_container = scheme_text.locator("xpath=ancestor::div[contains(@class, 'MuiBox-root') or contains(@class, 'row')][1]")
                        download_btn = parent_container.locator("button.MuiIconButton-root, button").last

                    with page.expect_download(timeout=60000) as download_info:
                        download_btn.click(force=True)
                    
                    download = download_info.value
                    suggested = download.suggested_filename
                    
                    save_path = download_folder / suggested
                    download.save_as(save_path)
                    logger.info(f"    [OK] Saved: {suggested}")
                    
                    downloaded_count += 1
                    time.sleep(1)
                    
                except PlaywrightTimeout:
                    logger.warning(f"    ⊘ Timeout: Download did not start")
                except Exception as e:
                    logger.warning(f"    [FAIL] Error: {str(e)[:100]}")
                    continue

            return downloaded_count

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = ChoiceDownloader()
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
