# src/downloaders/boi_downloader.py

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


class BOIDownloader(BaseDownloader):
    """
    Bank of India Mutual Fund - Portfolio Downloader
    
    URL: https://www.boimf.in/investor-corner#t2
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Bank of India Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "boi"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "BOI",
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
        
        logger.warning(f"BOI: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("BOI", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("BOI MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"BOI: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"BOI: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"BOI: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("BOI", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                duration = time.time() - start_time
                self.notifier.notify_success("BOI", year, month, files_downloaded=1, duration=duration)
                logger.success(f"✅ BOI download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("BOI", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        url = "https://www.boimf.in/investor-corner#t2"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="networkidle", timeout=60000)
            time.sleep(5)
            logger.info("  ✓ Page loaded")

            # 1) Ensure Monthly Portfolio tab is active
            logger.info("Ensuring 'Monthly Portfolio' tab is active...")
            try:
                page.get_by_role("link", name="Monthly Portfolio").click()
                time.sleep(2)
                logger.info("  ✓ Tab activated")
            except:
                logger.info("  → Tab already active")

            # 2) Search for portfolio link across pages
            month_upper = month_name.upper()
            logger.info(f"Searching for {month_upper} {target_year} portfolio...")
            
            found = False
            page_num = 1
            max_pages = 20
            
            while not found and page_num <= max_pages:
                logger.info(f"  → Checking page {page_num}...")
                
                # Pattern: MONTHLY-PORTFOLIO - 31-DECEMBER-2025
                link_pattern = rf"MONTHLY-PORTFOLIO.*{month_upper}"
                links = page.get_by_role("link", name=re.compile(link_pattern, re.IGNORECASE)).all()
                
                for link in links:
                    if link.is_visible():
                        link_text = link.text_content().strip()
                        
                        # Verify year is in the link text if possible
                        if str(target_year) in link_text or not re.search(r'\d{4}', link_text):
                            logger.info(f"  ✓ Found: {link_text}")
                            
                            # Download the file
                            logger.info("Starting download...")
                            try:
                                with page.expect_download(timeout=60000) as download_info:
                                    with page.expect_popup() as popup_info:
                                        link.click()
                                
                                download = download_info.value
                                final_filename = download.suggested_filename
                                save_path = download_folder / final_filename
                                
                                download.save_as(save_path)
                                logger.info(f"  ✓ Saved: {final_filename}")
                                
                                found = True
                                return save_path
                            except Exception as e:
                                logger.warning(f"  ✗ Download error: {str(e)[:100]}")
                                continue
                
                if found:
                    break
                
                # Navigate to next page
                logger.info(f"  → Not found on page {page_num}, checking next page...")
                next_btn = page.locator("#pagination-demo-t2 .page-item.next .page-link")
                
                if next_btn.count() > 0 and next_btn.is_visible():
                    next_btn.click()
                    time.sleep(3)
                    page_num += 1
                else:
                    # Try clicking by page number
                    next_page_num = str(page_num + 1)
                    next_page_btn = page.get_by_role("link", name=next_page_num, exact=True)
                    if next_page_btn.count() > 0 and next_page_btn.is_visible():
                        next_page_btn.click()
                        time.sleep(3)
                        page_num += 1
                    else:
                        logger.warning(f"  ✗ No more pages to search")
                        break
            
            if not found:
                logger.warning(f"  ✗ Portfolio not found after searching {page_num} pages")
                return None

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = BOIDownloader()
    downloader.download(args.year, args.month)
