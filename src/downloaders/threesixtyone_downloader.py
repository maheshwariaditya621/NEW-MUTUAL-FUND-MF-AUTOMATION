# src/downloaders/threesixtyone_downloader.py

import os
import time
import json
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
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


class ThreeSixtyOneDownloader(BaseDownloader):
    """
    360 ONE Mutual Fund (formerly IIFL) - Portfolio Downloader
    
    URL: https://archive.iiflmf.com/downloads/disclosures
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("360 ONE Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "threesixtyone"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "360ONE",
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
        
        logger.warning(f"360ONE: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("360ONE", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("360 ONE MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"360 ONE: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"360ONE: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"360ONE: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("360ONE", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("360ONE", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] 360 ONE download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("360ONE", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, download_folder: Path) -> Optional[Path]:
        url = "https://archive.iiflmf.com/downloads/disclosures"

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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            month_name = self.MONTH_NAMES[target_month]
            if page.url != url:
                logger.info(f"Navigating to {url}...")
                page.goto(url, wait_until="load", timeout=90000)
            
            # 1) Expand Accordion
            accordion_selector = 'a[href="#collapse0"]'
            section_selector = '#collapse0'
            
            # Use wait_for_timeout to let any JS load
            page.wait_for_selector(accordion_selector, timeout=10000)
            
            # Check if expanded (class 'in' on #collapse0 often indicates expansion in Bootstrap 3)
            # or just check visibility
            is_visible = page.is_visible(section_selector)
            if not is_visible:
                logger.info("Expanding 'Monthly Portfolio' section...")
                page.click(accordion_selector)
                page.wait_for_selector(f"{section_selector}.in, {section_selector}:visible", timeout=10000)
            
            # 2) Find Link using Year h4 and Month text
            # XPath finds h4 with Year, then looks for standard sibling structure
            xpath = (
                f"//div[@id='collapse0']//h4[normalize-space()='{target_year}']"
                f"/following-sibling::ul[preceding-sibling::h4[1][normalize-space()='{target_year}']]"
                f"//a[normalize-space()='{month_name}']"
            )
            
            link_count = page.locator(xpath).count()
            if link_count == 0:
                # Try partial month or case-insensitive if needed, but normative is full name
                logger.debug(f"Retrying with broader XPath for {month_name}...")
                xpath = (
                    f"//div[@id='collapse0']//h4[contains(., '{target_year}')]"
                    f"/following-sibling::ul[preceding-sibling::h4[1][contains(., '{target_year}')]]"
                    f"//a[contains(normalize-space(.), '{month_name}')]"
                )
            
            link_loc = page.locator(xpath).first
            if not link_loc.is_visible():
                return None

            # 3) Trigger Download
            logger.info(f"Found link for {month_name} {target_year}. Downloading...")
            with page.expect_download(timeout=60000) as download_info:
                link_loc.click()
            
            download = download_info.value
            save_path = download_folder / download.suggested_filename
            download.save_as(save_path)
            
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

    downloader = ThreeSixtyOneDownloader()
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
