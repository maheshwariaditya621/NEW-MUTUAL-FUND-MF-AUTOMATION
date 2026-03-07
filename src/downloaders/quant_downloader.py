# src/downloaders/quant_downloader.py

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


class QuantDownloader(BaseDownloader):
    """
    Quant Mutual Fund - Portfolio Downloader
    
    URL: https://quantmutual.com/statutory-disclosures
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Quant Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "quant"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "QUANT",
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
        
        logger.warning(f"QUANT: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("QUANT", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("QUANT MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Quant: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"QUANT: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"QUANT: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("QUANT", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("QUANT", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] QUANT download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("QUANT", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        url = "https://quantmutual.com/statutory-disclosures"

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
            page.goto(url, wait_until="load", timeout=90000)
            time.sleep(5)
            logger.info("  [OK] Page loaded")

            # 1) Find and click "MONTHLY PORTFOLIO" accordion header (excluding "FUND - WISE")
            logger.info("Finding 'MONTHLY PORTFOLIO' section...")
            headers = page.locator(".statutory.disclouser").all()
            target_header = None
            
            for h in headers:
                try:
                    text = h.text_content().strip()
                    if "MONTHLY PORTFOLIO" in text and "FUND - WISE" not in text:
                        target_header = h
                        break
                except:
                    continue
            
            if not target_header:
                raise Exception("Could not find 'MONTHLY PORTFOLIO' section")
            
            logger.info("  [OK] Found 'MONTHLY PORTFOLIO' section")
            logger.info("Expanding accordion...")
            target_header.scroll_into_view_if_needed()
            # Use JS click for reliability with animations
            page.evaluate("(el) => el.click()", target_header.element_handle())
            time.sleep(5)
            logger.info("  [OK] Accordion expanded")

            # 2) Select the Year
            logger.info(f"Selecting year: {target_year}...")
            content_area = target_header.locator("xpath=./following-sibling::div[1]")
            year_li = content_area.locator(f"li.yearurl:has-text('{target_year}')").first
            
            if year_li.count() > 0:
                page.evaluate("(el) => el.click()", year_li.element_handle())
                time.sleep(7)  # Wait for AJAX load
                logger.info(f"  [OK] Year {target_year} selected")
            else:
                # Fallback global search
                year_li = page.locator(f"li.yearurl:has-text('{target_year}')").filter(
                    has=page.locator("xpath=self::*[contains(@onclick, 'MONTHLY PORTFOLIO')]")
                ).first
                
                if year_li.count() > 0:
                    page.evaluate("(el) => el.click()", year_li.element_handle())
                    time.sleep(7)
                    logger.info(f"  [OK] Year {target_year} selected (fallback)")
                else:
                    raise Exception(f"Year '{target_year}' not found in Monthly Portfolio section")

            # 3) Find Month Download Link
            logger.info(f"Searching for {month_name} {target_year} download link...")
            container_sel = 'div[id="MONTHLY PORTFOLIO"]'
            
            # Try to find the link directly
            month_link = page.locator(container_sel).get_by_text(f"{month_name} {target_year}", exact=False).first
            
            if month_link.count() == 0:
                # Alternative search
                month_link = page.locator(f'{container_sel} a:has-text("{month_name}"):has-text("{target_year}")').first

            if month_link.count() == 0:
                logger.warning(f"  [FAIL] Month link for '{month_name} {target_year}' not found")
                return None
            
            logger.info(f"  [OK] Found download link")
            logger.info("Starting download...")
            month_link.scroll_into_view_if_needed()
            
            with page.expect_download(timeout=120000) as download_info:
                try:
                    month_link.click(timeout=10000)
                except:
                    # Use JS click if normal click fails
                    page.evaluate("(el) => el.click()", month_link.element_handle())
            
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

    downloader = QuantDownloader()
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
