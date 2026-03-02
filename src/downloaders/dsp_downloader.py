# src/downloaders/dsp_downloader.py

import os
import time
import json
import shutil
import zipfile
import calendar
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


class DSPDownloader(BaseDownloader):
    """
    DSP Mutual Fund - Portfolio Downloader
    
    URL: https://www.dspim.com/mandatory-disclosures/portfolio-disclosures
    Downloads consolidated ZIP file and extracts contents
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("DSP Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "dsp"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "DSP",
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
        
        logger.warning(f"DSP: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("DSP", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("DSP MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"DSP: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("✅ Month already complete — UPDATED")
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
                    logger.info(f"DSP: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_extracted = self._run_download_flow(year, month, month_name, target_dir)
                
                if files_extracted == 0:
                    logger.warning(f"DSP: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("DSP", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_extracted)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("DSP", year, month, files_downloaded=files_extracted, duration=duration)
                logger.success(f"✅ DSP download completed: {files_extracted} files extracted")
                return {"status": "success", "files_downloaded": files_extracted, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("DSP", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> int:
        url = "https://www.dspim.com/mandatory-disclosures/portfolio-disclosures"

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
            logger.info("  ✓ Page loaded")

            # Click "Month End Portfolio" section if it exists
            logger.info("Opening 'Month End Portfolio' section...")
            try:
                month_end_btn = page.locator("*:has-text('Month End Portfolio')").filter(has_text="Disclosures").first
                if month_end_btn.count() == 0:
                    month_end_btn = page.get_by_text("Month End Portfolio").first
                
                if month_end_btn.count() > 0:
                    month_end_btn.click()
                    time.sleep(3)
                    logger.info("  ✓ Section opened")
            except:
                logger.info("  → Section toggle not found, continuing...")

            # Build the expected link text
            last_day = calendar.monthrange(target_year, target_month)[1]
            target_link_name = f"Portfolio Details as on {month_name} {last_day}, {target_year}"
            
            logger.info(f"Searching for: '{target_link_name}'...")
            
            # Try exact match first
            download_link = page.get_by_role("link", name=target_link_name, exact=False)
            
            if download_link.count() == 0:
                # Fallback: Filter by text components
                download_link = page.locator("a").filter(has_text="Portfolio Details").filter(has_text=month_name).filter(has_text=str(target_year))
                
                # Try short month name
                if download_link.count() == 0:
                    short_month = month_name[:3]
                    download_link = page.locator("a").filter(has_text="Portfolio Details").filter(has_text=short_month).filter(has_text=str(target_year))

            if download_link.count() == 0:
                logger.warning(f"  ✗ Link not found for {month_name} {target_year}")
                return 0

            actual_text = download_link.first.text_content().strip()
            logger.info(f"  ✓ Found: '{actual_text}'")
            
            # Download the ZIP file
            logger.info("Downloading ZIP file...")
            with page.expect_download(timeout=120000) as download_info:
                download_link.first.scroll_into_view_if_needed()
                download_link.first.click()
            
            download = download_info.value
            zip_name = download.suggested_filename or "portfolio.zip"
            zip_path = download_folder / zip_name
            download.save_as(zip_path)
            logger.info(f"  ✓ Downloaded: {zip_name}")

            # Extract ZIP contents
            logger.info("Extracting ZIP contents...")
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(download_folder)
                    file_count = len(zip_ref.namelist())
                
                logger.info(f"  ✓ Extracted {file_count} files")
                
                # Remove the ZIP file
                zip_path.unlink()
                logger.info("  ✓ Cleaned up ZIP file")
                
                return file_count
                
            except Exception as e:
                logger.error(f"  ✗ Extraction failed: {e}")
                return 0

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = DSPDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"✅ Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success(f"✅ Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"ℹ️  Info: Month not yet published")
    else:
        logger.error(f"❌ Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
