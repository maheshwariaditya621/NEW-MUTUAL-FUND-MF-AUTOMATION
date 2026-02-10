# src/downloaders/uti_downloader.py

import os
import time
import json
import shutil
import zipfile
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


class UTIDownloader(BaseDownloader):
    """
    UTI Mutual Fund - Portfolio Downloader
    
    URL: https://www.utimf.com/downloads/consolidate-all-portfolio-disclosure
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("UTI Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "uti"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "UTI",
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
        
        logger.warning(f"UTI: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("UTI", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("UTI MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"UTI: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"UTI: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"UTI: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("UTI", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                duration = time.time() - start_time
                self.notifier.notify_success("UTI", year, month, files_downloaded=1, duration=duration)
                logger.success(f"✅ UTI download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("UTI", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        url = "https://www.utimf.com/downloads/consolidate-all-portfolio-disclosure"

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
            page.goto(url, timeout=60000)
            time.sleep(3)
            logger.info("  ✓ Page loaded")

            # 1) Select Year
            logger.info(f"Selecting year: {target_year}...")
            page.get_by_placeholder("Select Year").click()
            time.sleep(1)
            page.get_by_text(str(target_year), exact=True).click()
            time.sleep(2)
            logger.info(f"  ✓ Year {target_year} selected")

            # 2) Select Month (with virtual scroll support)
            logger.info(f"Selecting month: {month_name}...")
            page.get_by_placeholder("select").nth(1).click()
            time.sleep(1)
            
            # Try to click the month without scrolling first
            try:
                month_element = page.get_by_text(month_name, exact=True)
                month_element.click(timeout=2000)
                logger.info(f"  ✓ Month {month_name} selected (no scroll needed)")
            except:
                # Month not visible, need to scroll within dropdown
                logger.info(f"  → Scrolling to find {month_name}...")
                try:
                    # Locate the virtual scroll viewport and scroll to bottom
                    viewport = page.locator("cdk-virtual-scroll-viewport")
                    viewport.evaluate("el => el.scrollTop = el.scrollHeight")
                    time.sleep(0.5)
                    
                    # Now click the month
                    page.get_by_text(month_name, exact=True).click()
                    logger.info(f"  ✓ Month {month_name} selected (after scroll)")
                except Exception as e:
                    raise Exception(f"Could not select month: {str(e)[:100]}")
            
            time.sleep(2)

            # 3) Click Get Portfolio
            logger.info("Clicking 'Get Portfolio' button...")
            page.get_by_role("button", name="Get Portfolio").click()
            time.sleep(5)
            logger.info("  ✓ Button clicked")

            # 4) Download ZIP file
            logger.info("Downloading ZIP file...")
            with page.expect_download(timeout=60000) as download_info:
                try:
                    with page.expect_popup(timeout=5000) as page1_info:
                        page.get_by_text("Consolidated Portfolio").click()
                    page1 = page1_info.value
                    page1.close()
                except PlaywrightTimeout:
                    pass
            
            download = download_info.value
            
            # Save ZIP to temp location
            temp_zip = download_folder / f"temp_{month_name}_{target_year}.zip"
            download.save_as(temp_zip)
            logger.info(f"  ✓ ZIP downloaded")

            # 5) Extract SEBI Exposure file from ZIP
            logger.info("Extracting SEBI Exposure file...")
            final_path = self._extract_sebi_file(temp_zip, download_folder, month_name, target_year)
            
            if not final_path:
                raise Exception("SEBI Exposure file not found in ZIP")
            
            return final_path

        finally:
            if browser: browser.close()
            if pw: pw.stop()

    def _extract_sebi_file(self, zip_path: Path, target_folder: Path, month_name: str, year: int) -> Optional[Path]:
        """Extract SEBI Exposure file from ZIP."""
        temp_extract = target_folder / "temp_extract"
        temp_extract.mkdir(exist_ok=True)
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_extract)
            
            # Look for SEBI Exposure file
            found_file = None
            for root, dirs, files in os.walk(temp_extract):
                for file in files:
                    normalized_name = file.lower().replace(" ", "")
                    
                    if (normalized_name.startswith("sebiexposure") or 
                        normalized_name.startswith("sebi_exposure")) and \
                       (file.endswith(".xlsx") or file.endswith(".xls")):
                        found_file = os.path.join(root, file)
                        break
                if found_file:
                    break
            
            if found_file:
                # Use original filename from ZIP
                original_name = Path(found_file).name
                final_path = target_folder / original_name
                
                # Check for collision
                if final_path.exists():
                    final_path = target_folder / f"{month_name}_{year}_{original_name}"
                    
                shutil.move(found_file, final_path)
                logger.info(f"  ✓ Extracted: {final_path.name}")
                return final_path
            else:
                logger.warning("  ✗ SEBI Exposure file not found in ZIP")
                return None
                
        finally:
            # Cleanup
            if zip_path.exists(): zip_path.unlink()
            if temp_extract.exists(): shutil.rmtree(temp_extract, ignore_errors=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = UTIDownloader()
    downloader.download(args.year, args.month)
