# src/downloaders/groww_downloader.py

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


class GrowwDownloader(BaseDownloader):
    """
    Groww Mutual Fund - Portfolio Downloader
    
    URL: https://growwmf.in/statutory-disclosure/portfolio
    Uses Financial Year (FY) system: Jan-Mar uses previous year, Apr-Dec uses current year
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    
    MONTH_SHORT = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    def __init__(self):
        super().__init__("Groww Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "groww"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "GROWW",
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
        
        logger.warning(f"GROWW: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("GROWW", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _calculate_fy(self, year: int, month: int) -> str:
        """Calculate Financial Year based on month. Jan-Mar uses previous year, Apr-Dec uses current year."""
        if month >= 1 and month <= 3:  # Jan, Feb, Mar
            fy_start_year = year - 1
        else:  # Apr to Dec
            fy_start_year = year
        
        fy_end_year = fy_start_year + 1
        # Groww uses "YYYY- YYYY" format with space after hyphen
        return f"{fy_start_year}- {fy_end_year}"


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        month_short = self.MONTH_SHORT[month]
        
        logger.info("=" * 60)
        logger.info("GROWW MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"GROWW: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"GROWW: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_short, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"GROWW: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("GROWW", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                duration = time.time() - start_time
                self.notifier.notify_success("GROWW", year, month, files_downloaded=1, duration=duration)
                logger.success(f"✅ GROWW download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("GROWW", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_short: str, download_folder: Path) -> Optional[Path]:
        url = "https://growwmf.in/statutory-disclosure/portfolio"

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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="networkidle", timeout=60000)
            time.sleep(5)
            logger.info("  ✓ Page loaded")

            # Calculate and select Financial Year
            fy_text = self._calculate_fy(target_year, target_month)
            logger.info(f"Selecting Financial Year: {fy_text}...")
            
            try:
                inputs = page.locator('input')
                if inputs.count() > 1:
                    inputs.nth(1).click()
                    time.sleep(1)
                    
                    # Robust selection handling both "2025-2026" and "2025- 2026"
                    fy_parts = fy_text.split("-")
                    fy_regex = f"{fy_parts[0].strip()}-\\s*{fy_parts[1].strip()}"
                    
                    year_options = page.locator("div, li, span").filter(has_text=re.compile(fy_regex))
                    if year_options.count() > 0:
                        # Find the one that actually matches the year range precisely
                        found = False
                        for i in range(year_options.count()):
                            opt_text = year_options.nth(i).inner_text().strip()
                            # Clean up spaces to compare: "2025- 2026" -> "2025-2026"
                            clean_opt = re.sub(r"\s+", "", opt_text)
                            clean_fy = re.sub(r"\s+", "", fy_text)
                            if clean_opt == clean_fy:
                                year_options.nth(i).click()
                                logger.info(f"  ✓ Selected FY: {opt_text}")
                                found = True
                                break
                        
                        if not found:
                             logger.warning(f"  ✗ FY {fy_text} matched by regex but no precise text match found")
                             return None
                        
                        time.sleep(2)
                    else:
                        logger.warning(f"  ✗ FY regex '{fy_regex}' not found")
                        return None
            except Exception as e:
                logger.warning(f"  ✗ Error selecting FY: {e}")
                return None

            # Find and download the portfolio link
            link_pattern = f"Monthly Portfolio- {month_short}"
            logger.info(f"Searching for link: '{link_pattern}'...")
            
            download_link = page.get_by_role("link", name=re.compile(rf"Monthly Portfolio- {month_short}", re.I))
            
            if download_link.count() == 0:
                download_link = page.locator("a").filter(has_text=re.compile(rf"Portfolio.*{month_short}", re.I))

            if download_link.count() == 0:
                logger.warning(f"  ✗ Link not found for {month_short} {target_year}")
                return None

            link_text = download_link.first.inner_text().strip()
            logger.info(f"  ✓ Found: '{link_text}'")

            # Download the file
            logger.info("Downloading file...")
            with page.expect_download(timeout=60000) as download_info:
                download_link.first.click()
            
            download = download_info.value
            final_filename = download.suggested_filename
            save_path = download_folder / final_filename
            
            download.save_as(save_path)
            logger.info(f"  ✓ Saved: {final_filename}")
            
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

    downloader = GrowwDownloader()
    downloader.download(args.year, args.month)
