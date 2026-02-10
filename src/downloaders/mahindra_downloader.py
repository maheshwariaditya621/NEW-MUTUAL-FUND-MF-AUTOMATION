# src/downloaders/mahindra_downloader.py

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


class MahindraDownloader(BaseDownloader):
    """
    Mahindra Manulife Mutual Fund - Portfolio Downloader
    
    URL: https://www.mahindramanulife.com/downloads#MANDATORY-DISCLOSURES-+-MONTHLY-PORTFOLIO-DISCLOSURE
    Uses year-based accordions and month-specific download links.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Mahindra Manulife Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "mahindra"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "MAHINDRA",
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
        
        logger.warning(f"MAHINDRA: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("MAHINDRA", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("MAHINDRA MANULIFE MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"MAHINDRA: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"MAHINDRA: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                file_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not file_path:
                    logger.warning(f"MAHINDRA: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("MAHINDRA", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                duration = time.time() - start_time
                self.notifier.notify_success("MAHINDRA", year, month, files_downloaded=1, duration=duration)
                logger.success(f"✅ MAHINDRA download completed: {file_path.name}")
                return {"status": "success", "file": str(file_path), "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("MAHINDRA", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        url = "https://www.mahindramanulife.com/downloads#MANDATORY-DISCLOSURES-+-MONTHLY-PORTFOLIO-DISCLOSURE"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to {url}...")
            # Increase timeout to 120s as the site is very slow
            page.goto(url, wait_until="load", timeout=120000)
            time.sleep(10) # Give extra time for components to initialize
            logger.info("  ✓ Page loaded")

            # Handle Declaration Modal
            logger.info("Handling declaration modal...")
            # Try multiple ways to find the US Person link
            declaration_link = page.get_by_role("link", name=re.compile("I AM NOT A US PERSON", re.I)).first
            if not declaration_link.is_visible():
                declaration_link = page.locator("a:has-text('I AM NOT A US PERSON')").first

            if declaration_link.is_visible():
                declaration_link.click()
                logger.info("  ✓ Clicked declaration link")
                time.sleep(5)
            else:
                logger.info("  ✓ Declaration modal not visible, proceeding...")

            # Select Year
            logger.info(f"Selecting Year: {target_year}...")
            year_locators = page.get_by_text(str(target_year), exact=True)
            count = year_locators.count()
            
            if count == 0:
                logger.warning(f"  ✗ Year {target_year} not found on page")
                return None
                
            success_year = False
            # Usually the later ones in the list are the ones in Mandatory Disclosures
            # We'll try from last to first since newer years are usually towards the top but might be duplicated in other sections
            for i in range(count - 1, -1, -1):
                loc = year_locators.nth(i)
                if loc.is_visible():
                    try:
                        loc.scroll_into_view_if_needed()
                        loc.click()
                        logger.info(f"  ✓ Selected year at index {i}")
                        success_year = True
                        break
                    except:
                        continue
            
            if not success_year:
                logger.error(f"  ✗ Failed to click year {target_year}")
                return None
            
            time.sleep(3) # Wait for accordion to expand

            # Find Download Link
            logger.info(f"Searching for {month_name} link...")
            
            # Format: "Monthly Portfolio Disclosure - December,"
            regex_pattern = re.compile(rf"Monthly Portfolio Disclosure.*{month_name}", re.IGNORECASE)
            download_links = page.get_by_role("link").filter(has_text=regex_pattern)
            
            if download_links.count() == 0:
                # Fallback: maybe just the month name?
                download_links = page.get_by_role("link").filter(has_text=month_name)
                
            if download_links.count() == 0:
                # Last fallback: generic link
                generic_link = page.get_by_role("link", name="Monthly Portfolio Disclosure", exact=True)
                if generic_link.is_visible():
                     download_links = generic_link
                else:
                    logger.warning(f"  ✗ No download link found for {month_name} {target_year}")
                    return None

            target_link = download_links.first
            link_text = target_link.inner_text().strip()
            logger.info(f"  ✓ Found link: {link_text}")
            target_link.scroll_into_view_if_needed()

            # Download
            logger.info("Downloading file...")
            with page.expect_download(timeout=60000) as download_info:
                target_link.click()
            
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

    downloader = MahindraDownloader()
    downloader.download(args.year, args.month)
