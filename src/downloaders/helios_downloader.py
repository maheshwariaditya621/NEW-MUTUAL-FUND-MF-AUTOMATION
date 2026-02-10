# src/downloaders/helios_downloader.py

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


class HeliosDownloader(BaseDownloader):
    """
    Helios Mutual Fund - Portfolio Downloader
    
    URL: https://www.heliosmf.in/portfolio-disclosure/
    Uses nested accordion navigation: Monthly Portfolio → Scheme → Year → Month
    Dynamically discovers all available schemes
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Helios Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "helios"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "HELIOS",
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
        
        logger.warning(f"HELIOS: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("HELIOS", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("HELIOS MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"HELIOS: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"HELIOS: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"HELIOS: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("HELIOS", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                duration = time.time() - start_time
                self.notifier.notify_success("HELIOS", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ HELIOS download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("HELIOS", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> int:
        url = "https://www.heliosmf.in/portfolio-disclosure/"

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
            time.sleep(2)
            logger.info("  ✓ Page loaded")

            # Open 'Monthly Portfolio' main accordion
            logger.info("Opening 'Monthly Portfolio' section...")
            page.get_by_role("heading", name="Monthly Portfolio").click()
            time.sleep(2)
            
            # Expand the main accordion by clicking the caret twice
            page.locator(".fas.fa-caret-right").first.click()
            time.sleep(1)
            page.locator(".fas.fa-caret-right").first.click()
            time.sleep(2)
            logger.info("  ✓ Section opened and expanded")

            # Dynamically discover all scheme headings
            logger.info("Discovering available schemes...")
            scheme_headings = page.locator("div[id^='heading-']").filter(has_text=re.compile(r"Helios.*Fund", re.I))
            scheme_count = scheme_headings.count()
            
            schemes_info = []
            seen_schemes = set()
            
            for i in range(scheme_count):
                try:
                    heading = scheme_headings.nth(i)
                    heading_id = heading.get_attribute("id")
                    scheme_text = heading.inner_text().strip()
                    
                    if "Fund" in scheme_text and heading_id and scheme_text not in seen_schemes:
                        schemes_info.append({
                            "name": scheme_text,
                            "heading_id": heading_id
                        })
                        seen_schemes.add(scheme_text)
                except:
                    continue
            
            logger.info(f"  ✓ Found {len(schemes_info)} unique schemes")
            
            total_downloaded = 0
            
            for idx, scheme in enumerate(schemes_info, 1):
                scheme_name = scheme["name"]
                heading_id = scheme["heading_id"]
                
                logger.info(f"  [{idx}/{len(schemes_info)}] {scheme_name}")
                
                try:
                    # Click on the scheme heading
                    scheme_locator = page.locator(f"#{heading_id}")
                    scheme_locator.scroll_into_view_if_needed()
                    time.sleep(0.5)
                    scheme_locator.click()
                    time.sleep(2)
                    
                    # Find and click the year
                    collapse_id = heading_id.replace("heading-", "collapse-")
                    year_locator = page.locator(f"#{collapse_id}").locator(f"div[id^='heading-']").filter(has_text=str(target_year)).first
                    
                    if year_locator.count() > 0:
                        year_locator.click()
                        time.sleep(2)
                        
                        # Get the year's heading ID to find its collapse section
                        year_heading_id = year_locator.get_attribute("id")
                        year_collapse_id = year_heading_id.replace("heading-", "collapse-")
                        
                        # Find the month link
                        month_link = page.locator(f"#{year_collapse_id}").get_by_role("link", name=month_name)
                        
                        if month_link.count() > 0:
                            try:
                                with page.expect_download(timeout=60000) as download_info:
                                    try:
                                        with page.expect_popup(timeout=5000) as page1_info:
                                            month_link.click()
                                        page1 = page1_info.value
                                        page1.close()
                                    except PlaywrightTimeout:
                                        pass
                                
                                download = download_info.value
                                save_filename = download.suggested_filename
                                save_path = download_folder / save_filename
                                
                                # Handle duplicate suggested filenames across schemes
                                if save_path.exists():
                                    stem = os.path.splitext(save_filename)[0]
                                    ext = os.path.splitext(save_filename)[1]
                                    # Append scheme name for uniqueness if same file name is used across schemes
                                    clean_scheme = scheme_name.replace("Helios ", "").replace(" ", "_").replace("/", "_")
                                    save_filename = f"{stem}_{clean_scheme}{ext}"
                                    save_path = download_folder / save_filename

                                download.save_as(save_path)
                                logger.info(f"    ✓ Downloaded: {save_filename}")
                                total_downloaded += 1
                                time.sleep(1)
                                
                            except Exception as dl_err:
                                logger.warning(f"    ✗ Download failed: {str(dl_err)[:80]}")
                        
                        # Close the year accordion
                        year_locator.click()
                        time.sleep(1)
                    
                    # Close the scheme accordion
                    scheme_locator.click()
                    time.sleep(1)
                    
                except Exception as scheme_err:
                    logger.warning(f"    ✗ Error: {str(scheme_err)[:100]}")
                    continue

            return total_downloaded

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = HeliosDownloader()
    downloader.download(args.year, args.month)
