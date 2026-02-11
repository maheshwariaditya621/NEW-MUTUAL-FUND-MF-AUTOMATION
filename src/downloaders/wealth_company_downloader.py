# src/downloaders/wealth_company_downloader.py

import os
import time
import json
import shutil
import re
import calendar
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


class WealthCompanyDownloader(BaseDownloader):
    """
    Wealth Company Mutual Fund - Portfolio Downloader
    
    URL: https://www.wealthcompanyamc.in/literature-forms/?tab=portfolio-documents
    Features:
    - Persistent Session.
    - Date Barrier: No data before Oct 2025.
    - Regex pattern matching for links.
    - Gold Standard compliance.
    """
    
    MONTH_ABBR = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }
    
    MONTH_FULL = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Wealth Company Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "wealth_company"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Wealth Company",
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
        
        logger.warning(f"{self.AMC_NAME}: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("Wealth Company", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_abbr = self.MONTH_ABBR[month]
        month_full = self.MONTH_FULL[month]
        
        logger.info("=" * 60)
        logger.info(f"WEALTH COMPANY DOWNLOADER: {year}-{month:02d} ({month_abbr})")
        logger.info("=" * 60)
        
        # Date Barrier: Wealth Company started in Oct 2025
        if year < 2025 or (year == 2025 and month < 10):
            logger.info(f"{self.AMC_NAME}: {year}-{month:02d} is before inception (Oct 2025). Skipping.")
            return {"status": "skipped", "reason": "before_inception"}

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Wealth Company: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"{self.AMC_NAME}: [DRY RUN] Would download {month_abbr} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_abbr, month_full, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"{self.AMC_NAME}: No portfolios found for {month_abbr} {year}")
                    self.notifier.notify_not_published("Wealth Company", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("Wealth Company", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if self._page:
                     try:
                        self._page.screenshot(path=f"wealth_company_debug_{year}_{month}_attempt_{attempt}.png")
                     except: pass
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Wealth Company", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_abbr: str, month_full: str, download_folder: Path) -> int:
        url = "https://www.wealthcompanyamc.in/literature-forms/?tab=portfolio-documents"
        
        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-infobars"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info("Navigating to Wealth Company AMC website...")
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
            logger.info("Waiting 5s for load...")
            time.sleep(5)
            
            # 1. Open 'Monthly' portfolio section
            logger.info("Opening 'Monthly' portfolio section...")
            monthly_button = page.get_by_role("button", name="file opener Monthly")
            
            if monthly_button.count() > 0:
                monthly_button.click()
                time.sleep(3)
                logger.info("Monthly section opened")
            else:
                logger.error("Monthly section button not found")
                return 0
            
            # 2. Search for schemes
            # Pattern matching logic from user script
            last_day = calendar.monthrange(target_year, target_month)[1]
            search_patterns = [
                f"Monthly – The Wealth Company.*{month_full} {last_day},{target_year}",  # October format (comma no space)
                f"Monthly - The Wealth Company.*{month_full} {last_day}, {target_year}", # November format (dash, comma space)
                f"Monthly.*The Wealth Company.*{month_full}.*{target_year}"              # Generic fallback
            ]
            
            matching_links = []
            logger.info(f"Scanning for links matching {month_full} {target_year}...")
            
            for pattern in search_patterns:
                regex = re.compile(pattern, re.IGNORECASE)
                links = page.get_by_role("link").filter(has_text=regex)
                count = links.count()
                if count > 0:
                    logger.info(f"  Found {count} matches with pattern: {pattern[:40]}...")
                    # We need to collect indices or elements because re-querying in loop is safer for Playwright
                    # But we also need to know which ones matched.
                    # Standard practice: iterate carefully.
                    for i in range(count):
                        # Store index and pattern to re-locate
                        matching_links.append({"index": i, "pattern": pattern})
                    break
            
            if not matching_links:
                logger.warning(f"No schemes found for {month_full} {target_year}")
                return 0
            
            files_downloaded = 0
            
            # 3. Download
            for idx, item in enumerate(matching_links):
                pattern = item["pattern"]
                link_index = item["index"]
                
                try:
                    # Re-locate to avoid stale element handle
                    regex = re.compile(pattern, re.IGNORECASE)
                    current_links = page.get_by_role("link").filter(has_text=regex)
                    
                    if link_index >= current_links.count():
                        logger.warning("Link index out of bounds (stale?)")
                        continue
                    
                    link_element = current_links.nth(link_index)
                    link_text = link_element.text_content() or "Unknown"
                    logger.info(f"  [{idx+1}/{len(matching_links)}] Downloading: {link_text[:50]}...")
                    
                    with page.expect_download(timeout=60000) as download_info:
                        # Handle potential popup
                        try:
                            with page.expect_popup(timeout=10000) as popup_info:
                                link_element.click()
                            p = popup_info.value
                            p.close()
                        except:
                            link_element.click()
                    
                    dl = download_info.value
                    fname = dl.suggested_filename
                    
                    # Handle generic filenames (though likely specific given the source)
                    if fname.lower() in ["portfolio.pdf", "monthly_portfolio.pdf", "download.pdf", "portfolio.xlsx", "report.xlsx"]:
                        fname = f"WEALTH_COMPANY_{month_abbr}_{target_year}_{idx+1:02d}_{fname}"
                    
                    dl.save_as(download_folder / fname)
                    logger.info(f"    ✓ Saved: {fname}")
                    files_downloaded += 1
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"    ✗ Error downloading link {idx}: {e}")
            
            return files_downloaded

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = WealthCompanyDownloader()
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
