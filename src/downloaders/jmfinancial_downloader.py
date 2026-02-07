# src/downloaders/jmfinancial_downloader.py

import os
import time
import json
import shutil
import re
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple
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


class JMFinancialDownloader(BaseDownloader):
    """
    JM Financial Mutual Fund - Portfolio Downloader
    
    URL: https://www.jmfinancialmf.com/downloads/Portfolio-Disclosure/Monthly-Portfolio-of-Schemes
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("JM Financial Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "jmfinancial"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "JM_FINANCIAL",
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
        
        logger.warning(f"JM_FINANCIAL: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("JM_FINANCIAL", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def open_session(self):
        """Open a persistent browser session."""
        if self._page:
            return
            
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        self._context = self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            accept_downloads=True
        )
        self._page = self._context.new_page()
        Stealth().apply_stealth_sync(self._page)
        logger.info("Persistent Chrome session opened for JM Financial.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page: self._page.close()
        if self._browser: self._browser.close()
        if self._playwright: self._playwright.stop()
        self._page = self._context = self._browser = self._playwright = None
        logger.info("Persistent Chrome session closed for JM Financial.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("JM FINANCIAL MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"JM_FINANCIAL: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"JM_FINANCIAL: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                file_count = self._run_download_flow(year, month, month_name, target_dir)
                
                if file_count == 0:
                    logger.warning(f"JM_FINANCIAL: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("JM_FINANCIAL", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, file_count)
                duration = time.time() - start_time
                self.notifier.notify_success("JM_FINANCIAL", year, month, files_downloaded=file_count, duration=duration)
                logger.success(f"✅ JM_FINANCIAL download completed. Total files: {file_count}")
                return {"status": "success", "files_downloaded": file_count, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("JM_FINANCIAL", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> int:
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        url = "https://www.jmfinancialmf.com/downloads/Portfolio-Disclosure/Monthly-Portfolio-of-Schemes"

        try:
            logger.info(f"Navigating to {url}...")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
            except:
                logger.warning("  ⚠ Navigation timed out, proceeding anyway...")
            
            time.sleep(5)
            logger.info("  ✓ Page loaded")

            # Scan through pages to find target month
            all_links = []
            page_num = 1
            max_pages = 50  # Reasonable limit
            
            logger.info(f"Scanning for {month_name} {target_year}...")
            
            while page_num <= max_pages:
                # Wait for items to be visible
                try:
                    page.wait_for_selector(".downlode-box", timeout=10000)
                except:
                    logger.warning(f"  No items found on page {page_num}")
                    break
                
                items = page.locator(".downlode-box").all()
                found_for_month = False
                
                for item in items:
                    try:
                        # Title is usually the second <p>
                        title_elem = item.locator("p").nth(1)
                        title = title_elem.inner_text()
                        
                        # Check if title matches target month and year
                        if month_name.lower() in title.lower() and str(target_year) in title:
                            found_for_month = True
                            # Find the download link
                            download_btn = item.locator("a").filter(has_text="Download")
                            if download_btn.count() > 0:
                                data_head = download_btn.get_attribute("data-head")
                                if data_head:
                                    full_url = f"https://www.jmfinancialmf.com/{data_head}"
                                    full_url = full_url.replace("com//", "com/").replace(" ", "%20")
                                    all_links.append((title, full_url))
                    except:
                        continue
                
                if found_for_month:
                    logger.info(f"  Page {page_num}: Found {len(all_links)} files so far")
                elif len(all_links) > 0:
                    # Found links before but not on this page - probably past the month
                    logger.info(f"  Page {page_num}: Month ended, stopping scan")
                    break
                
                # Try to go to next page
                try:
                    next_btn = page.locator(".rc-pagination-next")
                    if next_btn.count() > 0 and next_btn.get_attribute("aria-disabled") != "true":
                        next_btn.click()
                        time.sleep(2)
                        page_num += 1
                    else:
                        logger.info(f"  No more pages")
                        break
                except:
                    break

            if not all_links:
                logger.warning(f"  ✗ No files found for {month_name} {target_year}")
                return 0

            # Deduplicate
            unique_links = []
            seen = set()
            for title, link in all_links:
                if link not in seen:
                    unique_links.append((title, link))
                    seen.add(link)

            logger.info(f"Downloading {len(unique_links)} files...")
            
            # Download files using requests
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            count = 0
            for idx, (title, link) in enumerate(unique_links):
                try:
                    # Clean title for filename
                    clean_title = re.sub(r'[^\w\-_\. ]', '_', title).replace(' ', '_')
                    final_filename = f"JM_{clean_title}.xlsx"
                    save_path = download_folder / final_filename
                    
                    logger.info(f"  [{idx+1}/{len(unique_links)}] {final_filename[:60]}...")
                    
                    response = requests.get(link, headers=headers, stream=True, timeout=30)
                    if response.status_code == 200:
                        with open(save_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        count += 1
                    else:
                        logger.warning(f"    ✗ Failed (HTTP {response.status_code})")
                except Exception as e:
                    logger.error(f"    ✗ Error: {str(e)[:50]}")

            return count

        finally:
            if close_needed: self.close_session()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = JMFinancialDownloader()
    downloader.download(args.year, args.month)
