"""
HSBC Mutual Fund Downloader (Playwright Refactor).

Refactored to handle slow loading and unreliable website behavior using:
1. Playwright with Stealth
2. "New Tab" retry logic (4-5 attempts)
3. Non-headless mode for local visibility
"""

import os
import time
import json
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from playwright_stealth import Stealth

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

# Import configuration
try:
    from src.config.downloader_config import MAX_RETRIES, RETRY_BACKOFF, HEADLESS
except ImportError:
    MAX_RETRIES = 5
    RETRY_BACKOFF = [5, 10, 15, 20, 30]
    HEADLESS = True # Default, but we will force False for HSBC locally if needed

class HSBCDownloader(BaseDownloader):
    """
    HSBC Mutual Fund Downloader.
    Refactored for robustness using Playwright tab-retries.
    """
    
    AMC_NAME = "hsbc"
    LIBRARY_URL = "https://www.assetmanagement.hsbc.co.in/en/mutual-funds/investor-resources/information-library"
    BASE_URL = "https://www.assetmanagement.hsbc.co.in"
    
    EXPECTED_FILE_COUNT_MIN = 20
    EXPECTED_FILE_COUNT_MAX = 100
    
    def __init__(self):
        super().__init__(self.AMC_NAME)
        self.notifier = get_notifier()

    def _parse_link(self, url: str) -> Optional[Tuple[str, datetime]]:
        """Extract fund slug and date from folder path."""
        folder_pattern = re.compile(r'/documents?-(\d{8})/', re.IGNORECASE)
        folder_match = folder_pattern.search(url)
        
        if not folder_match:
            return None
        
        try:
            date_str = folder_match.group(1)
            date_obj = datetime.strptime(date_str, "%d%m%Y")
            
            # Extract fund slug from filename
            filename = url.split('/')[-1].replace('.xlsx', '').replace('.XLSX', '')
            fund_slug = filename.lower()
            fund_slug = re.sub(r'-\d{2}-[a-z]+-\d{4}$', '', fund_slug)
            fund_slug = re.sub(r'-[a-z]+-\d{4}$', '', fund_slug)
            fund_slug = re.sub(r'-\d{4}$', '', fund_slug)
            
            if not fund_slug.startswith('hsbc-'):
                fund_slug = f"hsbc-{fund_slug}"
                
            return fund_slug, date_obj
        except Exception:
            return None

    def _get_links_with_playwright(self, year: int, month: int) -> List[str]:
        """
        Open browser and try to load the page.
        Uses "New Tab" retry logic: tries 5 times, closing the tab if it doesn't load in 60s.
        """
        all_links = []
        
        with sync_playwright() as pw:
            # Force non-headless as requested by user
            browser = pw.chromium.launch(
                headless=False, 
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
            )
            context = browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            
            success = False
            for attempt in range(1, 6): # Try 5 times
                logger.info(f"HSBC: Attempting to open page in new tab (Attempt {attempt}/5)...")
                page = context.new_page()
                Stealth().apply_stealth_sync(page)
                
                try:
                    # Navigation with 60s timeout
                    page.goto(self.LIBRARY_URL, wait_until="networkidle", timeout=60000)
                    
                    # Wait for a key element to confirm load
                    # ".library-results" or any descriptive class from HSBC page
                    page.wait_for_selector("a[href*='.xlsx']", timeout=30000)
                    
                    logger.success(f"HSBC: Page loaded successfully on attempt {attempt}")
                    
                    # Extract all xlsx links
                    anchors = page.locator("a[href*='.xlsx']").all()
                    for anchor in anchors:
                        href = anchor.get_attribute("href")
                        if href and "/mutual-funds/portfolios/" in href.lower():
                            full_url = urljoin(self.BASE_URL, href)
                            all_links.append(full_url)
                    
                    success = True
                    page.close()
                    break
                    
                except PWTimeoutError:
                    logger.warning(f"HSBC: Tab load timed out (60s) on attempt {attempt}. Closing tab.")
                    page.close()
                    continue
                except Exception as e:
                    logger.error(f"HSBC: Tab error on attempt {attempt}: {str(e)}")
                    page.close()
                    continue
            
            browser.close()
            
            if not success:
                raise Exception("HSBC: Failed to load website after 5 attempts with new tabs.")
                
        return sorted(list(set(all_links)))

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        logger.info("=" * 70)
        logger.info(f"HSBC PLAYWRIGHT DOWNLOADER | Period: {year}-{month:02d}")
        logger.info("=" * 70)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if (target_dir / "_SUCCESS.json").exists():
            logger.info(f"HSBC: {year}-{month:02d} already complete.")
            self.consolidate_downloads(year, month)
            return {"amc": self.AMC_NAME, "year": year, "month": month, "status": "skipped", "files_downloaded": 1}

        self.ensure_directory(str(target_dir))
        
        try:
            # 1. Fetch links using the robust Playwright tab-retry logic
            all_links = self._get_links_with_playwright(year, month)
            
            # 2. Filter links for selected month
            # HSBC publishes month-end files, sometimes in the first week of next month.
            next_month = month + 1 if month < 12 else 1
            next_year = year if month < 12 else year + 1
            
            to_download = {}
            for url in all_links:
                parsed = self._parse_link(url)
                if not parsed: continue
                
                fund_slug, date_obj = parsed
                if (date_obj.year == year and date_obj.month == month) or \
                   (date_obj.year == next_year and date_obj.month == next_month and date_obj.day <= 10):
                    filename = url.split('/')[-1]
                    to_download[filename] = url
            
            if not to_download:
                logger.info(f"HSBC: No files found for {year}-{month:02d}")
                shutil.rmtree(target_dir, ignore_errors=True)
                self.notifier.notify_not_published("HSBC", year, month)
                return {"amc": self.AMC_NAME, "year": year, "month": month, "status": "not_published"}

            # 3. Simple Download (using requests for the actual file fetch is fine once we have URLs)
            import requests
            files_count = 0
            for filename, url in to_download.items():
                dest = target_dir / filename
                logger.info(f"Downloading {filename}...")
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    f.write(resp.content)
                files_count += 1
                
            # Finalize
            marker_data = {
                "amc": self.AMC_NAME, "year": year, "month": month,
                "files_downloaded": files_count, "timestamp": datetime.now().isoformat()
            }
            with open(target_dir / "_SUCCESS.json", "w") as f:
                json.dump(marker_data, f, indent=2)
            
            self.consolidate_downloads(year, month)
            duration = time.time() - start_time
            self.notifier.notify_success("HSBC", year, month, files_downloaded=files_count, duration=duration)
            
            result = {
                "amc": self.AMC_NAME, "year": year, "month": month,
                "status": "success", "files_downloaded": files_count, "duration": duration
            }
            return result

        except Exception as e:
            logger.error(f"HSBC: Pipeline failed: {str(e)}")
            if target_dir.exists() and not (target_dir / "_SUCCESS.json").exists():
                shutil.rmtree(target_dir, ignore_errors=True)
            return {"amc": self.AMC_NAME, "year": year, "month": month, "status": "failed", "reason": str(e)}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    
    downloader = HSBCDownloader()
    res = downloader.download(args.year, args.month)
    
    # Print JSON result for Orchestrator
    print(json.dumps(res))
