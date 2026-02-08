# src/downloaders/whiteoak_downloader.py

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


class WhiteOakDownloader(BaseDownloader):
    """
    WhiteOak Mutual Fund - Portfolio Downloader
    
    URL: https://mf.whiteoakamc.com/regulatory-disclosures/scheme-portfolios
    Uses pagination and a "Monthly" tab.
    Downloads multiple files per month (one per scheme).
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("WhiteOak Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "whiteoak"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "WHITEOAK",
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
        
        logger.warning(f"WHITEOAK: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("WHITEOAK", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def open_session(self):
        """Open a persistent browser session."""
        if self._page:
            return
            
        self._playwright = sync_playwright().start()
        try:
            self._browser = self._playwright.chromium.launch(
                headless=HEADLESS,
                channel="chrome",
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-infobars"]
            )
        except:
            self._browser = self._playwright.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )

        self._context = self._browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            accept_downloads=True,
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://mf.whiteoakamc.com/",
                "Connection": "keep-alive"
            }
        )
        self._page = self._context.new_page()
        Stealth().apply_stealth_sync(self._page)
        logger.info("Persistent Chrome session opened for WhiteOak (with stealth fixes).")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page: self._page.close()
        if self._browser: self._browser.close()
        if self._playwright: self._playwright.stop()
        self._page = self._context = self._browser = self._playwright = None
        logger.info("Persistent Chrome session closed for WhiteOak.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        month_short = month_name[:3]
        
        logger.info("=" * 60)
        logger.info("WHITEOAK MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"WHITEOAK: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"WHITEOAK: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, month_short, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"WHITEOAK: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("WHITEOAK", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                duration = time.time() - start_time
                self.notifier.notify_success("WHITEOAK", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ WHITEOAK download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("WHITEOAK", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, month_short: str, download_folder: Path) -> int:
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        url = "https://mf.whiteoakamc.com/regulatory-disclosures/scheme-portfolios"

        try:
            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="load", timeout=90000)
            time.sleep(5)
            logger.info("  ✓ Page loaded")

            # 1. Selection of 'Monthly' filter (optional, let's keep it but handle failure)
            logger.info("Selecting 'Monthly' filter...")
            monthly_filter = page.locator("button, div, span").filter(has_text=re.compile("^Monthly$", re.I)).first
            if monthly_filter.count() > 0:
                monthly_filter.click()
                time.sleep(5)
                logger.info("  ✓ 'Monthly' filter clicked")
            else:
                logger.warning("  ⚠ 'Monthly' filter button not found. Using default 'All Type' view...")

            # 2. Iterate through pages
            logger.info(f"Searching for {month_name} {target_year} portfolio links...")
            
            success_count = 0
            processed_items = set() 
            
            page_num = 1
            max_pages = 50 
            
            # Since Dec 2025 might be far back, we need a robust stopping condition
            # We skip older months until we find our target month
            # We keep going until we've found matches and then see an older month OR year.
            # But the user says 19 files, so we scan until we find them.
            
            while page_num <= max_pages:
                logger.info(f"  --- Scanning Page {page_num} ---")
                
                # Wait for any loader to disappear and content to stabilize
                time.sleep(5)
                
                # Broadly target disclosure rows. 
                # Screenshot shows they look like cards or table rows.
                rows = page.locator("li, div").filter(has_text=re.compile("Portfolio Disclosure", re.I)).all()
                
                if not rows:
                    logger.info(f"  No disclosures found on page {page_num}. Checking for 'Next' button...")
                
                matches_on_page = 0
                older_rows_on_page = 0
                
                for row in rows:
                    txt = row.inner_text().strip().replace('\n', ' ')
                    if "Portfolio Disclosure" not in txt: continue
                    
                    # Pattern matching for Year and Month
                    if str(target_year) in txt and month_name.lower() in txt.lower():
                        if txt in processed_items:
                            continue
                        
                        logger.info(f"  Processing: {txt[:70]}...")
                        
                        try:
                            # Extract scheme name: text before "Monthly Portfolio Disclosure"
                            # Example: "WhiteOak Capital Flexi Cap Fund Monthly Portfolio Disclosure - 31st December 2025"
                            scheme_name = "WhiteOak_Scheme"
                            name_match = re.search(r'(WhiteOak Capital .*?) (Monthly|Fortnightly|Half-yearly) Portfolio', txt, re.IGNORECASE)
                            if name_match:
                                scheme_name = name_match.group(1).strip().replace(' ', '_')
                            
                            # Standard download button (down arrow icon in screenshot)
                            # Looking for ANY button/svg that looks like a download icon in this cell/row
                            download_btn = row.locator("button, a, svg").filter(has=page.locator("path")).last
                            if download_btn.count() == 0:
                                download_btn = row.locator("[class*='download']").first

                            if download_btn.count() > 0:
                                with page.expect_download(timeout=60000) as download_info:
                                    download_btn.click(force=True)
                                
                                download = download_info.value
                                orig_ext = os.path.splitext(download.suggested_filename)[1] or ".xlsx"
                                filename = f"WHITEOAK_{scheme_name}_{month_short}_{target_year}{orig_ext}"
                                save_path = download_folder / filename
                                
                                download.save_as(save_path)
                                logger.info(f"    ✓ Saved: {filename}")
                                success_count += 1
                                matches_on_page += 1
                                processed_items.add(txt)
                            else:
                                logger.warning(f"    ✗ Download button not found for: {txt[:30]}")
                            
                        except Exception as e:
                            logger.error(f"    ✗ Download failed for item: {str(e)[:100]}")
                    
                    # Check if this row is OLDER than our target period
                    # Logic: If Target is Dec 2025 and we see November 2025 or any 2024
                    if any(str(yr) in txt for yr in range(2000, target_year)):
                         older_rows_on_page += 1
                    elif str(target_year) in txt:
                        # Heuristic: if we see other months in the same year that are NOT our target
                        # we count them if we already found some target matches.
                        # This avoids stopping before reaching our month if list is not strictly sorted.
                        if month_name.lower() not in txt.lower() and success_count > 0:
                            # Simple heuristic: if we reached another month in the same year AFTER finding target matches
                            # let's assume we might be done with the target month.
                            # But with 19 files, they might span multiple pages.
                            # Let's count them and decide at page level.
                            older_rows_on_page += 1

                logger.info(f"  Summary Page {page_num}: Found {matches_on_page} matches. Matches so far: {success_count}.")

                # Robust Stopping Condition: Only stop if we've found files and now see mostly older rows
                if older_rows_on_page > 5 and success_count >= 19:
                    logger.info("  Reached end of target period (19+ files found and seeing older data). Stopping.")
                    break
                
                # Fallback: if we found SOME files but reached 2024 without reaching 19
                if any(str(yr) in page.content() for yr in range(2000, target_year)) and success_count > 0:
                    if success_count < 19:
                        logger.warning(f"  Only found {success_count} files before hitting older years. Continuing just in case.")
                    else:
                        logger.info("  Standard completion reached. Stopping.")
                        break

                # Pagination
                next_btn = page.get_by_role("button", name=re.compile("Next", re.I))
                if next_btn.count() == 0:
                    next_btn = page.locator("button").filter(has_text=re.compile("Next", re.I))
                
                if next_btn.count() > 0 and next_btn.is_visible() and next_btn.is_enabled():
                    logger.info("  Navigating to next page...")
                    next_btn.click()
                    page_num += 1
                else:
                    logger.info("  Reached last page or no 'Next' button.")
                    break

            return success_count

            return success_count

        finally:
            if close_needed: self.close_session()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = WhiteOakDownloader()
    downloader.download(args.year, args.month)
