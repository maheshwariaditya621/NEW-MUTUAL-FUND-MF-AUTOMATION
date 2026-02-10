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
        url = "https://mf.whiteoakamc.com/regulatory-disclosures/scheme-portfolios"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            try:
                browser = pw.chromium.launch(
                    headless=HEADLESS,
                    channel="chrome",
                    args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-infobars"]
                )
            except:
                browser = pw.chromium.launch(
                    headless=HEADLESS,
                    args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
                )

            context = browser.new_context(
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
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="load", timeout=90000)
            time.sleep(5)
            logger.info("  ✓ Page loaded")

            # 1. Selection of 'Monthly' filter
            logger.info("Selecting 'Monthly' filter...")
            monthly_filter = page.locator('label[for="monthly"]')
            if monthly_filter.count() > 0:
                monthly_filter.click()
                # Wait for results to refresh (look for the results total updating if possible, or just sleep)
                time.sleep(10) 
                logger.info("  ✓ 'Monthly' filter clicked and waited for refresh")
            else:
                # Fallback to finding label by text
                monthly_filter = page.locator('label').filter(has_text=re.compile("^Monthly$", re.I)).first
                if monthly_filter.count() > 0:
                    monthly_filter.click()
                    time.sleep(10)
                    logger.info("  ✓ 'Monthly' filter clicked (fallback)")
                else:
                    logger.warning("  ⚠ 'Monthly' filter button (label[for='monthly']) not found. Using default 'All Type' view...")

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
                
                # Target the list items (li) which are the actual rows
                rows = page.locator("li").all()
                
                if not rows:
                    logger.info(f"  No disclosures (li) found on page {page_num}.")
                    # Fallback to broader search if structure changed
                    rows = page.locator("div.DisclosuresPage_flex, div.row").all()
                
                matches_on_page = 0
                older_rows_on_page = 0
                
                months_list = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
                target_month_idx = months_list.index(month_name)

                for row in rows:
                    # Look for the name div within the li using partial class match for resilience
                    name_el = row.locator("[class*='DisclosuresPage_name']").first
                    if name_el.count() == 0:
                        continue
                        
                    txt = name_el.inner_text().strip().replace('\n', ' ')
                    if "Portfolio Disclosure" not in txt: continue
                    
                    # Pattern matching for Year and Month - handle "November2025" (no space)
                    month_pattern = rf"{month_name}"
                    year_pattern = rf"{target_year}"
                    
                    if re.search(month_pattern, txt, re.I) and re.search(year_pattern, txt):
                        if txt in processed_items:
                            continue
                        
                        logger.info(f"  [FOUND] {txt[:100]}...")
                        
                        try:
                            # The download button is in a div with class containing 'download'
                            download_btn = row.locator("[class*='DisclosuresPage_download']").first
                            
                            if download_btn.count() == 0:
                                # Fallback to the last action button if specific download class not found
                                download_btn = row.locator("[class*='DisclosuresPage_actionBtn']").last

                            if download_btn.count() > 0:
                                logger.info(f"    Triggering download...")
                                try:
                                    with page.expect_download(timeout=120000) as download_info:
                                        # Use standard click first, force=True to bypass overlapping elements
                                        download_btn.click(force=True, timeout=15000)
                                    
                                    download = download_info.value
                                    original_filename = download.suggested_filename
                                    save_path = download_folder / original_filename
                                    
                                    if save_path.exists():
                                        logger.warning(f"    File already exists: {original_filename}")
                                        success_count += 1
                                        processed_items.add(txt)
                                        continue

                                    download.save_as(str(save_path))
                                    logger.info(f"    ✓ Saved: {original_filename}")
                                    success_count += 1
                                    matches_on_page += 1
                                    processed_items.add(txt)
                                except Exception as inner_e:
                                    logger.error(f"    ✗ Download capture failed: {str(inner_e)[:100]}")
                            else:
                                logger.warning(f"    ✗ Download button not found in row container.")
                            
                        except Exception as e:
                            logger.error(f"    ✗ Row processing error: {str(e)[:100]}")
                    
                    # Check if this row is strictly OLDER than our target period to help decide when to stop
                    is_older = False
                    if any(re.search(rf"\b{yr}\b", txt) for yr in range(2000, target_year)):
                        is_older = True
                    elif re.search(rf"{target_year}", txt):
                        # Same year, check if month is older
                        for idx, m in enumerate(months_list):
                            if idx < target_month_idx and re.search(rf"{m}", txt, re.I):
                                is_older = True
                                break
                    
                    if is_older:
                        older_rows_on_page += 1

                logger.info(f"  Summary Page {page_num}: Found {matches_on_page} matches. Total so far: {success_count}.")

                # --- Pagination ---
                # Fixed pagination logic based on aria-label
                next_btn = page.locator('a[aria-label="Next page"]').first
                
                # Check if visible and not disabled
                is_disabled = next_btn.count() > 0 and ("disabled" in (next_btn.get_attribute("class") or "").lower() or next_btn.get_attribute("aria-disabled") == "true")
                
                if next_btn.count() > 0 and next_btn.is_visible() and not is_disabled:
                    # Stopping condition: Only stop if we've found files and now see significant older rows
                    # Or if we've found any older rows at all (aggressive)
                    if older_rows_on_page >= 3:
                        logger.info("  Reached end of target period (older months/years detected). Stopping.")
                        break
                    
                    logger.info("  Navigating to next page...")
                    next_btn.click()
                    page_num += 1
                    time.sleep(3) # Short wait for next page to start loading
                else:
                    logger.info("  No 'Next' button available (or disabled). Stopping.")
                    break

            return success_count

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = WhiteOakDownloader()
    downloader.download(args.year, args.month)
