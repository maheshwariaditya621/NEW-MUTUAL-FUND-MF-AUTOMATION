# src/downloaders/old_bridge_downloader.py

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


class OldBridgeDownloader(BaseDownloader):
    """
    Old Bridge Mutual Fund - Portfolio Downloader
    
    URL: https://www.oldbridgemf.com/statutory-disclosures.html#
    Transitions from Single Consolidated File to Multi-Scheme Files in Nov 2025.
    Uses FY system: Jan-Mar belongs to previous year's FY grouping.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    
    MONTH_ABBR = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    def __init__(self):
        super().__init__("Old Bridge Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "old_bridge"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Old Bridge",
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
        self.notifier.notify_error("Old Bridge", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        month_abbr = self.MONTH_ABBR[month]
        
        logger.info("=" * 60)
        logger.info(f"OLD BRIDGE MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Old Bridge: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
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
                    logger.info(f"{self.AMC_NAME}: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, month_abbr, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"{self.AMC_NAME}: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("Old Bridge", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("Old Bridge", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Old Bridge", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, month_abbr: str, download_folder: Path) -> int:
        url = "https://www.oldbridgemf.com/statutory-disclosures.html#"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to Old Bridge Statutory Disclosures page...")
            page.goto(url, wait_until="load", timeout=90000)
            time.sleep(3)

            is_multi_file = False
            if target_year > 2025:
                is_multi_file = True
            elif target_year == 2025 and target_month >= 11:
                is_multi_file = True

            # Handle Declaration
            declaration_btn = page.get_by_role("button", name=re.compile("I AM NOT A US PERSON", re.I))
            if declaration_btn.count() > 0:
                logger.info("Bypassing US person declaration...")
                declaration_btn.first.click()
                time.sleep(1)

            # Select 'Monthly Portfolio' tab
            tab = page.get_by_role("tab", name=re.compile("Monthly Portfolio", re.I))
            if tab.count() > 0:
                logger.info("Selecting 'Monthly Portfolio' tab...")
                tab.first.click()
                time.sleep(2)
            else:
                logger.error("'Monthly Portfolio' tab not found.")
                return 0

            # Find matching headings
            success_count = 0
            search_regex = re.compile(rf"\b{month_name}\b.*\b{target_year}\b", re.I)
            panel = page.locator("#v-pills-tabContent2")
            if panel.count() == 0: panel = page
            
            # headings are h6 usually
            headings = panel.locator("h6").all()
            logger.info(f"Found {len(headings)} total headings in portfolio section. Filtering for {month_name} {target_year}...")

            processed_hrefs = set()

            for h in headings:
                try:
                    if not h.is_visible(): continue
                    h_text = h.inner_text().strip().replace('\n', ' ')
                    
                    if not search_regex.search(h_text):
                        continue
                        
                    logger.info(f"  Matched: '{h_text}'")
                    row = h.locator("xpath=./ancestor::div[contains(@class, 'about-text')][1]")
                    if row.count() == 0: row = h.locator("xpath=..")
                    
                    lnk = row.locator("a.download-dotted-button")
                    if lnk.count() > 0:
                        target_lnk = lnk.first
                        href = target_lnk.get_attribute("href")
                        if href in processed_hrefs: continue

                        # Extract scheme name for file renaming
                        # h_text: 'Old Bridge Arbitrage Fund - December 2025'
                        scheme_name = "Old_Bridge_Scheme"
                        if "-" in h_text:
                            parts = h_text.split("-")
                            # First part is usually the fund name
                            raw_scheme = parts[0].strip().replace("Old Bridge ", "")
                            scheme_name = raw_scheme.replace(" ", "_").replace("/", "_")
                        elif is_multi_file:
                            # Fallback if no delimiter
                            scheme_name = h_text.replace(" ", "_").replace("/", "_")
                        else:
                            scheme_name = "CONSOLIDATED"

                        try:
                            # Scroll and download
                            target_lnk.scroll_into_view_if_needed(timeout=5000)
                            with page.expect_download(timeout=60000) as dinfo:
                                target_lnk.click(force=True)
                            
                            dl = dinfo.value
                            fname = dl.suggested_filename
                                
                            dl.save_as(download_folder / fname)
                            logger.info(f"    [OK] Saved: {fname}")
                            success_count += 1
                            processed_hrefs.add(href)
                            time.sleep(1)
                        except Exception as e:
                            logger.error(f"    [FAIL] Download failed: {str(e)[:100]}")
                except:
                    continue

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

    downloader = OldBridgeDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"[INFO]  Info: Month not yet published")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
