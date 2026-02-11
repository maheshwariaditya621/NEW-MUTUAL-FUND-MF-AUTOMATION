# src/downloaders/sundaram_downloader.py

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
    HEADLESS = True  # Default to True for production


class SundaramDownloader(BaseDownloader):
    """
    Sundaram Mutual Fund - Portfolio Downloader
    
    URL: https://www.sundarammutual.com/Monthly-Fortnightly-Adhoc-Portfolios
    Features:
    - Persistent Session for efficiency.
    - Logic for Year Expansion (FY based).
    - Exact selectors from user script.
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
        super().__init__("Sundaram Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "sundaram"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Sundaram",
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
        self.notifier.notify_error("Sundaram", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_abbr = self.MONTH_ABBR[month]
        month_full = self.MONTH_FULL[month]
        
        logger.info("=" * 60)
        logger.info(f"SUNDARAM MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_abbr})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Sundaram: {year}-{month:02d} files already downloaded.")
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
                    self.notifier.notify_not_published("Sundaram", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("Sundaram", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if self._page:
                    try:
                        self._page.screenshot(path=f"sundaram_debug_{year}_{month}_attempt_{attempt}.png")
                    except: pass
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Sundaram", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_abbr: str, month_full: str, download_folder: Path) -> int:
        url = "https://www.sundarammutual.com/Monthly-Fortnightly-Adhoc-Portfolios"
        
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

            logger.info("Navigating to Sundaram Portfolio page...")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)

            # 1. Select Category: Monthly
            logger.info("Selecting Category: Monthly...")
            page.locator("#Cbx_Category").select_option("Monthly")
            time.sleep(2)
            
            # 2. Click View
            logger.info("Clicking View button...")
            page.get_by_role("button", name=" View").click()
            time.sleep(3)
            
            # 3. Expand Year
            # Logic: Apr-Dec -> Next Year (Year+1), Jan-Mar -> Current Year (Year)
            if target_month in [1, 2, 3]:
                display_year = str(target_year)
            else:
                display_year = str(target_year + 1)
            
            logger.info(f"Expanding year section: {display_year} (for {month_abbr} {target_year})")
            
            # The button name format is "-{Year} +" based on user script
            year_btn_name = f"-{display_year} +"
            try:
                # Try specific button role first
                page.get_by_role("button", name=year_btn_name).click()
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Role click failed for {year_btn_name}, searching text...")
                # Search by text if precise role fails
                page.get_by_text(year_btn_name).click()
                time.sleep(2)

            # 4. Click Month Tab
            logger.info(f"Clicking Month Tab: {month_full}")
            active_pane_id = None
            try:
                # Filter for the visible month tab
                month_tabs = page.get_by_text(month_full, exact=True).all()
                visible_tab = None
                
                for tab in month_tabs:
                    if tab.is_visible():
                        visible_tab = tab
                        break
                
                if visible_tab:
                    # Get the target pane ID
                    target_attr = visible_tab.get_attribute("data-bs-target")
                    if not target_attr:
                        target_attr = visible_tab.get_attribute("href") # Fallback for some implementations
                    
                    active_pane_id = target_attr
                    logger.info(f"Found visible tab. Target Pane ID: {active_pane_id}")
                    
                    visible_tab.click()
                    time.sleep(2)
                    # Wait for network idle to ensure content load
                    try:
                        page.wait_for_load_state("networkidle", timeout=5000)
                    except: pass
                else:
                    logger.error(f"No visible tab found for '{month_full}'")
                    return 0
            except Exception as e:
                logger.error(f"Failed to click month tab '{month_full}': {e}")
                return 0

            # 5. Download All Files in Active Tab Scope
            logger.info(f"Scanning for files in {month_full} tab (Pane: {active_pane_id})...")
            
            if not active_pane_id:
                logger.error("Could not determine active pane ID")
                return 0
                
            # Scope to the active pane
            try:
                pane = page.locator(active_pane_id)
                if not pane.is_visible():
                    logger.info(f"Pane {active_pane_id} not visible, waiting...")
                    pane.wait_for(state="visible", timeout=10000)
                
                logger.info("Pane is visible. Finding links...")
                
                # Get all links in the pane
                candidates = pane.get_by_role("link").all()
                logger.info(f"Found {len(candidates)} potential links in pane.")
                
                files_downloaded = 0
                
                for i, link in enumerate(candidates):
                    try:
                        if not link.is_visible(): continue
                        text = link.text_content() or ""
                        href = link.get_attribute("href") or ""
                        
                        # Heuristic for valid download links
                        is_download = (
                            "Monthly Portfolio" in text or 
                            "Disclosures" in text or
                            href.endswith(".xlsx") or 
                            href.endswith(".xls") or 
                            href.endswith(".pdf")
                        )
                        
                        if is_download:
                            logger.info(f"  [{i}] Found candidate: {text.strip()[:60]}... (Href: {href[:40]}...)")
                            
                            try:
                                with page.expect_download(timeout=30000) as download_info:
                                    # Use force=True to bypass overlays
                                    link.click(force=True)
                                
                                dl = download_info.value
                                fname = dl.suggested_filename
                                
                                # Prefix with scheme identifier if filename is generic (e.g. "Monthly Portfolio.pdf")
                                if fname.lower() in ["monthly portfolio.pdf", "monthly_portfolio.pdf", "portfolio.pdf", "disclosures.pdf", "download.pdf", "portfolio.xlsx"]:
                                    fname = f"SUNDARAM_{safe_text}_{month_abbr}_{target_year}_{fname}"
                                
                                save_path = download_folder / fname
                                dl.save_as(save_path)
                                logger.info(f"    ✓ Saved: {fname}")
                                files_downloaded += 1
                                time.sleep(1)
                                
                            except Exception as e:
                                logger.error(f"    ✗ Download failed for '{text}': {e}")
                                
                    except Exception as e:
                        logger.error(f"Error processing link {i}: {e}")

                return files_downloaded

            except Exception as e:
                logger.error(f"Error searching/downloading in pane: {e}")
                return 0

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = SundaramDownloader()
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
