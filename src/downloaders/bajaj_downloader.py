# src/downloaders/bajaj_downloader.py

import os
import time
import json
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
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


class BajajDownloader(BaseDownloader):
    """
    Bajaj Finserv Mutual Fund - Portfolio Downloader
    
    Uses Playwright to navigate statutory disclosures and download monthly portfolios.
    Supports persistent browser sessions and "no-refresh" multi-month logic.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Bajaj Finserv Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "bajaj"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        
        marker_data = {
            "amc": "BAJAJ",
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
        
        logger.warning(f"BAJAJ: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))

        self.notifier.notify_error(
            amc="BAJAJ",
            year=year,
            month=month,
            error_type="Corruption Recovery",
            reason=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )

    def _get_fy_string(self, year: int, month: int) -> str:
        """Map calendar month/year to Bajaj's FY format (e.g., 2025-26)."""
        if month in [1, 2, 3]:
            return f"{year-1}-{str(year)[-2:]}"
        else:
            return f"{year}-{str(year+1)[-2:]}"


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("BAJAJ FINSERV MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # 1) Idempotency Check
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Bajaj: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("✅ Month already complete — UPDATED")
                logger.info(f"🕒 Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {
                    "amc": "Bajaj", 
                    "year": year, 
                    "month": month, 
                    "status": "skipped", 
                    "reason": "already_downloaded",
                    "duration": duration
                }
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json marker")

        self.ensure_directory(str(target_dir))

        # 2) Download Logic with Retry
        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"BAJAJ: [DRY RUN] Would download {month_name} {year}")
                    duration = time.time() - start_time
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: Bajaj")
                    logger.info(f"Mode: DRY RUN")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: SIMULATED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "Bajaj", "year": year, "month": month, "status": "success", "dry_run": True}

                file_path = self._run_download_flow(year, month, target_dir)
                
                if not file_path:
                    # Not Published Handling
                    duration = time.time() - start_time
                    logger.warning(f"BAJAJ: {year}-{month:02d} not yet published or found.")
                    self.notifier.notify_not_published("BAJAJ", year, month)
                    
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                        
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: Bajaj")
                    logger.info(f"Mode: AUTO")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: NOT PUBLISHED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "Bajaj", "year": year, "month": month, "status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("BAJAJ", year, month, files_downloaded=1, duration=duration)
                
                logger.success(f"✅ Bajaj download completed")
                logger.info("=" * 60)
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: Bajaj")
                logger.info(f"Mode: AUTO")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: 1")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: SUCCESS")
                logger.info("=" * 60)
                
                return {
                    "amc": "Bajaj",
                    "year": year,
                    "month": month,
                    "status": "success",
                    "files_downloaded": 1,
                    "duration": duration
                }

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])
                continue

        # 3) Final Failure
        if target_dir.exists() and not (target_dir / "_SUCCESS.json").exists():
            shutil.rmtree(target_dir, ignore_errors=True)
            
        duration = time.time() - start_time
        self.notifier.notify_error("BAJAJ", year, month, error_type="Download Failure", reason=last_error[:100])
        
        logger.info(f"[SUMMARY]")
        logger.info(f"AMC: Bajaj")
        logger.info(f"Mode: AUTO")
        logger.info(f"Month: {year}-{month:02d}")
        logger.info(f"Status: FAILED")
        logger.info(f"Duration: {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "amc": "Bajaj",
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration
        }

    def _run_download_flow(self, target_year: int, target_month: int, download_folder: Path) -> Optional[Path]:
        """Internal flow using Playwright with session support and no-refresh logic."""
        month_name = self.MONTH_NAMES[target_month]
        fy_str = self._get_fy_string(target_year, target_month)
        
        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=False,
                args=[
                    "--window-size=1920,1080",
                    "--start-maximized",
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
                slow_mo=1000
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                ignore_https_errors=True,
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            url = "https://www.bajajamc.com/downloads?statutory-disclosures="
            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
            time.sleep(10) # Hydration wait

            # 1. Close popups
            try:
                popups = page.locator(".close-btn, .popup-close, button[aria-label='Close']")
                if popups.count() > 0:
                    popups.first.click(timeout=3000)
                    logger.debug("Closed popup.")
            except:
                pass

            # 2. Portfolio -> Monthly Portfolio
            logger.info("📂 Expanding 'Portfolio' section...")
            # We look for the "Portfolio" section title. 
            # In some cases, we might already be on it if no-refresh is working.
            candidates = page.get_by_text("Portfolio", exact=True).all()
            target_el = None
            for c in candidates:
                if c.is_visible() and "monthly" not in c.text_content().lower():
                    target_el = c
                    break
            
            if target_el:
                target_el.scroll_into_view_if_needed()
                target_el.click()
                time.sleep(3)
            
            logger.info("📂 Selecting 'Monthly Portfolio' sub-item...")
            monthly_opt = page.get_by_text("Monthly Portfolio", exact=True).first
            if monthly_opt.is_visible():
                monthly_opt.click()
                time.sleep(5)
            else:
                logger.warning("'Monthly Portfolio' sub-item not visible. Trying alternative selection.")

            # 3. Select Financial Year
            logger.info(f"📅 Selecting Financial Year: {fy_str}...")
            visible_selects = [s for s in page.locator("select").all() if s.is_visible()]
            
            if len(visible_selects) < 1:
                logger.error("No visible selection dropdowns found for Year.")
                return None
                
            year_select = visible_selects[0]
            # Try selecting by label first
            try:
                year_select.select_option(label=fy_str)
            except:
                # Fallback to fuzzy text match on options
                opts = year_select.locator("option").all()
                found = False
                for opt in opts:
                    if fy_str in opt.inner_text():
                        year_select.select_option(value=opt.get_attribute("value"))
                        found = True
                        break
                if not found:
                    logger.error(f"FY {fy_str} not found in dropdown.")
                    return None
            
            time.sleep(3)

            # 4. Select Month
            logger.info(f"📅 Selecting Month: {month_name}...")
            # Refresh list of visible selects as the month one might have appeared
            visible_selects = [s for s in page.locator("select").all() if s.is_visible()]
            if len(visible_selects) < 2:
                logger.error("Month dropdown did not appear after Year selection.")
                return None
            
            month_select = visible_selects[1]
            try:
                # Get all options to find case-insensitive match
                opts = month_select.locator("option").all_inner_texts()
                match = None
                for opt in opts:
                    if opt.strip().lower() == month_name.lower():
                        match = opt.strip()
                        break
                
                if match:
                    month_select.select_option(label=match)
                    logger.info(f"Month {match} selected (case-insensitive match).")
                else:
                    # Fallback to direct selection which might fail if casing differs
                    month_select.select_option(label=month_name)
                    logger.info(f"Month {month_name} selected.")
            except Exception as e:
                logger.error(f"Month {month_name} not found in dropdown for FY {fy_str}. Error: {e}")
                return None
            
            time.sleep(5)

            # 5. Download Excel
            logger.info("🔍 Searching for Excel download link...")
            # Narrow down to visible Excel links/buttons
            dl_links = page.locator("a:visible, button:visible").filter(has_text=re.compile(r"Download|xls", re.I)).all()
            
            if not dl_links:
                logger.warning(f"No download links found for {month_name} {target_year}.")
                return None
            
            # Usually the first one is our target
            target_link = dl_links[0]
            
            with page.expect_download(timeout=120000) as download_info:
                target_link.scroll_into_view_if_needed()
                target_link.click()
            
            download = download_info.value
            filename = download.suggested_filename
            final_path = download_folder / filename
            download.save_as(str(final_path))
            
            logger.info(f"Downloaded: {filename}")
            return final_path

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Bajaj Finserv Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = BajajDownloader()
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
