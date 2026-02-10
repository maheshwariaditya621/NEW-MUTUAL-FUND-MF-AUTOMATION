
# src/downloaders/capitalmind_downloader.py

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


class CapitalMindDownloader(BaseDownloader):
    """
    CapitalMind Mutual Fund - Portfolio Downloader
    
    URL: https://capitalmindmf.com/statutory-disclosures.html
    Uses accordion-style navigation with FY groupings.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("CapitalMind Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "capitalmind"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        logger.info("CapitalMindDownloader initialized (Fixed Version 2.0)")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "CAPITALMIND",
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
        
        logger.warning(f"CAPITALMIND: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("CAPITALMIND", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

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
        logger.info("Persistent Chrome session opened for CapitalMind.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page: self._page.close()
        if self._browser: self._browser.close()
        if self._playwright: self._playwright.stop()
        self._page = self._context = self._browser = self._playwright = None
        logger.info("Persistent Chrome session closed for CapitalMind.")

    def calculate_fy(self, month: int, year: int) -> str:
        """Calculate Financial Year string: e.g. FY 2024 - 2025"""
        if month >= 4:
            fy_start = year
        else:
            fy_start = year - 1
        fy_end = fy_start + 1
        return f"FY {fy_start} - {fy_end}"

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        target_fy = self.calculate_fy(month, year)
        
        logger.info("=" * 60)
        logger.info("CAPITALMIND MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name}) | FY: {target_fy}")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"CAPITALMIND: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"CAPITALMIND: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, target_fy, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"CAPITALMIND: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("CAPITALMIND", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                duration = time.time() - start_time
                self.notifier.notify_success("CAPITALMIND", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ CAPITALMIND download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("CAPITALMIND", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, target_fy: str, download_folder: Path) -> int:
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        url = "https://capitalmindmf.com/statutory-disclosures.html#"
        try:
            logger.info(f"Navigating to {url}...")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                logger.warning(f"Navigation warning: {e}")
            
            time.sleep(5)
            logger.info("  ✓ Page loaded")

            # Handle US Person Declaration
            logger.info("Handling declaration modal...")
            selectors = [
                "button:has-text('I AM NOT A US PERSON')",
                "button.blue-button",
                ".modal-footer button",
                "button:has-text('RESIDENT OF CANADA')"
            ]
            for sel in selectors:
                btn = page.locator(sel).first
                if btn.count() > 0 and btn.is_visible():
                    btn.click()
                    logger.info(f"  ✓ Clicked modal button via selector: {sel}")
                    try:
                        page.locator(".modal-backdrop").wait_for(state="hidden", timeout=5000)
                    except:
                        pass
                    time.sleep(2)
                    break

            # Click on "Monthly Portfolio" tab
            logger.info("Selecting Monthly Portfolio tab...")
            monthly_tab = page.locator("#v-pills-home-tab4, button[role='tab']:has-text('Monthly Portfolio')").first
            try:
                monthly_tab.click(force=True, timeout=10000)
                time.sleep(3)
            except:
                page.mouse.click(179, 772) # Fallback pixel click if needed
                time.sleep(3)
            logger.info("  ✓ Tab selected")

            # Discover schemes
            logger.info("Discovering schemes...")
            tab_content = page.locator("#v-pills-tabContent4").first
            scheme_items = tab_content.locator(".accordion-item").all()
            
            scheme_info = []
            for item in scheme_items:
                btn = item.locator("button.accordion-button").first
                if btn.count() > 0:
                    name = btn.text_content().strip()
                    name = " ".join(name.split())
                    if "Capitalmind" in name:
                        scheme_info.append({
                            "name": name,
                            "button": btn,
                            "panel": item.locator(".accordion-collapse").first
                        })
            
            logger.info(f"  ✓ Found {len(scheme_info)} schemes")

            total_downloaded = 0
            
            for idx, s in enumerate(scheme_info, 1):
                scheme_name = s["name"]
                s_btn = s["button"]
                s_panel = s["panel"]
                
                logger.info(f"  [{idx}/{len(scheme_info)}] {scheme_name}")
                
                try:
                    # Expand scheme accordion
                    if s_btn.get_attribute("aria-expanded") != "true":
                        s_btn.click(force=True)
                        time.sleep(2)

                    # Find FY heading/button
                    logger.info(f"    Searching for {target_fy} section...")
                    
                    # Prioritize Button (Interactive)
                    fy_btn = s_panel.locator("button").filter(has_text=re.compile(re.escape(target_fy), re.I)).first
                    if fy_btn.count() == 0:
                        # Fallback: Try year range without FY prefix
                        year_range = target_fy.replace("FY ", "")
                        fy_btn = s_panel.locator("button").filter(has_text=re.compile(re.escape(year_range), re.I)).first

                    if fy_btn.count() > 0:
                        if fy_btn.get_attribute("aria-expanded") != "true":
                            logger.info(f"    Expanding {target_fy} section...")
                            fy_btn.click(force=True)
                            time.sleep(2)
                        search_area = s_panel
                    else:
                        # Fallback to Header (Static)
                        fy_heading = s_panel.locator("h6").filter(has_text=re.compile(re.escape(target_fy), re.I)).first
                        if fy_heading.count() > 0:
                            search_area = s_panel
                            logger.info(f"    ✓ Found FY heading (Static)")
                        else:
                            logger.warning(f"    ✗ FY section '{target_fy}' not found for {scheme_name}")
                            continue

                    # Find Month row
                    logger.info(f"    Searching for {month_name} row...")
                    month_row = search_area.locator("div.about-text").filter(has=page.locator("h6").filter(has_text=re.compile(f"^{month_name}( {target_year})?$", re.I))).first
                    
                    if month_row.count() == 0:
                        month_row = search_area.locator("div.about-text").filter(has=page.locator("h6").filter(has_text=re.compile(month_name, re.I))).first
                    
                    if month_row.count() > 0:
                        d_link = month_row.locator("a").filter(has_text=re.compile("Download", re.I)).first
                        
                        if d_link.count() > 0:
                            # Ensure link is visible
                            d_link.scroll_into_view_if_needed()
                            time.sleep(0.5)
                            try:
                                with page.expect_download(timeout=60000) as download_info:
                                    try:
                                        with page.expect_popup(timeout=5000) as popup_info:
                                            d_link.click(force=True)
                                        popup = popup_info.value
                                        popup.close()
                                    except:
                                        d_link.click(force=True)
                                
                                download = download_info.value
                                filename = download.suggested_filename
                                save_path = download_folder / filename
                                download.save_as(save_path)
                                
                                logger.info(f"    ✓ Downloaded: {filename}")
                                total_downloaded += 1
                                time.sleep(1)
                                
                            except Exception as d_err:
                                logger.error(f"    ✗ Download failed: {str(d_err)[:80]}")
                        else:
                            logger.warning(f"    ✗ Download link not found in row for {month_name}")
                    else:
                        logger.warning(f"    ✗ Month '{month_name}' row not found")
                        
                except Exception as scheme_err:
                    logger.error(f"    ✗ Error processing scheme: {str(scheme_err)[:100]}")
                    continue

            return total_downloaded

        finally:
            if close_needed:
                self.close_session()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = CapitalMindDownloader()
    downloader.download(args.year, args.month)
