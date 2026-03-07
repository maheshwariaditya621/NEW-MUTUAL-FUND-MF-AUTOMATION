# src/downloaders/jio_br_downloader.py

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


class JioBRDownloader(BaseDownloader):
    """
    Jio BlackRock Mutual Fund - Portfolio Downloader
    
    URL: https://www.jioblackrockamc.com/statutory-disclosure/disclosures/monthly-portfolio-disclosure
    Uses Ant Design dropdowns and consolidated download links.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Jio BlackRock Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "jio_br"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "JIO_BR",
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
        
        logger.warning(f"JIO_BR: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("JIO_BR", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def calculate_fy(self, month: int, year: int) -> str:
        """Calculate FY string: Nov 2025 -> 2025-2026"""
        if month >= 4:
            fy_start = year
        else:
            fy_start = year - 1
        fy_end = fy_start + 1
        return f"{fy_start}-{fy_end}"

    def download(self, year: int, month: int) -> Dict:
        # Jio BlackRock started in July 2025
        if year < 2025 or (year == 2025 and month < 7):
            logger.info(f"JIO_BR: Started July 2025. No data for {year}-{month:02d}. Skipping.")
            return {"status": "skipped", "reason": "pre_launch"}

        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        fy_str = self.calculate_fy(month, year)
        
        logger.info("=" * 60)
        logger.info("JIO BLACKROCK MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name}) | FY: {fy_str}")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Jio BlackRock: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"JIO_BR: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                file_path = self._run_download_flow(year, month, month_name, fy_str, target_dir)
                
                if not file_path:
                    logger.warning(f"JIO_BR: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("JIO_BR", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("JIO_BR", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] JIO_BR download completed: {file_path.name}")
                return {"status": "success", "file": str(file_path), "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("JIO_BR", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, fy_str: str, download_folder: Path) -> Optional[Path]:
        url = "https://www.jioblackrockamc.com/statutory-disclosure/disclosures/monthly-portfolio-disclosure"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to {url}...")
            # Use 'load' instead of 'networkidle' for better stability on Ant Design sites
            page.goto(url, wait_until="load", timeout=90000)
            # Extra sleep to let JS components (Ant Design) hydrate
            time.sleep(5)
            logger.info("  [OK] Page loaded")

            # Selection logic
            selectors = page.locator(".ant-select-selector")
            if selectors.count() < 2:
                logger.error("  [FAIL] Selectors for Year/Month not found. Page might not have loaded correctly.")
                return None

            # 1. Select Year
            logger.info(f"Selecting Year: {fy_str}...")
            selectors.nth(0).click()
            time.sleep(2)
            year_option = page.locator(".ant-select-item-option-content").filter(has_text=fy_str).first
            if year_option.count() > 0:
                year_option.click()
            else:
                # Fallback to get_by_text
                page.get_by_text(fy_str, exact=True).last.click(force=True)
            time.sleep(2)
            logger.info("  [OK] Year selected")

            # 2. Select Month
            logger.info(f"Selecting Month: {month_name}...")
            selectors.nth(1).click()
            time.sleep(2)
            
            # Using type-to-filter approach as it's more robust with Ant Design
            page.keyboard.type(month_name)
            time.sleep(2)
            page.keyboard.press("Enter")
            time.sleep(3)
            
            # Verify selection
            selected_text = selectors.nth(1).inner_text().strip()
            if month_name.lower() not in selected_text.lower():
                logger.warning(f"  Typing {month_name} failed. Falling back to scroll-and-click...")
                # Try to re-open and find
                popup_options = page.locator(".ant-select-item-option-content")
                if popup_options.count() == 0 or not popup_options.first.is_visible():
                    selectors.nth(1).click()
                    time.sleep(2)
                
                found_month = False
                for _ in range(15): # Max 15 attempts to scroll/find
                    options = page.locator(".ant-select-item-option-content")
                    for i in range(options.count()):
                        opt = options.nth(i)
                        if opt.is_visible() and month_name.lower() in opt.inner_text().lower():
                            opt.click()
                            found_month = True
                            break
                    if found_month: break
                    page.keyboard.press("ArrowDown")
                    time.sleep(0.3)
                
                if not found_month:
                    logger.error(f"  [FAIL] Failed to find {month_name} in dropdown")
                    return None

            logger.info(f"  [OK] Month selected: {month_name}")
            time.sleep(5) # Wait for results grid to refresh

            # 3. Find Consolidated Link
            logger.info("Searching for consolidated portfolio link...")
            
            # We want the generic one, not scheme-specific
            all_links = page.get_by_role("link").all()
            target_link = None
            
            logger.debug(f"  Found {len(all_links)} total links on page")
            for link in all_links:
                try:
                    if not link.is_visible(): continue
                    text = (link.inner_text().strip() or 
                            link.get_attribute("aria-label") or 
                            link.get_attribute("title") or "").lower()
                    
                    if "monthly" in text and "portfolio" in text:
                        # Jio BlackRock consolidated indicators
                        is_generic = "mutual fund" in text and ("jioblackrock" in text or "jio blackrock" in text)
                        # Exclude schemes strictly
                        is_scheme = any(x in text for x in [
                            "nifty", "index", "g-sec", "yr", "8-13", "overnight", 
                            "arbitrage", "liquid", "flexi", "equity", "midcap", 
                            "smallcap", "large", "dynamic", "money market", "tax saver"
                        ])
                        
                        if is_generic and not is_scheme:
                            target_link = link
                            break
                except:
                    continue
            
            # Fallback shortest link
            if not target_link:
                logger.info("  Precise match not found, looking for shortest generic link...")
                shortest_len = 999
                for link in all_links:
                    try:
                        if not link.is_visible(): continue
                        text = (link.inner_text().strip() or "").lower()
                        if "monthly" in text and "portfolio" in text and len(text) < shortest_len:
                            # Still exclude obvious scheme-only links
                            if not any(x in text for x in ["nifty", "index", "g-sec", "8-13"]):
                                target_link = link
                                shortest_len = len(text)
                    except:
                        continue

            if not target_link:
                logger.warning(f"  [FAIL] No consolidated link found for {month_name} {target_year}")
                return None

            link_text = target_link.inner_text().strip()
            logger.info(f"  [OK] Found link: {link_text}")
            target_link.scroll_into_view_if_needed()
            time.sleep(1)

            # 4. Download
            logger.info("Downloading file...")
            try:
                with page.expect_download(timeout=60000) as download_info:
                    target_link.click()
                
                download = download_info.value
                filename = download.suggested_filename
                save_path = download_folder / filename
                
                download.save_as(save_path)
                logger.info(f"  [OK] Saved: {filename}")
                return save_path
                
            except Exception as e:
                logger.error(f"  [FAIL] Download interaction failed: {str(e)[:100]}")
                return None

        finally:
            if browser:
                try:
                    browser.close()
                except Exception as e:
                    logger.debug(f"Error closing browser: {e}")
            if pw:
                try:
                    pw.stop()
                except Exception as e:
                    logger.debug(f"Error stopping playwright: {e}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = JioBRDownloader()
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
