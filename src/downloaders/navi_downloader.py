# src/downloaders/navi_downloader.py

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


class NaviDownloader(BaseDownloader):
    """
    Navi Mutual Fund - Portfolio Downloader
    
    URL: https://navi.com/mutual-fund/downloads/portfolio
    Features:
    - Persistent Session for efficiency.
    - Financial Year (FY) calculation logic.
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
        super().__init__("Navi Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "navi"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Navi",
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
        self.notifier.notify_error("Navi", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _get_financial_year(self, month: int, year: int) -> str:
        """
        Convert calendar month/year to Financial Year.
        FY runs from April to March.
        - April 2025 to March 2026 → FY 2025-2026
        - December 2025 → FY 2025-2026
        - January 2025 → FY 2024-2025
        """
        if month >= 4:  # April to December
            fy_start = year
            fy_end = year + 1
        else:  # January to March
            fy_start = year - 1
            fy_end = year
        return f"{fy_start}-{fy_end}"


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_abbr = self.MONTH_ABBR[month]
        month_full_name = self.MONTH_FULL[month]
        
        logger.info("=" * 60)
        logger.info(f"NAVI MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_abbr})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Navi: {year}-{month:02d} files already downloaded.")
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

                files_downloaded = self._run_download_flow(year, month, month_abbr, month_full_name, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"{self.AMC_NAME}: No portfolios found for {month_abbr} {year}")
                    self.notifier.notify_not_published("Navi", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("Navi", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Navi", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_abbr: str, month_full_name: str, download_folder: Path) -> int:
        url = "https://navi.com/mutual-fund/downloads/portfolio"

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

            fy_year = self._get_financial_year(target_month, target_year)
            
            logger.info(f"Navigating to Navi Portfolio page...")
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
            time.sleep(3)

            # 1. Select "Monthly" tab
            logger.info("Selecting 'Monthly' portfolio type...")
            monthly_button = page.locator("div").filter(has_text="Monthly").nth(4) # Based on user script
            if monthly_button.count() > 0:
                monthly_button.click()
            else:
                # Fallback
                page.get_by_text("Monthly", exact=True).click()
            time.sleep(2)

            # 2. Select Financial Year
            logger.info(f"Selecting FY: {fy_year}")
            year_dropdown = page.get_by_role("combobox").nth(0)
            if year_dropdown.count() > 0:
                try:
                    # Check if option exists first
                    options = year_dropdown.locator("option").all_inner_texts()
                    if fy_year not in options:
                         logger.warning(f"FY {fy_year} not found in dropdown. Available: {options[:5]}")
                         return 0
                    year_dropdown.select_option(fy_year)
                    time.sleep(2)
                except Exception as e:
                    logger.error(f"Failed to select FY {fy_year}: {e}")
                    return 0
            else:
                logger.error("Year dropdown not found")
                return 0

            # 3. Select Month
            logger.info(f"Selecting Month: {month_full_name}")
            month_dropdown = page.get_by_role("combobox").nth(1)
            if month_dropdown.count() > 0:
                try:
                    month_dropdown.select_option(month_full_name)
                    time.sleep(3)
                except Exception as e:
                    logger.error(f"Failed to select month {month_full_name}: {e}")
                    return 0
            else:
                logger.error("Month dropdown not found")
                return 0

            # 4. Find Download Links
            # Filter for links with "Download" text that are NOT "Download App"
            # And potentially check if they have a PDF/Download icon (svg)
            
            # Using locator strategy from user script
            # Initial broad search
            download_links = page.locator("a:has-text('Download')").filter(has_not=page.locator("text=/Download App/i"))
            
            count = download_links.count()
            if count == 0:
                logger.info("No download links found.")
                return 0

            success_count = 0
            logger.info(f"Found {count} potential download links. Processing...")

            for i in range(count):
                try:
                    # Re-locate to avoid stale element
                    current_links = page.locator("a:has-text('Download')").filter(has_not=page.locator("text=/Download App/i"))
                    if i >= current_links.count(): break
                    
                    link = current_links.nth(i)
                    href = link.get_attribute("href")
                    
                    if not href or "http" not in href: continue
                    # Basic filter to ensure it looks like a document
                    if "portfolio" not in href.lower() and "documents" not in href.lower() and ".pdf" not in href.lower() and "public-assets" not in href.lower():
                        continue

                    # Try to infer scheme name from context (heading before link)
                    # We can try to assume scheme name is in the card. 
                    # If difficult, we default to "Scheme_{i}" but standardized naming is better.
                    # User script mentions "Navi Nifty India..." headings.
                    
                    # Try to get nearby text
                    scheme_name = f"Navi_Scheme_{i+1}"
                    try:
                         # Attempt to find a heading in the parent container
                         # This is heuristic and might need adjustment
                         heading = link.locator("xpath=./ancestor::div[contains(@class, 'card') or contains(@class, 'row')]//h3 | ./ancestor::div[contains(@class, 'card') or contains(@class, 'row')]//h4 | ./preceding-sibling::h3 | ./preceding-sibling::h4")
                         if heading.count() > 0:
                             raw_name = heading.first.text_content().strip()
                             scheme_name = raw_name.replace("Navi ", "").replace("Fund", "").strip()
                    except: pass
                    
                    scheme_name = scheme_name.replace(" ", "_").replace("/", "_").replace("&", "and")
                    
                    # Direct Download using Request Context (faster/reliable for static links)
                    logger.info(f"    Downloading: {scheme_name}...")
                    
                    try:
                        response = context.request.get(href)
                        if response.status == 200:
                            # Use original filename from content-disposition if possible
                            original_filename = ""
                            content_disp = response.headers.get("content-disposition", "")
                            if "filename=" in content_disp:
                                original_filename = content_disp.split("filename=")[1].strip('"\'')
                            
                            if not original_filename:
                                original_filename = href.split("/")[-1].split("?")[0]
                            
                            if not original_filename or "." not in original_filename:
                                ext = ".pdf"
                                if ".xlsx" in href: ext = ".xlsx"
                                elif ".xls" in href: ext = ".xls"
                                original_filename = f"NAVI_{scheme_name}_{month_abbr}_{target_year}{ext}"
                            
                            save_path = download_folder / original_filename
                            
                            with open(save_path, "wb") as f:
                                f.write(response.body())
                                
                            logger.info(f"      ✓ Saved: {original_filename}")
                            success_count += 1
                            time.sleep(0.5)
                        else:
                            logger.warning(f"      HTTP Error {response.status} for {href}")

                    except Exception as e:
                        logger.error(f"      Download failed: {e}")

                except Exception as e:
                    logger.error(f"Error processing link {i}: {e}")

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

    downloader = NaviDownloader()
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
