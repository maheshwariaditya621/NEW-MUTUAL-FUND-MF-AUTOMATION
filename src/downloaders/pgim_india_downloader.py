# src/downloaders/pgim_india_downloader.py

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


class PGIMIndiaDownloader(BaseDownloader):
    """
    PGIM India Mutual Fund - Portfolio Downloader
    
    URL: https://www.pgimindia.com/mutual-funds/disclosures/Portfolios/Monthly-Portfolio
    Features:
    - Persistent Session for efficiency.
    - Handles custom dropdowns for Year and Month.
    - Infinite scroll / "Load More" handling.
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
        super().__init__("PGIM India Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "pgim_india"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "PGIM India",
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
        self.notifier.notify_error("PGIM India", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_abbr = self.MONTH_ABBR[month]
        month_full = self.MONTH_FULL[month]
        
        logger.info("=" * 60)
        logger.info(f"PGIM INDIA MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_abbr})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"PGIM India: {year}-{month:02d} files already downloaded.")
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
                    self.notifier.notify_not_published("PGIM India", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("PGIM India", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("PGIM India", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_abbr: str, month_full: str, download_folder: Path) -> int:
        url = "https://www.pgimindia.com/mutual-funds/disclosures/Portfolios/Monthly-Portfolio"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            # PGIM India often needs headful to avoid 403
            browser = pw.chromium.launch(
                headless=False, 
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to PGIM India Portfolio page...")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)

            # 1. Select Year (Dropdown Index 1)
            logger.info(f"Selecting Year: {target_year}")
            # Target the container, not the text directly
            year_dropdown = page.locator(".drop-down-container").nth(1)
            if year_dropdown.count() > 0:
                year_dropdown.click()
                time.sleep(1)
                # Click on the year option (ID based as per user script)
                year_option = page.locator(f'[id="{target_year}"]')
                if year_option.count() > 0:
                    year_option.click()
                    time.sleep(2)
                else:
                    logger.error(f"Year dropdown option {target_year} not found")
                    return 0
            else:
                logger.error(f"Year dropdown container not found")
                return 0

            # 2. Select Month (Dropdown Index 2)
            logger.info(f"Selecting Month: {month_full}")
            month_dropdown = page.locator(".drop-down-container").nth(2)
            if month_dropdown.count() > 0:
                month_dropdown.click()
                time.sleep(1)
                month_option = page.locator(f"#{month_full}")
                if month_option.count() > 0:
                    month_option.click()
                    time.sleep(3)
                else:
                    logger.error(f"Month option {month_full} not found")
                    return 0
            else:
                logger.error("Month dropdown container not found")
                return 0

            # 3. Load All Schemes
            logger.info("Loading all schemes...")
            load_more_count = 0
            while True:
                try:
                    load_more_btn = page.get_by_role("button", name="Load More")
                    if load_more_btn.count() > 0 and load_more_btn.is_visible():
                        load_more_btn.click()
                        load_more_count += 1
                        time.sleep(2)
                        if load_more_count % 5 == 0:
                            logger.info(f"  Load More clicked {load_more_count} times...")
                    else:
                        break
                except:
                    break
            logger.info(f"  All schemes loaded ({load_more_count} clicks)")

            # 4. Find Schemes
            logger.info(f"Scanning for schemes matching '{month_abbr}'...")
            # Use regex to find links containing "PGIM INDIA" and the month abbr
            # The structure is usually <a> with text "PGIM INDIA ... Dec 2025" or similar
            scheme_links = page.get_by_text(re.compile(f"PGIM INDIA.*{month_abbr}", re.IGNORECASE))
            count = scheme_links.count()
            
            if count == 0:
                logger.warning(f"No schemes found matching '{month_abbr}'")
                return 0

            logger.info(f"Found {count} schemes.")
            files_downloaded = 0
            
            for i in range(count):
                try:
                    # Re-locate to avoid stale element
                    current_schemes = page.get_by_text(re.compile(f"PGIM INDIA.*{month_abbr}", re.IGNORECASE))
                    if i >= current_schemes.count(): break
                    
                    link = current_schemes.nth(i)
                    scheme_text = link.text_content().strip()
                    clean_scheme = scheme_text.replace("PGIM INDIA", "").replace("PGIM India", "").strip()
                    clean_scheme = re.sub(r'\s+', '_', clean_scheme)
                    clean_scheme = clean_scheme.replace("/", "_").replace("&", "and")
                    # Remove date/year from filename if present to keep it clean, or keep it unique
                    # Usually "Small_Cap_Fund_Dec_2025" is good.
                    
                    logger.info(f"    Downloading: {clean_scheme[:40]}...")

                    try:
                        with page.expect_download(timeout=60000) as download_info:
                            # Handle popup
                            with page.expect_popup(timeout=5000) as popup_info:
                                link.click()
                            try:
                                popup = popup_info.value
                                popup.close()
                            except: pass # Popup might not open/close
                        
                        dl = download_info.value
                        fname = dl.suggested_filename
                        
                        # Handle generic filenames by prefixing with scheme name
                        if fname.lower() in ["portfolio.pdf", "monthly_portfolio.pdf", "download.pdf", "portfolio.xlsx", "portfolio.xls"]:
                            clean_scheme = clean_scheme[:30] # Limit length
                            fname = f"{clean_scheme}_{fname}"
                        
                        save_path = download_folder / fname
                        dl.save_as(save_path)
                        logger.info(f"      ✓ Saved: {fname}")
                        files_downloaded += 1
                        time.sleep(0.5)

                    except Exception as e:
                        logger.error(f"      ✗ Download failed: {e}")

                except Exception as e:
                    logger.error(f"Error processing scheme {i}: {e}")

            return files_downloaded

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = PGIMIndiaDownloader()
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
