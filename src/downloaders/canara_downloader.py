# src/downloaders/canara_downloader.py

import os
import time
import json
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
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


class CanaraDownloader(BaseDownloader):
    """
    Canara Robeco Mutual Fund - Portfolio Downloader
    
    URL: https://www.canararobeco.com/documents/statutory-disclosures/scheme-dashboard/scheme-monthly-portfolio/
    Downloads all scheme-wise monthly portfolio files
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Canara Robeco Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "canara"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "CANARA",
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
        
        logger.warning(f"CANARA: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("CANARA", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

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
            accept_downloads=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        self._page = self._context.new_page()
        Stealth().apply_stealth_sync(self._page)
        logger.info("Persistent Chrome session opened for Canara.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page: self._page.close()
        if self._browser: self._browser.close()
        if self._playwright: self._playwright.stop()
        self._page = self._context = self._browser = self._playwright = None
        logger.info("Persistent Chrome session closed for Canara.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        month_idx = f"{month:02d}"
        
        logger.info("=" * 60)
        logger.info("CANARA ROBECO MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"CANARA: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"CANARA: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_idx, month_name, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"CANARA: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("CANARA", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                duration = time.time() - start_time
                self.notifier.notify_success("CANARA", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ CANARA download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("CANARA", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_idx: str, month_name: str, download_folder: Path) -> int:
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        url = "https://www.canararobeco.com/documents/statutory-disclosures/scheme-dashboard/scheme-monthly-portfolio/"

        try:
            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="load", timeout=90000)
            time.sleep(5)
            logger.info("  ✓ Page loaded")

            # Handle cookie/disclaimer banners
            try:
                if "Accept" in page.content():
                    page.get_by_role("button", name=re.compile("Accept", re.I)).click(timeout=2000)
                    time.sleep(1)
            except:
                pass

            # Select Year and Month
            logger.info(f"Selecting year: {target_year}, month: {month_idx}...")
            page.locator("#year").select_option(str(target_year))
            time.sleep(2)
            
            page.locator("#month").select_option(month_idx)
            time.sleep(1)

            # Submit search
            logger.info("Submitting search...")
            submit_btn = page.locator("#customSearchBtn")
            if submit_btn.count() == 0:
                submit_btn = page.get_by_role("button", name="Submit")
            
            submit_btn.click()
            time.sleep(10)
            logger.info("  ✓ Search submitted")

            # Download files from all pages
            total_downloaded = 0
            page_num = 1
            
            while True:
                logger.info(f"Processing page {page_num}...")
                
                # Find all scheme links
                all_links = page.locator("a").all()
                scheme_links_indices = []
                
                for idx, link in enumerate(all_links):
                    try:
                        text = link.text_content().strip()
                        if "Canara Robeco" in text and ("–" in text or "-" in text):
                            scheme_links_indices.append(idx)
                    except:
                        continue

                logger.info(f"  → Found {len(scheme_links_indices)} scheme links on page {page_num}")
                
                for idx in scheme_links_indices:
                    current_link = page.locator("a").nth(idx)
                    scheme_text = current_link.text_content().strip()
                    
                    logger.info(f"  [{total_downloaded + 1}] Downloading: {scheme_text[:60]}...")
                    
                    try:
                        with page.expect_popup() as popup_info:
                            with page.expect_download(timeout=60000) as download_info:
                                current_link.click(force=True, timeout=5000)
                        
                        popup_page = popup_info.value
                        download = download_info.value
                        
                        suggested = download.suggested_filename
                        ext = os.path.splitext(suggested)[1] if suggested else ".xlsx"
                        
                        # Clean filename
                        clean_name = scheme_text.replace("–", "-").replace(":", "")
                        clean_name = re.sub(r'[\\/*?:"<>|]', "", clean_name)
                        
                        save_path = download_folder / f"{clean_name}{ext}"
                        download.save_as(save_path)
                        logger.info(f"    ✓ Saved: {save_path.name}")
                        
                        popup_page.close()
                        total_downloaded += 1
                        time.sleep(1)
                        
                    except Exception as e:
                        logger.warning(f"    ✗ Download error: {str(e)[:100]}")
                        try:
                            popup_page.close()
                        except:
                            pass

                # Check for next page
                next_page_num = page_num + 1
                next_link = page.locator(f"a.page-numbers:link:has-text('{next_page_num}')").first
                if next_link.count() == 0:
                    next_link = page.get_by_role("link", name=str(next_page_num), exact=True).first

                if next_link.count() > 0 and next_link.is_visible():
                    logger.info(f"  → Moving to page {next_page_num}...")
                    next_link.click()
                    time.sleep(5)
                    page_num = next_page_num
                else:
                    logger.info(f"  ✓ End reached at page {page_num}")
                    break

            return total_downloaded

        finally:
            if close_needed: self.close_session()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = CanaraDownloader()
    downloader.download(args.year, args.month)
