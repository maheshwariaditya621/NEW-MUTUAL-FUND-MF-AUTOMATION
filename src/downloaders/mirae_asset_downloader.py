# src/downloaders/mirae_asset_downloader.py

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


class MiraeAssetDownloader(BaseDownloader):
    """
    Mirae Asset Mutual Fund - Portfolio Downloader
    
    URL: https://www.miraeassetmf.co.in/downloads/portfolio
    Uses search patterns in link text and numbered pagination.
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
        super().__init__("Mirae Asset Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "mirae_asset"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "MIRAE_ASSET",
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
        
        logger.warning(f"MIRAE_ASSET: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("MIRAE_ASSET", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def open_session(self):
        """Open a persistent browser session for MIRAE_ASSET."""
        if self._page:
            return
            
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=HEADLESS,
            channel="chrome",
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-infobars"]
        )

        self._context = self._browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            accept_downloads=True
        )
        self._page = self._context.new_page()
        Stealth().apply_stealth_sync(self._page)
        logger.info("Persistent browser session opened for Mirae Asset.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page: self._page.close()
        if self._browser: self._browser.close()
        if self._playwright: self._playwright.stop()
        self._page = self._context = self._browser = self._playwright = None
        logger.info("Persistent browser session closed for Mirae Asset.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        month_abbr = self.MONTH_ABBR[month]
        
        logger.info("=" * 60)
        logger.info(f"MIRAE ASSET MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"MIRAE_ASSET: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"MIRAE_ASSET: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, month_abbr, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"MIRAE_ASSET: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("MIRAE_ASSET", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                duration = time.time() - start_time
                self.notifier.notify_success("MIRAE_ASSET", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ MIRAE_ASSET download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("MIRAE_ASSET", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, month_abbr: str, download_folder: Path) -> int:
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        url = "https://www.miraeassetmf.co.in/downloads/portfolio"

        try:
            logger.info(f"Navigating to Mirae Asset Downloads page...")
            page.goto(url, wait_until="load", timeout=90000)
            time.sleep(3)

            # Accept cookies
            try:
                accept_btn = page.get_by_role("link", name=re.compile("Accept", re.I)).first
                if accept_btn.count() > 0 and accept_btn.is_visible(timeout=3000):
                    accept_btn.click()
                    time.sleep(1)
            except:
                pass

            search_pattern = f"{month_name} {target_year}"
            logger.info(f"Searching for portfolios matching: {search_pattern}")
            
            downloaded_funds = set()
            download_count = 0
            page_number = 1

            while True:
                logger.info(f"Processing Page {page_number}...")
                time.sleep(2)
                
                # Find all links matching this month/year
                all_links = page.get_by_role("link").all()
                download_links_metadata = []
                
                for link in all_links:
                    try:
                        text = link.text_content()
                        if text and search_pattern in text and "Portfolio Details" in text:
                            # Extract fund name to use as ID
                            fund_name = "Unknown"
                            if "for Mirae Asset " in text:
                                fund_name = text.split("for Mirae Asset ")[1].strip()
                            else:
                                fund_name = text.strip()
                            
                            if fund_name not in downloaded_funds:
                                download_links_metadata.append({"text": text, "fund_name": fund_name})
                    except:
                        continue
                
                if not download_links_metadata:
                    logger.info(f"No new matching portfolio links on page {page_number}")
                    break
                
                logger.info(f"Found {len(download_links_metadata)} portfolio links on page {page_number}")
                
                for meta in download_links_metadata:
                    fund_name = meta["fund_name"]
                    link_text = meta["text"]
                    
                    if fund_name in downloaded_funds:
                        continue
                        
                    logger.info(f"  Downloading: {fund_name[:60]}...")
                    
                    try:
                        # Re-locate the link to avoid detachment
                        link = page.get_by_role("link", name=link_text, exact=True).first
                        if link.count() == 0:
                            # Try broad match if exact failed
                            link = page.locator("a").filter(has_text=link_text).first
                            
                        if link.count() == 0:
                            logger.warning(f"    ⚠ Could not re-locate link: {fund_name}")
                            continue

                        # Mirae opens a popup for some downloads, or triggers direct download.
                        # The reference script handles expect_popup.
                        try:
                            with page.expect_download(timeout=30000) as download_info:
                                with page.expect_popup(timeout=10000) as popup_info:
                                    link.click(force=True)
                                popup = popup_info.value
                                popup.close()
                            
                            download = download_info.value
                            orig_ext = os.path.splitext(download.suggested_filename)[1] or ".pdf"
                            
                            download_count += 1
                            filename = f"MIRAE_ASSET_{month_abbr}_{target_year}_{download_count:02d}{orig_ext}"
                            save_path = download_folder / filename
                            
                            download.save_as(save_path)
                            logger.info(f"    ✓ Saved: {filename}")
                            downloaded_funds.add(fund_name)
                            time.sleep(1.5)
                        except PlaywrightTimeout:
                            # Sometimes no popup, just direct download
                            with page.expect_download(timeout=30000) as download_info:
                                link.click(force=True)
                            download = download_info.value
                            orig_ext = os.path.splitext(download.suggested_filename)[1] or ".pdf"
                            download_count += 1
                            filename = f"MIRAE_ASSET_{month_abbr}_{target_year}_{download_count:02d}{orig_ext}"
                            save_path = download_folder / filename
                            download.save_as(save_path)
                            logger.info(f"    ✓ Saved: {filename}")
                            downloaded_funds.add(fund_name)
                            time.sleep(1.5)
                            
                    except Exception as e:
                        logger.error(f"    ✗ Download failed for {fund_name}: {str(e)[:100]}")

                # Pagination
                page_number += 1
                try:
                    # Look for numbered button
                    next_page_btn = page.get_by_role("link", name=str(page_number), exact=True)
                    if next_page_btn.count() > 0:
                        logger.info(f"Navigating to page {page_number}...")
                        next_page_btn.first.click()
                        time.sleep(3)
                    else:
                        # Try "Next" button
                        next_btn = page.get_by_role("link", name=re.compile("Next", re.I)).first
                        if next_btn.count() > 0:
                            # Check if disabled
                            classes = next_btn.get_attribute("class") or ""
                            if "disabled" in classes.lower():
                                logger.info("Next button is disabled. Reached the end.")
                                break
                                
                            logger.info(f"Clicking 'Next' to reveal more pages...")
                            next_btn.click()
                            time.sleep(3)
                            
                            # Check if page number now exists
                            next_page_btn = page.get_by_role("link", name=str(page_number), exact=True)
                            if next_page_btn.count() > 0:
                                next_page_btn.first.click()
                                time.sleep(3)
                            else:
                                break
                        else:
                            logger.info("No more pagination buttons found.")
                            break
                except Exception as e:
                    logger.info(f"Pagination completed or interrupted: {e}")
                    break

            return download_count

        finally:
            if close_needed: self.close_session()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = MiraeAssetDownloader()
    downloader.download(args.year, args.month)
