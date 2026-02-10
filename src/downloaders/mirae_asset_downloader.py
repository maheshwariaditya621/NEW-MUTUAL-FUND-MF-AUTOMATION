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
        url = "https://www.miraeassetmf.co.in/downloads/portfolio"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to Mirae Asset Downloads page...")
            page.goto(url, wait_until="load", timeout=90000)
            time.sleep(5)

            # Accept cookies
            try:
                accept_btn = page.get_by_role("link", name=re.compile("Accept", re.I)).first
                if accept_btn.count() > 0 and accept_btn.is_visible(timeout=3000):
                    accept_btn.click()
                    time.sleep(1)
            except:
                pass

            search_pattern = rf"{month_name}\s*,?\s*{target_year}"
            logger.info(f"Searching for portfolios matching regex: {search_pattern}")
            
            downloaded_funds = set()
            download_count = 0
            page_number = 1
            max_pages = 40 # Sanity limit
            matching_found_once = False # To help decide when to stop

            while page_number <= max_pages:
                logger.info(f"Processing Page {page_number}...")
                time.sleep(3)
                
                # Use a specific container if possible for better performance
                container = page.locator("#nav-portfolio-tab1")
                if container.count() > 0:
                    all_links = container.locator("a").all()
                else:
                    all_links = page.get_by_role("link").all()
                
                download_links_metadata = []
                newer_records_on_page = 0
                older_records_on_page = 0
                matches_on_page = 0
                
                # Date detection logic
                months_list = ["January", "February", "March", "April", "May", "June", 
                               "July", "August", "September", "October", "November", "December"]
                target_month_idx = target_month - 1

                for link in all_links:
                    try:
                        text = link.text_content()
                        if not text or "Portfolio Details" not in text: continue
                        
                        text = text.strip()
                        # Date detection logic
                        is_older = False
                        is_match = False
                        
                        if re.search(search_pattern, text, re.I):
                            is_match = True
                            matches_on_page += 1
                            matching_found_once = True
                        else:
                            # Check if it's strictly older or newer
                            year_match = re.search(r"(\d{4})", text)
                            if year_match:
                                yr = int(year_match.group(1))
                                if yr < target_year:
                                    is_older = True
                                elif yr == target_year:
                                    # Same year, check month
                                    for idx, m in enumerate(months_list):
                                        if re.search(rf"\b{m}\b", text, re.I):
                                            if idx < target_month_idx:
                                                is_older = True
                                            break
                        
                        if is_match:
                            # Extract fund name
                            fund_name = "Unknown"
                            if "for Mirae Asset " in text:
                                fund_name = text.split("for Mirae Asset ")[1].strip()
                            else:
                                fund_name = text.strip()
                            
                            if fund_name not in downloaded_funds:
                                download_links_metadata.append({"text": text, "fund_name": fund_name, "locator": link})
                        elif is_older:
                            older_records_on_page += 1
                        else:
                            newer_records_on_page += 1
                            if page_number > 15: # Only log skip details on deeper pages to avoid spam
                                logger.debug(f"    Skipping (Newer): {text[:60]}")
                    except:
                        continue
                
                logger.info(f"  Page {page_number} stats: Newer: {newer_records_on_page}, Matches: {matches_on_page}, Older: {older_records_on_page}")
                
                # Process downloads on this page
                if download_links_metadata:
                    logger.info(f"Found {len(download_links_metadata)} NEW portfolio links on page {page_number}")
                    for meta in download_links_metadata:
                        fund_name = meta["fund_name"]
                        link_text = meta["text"]
                        link = meta["locator"]
                        
                        logger.info(f"  Downloading: {fund_name[:60]}...")
                        
                        try:
                            # Re-locate the link within the container to avoid detachment
                            # We use a more specific selector to be sure
                            try:
                                with page.expect_download(timeout=60000) as download_info:
                                    # Mirae Asset often triggers download AND opens a wrapper popup
                                    try:
                                        with page.expect_popup(timeout=8000) as popup_info:
                                            link.click(force=True)
                                        popup = popup_info.value
                                        popup.close()
                                    except:
                                        # No popup, but expect_download is still waiting
                                        pass
                                
                                download = download_info.value
                                download_count += 1
                                # Keep original filename but prefix with count for sorting
                                safe_fund_name = fund_name[:30].replace(" ", "_").replace("/", "_")
                                filename = f"{safe_fund_name}_{download.suggested_filename}"
                                save_path = download_folder / filename
                                
                                download.save_as(save_path)
                                logger.info(f"    ✓ Saved: {filename}")
                                downloaded_funds.add(fund_name)
                                time.sleep(1)
                            except Exception as dl_inner:
                                logger.debug(f"    Simplified download attempt for {fund_name}...")
                                with page.expect_download(timeout=45000) as dl_info:
                                    link.click(force=True)
                                dl = dl_info.value
                                dl_path = download_folder / dl.suggested_filename
                                if not dl_path.exists():
                                    dl.save_as(dl_path)
                                downloaded_funds.add(fund_name)
                                download_count += 1

                        except Exception as e:
                            logger.error(f"    ✗ Download failed for {fund_name}: {str(e)[:100]}")
                # Decide if we can stop
                if older_records_on_page >= 3:
                     # We've reached the end of the target period records
                    if matching_found_once:
                        logger.info("Reached end of target period (older records detected). Stopping.")
                        break
                    else:
                        # This should only happen if the target month was never published
                        # but we saw older ones.
                        logger.warning("Reached older records without finding any matches. Target month might be missing.")
                        break

                # Pagination
                page_number += 1
                try:
                    # Look for next page number button specifically in the active tab's pagination
                    page.mouse.wheel(0, 1000)
                    time.sleep(2)

                    pagination_container = container.locator(".pagination, .paging").first
                    if pagination_container.count() == 0:
                        pagination_container = page.locator(".pagination, .paging").first

                    # Strategy: Try clicking page number directly
                    num_btn = pagination_container.get_by_role("link", name=str(page_number), exact=True).first
                    if num_btn.count() > 0 and num_btn.is_visible():
                        logger.info(f"Navigating to page {page_number}...")
                        num_btn.click()
                        time.sleep(3)
                    else:
                        # Try "Next" or ">" button in the same container
                        next_btn = pagination_container.get_by_role("link", name=re.compile(r"Next|>", re.I)).first
                        if next_btn.count() > 0 and next_btn.is_visible():
                            logger.info(f"Clicking 'Next' (>) to reveal more pages (Current max on UI: {page_number-1})...")
                            next_btn.click()
                            time.sleep(4)
                            
                            # Re-check if the number now appeared
                            num_btn = pagination_container.get_by_role("link", name=str(page_number), exact=True).first
                            if num_btn.count() > 0:
                                num_btn.click()
                                time.sleep(3)
                        else:
                            logger.info(f"No more pagination buttons for page {page_number} and no 'Next' button.")
                            break
                except Exception as e:
                    logger.info(f"Pagination completed or interrupted: {e}")
                    break

            return download_count

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = MiraeAssetDownloader()
    downloader.download(args.year, args.month)
