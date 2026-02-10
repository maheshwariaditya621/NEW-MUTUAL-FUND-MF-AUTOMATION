# src/downloaders/nippon_downloader.py

import os
import time
import json
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
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


class NipponDownloader(BaseDownloader):
    """
    Nippon India Mutual Fund - Portfolio Downloader
    
    Uses Playwright (Stealth) to navigate discovery pages and download monthly portfolios.
    Supports persistent browser sessions and no-refresh optimizations.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Nippon India Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "nippon"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        
        marker_data = {
            "amc": "NIPPON",
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
        
        logger.warning(f"NIPPON: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))

        self.notifier.notify_error(
            amc="NIPPON",
            year=year,
            month=month,
            error_type="Corruption Recovery",
            reason=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("NIPPON INDIA MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # 1) Idempotency Check
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                duration = time.time() - start_time
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: Nippon")
                logger.info(f"Mode: SKIPPED")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Status: ALREADY COMPLETE")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {"amc": "Nippon", "year": year, "month": month, "status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json marker")

        self.ensure_directory(str(target_dir))

        # 2) Download Logic with Retry
        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"NIPPON: [DRY RUN] Would download {month_name} {year}")
                    duration = time.time() - start_time
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: Nippon")
                    logger.info(f"Mode: DRY RUN")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: SIMULATED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "Nippon", "year": year, "month": month, "status": "success", "dry_run": True}

                file_path = self._run_download_flow(year, month_name, target_dir)
                
                if not file_path:
                    # Not Published Handling
                    duration = time.time() - start_time
                    logger.warning(f"NIPPON: {year}-{month:02d} not yet published.")
                    self.notifier.notify_not_published("NIPPON", year, month)
                    
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                        
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: Nippon")
                    logger.info(f"Mode: AUTO")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: NOT PUBLISHED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "Nippon", "year": year, "month": month, "status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                duration = time.time() - start_time
                self.notifier.notify_success("NIPPON", year, month, files_downloaded=1, duration=duration)
                
                logger.success(f"✅ Nippon download completed")
                logger.info("=" * 60)
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: Nippon")
                logger.info(f"Mode: AUTO")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: 1")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: SUCCESS")
                logger.info("=" * 60)
                
                return {
                    "amc": "Nippon",
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
        self.notifier.notify_error("NIPPON", year, month, error_type="Download Failure", reason=last_error[:100])
        
        logger.info(f"[SUMMARY]")
        logger.info(f"AMC: Nippon")
        logger.info(f"Mode: AUTO")
        logger.info(f"Month: {year}-{month:02d}")
        logger.info(f"Status: FAILED")
        logger.info(f"Duration: {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "amc": "Nippon",
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration
        }

    def _run_download_flow(self, target_year: int, target_month_name: str, download_folder: Path) -> Optional[Path]:
        """Internal flow using Playwright."""
        url = "https://mf.nipponindiaim.com/investor-service/downloads/factsheet-portfolio-and-other-disclosures"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=HEADLESS, # Use global config
                args=[
                    "--window-size=1920,1080",
                    "--start-maximized",
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ]
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
            time.sleep(3)
            
            # 1. Wait for content to load
            logger.info("Waiting for portfolio list to load...")
            page.wait_for_selector(".lhsLbl", timeout=30000)
            
            # 2. Nippon Flat structure row search
            logger.info(f"Searching for 'Monthly portfolio' for {target_month_name} {target_year}...")
            
            # Find all disclosure rows
            # Based on browser tool, they are in a structure where .lhsLbl and .rhsLbl are siblings or in same container
            # We'll target the labels directly and find their parents
            labels = page.locator(".lhsLbl").all()
            target_text = f"{target_month_name} {target_year}"
            
            for label in labels:
                try:
                    # Sanitize label text - remove \u200b (Zero Width Space) and \u00a0 (Non-breaking space)
                    original_text = label.inner_text().strip()
                    label_text = original_text.replace('\u200b', '').replace('\u00a0', ' ')
                    
                    if "monthly portfolio" in label_text.lower() and target_text.lower() in label_text.lower():
                        logger.info(f"Match found: {label_text} (Original text: {repr(original_text)})")
                        
                        # Use parent to find the download link
                        parent = page.locator(f"li:has-text('{label_text}')").first
                        if parent.count() == 0:
                            # Fallback using a more flexible selector if direct match fails due to hidden chars in DOM
                            parent = label.locator("xpath=ancestor::li").first
                            
                        dl_link = parent.locator(".rhsLbl a.xls").first
                        if dl_link.count() > 0:
                            with page.expect_download(timeout=60000) as download_info:
                                dl_link.scroll_into_view_if_needed()
                                dl_link.click()
                            
                            download = download_info.value
                            final_path = download_folder / download.suggested_filename
                            download.save_as(str(final_path))
                            logger.info(f"Downloaded: {final_path.name}")
                            return final_path
                        else:
                            logger.warning(f"Label matched but Excel link (.xls) not found in sibling container.")
                except Exception as e:
                    continue

            logger.warning(f"Month {target_month_name} {target_year} not found.")
            return None

        except Exception as e:
            # Re-raise to allow retry logic in download()
            raise e
        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Nippon India Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = NipponDownloader()
    downloader.download(args.year, args.month)
