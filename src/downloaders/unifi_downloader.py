# src/downloaders/unifi_downloader.py

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


class UnifiDownloader(BaseDownloader):
    """
    Unifi Mutual Fund - Portfolio Downloader
    
    URL: https://unifimf.com/statutorydocuments/#monthly-portfolio-disclosure
    Features:
    - Persistent Session.
    - Iterates through all "Unifi" schemes dynamically.
    - Downloads files matching "Document {Month} {Year}".
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
        super().__init__("Unifi Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "unifi"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Unifi",
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
        self.notifier.notify_error("Unifi", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_abbr = self.MONTH_ABBR[month]
        month_full = self.MONTH_FULL[month]
        
        logger.info("=" * 60)
        logger.info(f"UNIFI MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_abbr})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Unifi: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"{self.AMC_NAME}: [DRY RUN] Would download {month_abbr} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_abbr, month_full, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"{self.AMC_NAME}: No portfolios found for {month_abbr} {year}")
                    self.notifier.notify_not_published("Unifi", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("Unifi", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Unifi", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_abbr: str, month_full: str, download_folder: Path) -> int:
        url = "https://unifimf.com/statutorydocuments/#monthly-portfolio-disclosure"
        target_link_text = f"Document {month_full} {target_year}"
        
        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-infobars", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info("Navigating to Unifi Statutory Documents page...")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            logger.info("Waiting 4s for navigation...")
            time.sleep(4)
            
            # Wait for selector
            try:
                page.wait_for_selector("#monthly-portfolio-disclosure", timeout=10000)
                time.sleep(2)
            except Exception as e:
                logger.error(f"Failed waiting for disclosure section: {e}")
                return 0
            
            scheme_section = page.locator("#monthly-portfolio-disclosure")
            
            # Find all potential scheme links
            logger.info("Scanning for schemes...")
            all_links = scheme_section.locator("a").all()
            
            scheme_names = []
            for link in all_links:
                try:
                    txt = link.text_content().strip()
                    if txt.startswith("Unifi") and "Document" not in txt:
                        if txt not in scheme_names:
                            scheme_names.append(txt)
                except: pass
            
            if not scheme_names:
                logger.error("No schemes found.")
                return 0
                
            logger.info(f"Found {len(scheme_names)} schemes: {scheme_names}")
            
            files_downloaded = 0
            
            for idx, scheme_name in enumerate(scheme_names):
                logger.info(f"[{idx+1}/{len(scheme_names)}] Processing Scheme: {scheme_name}")
                
                try:
                    # Click scheme to expand
                    link = scheme_section.get_by_role("link", name=scheme_name, exact=True)
                    # If exact match fails, try relaxed
                    if link.count() == 0:
                        link = scheme_section.get_by_text(scheme_name)
                    
                    if link.count() > 0:
                        link.first.click()
                        time.sleep(2)
                    else:
                        logger.warning(f"  Could not find link for scheme: {scheme_name}")
                        continue
                    
                    # Look for document link: "Document {Month} {Year}"
                    doc_link = page.get_by_role("link", name=target_link_text)
                    
                    if doc_link.count() == 0:
                        # Fallback search in case it's not strictly a role=link or has subtle diffs
                        logger.info(f"  Document link '{target_link_text}' not found by role, trying text filter...")
                        doc_link = page.locator("a").filter(has_text=target_link_text)
                    
                    if doc_link.count() > 0:
                        logger.info(f"  Found document for {scheme_name}")
                        
                        try:
                            # Use expect_download because user script says so
                            with page.expect_download(timeout=30000) as download_info:
                                # Handle potential popup
                                try:
                                    with page.expect_popup(timeout=5000) as popup_info:
                                        doc_link.first.click()
                                    p = popup_info.value
                                    p.close()
                                except:
                                    doc_link.first.click()
                            
                            dl = download_info.value
                            original_filename = dl.suggested_filename
                            ext = os.path.splitext(original_filename)[1] or ".pdf"
                            
                            save_path = download_folder / original_filename
                            
                            # Handle duplicates only if the exact same name exists
                            if save_path.exists():
                                ts = int(time.time())
                                stem = os.path.splitext(original_filename)[0]
                                fname_alt = f"{stem}_{ts}{ext}"
                                save_path = download_folder / fname_alt

                            dl.save_as(save_path)
                            logger.info(f"    [OK] Saved: {save_path.name}")
                            files_downloaded += 1
                            time.sleep(1)
                            
                        except Exception as e:
                            logger.error(f"    [FAIL] Download failed for {scheme_name}: {e}")
                    else:
                        logger.warning(f"  No document found for {month_abbr} {target_year}")

                except Exception as e:
                    logger.error(f"  Error processing scheme {scheme_name}: {e}")
            
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

    downloader = UnifiDownloader()
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
