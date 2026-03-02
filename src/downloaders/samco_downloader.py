# src/downloaders/samco_downloader.py

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


class SamcoDownloader(BaseDownloader):
    """
    Samco Mutual Fund - Portfolio Downloader
    
    URL: https://www.samcomf.com/StatutoryDisclosure
    Uses tabs for "Portfolio Disclosures" and "Monthly" portfolios.
    Downloads multiple files per month (one per scheme).
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Samco Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "samco"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "SAMCO",
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
        
        logger.warning(f"SAMCO: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("SAMCO", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("SAMCO MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Samco: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"SAMCO: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"SAMCO: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("SAMCO", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("SAMCO", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ SAMCO download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("SAMCO", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> int:
        url = "https://www.samcomf.com/StatutoryDisclosure"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="load", timeout=90000)
            time.sleep(5)
            logger.info("  ✓ Page loaded")

            # 1. Select "Portfolio Disclosures"
            logger.info("Navigating to 'Portfolio Disclosures'...")
            pf_link = page.get_by_role("link", name="Portfolio Disclosures")
            if pf_link.count() == 0:
                pf_link = page.get_by_text("Portfolio Disclosures", exact=False)
            
            if pf_link.count() > 0:
                pf_link.first.click(force=True)
                time.sleep(3)
            else:
                logger.error("  ✗ Portfolio Disclosures tab not found")
                return 0

            # 2. Select "Monthly"
            logger.info("Selecting 'Monthly' sub-tab...")
            m_link = page.get_by_role("link", name="Monthly")
            if m_link.count() == 0:
                 m_link = page.get_by_text("Monthly", exact=True)
            
            if m_link.count() > 0:
                m_link.first.click(force=True)
                time.sleep(5)
            else:
                logger.error("  ✗ Monthly sub-tab not found")
                return 0

            # 3. Find matching rows
            logger.info(f"Searching for {month_name} {target_year} rows...")
            row_selector = "tr"
            all_rows = page.locator(row_selector).all()
            
            matching_rows_info = []
            for row in all_rows:
                txt = row.inner_text().strip().replace('\n', ' ')
                if "MONTHLY_PORTFOLIO" in txt and month_name in txt and str(target_year) in txt:
                    if "FORTNIGHTLY" not in txt.upper():
                        matching_rows_info.append(row)
            
            logger.info(f"  ✓ Found {len(matching_rows_info)} potential rows")
            
            success_count = 0
            downloaded_schemes = set()

            for i, row in enumerate(matching_rows_info):
                row_text = row.inner_text().strip().replace('\n', ' ')
                
                # Extract scheme name: e.g. "MONTHLY_PORTFOLIO_December_2025_SAMCO_Flexi_Cap_Fund"
                scheme_name = "SAMCO_Scheme"
                name_match = re.search(rf"{target_year}_?(.*)", row_text, re.IGNORECASE)
                if name_match:
                    scheme_name = name_match.group(1).strip().replace(' ', '_').replace('__', '_')
                
                scheme_name = re.sub(r'^_+', '', scheme_name)
                
                if scheme_name in downloaded_schemes:
                    continue

                logger.info(f"  [{i+1}/{len(matching_rows_info)}] Downloading: {scheme_name}")
                
                try:
                    # Target link usually in 2nd column
                    target_link = row.locator("td").nth(1).locator("a").last
                    if not target_link.is_visible():
                        target_link = row.locator("a").last

                    with page.expect_download(timeout=60000) as download_info:
                        target_link.click(force=True)
                    
                    download = download_info.value
                    filename = download.suggested_filename
                    
                    download.save_as(download_folder / filename)
                    logger.info(f"    ✓ Saved: {filename}")
                    success_count += 1
                    downloaded_schemes.add(scheme_name)
                    
                except Exception as e:
                    logger.error(f"    ✗ Download failed for {scheme_name}: {str(e)[:100]}")

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

    downloader = SamcoDownloader()
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
