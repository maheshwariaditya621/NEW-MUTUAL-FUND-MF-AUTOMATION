# src/downloaders/bandhan_downloader.py

import os
import time
import json
import shutil
import re
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
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
    HEADLESS = True


class BandhanDownloader(BaseDownloader):
    """
    Bandhan Mutual Fund - Portfolio Downloader
    
    URL: https://bandhanmutual.com/downloads/other-disclosures
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Bandhan Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "bandhan"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "BANDHAN",
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
        
        logger.warning(f"BANDHAN: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("BANDHAN", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

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
        logger.info("Persistent Chrome session opened for Bandhan.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page: self._page.close()
        if self._browser: self._browser.close()
        if self._playwright: self._playwright.stop()
        self._page = self._context = self._browser = self._playwright = None
        logger.info("Persistent Chrome session closed for Bandhan.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("BANDHAN MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"BANDHAN: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"BANDHAN: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                total_downloaded = self._run_download_flow(year, month, target_dir)
                
                if total_downloaded == 0:
                    logger.warning(f"BANDHAN: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("BANDHAN", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, total_downloaded)
                duration = time.time() - start_time
                self.notifier.notify_success("BANDHAN", year, month, files_downloaded=total_downloaded, duration=duration)
                logger.success(f"✅ BANDHAN download completed. Total files: {total_downloaded}")
                return {"status": "success", "files_downloaded": total_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("BANDHAN", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, download_folder: Path) -> int:
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        month_name = self.MONTH_NAMES[target_month]
        last_day = calendar.monthrange(target_year, target_month)[1]
        exact_date_str = f"{last_day} {month_name} {target_year}"
        
        url = "https://bandhanmutual.com/downloads/other-disclosures"

        try:
            if page.url != url:
                logger.info(f"Navigating to {url}...")
                page.goto(url, wait_until="load", timeout=90000)
                time.sleep(3)
            
            # 1) Handle initial popups
            try:
                maybe_later = page.get_by_role("button", name="May be later")
                if maybe_later.is_visible(timeout=3000):
                    maybe_later.click()
                    logger.info("Closed 'May be later' popup.")
            except: pass

            # 2) Deep Scroll to load all dynamic content
            logger.info(f"Scrolling to load document list for {exact_date_str}...")
            # For 2024/2025, 60-80 scrolls are usually enough to cover the massive history
            for _ in range(80):
                page.evaluate("window.scrollBy(0, 1500)")
                time.sleep(0.3)
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(1)

            # 3) Scan for schemes matching the target month/year
            # Only download month-end portfolios (last day of month), not fortnightly reports
            date_patterns = [
                f"{last_day} {month_name} {target_year}",  # "31 January 2025"
            ]
            
            logger.info(f"Searching for month-end portfolios with date: {date_patterns[0]}")
            
            raw_schemes = page.evaluate(f"""() => {{
                const patterns = {json.dumps(date_patterns)};
                return Array.from(document.querySelectorAll('p.text-sky-950'))
                    .filter(el => {{
                        const text = el.textContent;
                        return text.includes("Bandhan") && patterns.some(p => text.includes(p));
                    }})
                    .map(el => el.textContent.trim());
            }}""")

            logger.info(f"Found {len(raw_schemes)} total scheme listings.")
            
            # 4) Deduplicate by normalizing fund names
            # Group by normalized name AND date to preserve fortnightly reports (15th and 31st)
            # while removing true duplicates (e.g., "Banking & PSU" vs "Banking and Psu")
            fund_groups = {}
            for scheme_text in raw_schemes:
                # Extract the date portion (e.g., "31 January 2025" or "15 January 2025")
                date_match = re.search(r'(\d{1,2}\s+\w+\s+\d{4})', scheme_text)
                date_part = date_match.group(1) if date_match else ""
                
                # Normalize fund name: lowercase, remove special chars, collapse whitespace
                # But preserve the date for uniqueness
                fund_name_only = re.sub(r'\d{1,2}\s+\w+\s+\d{4}', '', scheme_text)  # Remove date
                normalized_name = re.sub(r'[^\w\s]', '', fund_name_only.lower())
                normalized_name = re.sub(r'\s+', ' ', normalized_name).strip()
                
                # Create unique key: normalized_name + date
                unique_key = f"{normalized_name}|{date_part}"
                
                # Keep the version with more characters (usually more complete)
                if unique_key not in fund_groups or len(scheme_text) > len(fund_groups[unique_key]):
                    fund_groups[unique_key] = scheme_text
            
            target_schemes = list(fund_groups.values())
            logger.info(f"After deduplication: {len(target_schemes)} unique funds.")
            
            if not target_schemes:
                return 0

            # 4) Batch Download
            total_downloaded = 0
            for i, scheme_text in enumerate(target_schemes):
                clean_filename = re.sub(r'[\\/*?:"<>|]', "", scheme_text).replace("  ", " ").strip()
                logger.info(f"  [{i+1}/{len(target_schemes)}] Downloading: {scheme_text}")
                
                try:
                    item_loc = page.get_by_text(scheme_text, exact=True).first
                    item_loc.scroll_into_view_if_needed()
                    time.sleep(0.5)
                    
                    with page.expect_download(timeout=45000) as download_info:
                        item_loc.click(timeout=15000, force=True)
                    
                    download = download_info.value
                    suggested = download.suggested_filename
                    ext = os.path.splitext(suggested)[1] if suggested else ".xlsx"
                    save_path = download_folder / f"{clean_filename}{ext}"
                    
                    download.save_as(save_path)
                    total_downloaded += 1
                except Exception as e:
                    logger.error(f"    ✗ Download failed for {scheme_text}: {str(e)[:100]}")
            
            return total_downloaded

        finally:
            if close_needed: self.close_session()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = BandhanDownloader()
    downloader.download(args.year, args.month)
