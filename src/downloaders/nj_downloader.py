# src/downloaders/nj_downloader.py

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


class NJDownloader(BaseDownloader):
    """
    NJ Mutual Fund - Portfolio Downloader
    
    URL: https://downloads.njmutualfund.com/njmf_download.php?nme=127
    Uses accordion-based navigation with "lying" aria attributes.
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
        super().__init__("NJ Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "nj"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "NJ",
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
        
        logger.warning(f"NJ: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("NJ", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def open_session(self):
        """Open a persistent browser session for NJ."""
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
        logger.info("Persistent browser session opened for NJ.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page: self._page.close()
        if self._browser: self._browser.close()
        if self._playwright: self._playwright.stop()
        self._page = self._context = self._browser = self._playwright = None
        logger.info("Persistent browser session closed for NJ.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        month_abbr = self.MONTH_ABBR[month]
        
        logger.info("=" * 60)
        logger.info(f"NJ MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"NJ: {year}-{month:02d} already complete. Skipping.")
                return {"status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"NJ: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, month_abbr, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"NJ: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("NJ", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                duration = time.time() - start_time
                self.notifier.notify_success("NJ", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ NJ download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("NJ", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, month_abbr: str, download_folder: Path) -> int:
        close_needed = False
        if not self._page:
            self.open_session()
            close_needed = True

        page = self._page
        url = "https://downloads.njmutualfund.com/njmf_download.php?nme=127"

        # Multi-file threshold: Sept 2025
        is_multi_file = False
        if target_year > 2025:
            is_multi_file = True
        elif target_year == 2025 and target_month >= 9:
            is_multi_file = True

        try:
            logger.info(f"Navigating to NJ Downloads page...")
            page.goto(url, wait_until="load", timeout=90000)
            time.sleep(3)

            # Expand Year Accordion
            # NJ follows a Financial Year (FY) system: April to March.
            # Jan-Mar [Year] belongs to FY [Year-1]-[Year]
            # Apr-Dec [Year] belongs to FY [Year]-[Year+1]
            if target_month in [1, 2, 3]:
                fy_start = target_year - 1
                fy_end_full = str(target_year)
                fy_end_short = fy_end_full[-2:]
            else:
                fy_start = target_year
                fy_end_full = str(target_year + 1)
                fy_end_short = fy_end_full[-2:]
            
            # Pattern: "Disclosure 2025-2026" or "Disclosure 2025-26"
            pattern = re.compile(rf"Disclosure\s+{fy_start}-({fy_end_full}|{fy_end_short})", re.I)
            accordion = page.get_by_role("button", name=pattern)
            
            if accordion.count() == 0:
                logger.info(f"Stricter pattern failed, trying 'Disclosure {fy_start}'")
                # Broad match including non-breaking spaces (\s+)
                accordion = page.get_by_role("button", name=re.compile(rf"Disclosure\s+{fy_start}", re.I))
            
            if accordion.count() == 0:
                logger.warning(f"Could not find accordion for FY starting {fy_start}")
                return 0
            
            # Take the last one if multiple (newer years are usually bottom)
            best_accordion = accordion.last
            
            # DEBUG and AGGRESSIVE expansion
            # Don't trust aria-expanded. Check 'collapsed' class instead.
            classes = best_accordion.get_attribute("class") or ""
            if "collapsed" in classes or "true" not in str(best_accordion.get_attribute("aria-expanded")).lower():
                logger.info(f"Expanding accordion: {best_accordion.inner_text().strip()}")
                best_accordion.click()
                time.sleep(3)
            else:
                # Still click if links not visible later, but for now we trust visual state if not collapsed
                logger.info("Accordion appears expanded (class not collapsed).")

            success_count = 0
            
            if is_multi_file:
                # Search for specific links
                link_pattern = re.compile(rf"{month_name}.*{target_year}", re.I)
                links = page.get_by_role("link", name=link_pattern)
                
                # If zero but we think it's open, try clicking again to be sure
                if links.count() == 0:
                    logger.info("No links found on first check. Re-clicking accordion just in case...")
                    best_accordion.click()
                    time.sleep(3)
                    links = page.get_by_role("link", name=link_pattern)
                
                link_count = links.count()
                if link_count == 0:
                    logger.warning(f"No matching links for {month_name} {target_year}")
                    return 0
                
                logger.info(f"Found {link_count} matching portfolio links.")
                
                # Collect texts
                texts = []
                for i in range(link_count):
                    texts.append(links.nth(i).inner_text().strip())
                
                for i, txt in enumerate(texts):
                    # Extract scheme name
                    scheme_name = "NJ_Scheme"
                    if "-" in txt:
                        parts = txt.split("-")
                        if len(parts) >= 3:
                            scheme_name = parts[-1].strip().replace(" ", "_").replace("/", "_")
                        else:
                            scheme_name = parts[0].strip().replace(" ", "_")

                    logger.info(f"  [{i+1}/{len(texts)}] Downloading: {txt[:60]}...")
                    
                    try:
                        # Direct location
                        target_lnk = page.get_by_role("link", name=txt, exact=True).first
                        
                        with page.expect_download(timeout=60000) as dinfo:
                            # Force click to handle scroll/transparency
                            target_lnk.click(force=True)
                        
                        dl = dinfo.value
                        ext = os.path.splitext(dl.suggested_filename)[1] or ".xlsx"
                        fname = f"NJ_{scheme_name}_{month_abbr}_{target_year}{ext}"
                        dl.save_as(download_folder / fname)
                        logger.info(f"    ✓ Saved")
                        success_count += 1
                        time.sleep(1)
                    except Exception as e:
                        logger.error(f"    ✗ {str(e)[:100]}")
            else:
                # Single-file mode
                logger.info(f"Searching for single consolidated link for {month_name}...")
                lnk = page.get_by_role("link", name=re.compile(rf"^{month_name}.*", re.I))
                
                # Re-click logic for single file too
                if lnk.count() == 0:
                    best_accordion.click()
                    time.sleep(3)
                    lnk = page.get_by_role("link", name=re.compile(rf"^{month_name}.*", re.I))

                if lnk.count() == 0:
                    logger.warning(f"Could not find single-file link for {month_name}")
                    return 0
                
                txt = lnk.first.inner_text().strip()
                logger.info(f"Found: {txt}")
                
                try:
                    with page.expect_download(timeout=60000) as dinfo:
                        lnk.first.click(force=True)
                    
                    dl = dinfo.value
                    ext = os.path.splitext(dl.suggested_filename)[1] or ".xlsx"
                    fname = f"NJ_CONSOLIDATED_{month_abbr}_{target_year}{ext}"
                    dl.save_as(download_folder / fname)
                    logger.info(f"    ✓ Saved")
                    success_count = 1
                except Exception as e:
                    logger.error(f"    ✗ Download failed: {str(e)[:100]}")

            return success_count

        finally:
            if close_needed: self.close_session()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = NJDownloader()
    downloader.download(args.year, args.month)
