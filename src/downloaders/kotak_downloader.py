# src/downloaders/kotak_downloader.py

import os
import time
import re
import json
import shutil
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import openpyxl
from curl_cffi import requests as cffi_requests
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

# Import downloader config
try:
    from src.config.downloader_config import (
        DRY_RUN, MAX_RETRIES, RETRY_BACKOFF
    )
except ImportError:
    DRY_RUN = False
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]


class KotakDownloader(BaseDownloader):
    """
    Kotak Mutual Fund - Portfolio Downloader
    
    Downloads official monthly consolidated portfolio workbooks via direct CMS API and CDN storage,
    with Playwright fallback if needed.
    """

    API_BASE = "https://www.kotakmf.com/api/kotakapi/forms/user/getsubheaderList/417"
    CDN_BASE = "https://vatseelabs-s3.kotakmf.com/"
    PORTFOLIO_OPTION_ID = 51

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.kotakmf.com/Information/forms-and-downloads",
        "Origin": "https://www.kotakmf.com",
        "Accept": "application/json, text/plain, */*",
    }

    CDN_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.kotakmf.com/",
        "Origin": "https://www.kotakmf.com",
        "Accept": "*/*",
    }
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Kotak Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "kotak"
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _safe_api_get(self, session: cffi_requests.Session, url: str, headers: Dict[str, str], retries: int = 4, timeout: int = 30):
        last_err = None
        for attempt in range(retries):
            try:
                r = session.get(url, headers=headers, timeout=timeout)
                if r.status_code == 200:
                    return r
                time.sleep(1)
            except Exception as e:
                last_err = e
                time.sleep(1 + attempt * 0.5)
        raise RuntimeError(f"API request failed after {retries} attempts: {last_err}")

    def _download_via_api(self, target_year: int, target_month: int, download_folder: Path) -> Optional[Path]:
        """Direct REST API + CDN download flow for Kotak without browser automation."""
        try:
            logger.info(f"KOTAK: Querying CMS API for {target_year}-{target_month:02d} monthly portfolio...")
            session = cffi_requests.Session(impersonate="chrome124")

            # 1. Fetch records with pagination
            records = []
            page = 1
            page_size = 50

            while True:
                url = (
                    f"{self.API_BASE}?option={self.PORTFOLIO_OPTION_ID}"
                    f"&pagination=1&pageSize={page_size}&pageNumber={page}"
                )
                resp = self._safe_api_get(session, url, headers=self.DEFAULT_HEADERS)
                data = resp.json()

                items = data.get("subHeaderList", [])
                total = data.get("total", 0)
                if not items:
                    break

                records.extend(items)
                if len(records) >= total:
                    break
                page += 1

            # 2. Find target monthly portfolio record
            target_month_name = calendar.month_name[target_month].lower()
            year_str = str(target_year)
            target_record = None

            for r in records:
                title = str(r.get("subHeaderTitle", "")).strip()
                title_lower = title.lower()
                fname = str(r.get("fileName", "")).lower()

                if "fortnightly" in title_lower or "weekly" in title_lower or "fortnightly" in fname:
                    continue

                is_consolidated = "consolidated" in title_lower or "portfolio" in title_lower
                has_month = target_month_name in title_lower or target_month_name in fname
                has_year = year_str in title_lower or year_str in fname

                if is_consolidated and has_month and has_year:
                    if "15" not in title:
                        target_record = r
                        break

            if not target_record:
                logger.warning(f"KOTAK: No monthly portfolio record found in API for {target_year}-{target_month:02d}")
                return None

            content_path = target_record.get("content", "")
            file_name = target_record.get("fileName") or f"ConsolidatedSEBIPortfolio_{target_year}_{target_month:02d}.xlsx"
            if not content_path:
                logger.error("KOTAK: Target record missing 'content' path")
                return None

            download_url = f"{self.CDN_BASE}{content_path.lstrip('/')}"
            dest_path = download_folder / file_name

            logger.info(f"KOTAK: Downloading {dest_path.name} from {download_url}...")
            resp = self._safe_api_get(session, download_url, headers=self.CDN_HEADERS, timeout=60)
            content = resp.content

            if not content.startswith(b"PK\x03\x04"):
                logger.error(f"KOTAK: Downloaded file has invalid magic bytes: {content[:4]}")
                return None

            with open(dest_path, "wb") as f:
                f.write(content)

            # Validate openpyxl readable
            wb = openpyxl.load_workbook(dest_path, read_only=True)
            sheet_count = len(wb.sheetnames)
            wb.close()

            logger.info(f"KOTAK: Downloaded and verified {dest_path.name} ({len(content):,} bytes, {sheet_count} sheets)")
            return dest_path

        except Exception as e:
            logger.error(f"KOTAK: Direct API download failed: {e}")
            return None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        
        marker_data = {
            "amc": "KOTAK",
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
        
        logger.warning(f"KOTAK: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))

        self.notifier.notify_error(
            amc="KOTAK",
            year=year,
            month=month,
            error_type="Corruption Recovery",
            reason=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )

    def open_session(self):
        """Open a persistent browser session."""
        if self._page:
            return
            
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=False,
            args=["--window-size=1920,1080", "--start-maximized", "--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            slow_mo=500
        )
        self._context = self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            accept_downloads=True
        )
        self._page = self._context.new_page()
        Stealth().apply_stealth_sync(self._page)
        logger.info("Persistent Chrome session opened.")

    def close_session(self):
        """Close the persistent browser session."""
        if self._page:
            self._page.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()
            
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None
        logger.info("Persistent Chrome session closed.")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("KOTAK MUTUAL FUND PLAYWRIGHT DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # 1) Idempotency Check
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Kotak: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: Kotak")
                logger.info(f"Mode: SKIPPED")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Status: COMPLETE")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {
                    "amc": "Kotak", 
                    "year": year, 
                    "month": month, 
                    "status": "skipped", 
                    "reason": "already_downloaded",
                    "duration": duration
                }
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json marker")

        self.ensure_directory(str(target_dir))

        # 2) Download Logic with Retry
        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"KOTAK: [DRY RUN] Would download {month_name} {year}")
                    duration = time.time() - start_time
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: Kotak")
                    logger.info(f"Mode: DRY RUN")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: SIMULATED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "Kotak", "year": year, "month": month, "status": "success", "dry_run": True}

                # 1) Try fast direct API/CDN download
                file_path = self._download_via_api(year, month, target_dir)

                # 2) Fallback to Playwright if needed and session is active
                if not file_path and self._page:
                    logger.info("KOTAK: Falling back to Playwright flow...")
                    file_path = self._download_via_playwright(year, month_name, target_dir, page=self._page)
                
                if not file_path:
                    # Not Published Handling
                    duration = time.time() - start_time
                    logger.warning(f"KOTAK: {year}-{month:02d} not yet published.")
                    self.notifier.notify_not_published("KOTAK", year, month)
                    
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                        
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: Kotak")
                    logger.info(f"Mode: AUTO")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: NOT PUBLISHED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "Kotak", "year": year, "month": month, "status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("KOTAK", year, month, files_downloaded=1, duration=duration)
                
                logger.success("[SUCCESS] Kotak download completed")
                logger.info("=" * 60)
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: Kotak")
                logger.info(f"Mode: AUTO")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: 1")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: SUCCESS")
                logger.info("=" * 60)
                
                return {
                    "amc": "Kotak",
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
        self.notifier.notify_error("KOTAK", year, month, error_type="Download Failure", reason=last_error[:100])
        
        logger.info(f"[SUMMARY]")
        logger.info(f"AMC: Kotak")
        logger.info(f"Mode: AUTO")
        logger.info(f"Month: {year}-{month:02d}")
        logger.info(f"Status: FAILED")
        logger.info(f"Duration: {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "amc": "Kotak",
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration
        }

    def _download_via_playwright(self, target_year: int, target_month_name: str, download_folder: Path, page=None) -> Optional[Path]:
        """Playwright flow for Kotak."""
        if page:
            return self._run_download_flow(page, target_year, target_month_name, download_folder)

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                args=["--window-size=1920,1080", "--start-maximized", "--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                slow_mo=500
            )
            
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                accept_downloads=True
            )
            
            page = context.new_page()
            Stealth().apply_stealth_sync(page)
            
            try:
                return self._run_download_flow(page, target_year, target_month_name, download_folder)
            finally:
                browser.close()

    def _run_download_flow(self, page, target_year: int, target_month_name: str, download_folder: Path) -> Optional[Path]:
        """Internal flow using an active page."""
        try:
            url = "https://www.kotakmf.com/Information/forms-and-downloads"
            logger.info(f"Navigating to {url}...")
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
            
            # Check for blocking
            if "Anomaly Detected" in page.content() or "Captcha" in page.title():
                logger.error("Access blocked by security.")
                return None

            # Apply Filters
            page.wait_for_selector("select", state="visible", timeout=30000)
            logger.info("Applying 'Portfolio Monthly' filter...")
            page.get_by_role("combobox").nth(2).select_option("51")
            time.sleep(5) # Wait for table refresh

            # Search Logic
            current_page = 1
            while current_page <= 5: # Limit pagination search
                rows = page.locator("div, tr, p", has_text=re.compile(r"Consolidated Portfolio", re.I)).all()
                
                for row in rows:
                    try:
                        text = row.inner_text().strip()
                        if not text: continue
                        
                        lines = text.splitlines()
                        title = lines[0].strip()
                        
                        # Match rules: Consolidated Portfolio, Full Month Name, and Year
                        if "consolidated portfolio" in title.lower() and "fortnightly" not in title.lower():
                            if target_month_name.lower() in title.lower() and str(target_year) in title:
                                logger.info(f"Match found: {title}")
                                
                                dl_link = row.locator("text=Download").first
                                if dl_link.count() > 0:
                                    with page.expect_download(timeout=60000) as download_info:
                                        try:
                                            # Kotak sometimes opens download in new tab, handling both
                                            with page.context.expect_page(timeout=3000) as popup_info:
                                                dl_link.click()
                                            popup = popup_info.value
                                            popup.close()
                                        except:
                                            # If no popup, just wait for download on same page
                                            if not download_info.is_done():
                                                dl_link.click()

                                    download = download_info.value
                                    final_path = download_folder / download.suggested_filename
                                    download.save_as(str(final_path))
                                    logger.info(f"Downloaded: {final_path.name}")
                                    return final_path
                    except:
                        continue
                
                # Pagination
                next_page = current_page + 1
                next_btn = page.get_by_text(f"page {next_page}", exact=True).first
                if not (next_btn.count() > 0 and next_btn.is_visible()):
                     next_btn = page.get_by_role("link", name=str(next_page), exact=True).first
                
                if next_btn.count() > 0 and next_btn.is_visible():
                    logger.info(f"Moving to Page {next_page}...")
                    next_btn.scroll_into_view_if_needed()
                    next_btn.click()
                    time.sleep(5)
                    current_page = next_page
                else:
                    break

            return None
        except Exception as e:
            logger.error(f"Error in download flow: {e}")
            return None


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Kotak Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = KotakDownloader()
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
