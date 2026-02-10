# src/downloaders/ppfas_downloader.py

import os
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

# Import downloader config
try:
    from src.config.downloader_config import (
        DRY_RUN, MAX_RETRIES, RETRY_BACKOFF
    )
except ImportError:
    # Fallback defaults
    DRY_RUN = False
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]


class PPFASDownloader(BaseDownloader):
    """
    PPFAS Mutual Fund - Portfolio Downloader
    
    Uses Playwright to navigate the month accordion and download consolidated portfolio disclosures.
    """
    
    def __init__(self):
        super().__init__("PPFAS Mutual Fund")
        self.notifier = get_notifier()

    def _normalize_month_name(self, month: int) -> str:
        """Convert month number to full name used by PPFAS."""
        month_names = {
            1: "January", 2: "February", 3: "March", 4: "April",
            5: "May", 6: "June", 7: "July", 8: "August",
            9: "September", 10: "October", 11: "November", 12: "December"
        }
        return month_names[month]

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int) -> None:
        """Create atomic completion marker."""
        marker_path = target_dir / "_SUCCESS.json"
        
        marker_data = {
            "amc": "PPFAS",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        
        logger.info(f"Created completion marker: {marker_path.name}")

    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str) -> None:
        """Move incomplete/corrupt folder to quarantine."""
        corrupt_base = Path("data/raw/ppfas/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)
        
        corrupt_target = corrupt_base / f"{year}_{month:02d}"
        
        if corrupt_target.exists():
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            corrupt_target = corrupt_target.parent / f"{corrupt_target.name}__{ts}"
        
        logger.warning(f"PPFAS: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))

        # Emit warning event
        self.notifier.notify_warning(
            amc="PPFAS",
            year=year,
            month=month,
            warning_type="Corruption Recovery",
            message=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )

    def _download_via_playwright(self, target_year: int, target_month_name: str, download_folder: Path) -> Path:
        """
        Refined Playwright UI flow for PPFAS based on working implementation.
        """
        from playwright.sync_api import sync_playwright
        
        listing_url = "https://amc.ppfas.com/downloads/portfolio-disclosure/"
        
        with sync_playwright() as p:
            # Use Chrome channel and non-headless as per proven success
            browser = p.chromium.launch(channel="chrome", headless=False)
            context = browser.new_context(
                accept_downloads=True,
                user_agent="Mozilla/5.0"
            )
            page = context.new_page()
            
            try:
                logger.info(f"PPFAS: Navigating to {listing_url}")
                page.goto(listing_url, timeout=120000, wait_until="load")
                time.sleep(10)
                
                # 1. Select Year Tab
                year_tab_selector = f'a[id="{target_year}-tab"]'
                year_tab = page.locator(year_tab_selector)
                
                if year_tab.count() > 0:
                    logger.info(f"PPFAS: Clicking Year Tab: {target_year}")
                    year_tab.click()
                    time.sleep(5)
                else:
                    # Fallback roles/labels
                    year_tab = page.get_by_role("tab", name=str(target_year)).first
                    if year_tab.count() > 0:
                        year_tab.click()
                        time.sleep(5)
                    else:
                        raise Exception(f"PPFAS: Year tab for {target_year} not found")

                # 2. Expand Month Accordion
                month_id = f"heading{target_month_name}{target_year}"
                month_header = page.locator(f'div[id="{month_id}"] a')
                
                logger.info(f"PPFAS: Expanding Month Accordion: {target_month_name} {target_year}")
                if month_header.count() > 0:
                    # Use JS click as it's more reliable for accordions
                    page.evaluate("(el) => el.click()", month_header.element_handle())
                    time.sleep(10)
                else:
                    month_header_alt = page.get_by_text(f"{target_month_name} {target_year}", exact=False).first
                    if month_header_alt.count() > 0:
                        page.evaluate("(el) => el.click()", month_header_alt.element_handle())
                        time.sleep(10)
                    else:
                        raise Exception(f"PPFAS: Month header for {target_month_name} {target_year} not found")

                # 3. Click "Consolidated" link
                collapse_id = f"collapse{target_month_name}{target_year}"
                container = page.locator(f'div[id="{collapse_id}"]')
                consolidated_btn = container.get_by_text("Consolidated", exact=False).first
                
                if consolidated_btn.count() == 0:
                    consolidated_btn = page.get_by_text("Consolidated", exact=False).first

                if consolidated_btn.count() > 0:
                    logger.info("PPFAS: Triggering download for 'Consolidated'")
                    try:
                        consolidated_btn.scroll_into_view_if_needed(timeout=5000)
                    except:
                        pass
                    
                    with page.expect_download(timeout=120000) as download_info:
                        try:
                            consolidated_btn.click(timeout=10000)
                        except:
                            page.evaluate("(el) => el.click()", consolidated_btn.element_handle())
                    
                    download = download_info.value
                    filename = download.suggested_filename
                    save_path = download_folder / filename
                    download.save_as(str(save_path))
                    logger.success(f"PPFAS: Saved: {save_path.name}")
                    return save_path
                else:
                    raise Exception(f"PPFAS: Consolidated link not found for {target_month_name} {target_year}")

            finally:
                context.close()
                browser.close()

    def _check_file_count(self, file_count: int, year: int, month: int):
        """Sanity check file count (expected 1 for PPFAS consolidated)."""
        if file_count < 1:
            logger.warning(f"PPFAS: No files downloaded for {year}-{month:02d}")
        elif file_count > 1:
            logger.warning(f"PPFAS: More than 1 file downloaded ({file_count}) for {year}-{month:02d}")
        else:
            logger.info(f"PPFAS: File count (1) within normal range")

    def download(self, year: int, month: int) -> Dict:
        """
        Download PPFAS consolidated monthly portfolio file with Gold Standard retry logic.
        """
        start_time = time.time()
        
        # 1) Validation
        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month: {month}")

        logger.info("=" * 60)
        logger.info("PPFAS MUTUAL FUND PLAYWRIGHT DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        if year < 2021 or (year == 2021 and month < 9):
            logger.warning(f"PPFAS: {year}-{month:02d} is before supported period (Sep 2021)")
            duration = time.time() - start_time
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: PPFAS")
            logger.info(f"Mode: SKIPPED")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Status: UNSUPPORTED PERIOD")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info("=" * 60)
            return {"amc": "PPFAS Mutual Fund", "year": year, "month": month, "status": "skipped", "reason": "before_supported_period"}
            
        # 2) Idempotency
        target_dir = Path(self.get_target_folder("ppfas", year, month))
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                duration = time.time() - start_time
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: PPFAS")
                logger.info(f"Mode: SKIPPED")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Status: ALREADY COMPLETE")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {"amc": "PPFAS Mutual Fund", "year": year, "month": month, "status": "skipped", "reason": "already_downloaded"}
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json marker")

        self.ensure_directory(str(target_dir))

        # 3) Download with Retries
        last_error = "Unknown error"
        month_name = self._normalize_month_name(month)
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"PPFAS: [DRY RUN] Would download {month_name} {year}")
                    duration = time.time() - start_time
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: PPFAS")
                    logger.info(f"Mode: DRY RUN")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: SIMULATED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "PPFAS Mutual Fund", "year": year, "month": month, "status": "success", "dry_run": True}
                
                file_path = self._download_via_playwright(year, month_name, target_dir)
                
                # 4) Success Marker
                self._create_success_marker(target_dir, year, month, 1)
                
                # 5) Sanity Check
                self._check_file_count(1, year, month)
                
                # 6) Notification
                duration = time.time() - start_time
                self.notifier.notify_success("PPFAS", year, month, files_downloaded=1, duration=duration)
                
                logger.success("✅ PPFAS download completed")
                logger.info("=" * 60)
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: PPFAS")
                logger.info(f"Mode: AUTO")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: 1")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: SUCCESS")
                logger.info("=" * 60)
                
                return {
                    "amc": "PPFAS Mutual Fund",
                    "year": year,
                    "month": month,
                    "status": "success",
                    "file_path": str(file_path),
                    "files_downloaded": 1,
                    "duration": duration
                }

            except Exception as e:
                last_error = str(e)
                
                # Specialized handling for "Not Published"
                if "Month header" in last_error or "not found" in last_error.lower():
                    logger.warning(f"PPFAS: {year}-{month:02d} not yet published: {last_error}")
                    self.notifier.notify_not_published("PPFAS", year, month)
                    
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                        
                    duration = time.time() - start_time
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: PPFAS")
                    logger.info(f"Mode: AUTO")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: NOT PUBLISHED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": "PPFAS Mutual Fund", "year": year, "month": month, "status": "not_published", "reason": last_error}

                if attempt < MAX_RETRIES:
                    backoff = RETRY_BACKOFF[attempt]
                    logger.warning(f"PPFAS: Download attempt {attempt + 1} failed: {last_error}. Retrying in {backoff}s...")
                    time.sleep(backoff)
                else:
                    logger.error(f"PPFAS: Max retries exceeded for {year}-{month:02d}")

        # If we reached here, it's a final failure
        self.notifier.notify_error(
            amc="PPFAS",
            year=year,
            month=month,
            error_type="Download Error",
            reason=last_error[:100]
        )
        
        # Clean up partial folder
        if target_dir.exists() and not (target_dir / "_SUCCESS.json").exists():
            shutil.rmtree(target_dir, ignore_errors=True)
            
        duration = time.time() - start_time
        logger.info(f"[SUMMARY]")
        logger.info(f"AMC: PPFAS")
        logger.info(f"Mode: AUTO")
        logger.info(f"Month: {year}-{month:02d}")
        logger.info(f"Status: FAILED")
        logger.info(f"Duration: {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "amc": "PPFAS Mutual Fund",
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration
        }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="PPFAS Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Calendar year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    
    args = parser.parse_args()
    
    downloader = PPFASDownloader()
    result = downloader.download(year=args.year, month=args.month)
    
    if result["status"] == "success":
        logger.success(f"✅ Downloaded {result['files_downloaded']} file(s)")
    elif result["status"] == "not_published":
        logger.info(f"ℹ️  Not published yet: {result.get('reason')}")
    else:
        logger.error(f"❌ Failed: {result.get('reason', 'Unknown error')}")
        exit(1)
