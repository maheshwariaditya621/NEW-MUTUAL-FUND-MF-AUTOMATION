# src/downloaders/shriram_downloader.py

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


class ShriramDownloader(BaseDownloader):
    """
    Shriram Mutual Fund - Portfolio Downloader

    URL: https://www.shriramamc.in/investor-statutory-disclosures
    Uses FY-based dropdown and month-specific download buttons.

    FY Label Convention on the website:
      - "2024-2025" covers months April 2024 → March 2025
      - "2023-2024" covers months April 2023 → March 2024
    Dropdown option text ends with the FY end year, e.g. "-2025" for FY 2024-2025.
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
        super().__init__("Shriram Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "shriram"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "SHRIRAM",
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

        logger.warning(f"SHRIRAM: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("SHRIRAM", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _get_fy_option_text(self, year: int, month: int) -> str:
        """
        Compute the FY option text suffix used in the Shriram website dropdown.

        Indian FY runs April → March.
        - Months Jan/Feb/Mar belong to the FY that ENDS in that calendar year.
          e.g. Jan 2025 → FY 2024-2025 → option suffix "-2025"
        - Months Apr→Dec belong to the FY that STARTS in that calendar year.
          e.g. Nov 2025 → FY 2025-2026 → option suffix "-2026"
        """
        if month <= 3:
            # Jan/Feb/Mar: FY end year = same calendar year
            fy_end = year
        else:
            # Apr→Dec: FY end year = next calendar year
            fy_end = year + 1

        return f"-{fy_end}"

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]

        logger.info("=" * 60)
        logger.info(f"SHRIRAM MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"SHRIRAM: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"SHRIRAM: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                file_path = self._run_download_flow(year, month, month_name, target_dir)

                if not file_path:
                    logger.warning(f"SHRIRAM: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("SHRIRAM", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("SHRIRAM", year, month, files_downloaded=1, duration=duration)
                logger.success(f"✅ SHRIRAM download completed: {file_path.name}")
                return {"status": "success", "file": str(file_path), "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("SHRIRAM", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        url = "https://www.shriramamc.in/investor-statutory-disclosures"

        # e.g. "Nov 2025"
        month_abbr = self.MONTH_ABBR[target_month]
        search_label = f"{month_abbr} {target_year}"

        # e.g. "-2026" for Apr-Dec 2025, "-2025" for Jan-Mar 2025
        fy_option_suffix = self._get_fy_option_text(target_year, target_month)

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

            # Step 1: Click "Monthly & Fortnightly" section to expand it
            logger.info("Clicking 'Monthly & Fortnightly'...")
            try:
                page.get_by_text("Monthly & Fortnightly").click(timeout=10000)
                time.sleep(2)
                logger.info("  ✓ Monthly & Fortnightly section opened")
            except Exception as e:
                logger.warning(f"  ⚠ Could not click 'Monthly & Fortnightly': {str(e)[:60]}")

            # Step 2: Click "Monthly Portfolio for the FY" button/tab
            logger.info("Clicking 'Monthly Portfolio for the FY' button...")
            try:
                page.get_by_role("button", name="Monthly Portfolio for the FY").click(timeout=10000)
                time.sleep(2)
                logger.info("  ✓ Monthly Portfolio section opened")
            except Exception as e:
                logger.warning(f"  ⚠ Could not click 'Monthly Portfolio for the FY': {str(e)[:60]}")

            # Step 3: Select the correct FY from the dropdown
            # Dropdown options like "2025-2026", "2024-2025". We match by suffix e.g. "-2026"
            logger.info(f"Selecting FY option ending with '{fy_option_suffix}'...")
            try:
                year_select = page.locator("[id^='select-year_']")
                if year_select.count() > 0:
                    year_select.first.click(timeout=5000)
                    time.sleep(1)
                    # Match option that ends with the FY end year, e.g. "2025-2026" matches "-2026"
                    fy_option = page.get_by_role("option", name=fy_option_suffix)
                    if fy_option.count() > 0:
                        fy_option.first.click()
                        logger.info(f"  ✓ Selected FY ending {fy_option_suffix}")
                    else:
                        # Fallback: look for listitem/option containing the suffix text
                        fy_option_text = page.locator(f"[role='option']:has-text('{fy_option_suffix}')")
                        if fy_option_text.count() > 0:
                            fy_option_text.first.click()
                            logger.info(f"  ✓ Selected FY via text fallback: {fy_option_suffix}")
                        else:
                            logger.warning(f"  ⚠ FY option '{fy_option_suffix}' not found, using default FY")
                    time.sleep(3)
                else:
                    logger.warning("  ⚠ Year selector not found, using default FY")
            except Exception as e:
                logger.error(f"  ✗ FY selection error: {str(e)[:100]}")

            # Step 4: Find the month card and click its "Download" link
            # The grid shows cards like: [PDF icon] [Month Year] [Download]
            # We scan all grid item divs, find the one containing our month label,
            # then click the "Download" link inside it.
            logger.info(f"Searching for month card: '{search_label}'...")

            # Try grid items first
            grid_items = page.locator(".max-h-auto > div:nth-child(2) > .grid > div").all()
            logger.info(f"  Found {len(grid_items)} grid items to scan")

            target_download_link = None
            for item in grid_items:
                try:
                    item_text = item.inner_text()
                    if search_label in item_text:
                        logger.info(f"  ✓ Found card for: {search_label}")
                        # The download element is a link with text "Download" inside the card
                        dl_link = item.get_by_text("Download", exact=False).first
                        if dl_link.count() > 0:
                            target_download_link = dl_link
                            break
                        # Fallback: any <a> tag inside the card
                        a_tag = item.locator("a").first
                        if a_tag.count() > 0:
                            target_download_link = a_tag
                            break
                except Exception:
                    continue

            if target_download_link is None:
                logger.warning(f"  ✗ No download link found for {search_label}")
                return None

            # Step 6: Trigger download (with popup handling as per codegen)
            logger.info("Triggering download...")
            try:
                with page.expect_download(timeout=60000) as download_info:
                    try:
                        with page.expect_popup(timeout=8000) as popup_info:
                            target_download_link.click(force=True)
                        popup_page = popup_info.value
                        if popup_page:
                            popup_page.close()
                            logger.info("  ✓ Popup handled and closed")
                    except PlaywrightTimeout:
                        # No popup created — direct download
                        logger.info("  ℹ No popup detected, proceeding with direct download")

                download = download_info.value
                suggested_name = download.suggested_filename

                if not suggested_name or "." not in suggested_name:
                    suggested_name = f"Shriram_{target_year}_{target_month:02d}.xlsx"

                save_path = download_folder / suggested_name
                download.save_as(save_path)
                logger.info(f"  ✓ Saved: {suggested_name}")
                return save_path

            except Exception as e:
                logger.error(f"  ✗ Download failed: {str(e)[:150]}")
                raise

        finally:
            if browser:
                browser.close()
            if pw:
                pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Shriram Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g. 2025)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    args = parser.parse_args()

    downloader = ShriramDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"✅ Success: Downloaded file")
    elif status == "skipped":
        logger.success(f"✅ Success: Month already complete")
    elif status == "not_published":
        logger.info(f"ℹ️  Info: Month not yet published")
    else:
        logger.error(f"❌ Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
