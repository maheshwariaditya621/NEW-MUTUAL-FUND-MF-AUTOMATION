# src/downloaders/tata_downloader.py

import os
import time
import json
import shutil
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


class TataDownloader(BaseDownloader):
    """
    Tata Mutual Fund - Portfolio Downloader
    
    URL: https://www.tatamutualfund.com/schemes-related/portfolio
    Handles declaration modal and Monthly frequency selection
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Tata Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "tata"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "TATA",
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
        
        logger.warning(f"TATA: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("TATA", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("TATA MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Tata: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"TATA: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"TATA: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("TATA", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("TATA", year, month, files_downloaded=1, duration=duration)
                logger.success(f"✅ TATA download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("TATA", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        url = "https://www.tatamutualfund.com/schemes-related/portfolio"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                accept_downloads=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            logger.info(f"Navigating to {url}...")
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            time.sleep(10)
            logger.info("  ✓ Page loaded")

            # Check for 403 error
            if "403 ERROR" in page.content():
                logger.error("  ✗ Detected 403 ERROR page")
                return None

            # Handle declaration modal
            logger.info("Handling declaration modal...")
            continue_btn = page.locator("button:has-text('Continue')")
            if continue_btn.count() > 0:
                continue_btn.click()
                time.sleep(3)
                logger.info("  ✓ Clicked 'Continue'")

            # Select 'Monthly' frequency
            logger.info("Selecting 'Monthly' frequency...")
            monthly_tab = page.locator("div, button").filter(has_text="Monthly").first
            if monthly_tab.count() > 0:
                monthly_tab.click()
                time.sleep(5)
                logger.info("  ✓ Selected 'Monthly'")

            # Open the year accordion using JavaScript for more reliable clicking
            logger.info(f"Opening accordion for year {target_year}...")
            
            # Use JavaScript to find and click the exact accordion button
            js_code = f"""
            (async () => {{
                // Find all buttons
                const buttons = Array.from(document.querySelectorAll('button'));
                // Find the button with exact text match
                const targetButton = buttons.find(b => b.textContent.trim() === 'For the year {target_year}');
                
                if (!targetButton) {{
                    return {{success: false, message: 'Button not found'}};
                }}
                
                // Scroll into view
                targetButton.scrollIntoView({{behavior: 'smooth', block: 'center'}});
                await new Promise(r => setTimeout(r, 1000));
                
                // Click the button
                targetButton.click();
                await new Promise(r => setTimeout(r, 3000));
                
                return {{success: true, message: 'Clicked'}};
            }})()
            """
            
            result = page.evaluate(js_code)
            if not result.get("success"):
                logger.warning(f"  ✗ Year accordion not found for {target_year}")
                return None
            
            time.sleep(3)  # Additional wait for accordion to fully expand
            logger.info(f"  ✓ Opened year {target_year} accordion")

            # Find the download link
            logger.info(f"Searching for portfolio link for {month_name}...")
            
            # Look for "Portfolio as on" link with target month and year
            # Format: "Portfolio as on 31st December, 2024"
            # Try multiple approaches to find the link
            links = page.locator("a").filter(has_text="Portfolio as on").all()
            
            logger.info(f"  Found {len(links)} total 'Portfolio as on' links")
            
            target_link = None
            for link in links:
                txt = link.text_content()
                # Check if both month name and year are in the text
                if month_name in txt and str(target_year) in txt:
                    # Check if link is visible
                    if link.is_visible():
                        target_link = link
                        break
            
            if not target_link:
                logger.warning(f"  ✗ Link not found for {month_name} {target_year}")
                logger.info("  Available visible links:")
                for link in links:
                    if link.is_visible():
                        logger.info(f"    - {link.text_content().strip()}")
                return None

            link_text = target_link.text_content().strip()
            logger.info(f"  ✓ Found: {link_text}")
            target_link.scroll_into_view_if_needed()
            time.sleep(1)

            # Download the file
            logger.info("Downloading file...")
            with page.expect_download(timeout=60000) as download_info:
                target_link.click()
            
            download = download_info.value
            filename = download.suggested_filename
            save_path = download_folder / filename
            
            download.save_as(save_path)
            logger.info(f"  ✓ Saved: {filename}")
            
            return save_path

        finally:
            if browser: browser.close()
            if pw: pw.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = TataDownloader()
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
