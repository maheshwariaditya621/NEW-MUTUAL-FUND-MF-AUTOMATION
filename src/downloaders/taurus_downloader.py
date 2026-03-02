# src/downloaders/taurus_downloader.py

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


class TaurusDownloader(BaseDownloader):
    """
    Taurus Mutual Fund - Portfolio Downloader
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    YEAR_MAP = {
        "2026": "567", "2025": "558", "2024": "514", "2023": "473",
        "2022": "456", "2021": "427", "2020": "418", "2019": "293",
        "2018": "57", "2017": "58", "2016": "59", "2015": "60"
    }
    
    MONTH_ID_MAP = {
        "January": "281", "February": "282", "March": "283", "April": "284",
        "May": "285", "June": "286", "July": "287", "August": "288",
        "September": "289", "October": "290", "November": "291", "December": "292"
    }

    YEAR_SELECTOR_BASE = "select[id^='edit-field-monthly-portfolio-target-id']"
    MONTH_SELECTOR_BASE = "select[id^='edit-field-month-target-id']"

    def __init__(self):
        super().__init__("Taurus Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "taurus"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "TAURUS",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        with open(marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)

    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str):
        corrupt_base = Path(f"data/raw/{self.AMC_NAME}/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)
        corrupt_target = corrupt_base / f"{year}_{month:02d}"
        if corrupt_target.exists():
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            corrupt_target = corrupt_target.parent / f"{corrupt_target.name}__{ts}"
        
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("TAURUS", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        month_short = month_name[:3]
        
        logger.info("=" * 60)
        logger.info(f"TAURUS: {year}-{month:02d}")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Taurus: {year}-{month:02d} files already downloaded.")
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
                files_downloaded = self._run_download_flow(year, month, month_name, month_short, target_dir)
                if files_downloaded == 0:
                    self.notifier.notify_not_published("TAURUS", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("TAURUS", year, month, files_downloaded=files_downloaded, duration=duration)
                return {"status": "success", "files_downloaded": files_downloaded}
            except Exception as e:
                last_error = str(e)
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("TAURUS", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, month_short: str, download_folder: Path) -> int:
        url = "https://taurusmutualfund.com/monthly-portfolio"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            try:
                browser = pw.chromium.launch(
                    headless=False,
                    channel="msedge",
                    args=["--no-sandbox", "--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--disable-gpu"]
                )
            except:
                browser = pw.chromium.launch(headless=False,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-blink-features=AutomationControlled"])

            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
                viewport={"width": 1280, "height": 800},
                accept_downloads=True
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            page.goto(url, wait_until="load", timeout=90000)
            time.sleep(5)

            # Dismiss Modal
            modal = page.locator("button, input, a").filter(has_text=re.compile("I am not a us person", re.I)).first
            if modal.count() > 0 and modal.is_visible(timeout=3000):
                modal.click(force=True)
                time.sleep(3)

            # 2. Select Year
            year_val = self.YEAR_MAP.get(str(target_year))
            month_val = self.MONTH_ID_MAP.get(month_name)
            if not year_val or not month_val: return 0

            logger.info(f"Selecting Year ({target_year})...")
            y_sel = page.locator(self.YEAR_SELECTOR_BASE).first
            y_sel.wait_for(state="visible", timeout=20000)
            y_sel.click()
            time.sleep(1)
            y_sel.select_option(value=year_val)
            page.evaluate(f'document.querySelector("{self.YEAR_SELECTOR_BASE}").dispatchEvent(new Event("change", {{ "bubbles": true }}))')
            time.sleep(1)
            page.keyboard.press("Enter")
            time.sleep(5)
            
            # 3. Select Month
            logger.info(f"Selecting Month ({month_name})...")
            m_sel = page.locator(self.MONTH_SELECTOR_BASE).first
            m_sel.wait_for(state="visible", timeout=30000)
            m_sel.click()
            time.sleep(1)
            m_sel.select_option(value=month_val)
            page.evaluate(f'document.querySelector("{self.MONTH_SELECTOR_BASE}").dispatchEvent(new Event("change", {{ "bubbles": true }}))')
            time.sleep(1)
            page.keyboard.press("Enter")
            
            time.sleep(15)
            page.screenshot(path="taurus_selection_proof.png")

            # 4. Collect Metadata
            logger.info(f"Collecting links...")
            res = page.locator(".view-content").first
            links_loc = res.locator("a") if res.count() > 0 else page.locator("a").filter(has_text=re.compile("Taurus", re.I))
            
            items = []
            seen = set()
            for lnk in links_loc.all():
                try:
                    txt = lnk.inner_text().strip()
                    if len(txt) < 5 or "navigation" in txt.lower(): continue
                    if txt in seen: continue
                    seen.add(txt)
                    items.append(txt)
                except: continue
            
            logger.info(f"  Found {len(items)} items.")

            success_count = 0
            for i, txt in enumerate(items):
                try:
                    safe_name = re.sub(r'[^a-zA-Z0-9]', '_', txt)
                    safe_name = re.sub(r'_+', '_', safe_name).strip('_')
                    
                    logger.info(f"  [{i+1}/{len(items)}] {txt}")

                    # Broad locator strategy
                    # Find all links in the view content again to ensure freshness
                    links_in_view = page.locator(".view-content a").all()
                    lnk = None
                    for l in links_in_view:
                        if txt in l.inner_text():
                            lnk = l
                            break
                    
                    if not lnk:
                        # Fallback to direct text search if view-content fails
                        lnk = page.locator("a").filter(has_text=txt).first

                    if lnk.count() == 0: continue

                    # Scroll and click
                    try:
                        lnk.scroll_into_view_if_needed(timeout=3000)
                        time.sleep(0.5)
                    except: pass
                    
                    try:
                        with page.expect_download(timeout=60000) as dinfo:
                            # Use standard force click as user wants to see it click the name
                            # Force bypasses 'not visible' checks but still clicks the element center
                            lnk.click(force=True)
                        
                        dl = dinfo.value
                        fname = dl.suggested_filename
                        
                        # Handle generic filenames by prefixing with scheme/month info
                        if fname.lower() in ["portfolio.pdf", "monthly_portfolio.pdf", "download.pdf", "portfolio.xlsx", "portfolio.xls"]:
                            fname = f"TAURUS_{safe_name}_{month_short}_{target_year}_{fname}"
                            
                        dl.save_as(download_folder / fname)
                        logger.info(f"    ✓ Saved: {fname}")
                        success_count += 1
                        time.sleep(2)
                    except Exception as e:
                        logger.error(f"    ✗ {str(e)[:50]}")
                        if "closed" in str(e).lower(): raise e
                except Exception as e:
                     if "closed" in str(e).lower(): raise e

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
    downloader = TaurusDownloader()
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
