# src/downloaders/union_downloader.py

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


class UnionDownloader(BaseDownloader):
    """
    Union Mutual Fund - Portfolio Downloader
    
    URL: https://www.unionmf.com/about-us/downloads
    Features:
    - Persistent Session for efficiency.
    - Chatbot and Modal bypass via JS injection.
    - Year/Month filter handling.
    - Pagination traversal ("Next" button).
    - Gold Standard compliance.
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
        super().__init__("Union Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "union"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Union",
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
        self.notifier.notify_error("Union", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        month_abbr = self.MONTH_ABBR[month]
        
        logger.info("=" * 60)
        logger.info(f"UNION MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"Union: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"{self.AMC_NAME}: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, month_abbr, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"{self.AMC_NAME}: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("Union", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("Union", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"✅ {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Union", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, month_abbr: str, download_folder: Path) -> int:
        url = "https://www.unionmf.com/about-us/downloads"

        pw = None
        browser = None
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=HEADLESS,
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

            logger.info(f"Navigating to Union Downloads page...")
            page.goto(url, wait_until="networkidle", timeout=90000)
            time.sleep(5)

            # Chatbot/Modal Bypass
            page.evaluate("""() => {
                const selectors = ['.chatbot-icon', '#chatbot', '.active-chatbot', '[class*="chatbot"]', '.u-chat-icon'];
                selectors.forEach(s => { const el = document.querySelector(s); if (el) el.style.display = 'none'; });
            }""")
            try: page.locator("button.close, .modal-header button[data-dismiss='modal']").first.click(timeout=5000)
            except: pass

            # Navigation
            factsheets = page.get_by_text("Factsheets & Portfolios", exact=True)
            if factsheets.count() > 0:
                factsheets.first.click()
                time.sleep(1)
            
            portfolios_tab = page.get_by_role("link", name="Portfolios", exact=True)
            if portfolios_tab.count() > 0: portfolios_tab.first.click()
            else: page.get_by_text("Portfolios", exact=True).first.click()
            
            logger.info("Waiting for portfolios to load...")
            time.sleep(10)

            # Filters
            page.wait_for_selector("select#yearFilter:visible", timeout=15000)
            year_select = page.locator("select#yearFilter:visible").first
            year_select.select_option(label=str(target_year))
            time.sleep(2)
            
            month_select = page.locator("select#monthFilter:visible").first
            month_select.select_option(label=month_name)
            time.sleep(8)

            # Pagination and Download
            success_count = 0
            page_num = 1
            processed_urls = set()
            
            while True:
                logger.info(f"  Processing page {page_num}...")
                page.wait_for_selector("div.factdownload_css:visible", timeout=20000)
                
                rows = page.locator("div.factdownload_css:visible").all()
                if not rows: break

                # Track first row to detect page change
                first_row_text_before = ""
                try: first_row_text_before = rows[0].locator("p.accord-desc-right").first.inner_text().strip()
                except: pass

                for row in rows:
                    try:
                        heading = row.locator("p.accord-desc-right")
                        if heading.count() == 0: continue
                        h_text = heading.inner_text().strip().replace('\n', ' ')
                        
                        if not re.search(r"Monthly", h_text, re.I) or str(target_year) not in h_text:
                            continue
                        
                        download_btn = row.locator("a").filter(has=row.locator("span.icon-dwonload")).first
                        if download_btn.count() == 0: download_btn = row.locator("a[href*='.xlsx'], a[href*='.pdf']").first
                        
                        if download_btn.count() > 0:
                            href = download_btn.get_attribute("href")
                            if href in processed_urls: continue
                                
                            # Scheme extraction
                            scheme_name = "Union_Scheme"
                            if "Monthly Portfolio Disclosure -" in h_text:
                                scheme_name = h_text.split("Monthly Portfolio Disclosure -")[-1].strip().split("-")[0].strip()
                            elif "Monthly Portfolio -" in h_text:
                                scheme_name = h_text.split("Monthly Portfolio -")[-1].strip().split("-")[0].strip()
                            else:
                                raw_scheme = h_text.split("-")[0].strip().replace("Union ", "")
                                scheme_name = raw_scheme.replace("Monthly Portfolio Disclosure", "").strip()

                            scheme_name = scheme_name.replace(" ", "_").replace("/", "_") or "CONSOLIDATED"

                            logger.info(f"    Downloading: {h_text[:50]}...")
                            try:
                                download_btn.evaluate("el => el.scrollIntoView({block: 'center'})")
                                with page.expect_download(timeout=60000) as dinfo:
                                    icon = download_btn.locator("span.icon-dwonload").first
                                    if icon.count() > 0: icon.click(force=True)
                                    else: download_btn.click(force=True)
                                
                                dl = dinfo.value
                                fname = dl.suggested_filename
                                
                                # Handle generic filenames by prefixing with scheme name
                                if fname.lower() in ["portfolio.pdf", "monthly_portfolio.pdf", "download.pdf", "portfolio.xlsx", "report.xlsx"]:
                                    fname = f"UNION_{scheme_name}_{month_abbr}_{target_year}_{fname}"
                                    
                                dl.save_as(download_folder / fname)
                                logger.info(f"      ✓ Saved: {fname}")
                                success_count += 1
                                processed_urls.add(href)
                                time.sleep(2)
                            except Exception as e:
                                logger.error(f"      ✗ Failed: {str(e)[:50]}")
                    except: continue

                # Pagination
                next_btn = page.locator("a").filter(has_text=re.compile(r"^Next$", re.I)).first
                if next_btn.count() > 0 and next_btn.is_visible():
                    is_disabled = next_btn.evaluate("el => el.closest('li').classList.contains('disabled')")
                    if not is_disabled:
                        next_btn.click(force=True)
                        time.sleep(5)
                        # Detect change
                        rows_now = page.locator("div.factdownload_css:visible").all()
                        if rows_now:
                            try:
                                text_now = rows_now[0].locator("p.accord-desc-right").first.inner_text().strip()
                                if text_now == first_row_text_before: break
                            except: break
                        else: break
                        page_num += 1
                    else: break
                else: break

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

    downloader = UnionDownloader()
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
