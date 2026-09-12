# src/downloaders/sundaram_downloader.py

import os
import time
import json
import shutil
import re
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Any
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup
import openpyxl

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


class SundaramDownloader(BaseDownloader):
    """
    Sundaram Mutual Fund - Portfolio Downloader
    
    URL: https://www.sundarammutual.com/Monthly-Fortnightly-Adhoc-Portfolios
    Downloads monthly portfolio workbooks (Equity & Fixed Income) using pure requests + BeautifulSoup
    via the site's ASP.NET AjaxPro endpoint.
    """

    PAGE_URL = "https://www.sundarammutual.com/Monthly-Fortnightly-Adhoc-Portfolios"
    BASE_URL = "https://www.sundarammutual.com"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "*/*",
    }

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

    ZIP_MAGIC = b"PK\x03\x04"

    def __init__(self):
        super().__init__("Sundaram Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "sundaram"
        self._ajax_url: Optional[str] = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Sundaram",
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
        self.notifier.notify_error("Sundaram", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    @staticmethod
    def get_financial_year(year: int, month: int) -> str:
        """Indian Financial Year (April - March)"""
        if month >= 4:
            return f"{year}-{year + 1}"
        else:
            return f"{year - 1}-{year}"

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_abbr = self.MONTH_ABBR[month]
        month_full = self.MONTH_FULL[month]

        logger.info("=" * 60)
        logger.info(f"SUNDARAM MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_abbr})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Sundaram: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
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
                    self.notifier.notify_not_published("Sundaram", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("Sundaram", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Sundaram", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _discover_monthly_portfolios(self, session: requests.Session, year: int, month: int, month_full: str) -> List[Dict[str, Any]]:
        # 1. Fetch main disclosures page to discover the dynamic .ashx handler URL
        logger.info(f"Fetching Sundaram statutory disclosures page to find Ajax handler...")
        resp_page = session.get(self.PAGE_URL, headers=self.HEADERS, timeout=30)
        resp_page.raise_for_status()

        soup_page = BeautifulSoup(resp_page.text, "html.parser")
        ashx_path = None
        for s in soup_page.find_all("script", src=True):
            if "Monthly_Fortnightly_Adhoc_Portfolios" in s["src"]:
                ashx_path = s["src"]
                break

        if not ashx_path:
            raise RuntimeError("Could not find AjaxPro handler script on Sundaram disclosures page")

        ajax_url = urljoin(self.BASE_URL, ashx_path) + "?_method=GetCategory&_session=no"
        logger.info(f"Discovered Ajax endpoint: {ajax_url}")

        # 2. Query AjaxPro endpoint for Monthly portfolios
        resp_ajax = session.post(
            ajax_url,
            data="Catid=Monthly",
            headers={**self.HEADERS, "Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp_ajax.raise_for_status()

        raw_html = resp_ajax.text.strip()
        if raw_html.startswith("'") and raw_html.endswith("'"):
            raw_html = raw_html[1:-1].replace(r"\'", "'")

        soup_ajax = BeautifulSoup(raw_html, "html.parser")
        fy_str = self.get_financial_year(year, month)

        # 3. Locate Financial Year accordion
        target_accordion = None
        for acc in soup_ajax.find_all("div", class_=lambda c: c and "accordion-item" in c):
            btn = acc.find("button", class_=lambda c: c and "accordion-button" in c)
            if btn and fy_str in btn.get_text(strip=True):
                target_accordion = acc
                break

        if not target_accordion:
            logger.warning(f"Sundaram: FY accordion '{fy_str}' not found")
            return []

        # 4. Locate Month Tab
        target_pane = None
        for nav_btn in target_accordion.find_all("button", class_=lambda c: c and "nav-link" in c):
            btn_text = nav_btn.get_text(strip=True)
            if btn_text.lower().startswith(month_full.lower()):
                target_id = nav_btn.get("data-bs-target", "").replace("#", "")
                target_pane = target_accordion.find("div", id=target_id)
                break

        if not target_pane:
            logger.warning(f"Sundaram: Month tab '{month_full}' not found in FY {fy_str}")
            return []

        # 5. Extract links
        discovered = []
        for a in target_pane.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            full_url = urljoin(self.BASE_URL, href)
            title = a.get_text(strip=True)
            filename = Path(unquote(full_url.split("?")[0])).name

            discovered.append({
                "title": title,
                "url": full_url,
                "filename": filename,
                "fy": fy_str,
            })

        return discovered

    def _run_download_flow(self, target_year: int, target_month: int, month_abbr: str, month_full: str, download_folder: Path) -> int:
        session = requests.Session()
        records = self._discover_monthly_portfolios(session, target_year, target_month, month_full)

        logger.info(f"Discovered {len(records)} monthly portfolio record(s) for {month_full} {target_year}")
        if not records:
            return 0

        files_downloaded = 0
        for idx, rec in enumerate(records, 1):
            url = rec["url"]
            filename = rec["filename"]
            title = rec["title"]
            target_path = download_folder / filename

            logger.info(f"  [{idx}/{len(records)}] Downloading: {title}")
            logger.info(f"      URL: {url}")
            logger.info(f"      Saving to: {filename}")

            try:
                with session.get(url, headers={"User-Agent": self.HEADERS["User-Agent"]}, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    with open(target_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            if chunk:
                                f.write(chunk)

                file_size = target_path.stat().st_size
                if file_size < 1000:
                    target_path.unlink(missing_ok=True)
                    logger.error(f"    [FAIL] File too small ({file_size} bytes)")
                    continue

                # Validate XLSX Magic Bytes (PK\x03\x04)
                with open(target_path, "rb") as f:
                    magic = f.read(4)

                if magic != self.ZIP_MAGIC:
                    logger.warning(f"    [WARN] Magic bytes {magic.hex()} (expected {self.ZIP_MAGIC.hex()})")

                # Validate openability with openpyxl
                try:
                    wb = openpyxl.load_workbook(target_path, read_only=True)
                    sheet_count = len(wb.sheetnames)
                    wb.close()
                    logger.info(f"    [OK] Validated {filename}: {sheet_count} sheet(s), {file_size:,} bytes")
                except Exception as e:
                    logger.warning(f"    [WARN] openpyxl load check: {e} (keeping file)")

                files_downloaded += 1

            except Exception as e:
                logger.error(f"    [FAIL] Failed downloading {filename}: {e}")

        return files_downloaded


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = SundaramDownloader()
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
