# src/downloaders/unifi_downloader.py

import os
import time
import json
import shutil
import re
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
from urllib.parse import urljoin, unquote

import requests
import openpyxl
import pandas as pd
from bs4 import BeautifulSoup

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


class UnifiDownloader(BaseDownloader):
    """
    Unifi Mutual Fund - Monthly Portfolio Downloader

    URL: https://unifimf.com/statutorydocuments/
    Features:
    - Pure requests + BeautifulSoup (no browser/Playwright required).
    - Discovers all active scheme tabs dynamically from "Monthly Portfolio Disclosure".
    - Scheme-level monthly portfolio workbooks (.xlsx / .xls).
    - Idempotency via _SUCCESS.json and automatic consolidation into merged workbook.
    """

    PAGE_URL = "https://unifimf.com/statutorydocuments/"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    ZIP_MAGIC = b"PK\x03\x04"
    OLE_MAGIC = b"\xD0\xCF\x11\xE0"

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

    def __init__(self):
        super().__init__("Unifi Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "unifi"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Unifi",
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
        self.notifier.notify_error("Unifi", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_abbr = self.MONTH_ABBR[month]
        month_full = self.MONTH_FULL[month]

        logger.info("=" * 60)
        logger.info(f"UNIFI MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_abbr})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Unifi: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"Duration: {duration:.2f}s")
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
                    self.notifier.notify_not_published("Unifi", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("Unifi", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Unifi", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _discover_monthly_portfolios(self, session: requests.Session, year: int, month: int, month_abbr: str, month_full: str) -> List[Dict[str, Any]]:
        logger.info("Fetching Unifi statutory documents page...")
        resp = session.get(self.PAGE_URL, headers=self.HEADERS, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Locate Monthly Portfolio Disclosure accordion
        monthly_sec = None
        for acc in soup.find_all("div", class_="umf__inacc"):
            head = acc.find("div", class_="umf__inahead")
            if head and "Monthly Portfolio Disclosure" in head.get_text(strip=True):
                monthly_sec = acc.find("div", class_="umf__inacnt")
                break

        if not monthly_sec:
            logger.error("Monthly Portfolio Disclosure section not found on page")
            return []

        menu_items = monthly_sec.find_all("li", class_="tab_menu_item")
        last_day = calendar.monthrange(year, month)[1]
        date_pattern_1 = re.compile(rf"\b{month_full}\b.*?\b{year}\b", re.IGNORECASE)
        date_pattern_2 = re.compile(rf"\b{month_abbr}\b.*?\b{year}\b", re.IGNORECASE)
        date_str_numeric = f"{last_day:02d}{month:02d}{year}"

        discovered = []
        for li in menu_items:
            a = li.find("a")
            if not a:
                continue
            tab_name = a.get_text(strip=True)
            if "notice" in tab_name.lower():
                continue

            panel_id = a.get("aria-controls")
            pane = monthly_sec.find("div", id=panel_id) if panel_id else None
            if not pane:
                continue

            for doc_a in pane.find_all("a", href=True):
                href = doc_a["href"].strip()
                if not href or href.startswith("#") or href.startswith("javascript:"):
                    continue

                full_url = urljoin(self.PAGE_URL, href)
                title = doc_a.get_text(strip=True)

                is_match = False
                if date_pattern_1.search(title) or date_pattern_2.search(title):
                    is_match = True
                elif date_str_numeric in full_url or f"{month:02d}{year}" in full_url:
                    is_match = True

                if is_match:
                    raw_filename = Path(unquote(full_url.split("?")[0])).name
                    discovered.append({
                        "scheme": tab_name,
                        "title": title,
                        "url": full_url,
                        "filename": raw_filename,
                    })
                    break  # One file per scheme per month

        return discovered

    def _run_download_flow(self, target_year: int, target_month: int, month_abbr: str, month_full: str, download_folder: Path) -> int:
        session = requests.Session()
        records = self._discover_monthly_portfolios(session, target_year, target_month, month_abbr, month_full)

        logger.info(f"Discovered {len(records)} monthly portfolio record(s) for {month_abbr} {target_year}")
        if not records:
            return 0

        files_downloaded = 0
        for idx, rec in enumerate(records, 1):
            url = rec["url"]
            scheme = rec["scheme"]
            raw_filename = rec["filename"]

            ext = Path(raw_filename).suffix.lower()
            if not ext:
                ext = ".xlsx"

            safe_scheme = re.sub(r"[^\w\-_.]", "_", scheme)
            filename = f"UNIFI_{target_year}_{target_month:02d}_{safe_scheme}_{raw_filename}"
            target_path = download_folder / filename

            logger.info(f"  [{idx}/{len(records)}] Downloading: {scheme} ({rec['title']})")
            logger.info(f"      URL: {url}")
            logger.info(f"      Saving to: {filename}")

            try:
                with session.get(url, headers=self.HEADERS, stream=True, timeout=60) as r:
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

                # Validate magic bytes
                with open(target_path, "rb") as f:
                    magic = f.read(8)

                is_xlsx = magic.startswith(self.ZIP_MAGIC)
                is_xls = magic.startswith(self.OLE_MAGIC)

                if not is_xlsx and not is_xls:
                    logger.warning(f"    [WARN] Unusual magic bytes: {magic[:4].hex()}")

                # Validate openability
                sheet_count = 0
                if is_xlsx:
                    wb = openpyxl.load_workbook(target_path, read_only=True)
                    sheet_count = len(wb.sheetnames)
                    wb.close()
                else:
                    xls_file = pd.ExcelFile(target_path)
                    sheet_count = len(xls_file.sheet_names)

                logger.info(f"    [OK] Validated {filename}: {sheet_count} sheet(s), {file_size:,} bytes")
                files_downloaded += 1

            except Exception as e:
                logger.error(f"    [FAIL] Error downloading {filename}: {e}")
                if target_path.exists():
                    target_path.unlink(missing_ok=True)

        return files_downloaded


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = UnifiDownloader()
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
