# src/downloaders/sbi_downloader.py

import os
import re
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple, Any
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


class SBIDownloader(BaseDownloader):
    """
    SBI Mutual Fund - Monthly Portfolio Downloader.

    Downloads the official monthly consolidated "All Schemes Monthly Portfolio"
    spreadsheet via direct POST API and HTML parsing without browser automation (Playwright/Selenium).

    API:
        POST https://www.sbimf.com/ajaxcall/CMS/GetSchemePortfolioSheets
        Payload: {"FundId": 0, "PSYear": "2026", "PSMonth": "August", "PSFrequency": "Monthly"}
    """

    AMC_NAME = "sbi"
    API_URL = "https://www.sbimf.com/ajaxcall/CMS/GetSchemePortfolioSheets"
    PORTFOLIOS_PAGE = "https://www.sbimf.com/portfolios"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "*/*",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.sbimf.com",
        "Referer": PORTFOLIOS_PAGE,
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self, timeout: int = 30):
        super().__init__("SBI Mutual Fund")
        self.notifier = get_notifier()
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "SBI",
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

        logger.warning(f"SBI: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("SBI", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _run_download_flow(
        self, target_year: int, target_month: int, month_name: str, download_folder: Path
    ) -> Optional[Path]:
        payload = {
            "FundId": 0,
            "PSYear": str(target_year),
            "PSMonth": month_name,
            "PSFrequency": "Monthly",
        }

        logger.info(f"Querying SBI MF API for {month_name} {target_year}...")
        resp = self.session.post(self.API_URL, json=payload, timeout=self.timeout)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.find_all("tr")

        consolidated_link = None
        consolidated_title = None

        for tr in rows:
            tds = tr.find_all("td")
            if not tds:
                continue

            text_col = tds[0].get_text(strip=True)
            if "no records found" in text_col.lower():
                continue

            # Prioritize the all-schemes monthly consolidated portfolio
            if "all schemes" in text_col.lower():
                links = tr.find_all("a")
                for a in links:
                    href = a.get("href", "").strip()
                    if href and (".xlsx" in href.lower() or ".xls" in href.lower()):
                        consolidated_link = href
                        consolidated_title = text_col
                        break
                if consolidated_link:
                    break

        if not consolidated_link:
            logger.warning(f"No consolidated 'All Schemes' portfolio found for {month_name} {target_year}")
            return None

        # Clean filename
        clean_url_name = consolidated_link.split("?")[0].split("/")[-1]
        if not (clean_url_name.endswith(".xlsx") or clean_url_name.endswith(".xls")):
            clean_title = re.sub(r'[\\/*?:"<>|]', "_", consolidated_title).strip()
            clean_url_name = f"{clean_title}.xlsx"

        save_path = download_folder / clean_url_name

        logger.info(f"Downloading consolidated portfolio: {consolidated_title}")
        logger.info(f"  URL: {consolidated_link}")

        r = self.session.get(consolidated_link, timeout=60)
        r.raise_for_status()

        content = r.content
        size = len(content)
        if size == 0:
            raise ValueError(f"Downloaded 0 bytes for {consolidated_title}")

        # Magic byte validation
        is_xlsx = content.startswith(b"PK\x03\x04")
        is_xls = content.startswith(b"\xd0\xcf\x11\xe0")
        if not (is_xlsx or is_xls):
            raise ValueError(f"Invalid spreadsheet format. Magic bytes: {content[:8]}")

        with open(save_path, "wb") as f:
            f.write(content)

        # Integrity verification
        try:
            wb = openpyxl.load_workbook(save_path, read_only=True)
            sheets_count = len(wb.sheetnames)
            wb.close()
            logger.info(f"  Validated XLSX: {sheets_count} sheets present")
        except Exception as e:
            logger.warning(f"  openpyxl inspection note: {e}")

        logger.info(f"  [OK] Saved: {clean_url_name} ({size:,} bytes)")
        return save_path

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        month_name = self.MONTH_NAMES.get(month, f"Month {month}")

        logger.info("=" * 60)
        logger.info("SBI MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"SBI: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {
                    "status": "skipped",
                    "reason": "already_downloaded",
                    "duration": duration,
                }
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"SBI: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)

                if not downloaded_path:
                    logger.warning(f"SBI: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("SBI", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)

                # Consolidate downloads (for SBI, the downloaded file is already the consolidated all-schemes file)
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("SBI", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] SBI download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("SBI", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = SBIDownloader()
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
