# src/downloaders/mirae_asset_downloader.py

import os
import re
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urljoin
import requests

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


class MiraeAssetDownloader(BaseDownloader):
    """
    Mirae Asset Mutual Fund - Monthly Portfolio Downloader.
    
    Extracts monthly portfolio disclosure files directly from the Mirae Asset
    GetDownloadsData REST API via lightweight HTTP requests without requiring
    Playwright or browser automation.
    """

    BASE_URL = "https://www.miraeassetmf.co.in"
    API_ENDPOINT = "https://www.miraeassetmf.co.in/AjaxService/GetDownloadsData"

    DEFAULT_HEADERS = {
        "Content-Type": "application/json; charset=UTF-8",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.miraeassetmf.co.in",
        "Referer": "https://www.miraeassetmf.co.in/downloads/portfolio",
    }

    MONTH_MAP = {
        "january": 1, "jan": 1,
        "february": 2, "feb": 2,
        "march": 3, "mar": 3,
        "april": 4, "apr": 4,
        "may": 5,
        "june": 6, "jun": 6,
        "july": 7, "jul": 7,
        "august": 8, "aug": 8,
        "september": 9, "sep": 9, "sept": 9,
        "october": 10, "oct": 10,
        "november": 11, "nov": 11,
        "december": 12, "dec": 12,
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    DATE_REGEX = re.compile(
        r'as on\s+(?:(\d{1,2})(?:st|nd|rd|th)?\s+)?([A-Za-z]+)\s+(\d{4})',
        re.IGNORECASE,
    )

    def __init__(self):
        super().__init__("Mirae Asset Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "mirae_asset"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "MIRAE_ASSET",
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
        
        logger.warning(f"MIRAE_ASSET: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("MIRAE_ASSET", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _parse_title(self, title: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
        """Parses title to extract (year, month, scheme_name)."""
        if not title:
            return None, None, None

        year = None
        month = None
        scheme_name = None

        m = self.DATE_REGEX.search(title)
        if m:
            month_str = m.group(2).lower()
            year_str = m.group(3)
            month = self.MONTH_MAP.get(month_str)
            try:
                year = int(year_str)
            except ValueError:
                year = None

        if " for " in title:
            scheme_name = title.split(" for ", 1)[-1].strip()
        elif " - " in title:
            scheme_name = title.split(" - ", 1)[-1].strip()
        else:
            scheme_name = title.strip()

        return year, month, scheme_name

    def _get_monthly_portfolios(
        self,
        session: requests.Session,
        year: int,
        month: int,
        pgsize: int = 100,
        max_pages: int = 40,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves all monthly portfolio records for the given year and month.
        Paginates through GetDownloadsData API until target records are collected
        or older records are observed.
        """
        matched_records = []
        pgno = 1
        target_found = False

        while pgno <= max_pages:
            payload = {
                "request": {
                    "modulename": "portfolio_tab1",
                    "pgno": pgno,
                    "pgsize": pgsize,
                }
            }

            resp = session.post(
                self.API_ENDPOINT,
                json=payload,
                headers=self.DEFAULT_HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            return_code = str(data.get("ReturnCode", ""))
            if return_code != "0":
                err_msg = data.get("ErrorMsg") or data.get("ReturnMsg")
                logger.warning(f"MIRAE_ASSET API non-zero ReturnCode {return_code}: {err_msg}")
                break

            items = data.get("Data") or []
            data_count = data.get("DataCount", 0)

            if not items:
                break

            page_older = 0
            for item in items:
                title = item.get("Title") or ""
                rec_year, rec_month, scheme_name = self._parse_title(title)

                if rec_year == year and rec_month == month:
                    target_found = True
                    rel_url = item.get("URL") or ""
                    full_url = urljoin(self.BASE_URL, rel_url)
                    matched_records.append({
                        "id": item.get("Id"),
                        "title": title,
                        "scheme_name": scheme_name,
                        "url": full_url,
                        "relative_url": rel_url,
                    })
                elif rec_year is not None and rec_month is not None:
                    if (rec_year < year) or (rec_year == year and rec_month < month):
                        page_older += 1

            # Early termination: reverse chronological ordering
            if target_found and page_older > 0:
                logger.debug("Encountered records older than target month. Terminating pagination.")
                break

            if not target_found and page_older == len(items):
                logger.debug("All records on page are older than target month. Not published.")
                break

            if pgno * pgsize >= data_count:
                break

            pgno += 1

        return matched_records

    def _is_valid_excel_content(self, content: bytes) -> bool:
        """Validates that bytes represent a valid Excel file and not an HTML error page."""
        if len(content) < 500:
            return False
        prefix = content[:200].lower()
        if b"<html" in prefix or b"<!doctype html" in prefix or b"<head" in prefix:
            return False
        return content.startswith(b"PK\x03\x04") or content.startswith(b"\xd0\xcf\x11\xe0")

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        month_name = self.MONTH_NAMES.get(month, f"Month-{month}")

        logger.info("=" * 60)
        logger.info(f"MIRAE ASSET MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # 1) Idempotency Check
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                logger.info(f"Mirae Asset: {year}-{month:02d} files already downloaded.")
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

        # 2) Download with retry
        last_error = ""
        session = requests.Session()

        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"MIRAE_ASSET: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                logger.info(f"Querying Mirae Asset API for {year}-{month:02d} portfolios...")
                portfolios = self._get_monthly_portfolios(session, year, month)

                if not portfolios:
                    duration = time.time() - start_time
                    logger.warning(f"MIRAE_ASSET: No portfolios found for {month_name} {year} (not yet published).")
                    self.notifier.notify_not_published("MIRAE_ASSET", year, month)
                    if target_dir.exists() and not any(target_dir.iterdir()):
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published", "amc": "MIRAE_ASSET", "year": year, "month": month}

                logger.info(f"Discovered {len(portfolios)} portfolio records for {year}-{month:02d}. Starting download...")

                files_downloaded = 0
                for idx, item in enumerate(portfolios, 1):
                    url = item["url"]
                    orig_filename = os.path.basename(item["relative_url"].split("?")[0])
                    if not orig_filename or "." not in orig_filename:
                        safe_name = re.sub(r'[^\w\-_\.]', '_', item.get("scheme_name", f"scheme_{idx}"))
                        orig_filename = f"{safe_name}.xlsx"

                    dest_file = target_dir / orig_filename

                    # Skip if file already exists and is valid
                    if dest_file.exists() and dest_file.stat().st_size > 500:
                        files_downloaded += 1
                        continue

                    # Download file
                    resp = session.get(url, headers=self.DEFAULT_HEADERS, timeout=30)
                    if resp.status_code != 200:
                        logger.error(f"Download HTTP {resp.status_code} for {url}")
                        continue

                    content = resp.content
                    if not self._is_valid_excel_content(content):
                        logger.error(f"Invalid Excel or HTML response received for {url}")
                        continue

                    with open(dest_file, "wb") as f:
                        f.write(content)

                    files_downloaded += 1
                    if idx % 10 == 0 or idx == len(portfolios):
                        logger.info(f"Progress: {files_downloaded}/{len(portfolios)} files downloaded.")

                if files_downloaded == 0:
                    raise RuntimeError("Failed to download any valid portfolio files.")

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("MIRAE_ASSET", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] MIRAE_ASSET download completed: {files_downloaded} files in {duration:.2f}s")
                return {
                    "status": "success",
                    "files_downloaded": files_downloaded,
                    "duration": duration,
                }

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists() and not (target_dir / "_SUCCESS.json").exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("MIRAE_ASSET", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = MiraeAssetDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"[INFO] Info: Month not yet published")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
