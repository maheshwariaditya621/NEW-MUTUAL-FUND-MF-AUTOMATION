# src/downloaders/baroda_downloader.py

import os
import re
import time
import json
import shutil
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Any
import requests
from bs4 import BeautifulSoup
import openpyxl

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier
from src.utils.file_validator import validate_and_fix_extension

# Import downloader config
try:
    from src.config.downloader_config import (
        DRY_RUN, MAX_RETRIES, RETRY_BACKOFF
    )
except ImportError:
    DRY_RUN = False
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]


class BarodaDownloader(BaseDownloader):
    """
    Baroda BNP Paribas Mutual Fund - Monthly Portfolio Downloader.
    
    Downloads the official monthly consolidated all-funds portfolio spreadsheet
    from https://www.barodabnpparibasmf.in/downloads/monthly-portfolio-scheme
    using pure HTTP requests and HTML/AJAX parsing (no Playwright required).
    """

    AMC_NAME = "baroda"
    BASE_URL = "https://www.barodabnpparibasmf.in"
    PAGE_URL = "https://www.barodabnpparibasmf.in/downloads/monthly-portfolio-scheme"
    AJAX_URL = "https://www.barodabnpparibasmf.in/ajax-load-more-documents"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self, timeout: int = 30):
        super().__init__("Baroda BNP Paribas Mutual Fund")
        self.notifier = get_notifier()
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)
        self.csrf_token: Optional[str] = None
        self.total_cnt: str = "437"
        self.category: str = "17"
        self._initialized: bool = False
        self._initial_html: str = ""
        self._year_cache: Dict[str, List[Dict[str, Any]]] = {}

    def _init_session(self):
        """Fetch main page to establish session cookies and extract CSRF token."""
        if self._initialized:
            return

        resp = self.session.get(self.PAGE_URL, timeout=self.timeout)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to fetch {self.PAGE_URL}: HTTP {resp.status_code}")

        self._initial_html = resp.text
        soup = BeautifulSoup(resp.text, "html.parser")

        csrf_input = soup.find("input", {"name": "csrf_test_name"})
        if csrf_input and csrf_input.get("value"):
            self.csrf_token = csrf_input["value"]
        else:
            self.csrf_token = self.session.cookies.get("csrf_cookie_name")

        total_cnt_input = soup.find("input", {"id": "total_cnt"})
        if total_cnt_input and total_cnt_input.get("value"):
            self.total_cnt = total_cnt_input["value"]

        cat_input = soup.find("input", {"id": "category"})
        if cat_input and cat_input.get("value"):
            self.category = cat_input["value"]

        self._initialized = True

    @staticmethod
    def _parse_card_items(html_snippet: str) -> List[Dict[str, Any]]:
        """Parse <li> elements into document metadata dictionaries."""
        soup = BeautifulSoup(html_snippet, "html.parser")
        items = []
        for li in soup.find_all("li"):
            title_el = li.find("p", class_="file-name")
            date_el = li.find("div", class_="uploadDate")
            link_el = li.find("a", attrs={"download": True}) or li.find("a", class_="orange-text")

            if title_el and link_el and link_el.get("href"):
                title = title_el.get_text(strip=True)
                url = link_el["href"]
                date_str = date_el.get_text(strip=True) if date_el else ""
                filename = url.split("/")[-1]
                is_all_funds = ("all funds" in title.lower()) or ("all fund" in title.lower())

                items.append({
                    "title": title,
                    "url": url,
                    "filename": filename,
                    "upload_date": date_str,
                    "is_all_funds": is_all_funds,
                })
        return items

    def _fetch_entries_for_year(self, year_str: str, target_month: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch monthly portfolio entries for a given year string using AJAX pagination.
        Caches results per year string.
        """
        if year_str in self._year_cache:
            return self._year_cache[year_str]

        self._init_session()

        all_entries: List[Dict[str, Any]] = []
        seen_urls = set()

        ajax_headers = {
            "Origin": self.BASE_URL,
            "Referer": self.PAGE_URL,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "*/*",
        }

        # Step 1: Initial year fetch (pagination = 0)
        payload = {
            "csrf_test_name": self.csrf_token,
            "cnt": self.total_cnt,
            "pagination": "0",
            "send_category": self.category,
            "send_year": year_str,
            "remaining_cnt": "0",
        }

        try:
            resp = self.session.post(self.AJAX_URL, data=payload, headers=ajax_headers, timeout=self.timeout)
            resp_json = resp.json()
            cards = self._parse_card_items(resp_json.get("data", ""))
            for item in cards:
                if item["url"] not in seen_urls:
                    seen_urls.add(item["url"])
                    all_entries.append(item)
            next_page = resp_json.get("pagination", 1)
            status = resp_json.get("status", "Y")
        except Exception as e:
            # Fallback for default year if AJAX fails
            cards = self._parse_card_items(self._initial_html)
            for item in cards:
                if item["url"] not in seen_urls:
                    seen_urls.add(item["url"])
                    all_entries.append(item)
            next_page = 1
            status = "Y"

        # Step 2: Paginate through AJAX batches until target all-funds file is found
        page = int(next_page)
        target_month_name = self.MONTH_NAMES.get(target_month, "").lower() if target_month else ""

        while status == "Y" and page < 40:
            # Check if all-funds file for target month is already found
            if target_month_name:
                has_target_all_funds = any(
                    it["is_all_funds"] and target_month_name in it["title"].lower()
                    for it in all_entries
                )
                if has_target_all_funds:
                    break

            payload = {
                "csrf_test_name": self.csrf_token,
                "cnt": self.total_cnt,
                "pagination": str(page),
                "send_category": self.category,
                "send_year": year_str,
                "remaining_cnt": "0",
            }
            try:
                r = self.session.post(self.AJAX_URL, data=payload, headers=ajax_headers, timeout=self.timeout)
                data = r.json()
                page_cards = self._parse_card_items(data.get("data", ""))
                for item in page_cards:
                    if item["url"] not in seen_urls:
                        seen_urls.add(item["url"])
                        all_entries.append(item)

                status = data.get("status", "N")
                page = int(data.get("pagination", page + 1))
            except Exception:
                break

        self._year_cache[year_str] = all_entries
        return all_entries

    def _find_all_funds_document(self, year: int, month: int) -> Optional[Dict[str, Any]]:
        """
        Locate the single consolidated 'all funds' portfolio document for target year and month.
        Checks primary year and handles year boundary fallback (e.g. December).
        """
        month_name = self.MONTH_NAMES.get(month)
        if not month_name:
            raise ValueError(f"Invalid month: {month}")

        # Primary search in target year
        year_str = str(year)
        entries = self._fetch_entries_for_year(year_str, target_month=month)

        target_pattern = re.compile(rf"all funds as on.*{month_name}.*{year}", re.I)
        fallback_pattern = re.compile(rf"{month_name}.*{year}", re.I)

        for item in entries:
            if item["is_all_funds"] and (target_pattern.search(item["title"]) or fallback_pattern.search(item["title"])):
                return item

        # If month is December (12), check next year's heading as Baroda occasionally files it under Year+1
        if month == 12:
            next_year_str = str(year + 1)
            entries_next = self._fetch_entries_for_year(next_year_str, target_month=month)
            for item in entries_next:
                if item["is_all_funds"] and fallback_pattern.search(item["title"]):
                    return item

        return None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        tmp_marker_path = target_dir / "_SUCCESS.json.tmp"
        
        marker_data = {
            "amc": "BARODA",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        with open(tmp_marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        tmp_marker_path.rename(marker_path)
        logger.info(f"Created completion marker: {marker_path.name}")

    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str):
        corrupt_base = Path(f"data/raw/{self.AMC_NAME}/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)
        corrupt_target = corrupt_base / f"{year}_{month:02d}"
        if corrupt_target.exists():
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            corrupt_target = corrupt_target.parent / f"{corrupt_target.name}__{ts}"
        
        logger.warning(f"BARODA: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("BARODA", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES.get(month, str(month))
        
        logger.info("=" * 60)
        logger.info("BARODA BNP PARIBAS MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency check
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Baroda: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {
                    "amc": "baroda",
                    "year": year,
                    "month": month,
                    "files_downloaded": 0,
                    "status": "skipped",
                    "reason": "already_downloaded",
                    "duration": duration
                }
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing success marker")

        self.ensure_directory(str(target_dir))

        if DRY_RUN:
            logger.info(f"BARODA: [DRY RUN] Would download {month_name} {year}")
            duration = time.time() - start_time
            return {
                "amc": "baroda",
                "year": year,
                "month": month,
                "files_downloaded": 0,
                "status": "dry_run",
                "duration": duration
            }

        try:
            logger.info(f"Searching for Baroda BNP Paribas all-funds portfolio: {month_name} {year}...")
            doc = self._find_all_funds_document(year, month)

            if not doc:
                logger.warning(f"BARODA: No consolidated all-funds portfolio found for {month_name} {year}")
                self.notifier.notify_not_published("BARODA", year, month)
                if target_dir.exists():
                    shutil.rmtree(target_dir, ignore_errors=True)
                duration = time.time() - start_time
                return {
                    "amc": "baroda",
                    "year": year,
                    "month": month,
                    "files_downloaded": 0,
                    "status": "not_published",
                    "duration": duration
                }

            file_url = doc["url"]
            filename = doc["filename"]
            save_path = target_dir / filename

            logger.info(f"Found all-funds portfolio: '{doc['title']}' -> {filename}")
            logger.info(f"Downloading from: {file_url}")

            download_headers = {
                "User-Agent": self.DEFAULT_HEADERS["User-Agent"],
                "Accept": "*/*",
            }

            dl_ok = False
            for attempt in range(MAX_RETRIES + 1):
                try:
                    resp = self.session.get(file_url, headers=download_headers, stream=True, timeout=(15, 90))
                    resp.raise_for_status()

                    with open(save_path, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=65536):
                            if chunk:
                                f.write(chunk)
                    dl_ok = True
                    break
                except (requests.Timeout, requests.RequestException) as err:
                    if attempt < MAX_RETRIES:
                        backoff = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                        logger.warning(f"Download attempt {attempt + 1} failed: {err}. Retrying in {backoff}s...")
                        time.sleep(backoff)
                    else:
                        raise

            if not dl_ok:
                raise RuntimeError(f"Failed downloading {file_url}")

            file_size = save_path.stat().st_size
            if file_size == 0:
                raise ValueError(f"Downloaded file is 0 bytes: {save_path.name}")

            # Validate file signature & fix extension if needed
            # Baroda serves OpenXML files (.xlsx) with .xls extension
            with open(save_path, "rb") as f:
                magic = f.read(8)
            is_openxml = magic.startswith(b"PK\x03\x04")
            is_ole2 = magic.startswith(b"\xd0\xcf\x11\xe0")

            if not (is_openxml or is_ole2):
                logger.warning(f"File {save_path.name} has unexpected magic bytes: {magic[:4]!r}")

            if is_openxml and save_path.name.lower().endswith(".xls"):
                corrected_path = save_path.with_suffix(".xlsx")
                if corrected_path.exists():
                    corrected_path.unlink()
                save_path.rename(corrected_path)
                save_path = corrected_path
                logger.info(f"Corrected extension from .xls to .xlsx: {save_path.name}")

            # Validate openpyxl read
            if is_openxml:
                try:
                    wb = openpyxl.load_workbook(save_path, read_only=True)
                    sheet_count = len(wb.sheetnames)
                    wb.close()
                    logger.info(f"Validated workbook ({sheet_count} scheme sheets present)")
                except Exception as e:
                    logger.warning(f"Workbook validation warning: {e}")

            logger.success(f"Saved ({save_path.stat().st_size:,} bytes): {save_path.name}")

            # Atomic success marker (1 file downloaded)
            self._create_success_marker(target_dir, year, month, 1)

            # Consolidate downloads into merged excels
            self.consolidate_downloads(year, month)

            duration = time.time() - start_time
            self.notifier.notify_success("BARODA", year, month, files_downloaded=1, duration=duration)
            logger.success(f"[SUCCESS] BARODA download completed: {save_path.name}")
            return {
                "amc": "baroda",
                "year": year,
                "month": month,
                "files_downloaded": 1,
                "status": "success",
                "file": str(save_path),
                "duration": duration
            }

        except requests.HTTPError as e:
            error_msg = f"API request failed: HTTP {e.response.status_code if e.response is not None else e}"
            logger.error(error_msg)
            if target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
            self.notifier.notify_error("BARODA", year, month, "HTTP Error", error_msg)
            duration = time.time() - start_time
            return {
                "amc": "baroda",
                "year": year,
                "month": month,
                "status": "failed",
                "reason": error_msg,
                "duration": duration
            }
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Download failed: {error_msg}")
            if target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
            self.notifier.notify_error("BARODA", year, month, "Download Exception", error_msg)
            duration = time.time() - start_time
            return {
                "amc": "baroda",
                "year": year,
                "month": month,
                "status": "failed",
                "reason": error_msg,
                "duration": duration
            }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Baroda BNP Paribas Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g. 2026)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    args = parser.parse_args()

    if args.month < 1 or args.month > 12:
        logger.error("Invalid month. Must be between 1 and 12.")
        raise SystemExit(1)

    downloader = BarodaDownloader()
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
