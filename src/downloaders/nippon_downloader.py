# src/downloaders/nippon_downloader.py

import os
import io
import time
import json
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin
import requests
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


class NipponDownloader(BaseDownloader):
    """
    Nippon India Mutual Fund - Monthly Portfolio Downloader.
    
    Extracts monthly portfolio disclosure files directly from the statutory disclosure page
    via lightweight HTTP requests and BeautifulSoup DOM traversal without requiring Playwright
    or browser automation.
    """

    BASE_URL = "https://mf.nipponindiaim.com"
    DISCLOSURES_URL = "https://mf.nipponindiaim.com/investor-service/downloads/factsheet-portfolio-and-other-disclosures"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
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

    DATE_IN_LABEL_REGEX = re.compile(
        r'\b(january|february|march|april|may|june|july|august|september|october|november|december|'
        r'jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\b[^\d]*(\b20\d\d\b)',
        re.IGNORECASE,
    )

    DATE_IN_FILENAME_REGEX = re.compile(
        r'(\d{1,2})[-_.]([A-Za-z]+|\d{1,2})[-_.](\d{2,4})',
        re.IGNORECASE,
    )

    def __init__(self):
        super().__init__("Nippon India Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "nippon"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "NIPPON",
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
        
        logger.warning(f"NIPPON: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error(
            amc="NIPPON",
            year=year,
            month=month,
            error_type="Corruption Recovery",
            reason=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )

    @staticmethod
    def _clean_text(text: Optional[str]) -> str:
        """Cleans text by stripping zero-width spaces (\\u200b) and normalizing whitespace."""
        if not text:
            return ""
        text = re.sub(r"[\u200b\u200c\u200d\u200e\u200f\ufeff\xa0]", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    def _parse_date_info(self, label: str, href: str) -> Tuple[Optional[int], Optional[int]]:
        """Parses (year, month) from label or href."""
        year = None
        month = None

        m = self.DATE_IN_LABEL_REGEX.search(label)
        if m:
            m_str = m.group(1).lower()
            y_str = m.group(2)
            month = self.MONTH_MAP.get(m_str)
            try:
                year = int(y_str)
            except ValueError:
                year = None

        if not (year and month):
            fn = os.path.basename(href.split("?")[0])
            m_fn = self.DATE_IN_FILENAME_REGEX.search(fn)
            if m_fn:
                m_str, y_str = m_fn.group(2).lower(), m_fn.group(3)
                if m_str in self.MONTH_MAP:
                    month = self.MONTH_MAP[m_str]
                elif m_str.isdigit() and 1 <= int(m_str) <= 12:
                    month = int(m_str)
                try:
                    y = int(y_str)
                    if y < 100:
                        y += 2000
                    year = y
                except ValueError:
                    pass

        return year, month

    def _get_monthly_portfolio_link(self, session: requests.Session, year: int, month: int) -> Optional[Dict[str, Any]]:
        """
        Fetches disclosure page HTML and locates the matching monthly portfolio record for (year, month).
        """
        resp = session.get(self.DISCLOSURES_URL, headers=self.DEFAULT_HEADERS, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        for li in soup.find_all("li"):
            lhs = li.find("label", class_="lhsLbl") or li.find("label")
            if not lhs:
                continue

            clean_lbl = self._clean_text(lhs.get_text())
            if "monthly portfolio" not in clean_lbl.lower():
                continue

            a = li.find("a", href=True)
            if not a:
                continue

            href = a["href"].strip()
            if not any(href.lower().endswith(ext) for ext in [".xls", ".xlsx"]):
                continue

            rec_year, rec_month = self._parse_date_info(clean_lbl, href)
            if rec_year == year and rec_month == month:
                full_url = urljoin(self.BASE_URL, href)
                filename = os.path.basename(href.split("?")[0])
                return {
                    "label": clean_lbl,
                    "url": full_url,
                    "filename": filename,
                }

        return None

    def _is_valid_excel_content(self, content: bytes) -> bool:
        """Validates that bytes represent a valid Excel file and not an HTML error page."""
        if len(content) < 1000:
            return False
        prefix = content[:300].lower()
        if b"<html" in prefix or b"<!doctype html" in prefix or b"<head" in prefix:
            return False
        return content.startswith(b"PK\x03\x04") or content.startswith(b"\xd0\xcf\x11\xe0")

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        month_name = self.MONTH_NAMES.get(month, f"Month-{month}")

        logger.info("=" * 60)
        logger.info("NIPPON INDIA MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # 1) Idempotency Check
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                logger.info(f"Nippon: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
                self.consolidate_downloads(year, month)
                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {
                    "amc": "Nippon",
                    "year": year,
                    "month": month,
                    "status": "skipped",
                    "reason": "already_downloaded",
                    "duration": duration,
                }
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json marker")

        self.ensure_directory(str(target_dir))

        # 2) Download Logic with Retry
        last_error = ""
        session = requests.Session()

        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"NIPPON: [DRY RUN] Would download {month_name} {year}")
                    duration = time.time() - start_time
                    return {"amc": "Nippon", "year": year, "month": month, "status": "success", "dry_run": True}

                logger.info(f"Querying Nippon disclosure page for {year}-{month:02d}...")
                portfolio_info = self._get_monthly_portfolio_link(session, year, month)

                if not portfolio_info:
                    duration = time.time() - start_time
                    logger.warning(f"NIPPON: {year}-{month:02d} not yet published.")
                    self.notifier.notify_not_published("NIPPON", year, month)
                    if target_dir.exists() and not any(target_dir.iterdir()):
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"amc": "Nippon", "year": year, "month": month, "status": "not_published"}

                file_url = portfolio_info["url"]
                filename = portfolio_info["filename"]
                dest_file = target_dir / filename

                logger.info(f"Downloading portfolio file from {file_url}...")
                resp = session.get(file_url, headers=self.DEFAULT_HEADERS, timeout=45)
                resp.raise_for_status()

                content = resp.content
                if not self._is_valid_excel_content(content):
                    raise ValueError(f"Downloaded content from {file_url} is not a valid Excel file.")

                with open(dest_file, "wb") as f:
                    f.write(content)

                logger.info(f"Saved: {dest_file.name} ({len(content):,} bytes)")

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("NIPPON", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] Nippon download completed: {filename} in {duration:.2f}s")
                return {
                    "amc": "Nippon",
                    "year": year,
                    "month": month,
                    "status": "success",
                    "files_downloaded": 1,
                    "file_path": str(dest_file),
                    "duration": duration,
                }

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])
                continue

        # 3) Final Failure
        if target_dir.exists() and not (target_dir / "_SUCCESS.json").exists():
            shutil.rmtree(target_dir, ignore_errors=True)

        duration = time.time() - start_time
        self.notifier.notify_error("NIPPON", year, month, error_type="Download Failure", reason=last_error[:100])
        return {
            "amc": "Nippon",
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration,
        }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Nippon India Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = NipponDownloader()
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

    print(json.dumps(result))
