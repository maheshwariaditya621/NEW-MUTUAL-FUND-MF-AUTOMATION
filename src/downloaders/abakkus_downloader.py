# src/downloaders/abakkus_downloader.py

import os
import re
import time
import json
import shutil
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
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


class AbakkusDownloader(BaseDownloader):
    """
    Abakkus Mutual Fund - Monthly Portfolio Downloader.
    
    Extracts monthly portfolio disclosure files directly from statutory disclosures
    via lightweight HTTP requests without requiring browser automation.
    """

    BASE_URL = "https://www.abakkusmf.com"
    DISCLOSURES_URL = f"{BASE_URL}/statutory-disclosures.html"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
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
        "december": 12, "dec": 12
    }

    def __init__(self):
        super().__init__("Abakkus Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "abakkus"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "ABAKKUS",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        with open(marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        logger.info(f"Created completion marker: {marker_path.name}")

    def _parse_date_string(self, date_str: str) -> Optional[Dict[str, int]]:
        """Parses date strings like 'July 31, 2026', 'August 31, 2026', '31.01.2026'."""
        if not date_str:
            return None
        cleaned = date_str.strip()

        # Format 1: "July 31, 2026"
        m1 = re.search(r'([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})', cleaned)
        if m1:
            month_name = m1.group(1).lower()
            day = int(m1.group(2))
            year = int(m1.group(3))
            if month_name in self.MONTH_MAP:
                return {"year": year, "month": self.MONTH_MAP[month_name], "day": day}

        # Format 2: "31.01.2026"
        m2 = re.search(r'(\d{1,2})[./-](\d{1,2})[./-](\d{4})', cleaned)
        if m2:
            day = int(m2.group(1))
            month = int(m2.group(2))
            year = int(m2.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                return {"year": year, "month": month, "day": day}

        return None

    def _get_all_monthly_portfolios(self, session: requests.Session) -> List[Dict[str, Any]]:
        """
        Fetches statutory disclosures and returns list of all monthly portfolio entries.
        Filters out notice PDFs.
        """
        resp = session.get(self.DISCLOSURES_URL, headers=self.DEFAULT_HEADERS, timeout=25)
        resp.raise_for_status()
        html = resp.text
        portfolios = []

        # 1. Parse embedded JSON (`const verticals = [...]`)
        match = re.search(r'const\s+verticals\s*=\s*(\[\{.*?\}\]);', html, re.DOTALL)
        if match:
            try:
                verticals = json.loads(match.group(1))
                for v in verticals:
                    title = (v.get("title") or "").strip()
                    subtitle = (v.get("subTitle") or "").strip()

                    if "Notice for" in title or "Notice for" in subtitle:
                        continue

                    if "Monthly Portfolio" in title or "Monthly Portfolio" in subtitle:
                        for sec in v.get("sections", []):
                            for sub in sec.get("subSections", []):
                                for item in sub.get("items", []):
                                    item_title = (item.get("title") or "").strip()
                                    media = item.get("downloadMedia") or {}
                                    file_url = media.get("url") or item.get("downloadUrl")
                                    file_name = media.get("name") or (os.path.basename(file_url) if file_url else None)
                                    file_ext = (media.get("ext") or "").lower()

                                    if not file_url:
                                        continue

                                    # Only Excel files
                                    if not (file_ext in [".xls", ".xlsx"] or file_url.lower().endswith((".xls", ".xlsx"))):
                                        continue

                                    parsed_dt = self._parse_date_string(item_title) or self._parse_date_string(file_name)
                                    if not parsed_dt:
                                        continue

                                    full_url = urllib.parse.urljoin(self.BASE_URL, file_url)
                                    portfolios.append({
                                        "year": parsed_dt["year"],
                                        "month": parsed_dt["month"],
                                        "day": parsed_dt["day"],
                                        "title": item_title,
                                        "filename": file_name,
                                        "url": full_url
                                    })
            except Exception as err:
                logger.debug(f"[Abakkus] JSON extraction fallback: {err}")

        # 2. Fallback to DOM parsing if JSON was not available
        if not portfolios:
            soup = BeautifulSoup(html, "html.parser")
            headings = soup.find_all(re.compile(r'^(h[1-6]|div|p|span)$'), string=re.compile(r'Monthly\s+Portfolio', re.I))
            for h in headings:
                container = h.find_parent("div")
                if not container:
                    continue
                for a in container.find_all("a", href=True):
                    href = a["href"]
                    if not href.lower().endswith((".xls", ".xlsx")):
                        continue
                    text_content = (a.parent.get_text(strip=True) if a.parent else "") + " " + a.get_text(strip=True)
                    parsed_dt = self._parse_date_string(text_content) or self._parse_date_string(href)
                    if not parsed_dt:
                        continue
                    portfolios.append({
                        "year": parsed_dt["year"],
                        "month": parsed_dt["month"],
                        "day": parsed_dt["day"],
                        "title": text_content.strip(),
                        "filename": os.path.basename(href),
                        "url": urllib.parse.urljoin(self.BASE_URL, href)
                    })

        return portfolios

    def _is_valid_excel_content(self, content: bytes) -> bool:
        """Validates that bytes represent a valid Excel file and not an HTML error page."""
        if len(content) < 8:
            return False
        sample = content[:200].lower()
        if b"<html" in sample or b"<!doctype html" in sample or b"<head" in sample:
            return False
        return content.startswith(bytes.fromhex("d0cf11e0")) or content.startswith(bytes.fromhex("504b0304"))

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        
        logger.info("=" * 60)
        logger.info("ABAKKUS MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # 1) Idempotency Check
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                logger.info(f"Abakkus: {year}-{month:02d} files already downloaded.")
                self.consolidate_downloads(year, month)
                duration = time.time() - start_time
                return {
                    "status": "skipped", 
                    "reason": "already_downloaded",
                    "duration": duration
                }

        self.ensure_directory(str(target_dir))

        # 2) Download with retry
        last_error = ""
        session = requests.Session()

        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"Abakkus: [DRY RUN] Would download {month} {year}")
                    return {"amc": "ABAKKUS", "year": year, "month": month, "status": "success", "dry_run": True}

                all_portfolios = self._get_all_monthly_portfolios(session)
                matching = [p for p in all_portfolios if p["year"] == year and p["month"] == month]

                if not matching:
                    duration = time.time() - start_time
                    logger.warning(f"Abakkus: {year}-{month:02d} month-end portfolio not found or not yet published.")
                    if target_dir.exists() and not any(target_dir.iterdir()):
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"amc": "ABAKKUS", "year": year, "month": month, "status": "not_published"}

                # Select month-end file
                matching.sort(key=lambda x: x["day"], reverse=True)
                entry = matching[0]

                logger.info(f"Discovered Abakkus {year}-{month:02d} portfolio: {entry['title']} -> {entry['url']}")
                
                resp = session.get(entry["url"], headers=self.DEFAULT_HEADERS, timeout=40)
                resp.raise_for_status()

                content = resp.content
                if not self._is_valid_excel_content(content):
                    raise ValueError(f"Downloaded content from {entry['url']} is not a valid Excel file.")

                filename = entry["filename"] or f"CONSOLIDATED_ABAKKUS_{year}_{month:02d}.xls"
                local_path = target_dir / filename
                with open(local_path, "wb") as f:
                    f.write(content)

                logger.info(f"Successfully saved {local_path.name} ({len(content)} bytes)")

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("Abakkus", year, month, files_downloaded=1, duration=duration)
                
                return {
                    "amc": "ABAKKUS",
                    "year": year,
                    "month": month,
                    "status": "success",
                    "files_downloaded": 1,
                    "duration": duration,
                    "file_path": str(local_path)
                }

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed for Abakkus: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])
                continue

        # 3) Final Failure
        duration = time.time() - start_time
        self.notifier.notify_error("Abakkus", year, month, error_type="Download Failure", reason=last_error[:100])
        
        return {
            "amc": "ABAKKUS",
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration
        }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Abakkus Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = AbakkusDownloader()
    result = downloader.download(args.year, args.month)
    print(json.dumps(result, indent=2))
