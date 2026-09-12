# src/downloaders/hsbc_downloader.py

import io
import os
import re
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

try:
    from src.config.downloader_config import DRY_RUN, MAX_RETRIES, RETRY_BACKOFF
except ImportError:
    DRY_RUN = False
    MAX_RETRIES = 3
    RETRY_BACKOFF = [5, 15, 30]


class HSBCDownloader(BaseDownloader):
    """
    HSBC Mutual Fund Downloader.
    
    Extracts monthly scheme portfolio files from the official Information Library
    under the 'Fund portfolios' section via lightweight HTTP requests and BeautifulSoup.
    """
    
    AMC_NAME = "hsbc"
    LIBRARY_URL = "https://www.assetmanagement.hsbc.co.in/en/mutual-funds/investor-resources/information-library"
    BASE_URL = "https://www.assetmanagement.hsbc.co.in"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
        "december": 12, "dec": 12,
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    DATE_IN_TITLE_REGEX = re.compile(
        r'\b(?:(\d{1,2})(?:st|nd|rd|th)?\s+)?(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+(\d{4})\b',
        re.IGNORECASE,
    )

    EXPECTED_FILE_COUNT_MIN = 20
    EXPECTED_FILE_COUNT_MAX = 80

    def __init__(self):
        super().__init__(self.AMC_NAME)
        self.notifier = get_notifier()

    def _fetch_html(self) -> str:
        """
        Fetch HTML from Information Library page with retries.
        """
        max_attempts = 4
        backoff = [5, 10, 20, 30]

        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"Fetching HTML from HSBC Information Library (attempt {attempt}/{max_attempts})...")
                response = requests.get(self.LIBRARY_URL, headers=self.DEFAULT_HEADERS, timeout=60)
                response.raise_for_status()
                html = response.text

                if len(html) < 50000:
                    raise Exception(f"Suspiciously small response ({len(html)} chars) - likely an error page")

                logger.success(f"HTML fetched successfully on attempt {attempt} ({len(html):,} chars)")
                return html

            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt < max_attempts:
                    wait_time = backoff[attempt - 1]
                    logger.warning(f"Network error (attempt {attempt}/{max_attempts}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed to fetch HSBC website after {max_attempts} attempts: {e}")
            except Exception as e:
                if attempt < max_attempts:
                    wait_time = backoff[attempt - 1]
                    logger.warning(f"Error on attempt {attempt}/{max_attempts}: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed to fetch HSBC website after {max_attempts} attempts: {e}")

        raise RuntimeError("Failed to fetch HTML from HSBC Information Library")

    def _locate_fund_portfolios_table(self, soup: BeautifulSoup):
        """
        Finds the specific 'Fund portfolios' table in the HTML.
        Excludes 'Half Yearly Portfolios', 'Fortnightly Debt Portfolio', etc.
        """
        for tag in soup.find_all(["h2", "h3", "h4", "a"]):
            text = tag.get_text(strip=True).lower()
            if text == "fund portfolios":
                table = tag.find_next("table")
                if table:
                    return table

        for heading in soup.find_all(class_=re.compile(r'accordion__heading', re.I)):
            if "fund portfolios" in heading.get_text(strip=True).lower():
                table = heading.find_next("table")
                if table:
                    return table

        return None

    def _parse_entry(self, title: str, href: str) -> Optional[Tuple[int, int, int, str]]:
        """Parses date (year, month, day) and clean scheme name from title/href."""
        m = self.DATE_IN_TITLE_REGEX.search(title)
        if not m:
            m = self.DATE_IN_TITLE_REGEX.search(href.replace("-", " ").replace("_", " "))

        if not m:
            return None

        day_str = m.group(1)
        month_str = m.group(2).lower()
        year_str = m.group(3)

        mo = self.MONTH_MAP.get(month_str)
        if not mo:
            return None

        try:
            yr = int(year_str)
        except ValueError:
            return None

        day = int(day_str) if day_str else 30
        if day == 30 and mo in (1, 3, 5, 7, 8, 10, 12):
            day = 31
        elif day == 30 and mo == 2:
            day = 28

        date_substr = m.group(0)
        scheme_name = title.replace(date_substr, "").strip()
        scheme_name = re.sub(r'[\s\-_,]+$', '', scheme_name).strip()

        return yr, mo, day, scheme_name

    def _get_monthly_portfolios(self, html: str, year: int, month: int) -> List[Dict[str, Any]]:
        """
        Extracts all monthly scheme portfolio files for (year, month) from the Fund portfolios table.
        """
        soup = BeautifulSoup(html, "html.parser")
        table = self._locate_fund_portfolios_table(soup)
        if not table:
            raise RuntimeError("Could not locate 'Fund portfolios' table in HSBC HTML")

        matching_records = []
        seen_urls = set()

        for tr in table.find_all("tr"):
            a = tr.find("a", href=True)
            if not a:
                continue

            href = a["href"].strip()
            if not any(href.lower().endswith(ext) for ext in [".xlsx", ".xls"]):
                continue

            title = a.get_text(" ", strip=True)
            parsed = self._parse_entry(title, href)
            if not parsed:
                continue

            rec_year, rec_month, rec_day, scheme_name = parsed
            if rec_year == year and rec_month == month:
                full_url = urljoin(self.BASE_URL, href)
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                filename = os.path.basename(href.split("?")[0])
                matching_records.append({
                    "scheme": scheme_name,
                    "title": title,
                    "url": full_url,
                    "filename": filename,
                })

        logger.info(f"Discovered {len(matching_records)} scheme portfolio files for {year}-{month:02d}.")
        return matching_records

    def _download_file(self, url: str, file_path: Path) -> bool:
        """Download single file with retry logic and OpenXML validation."""
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = requests.get(url, headers=self.DEFAULT_HEADERS, timeout=60)
                response.raise_for_status()
                content = response.content

                if len(content) < 1000:
                    logger.error(f"File too small ({len(content)} bytes) for {url}")
                    return False

                prefix = content[:300].lower()
                if b"<html" in prefix or b"<!doctype html" in prefix or b"<head" in prefix:
                    logger.error(f"Received HTML error page instead of XLSX for {url}")
                    return False

                if not content.startswith(b"PK\x03\x04"):
                    logger.warning(f"File signature not standard OpenXML ZIP for {url}")

                with open(file_path, "wb") as f:
                    f.write(content)

                return True

            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt < MAX_RETRIES:
                    wait_time = RETRY_BACKOFF[attempt]
                    logger.warning(f"Download error: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to download after {MAX_RETRIES + 1} attempts")
                    return False
            except requests.HTTPError as e:
                if 400 <= e.response.status_code < 500:
                    logger.error(f"Client error {e.response.status_code} downloading {url}")
                    return False
                elif attempt < MAX_RETRIES:
                    wait_time = RETRY_BACKOFF[attempt]
                    time.sleep(wait_time)
                else:
                    return False
            except Exception as e:
                logger.error(f"Unexpected download error: {e}")
                return False

        return False

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        """Create _SUCCESS.json marker atomically."""
        marker_data = {
            "amc": self.AMC_NAME, "year": year, "month": month,
            "files_downloaded": file_count, "timestamp": datetime.now().isoformat()
        }
        marker_path = target_dir / "_SUCCESS.json"
        tmp_marker_path = target_dir / "_SUCCESS.json.tmp"

        with open(tmp_marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)

        tmp_marker_path.rename(marker_path)
        logger.info(f"Created success marker: {marker_path.name}")

    def _move_to_corrupt(self, target_dir: Path, year: int, month: int):
        """Move incomplete folder to _corrupt/."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        corrupt_dir = Path(f"data/raw/{self.AMC_NAME}/_corrupt/{target_dir.name}_{timestamp}")
        corrupt_dir.parent.mkdir(parents=True, exist_ok=True)

        logger.warning(f"Moving incomplete folder to: {corrupt_dir}")
        shutil.move(str(target_dir), str(corrupt_dir))

        self.notifier.notify_warning(
            amc="HSBC", year=year, month=month,
            warning_type="Corruption Recovery",
            message=f"Incomplete download detected and moved to quarantine"
        )

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        month_name = self.MONTH_NAMES.get(month, f"Month-{month}")

        logger.info("=" * 70)
        logger.info("HSBC MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 70)

        if DRY_RUN:
            logger.info("[DRY RUN MODE] No actual downloads")
            return {
                "amc": self.AMC_NAME, "year": year, "month": month,
                "status": "success", "files_downloaded": 0, "duration": time.time() - start_time
            }

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        success_marker = target_dir / "_SUCCESS.json"

        # 1) Idempotency check
        if success_marker.exists():
            logger.info(f"HSBC: {year}-{month:02d} files already downloaded.")
            logger.info("Verifying consolidation/merged files...")
            self.consolidate_downloads(year, month)
            duration = time.time() - start_time
            logger.info("[SUCCESS] Month already complete — UPDATED")
            return {
                "amc": self.AMC_NAME, "year": year, "month": month,
                "status": "skipped", "reason": "already_downloaded", "duration": duration
            }

        if target_dir.exists() and not success_marker.exists():
            logger.warning(f"Incomplete folder detected: {target_dir}")
            self._move_to_corrupt(target_dir, year, month)

        target_dir.mkdir(parents=True, exist_ok=True)

        try:
            html = self._fetch_html()
            portfolios = self._get_monthly_portfolios(html, year, month)

            if not portfolios:
                duration = time.time() - start_time
                logger.info(f"No files found for {year}-{month:02d} (not yet published)")
                shutil.rmtree(target_dir, ignore_errors=True)
                self.notifier.notify_not_published(amc="HSBC", year=year, month=month)
                return {
                    "amc": self.AMC_NAME, "year": year, "month": month,
                    "status": "not_published", "duration": duration
                }

            files_downloaded = 0
            for item in portfolios:
                filename = item["filename"]
                url = item["url"]
                file_path = target_dir / filename

                if file_path.exists() and file_path.stat().st_size > 1000:
                    files_downloaded += 1
                    continue

                logger.info(f"Downloading: {filename}")
                if self._download_file(url, file_path):
                    files_downloaded += 1
                    if files_downloaded % 10 == 0 or files_downloaded == len(portfolios):
                        logger.info(f"Progress: {files_downloaded}/{len(portfolios)} files downloaded.")
                else:
                    raise Exception(f"Failed to download: {filename}")

            self._create_success_marker(target_dir, year, month, files_downloaded)
            self.consolidate_downloads(year, month)

            duration = time.time() - start_time
            self.notifier.notify_success("HSBC", year, month, files_downloaded=files_downloaded, duration=duration)

            logger.info("=" * 70)
            logger.success(f"[SUCCESS] Downloaded {files_downloaded} files in {duration:.2f}s")
            logger.info("=" * 70)

            return {
                "amc": self.AMC_NAME, "year": year, "month": month,
                "status": "success", "files_downloaded": files_downloaded, "duration": duration
            }

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"HSBC Download failed: {e}")
            self.notifier.notify_error("HSBC", year, month, error_type="Download Error", reason=str(e)[:100])
            if target_dir.exists() and not (target_dir / "_SUCCESS.json").exists():
                shutil.rmtree(target_dir, ignore_errors=True)
            return {
                "amc": self.AMC_NAME, "year": year, "month": month,
                "status": "failed", "reason": str(e), "duration": duration
            }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="HSBC Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    if args.month < 1 or args.month > 12:
        logger.error(f"Invalid month: {args.month}. Must be between 1 and 12.")
        exit(1)

    downloader = HSBCDownloader()
    result = downloader.download(year=args.year, month=args.month)

    if result["status"] == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif result["status"] == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete (Consolidation refreshed)")
    elif result["status"] == "not_published":
        logger.info(f"[INFO] Info: Month not yet published")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")

    print(json.dumps(result))
