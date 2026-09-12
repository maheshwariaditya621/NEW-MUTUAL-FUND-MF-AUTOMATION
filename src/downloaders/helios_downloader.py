# src/downloaders/helios_downloader.py

import os
import re
import time
import json
import shutil
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
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


class HeliosDownloader(BaseDownloader):
    """
    Helios Mutual Fund - Monthly Portfolio Downloader.

    Fetches the official monthly portfolio disclosures directly via HTTP requests
    and HTML parsing without browser automation (Playwright/Selenium).

    URL: https://www.heliosmf.in/portfolio-disclosure
    """

    AMC_NAME = "helios"
    PAGE_URL = "https://www.heliosmf.in/portfolio-disclosure"
    BASE_URL = "https://www.heliosmf.in"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.heliosmf.in/",
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

    def __init__(self, timeout: int = 45):
        super().__init__("Helios Mutual Fund")
        self.notifier = get_notifier()
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)
        self._cached_catalog: Optional[List[Dict[str, Any]]] = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "HELIOS",
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

        logger.warning(f"HELIOS: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("HELIOS", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _parse_month_year(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        """Extracts month (1-12) and 4-digit year from string."""
        text_lower = text.lower()

        # Year
        yr_match = re.search(r'\b(20\d\d)\b', text_lower)
        year = int(yr_match.group(1)) if yr_match else None

        # Month
        found_month = None
        for m_name, m_val in self.MONTH_MAP.items():
            if re.search(r'\b' + m_name + r'\b', text_lower):
                found_month = m_val
                break

        return found_month, year

    def fetch_portfolio_catalog(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        Fetches the portfolio disclosure page and parses all monthly portfolio links.
        """
        if self._cached_catalog is not None and not force_refresh:
            return self._cached_catalog

        logger.info(f"Fetching portfolio disclosure page: {self.PAGE_URL}...")
        resp = self.session.get(self.PAGE_URL, timeout=self.timeout)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        entries: List[Dict[str, Any]] = []

        # Find accordion section for "Monthly Portfolio"
        accordion = soup.find("div", class_="hlx-dl-accordion")
        monthly_cat = None

        if accordion:
            for cat in accordion.find_all("div", class_="hlx-dl-cat-item", recursive=False):
                header = cat.find(["button", "h2", "h3", "h4", "span"])
                if header and "monthly portfolio" in header.get_text(strip=True).lower():
                    monthly_cat = cat
                    break

        if monthly_cat:
            body = monthly_cat.find("div", class_="hlx-dl-cat-body")
            if body:
                scheme_items = body.find_all("div", class_="hlx-dl-cat-item", recursive=False)
                for sch in scheme_items:
                    sch_header = sch.find(["button", "h2", "h3", "h4", "span"])
                    scheme_name = sch_header.get_text(strip=True) if sch_header else "Unknown Scheme"

                    # Collect links under this scheme
                    file_links = sch.find_all("a", class_="hlx-dl-file")
                    for a in file_links:
                        href = a.get("href", "").strip()
                        if not href or not (href.lower().endswith(".xlsx") or href.lower().endswith(".xls")):
                            continue

                        full_url = urllib.parse.urljoin(self.BASE_URL, href)
                        text = a.get_text(" ", strip=True)
                        aria = a.get("aria-label", "")
                        filename = os.path.basename(urllib.parse.urlparse(href).path)
                        month, year = self._parse_month_year(f"{text} {aria} {filename}")

                        entries.append({
                            "scheme": scheme_name,
                            "month": month,
                            "year": year,
                            "title": text,
                            "aria_label": aria,
                            "href": href,
                            "url": full_url,
                            "filename": filename,
                        })
        else:
            # Fallback parser for non-standard HTML variations
            for a in soup.find_all("a"):
                href = a.get("href", "")
                if not any(ext in href.lower() for ext in [".xlsx", ".xls"]):
                    continue

                aria = a.get("aria-label", "")
                text = a.get_text(" ", strip=True)
                combined = f"{text} {aria} {href}".lower()

                if "fortnight" in combined or "half" in combined:
                    continue
                if "monthly" not in combined:
                    continue

                full_url = urllib.parse.urljoin(self.BASE_URL, href)
                filename = os.path.basename(urllib.parse.urlparse(href).path)
                month, year = self._parse_month_year(combined)

                sch_match = re.search(r'(Helios\s+[^,]+?Fund)', aria, re.IGNORECASE)
                scheme_name = sch_match.group(1).strip() if sch_match else "Helios Fund"

                entries.append({
                    "scheme": scheme_name,
                    "month": month,
                    "year": year,
                    "title": text,
                    "aria_label": aria,
                    "href": href,
                    "url": full_url,
                    "filename": filename,
                })

        logger.info(f"Discovered {len(entries)} monthly portfolio entries across all schemes")
        self._cached_catalog = entries
        return entries

    def _download_single_file(self, entry: Dict[str, Any], download_folder: Path) -> Path:
        """Downloads a single spreadsheet and verifies XLSX integrity."""
        url = entry["url"]
        scheme = entry["scheme"]
        raw_filename = entry["filename"]

        clean_filename = re.sub(r'[\\/*?:"<>|]', "_", raw_filename)
        save_path = download_folder / clean_filename

        # If duplicate filename exists, append clean scheme identifier
        if save_path.exists():
            clean_scheme = re.sub(r'[\\/*?:"<>| ]', "_", scheme)
            stem = save_path.stem
            ext = save_path.suffix
            save_path = download_folder / f"{stem}_{clean_scheme}{ext}"

        logger.info(f"Downloading: {scheme} -> {save_path.name}")
        logger.info(f"  URL: {url}")
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()

        content = resp.content
        size = len(content)
        if size == 0:
            raise ValueError(f"Downloaded 0 bytes for {save_path.name}")

        is_xlsx = content.startswith(b"PK\x03\x04")
        is_xls = content.startswith(b"\xd0\xcf\x11\xe0")
        if not (is_xlsx or is_xls):
            raise ValueError(f"Invalid spreadsheet magic bytes: {content[:8]}")

        with open(save_path, "wb") as f:
            f.write(content)

        # Inspect workbook integrity
        sheet_names = []
        try:
            wb = openpyxl.load_workbook(save_path, read_only=True)
            sheet_names = wb.sheetnames
            wb.close()
        except Exception as e:
            logger.warning(f"openpyxl inspection note: {e}")

        logger.info(f"  Saved: {save_path.name} ({size:,} bytes, sheets={sheet_names})")
        return save_path

    def _run_download_flow(
        self, target_year: int, target_month: int, month_name: str, download_folder: Path
    ) -> int:
        catalog = self.fetch_portfolio_catalog()
        matched = [
            p for p in catalog
            if p.get("year") == target_year and p.get("month") == target_month
        ]

        if not matched:
            logger.warning(f"HELIOS: No portfolio records found for {month_name} {target_year}")
            return 0

        logger.info(f"HELIOS: Found {len(matched)} scheme files for {month_name} {target_year}")
        downloaded_count = 0

        for idx, entry in enumerate(matched, 1):
            scheme_name = entry.get("scheme", "Unknown")
            logger.info(f"  [{idx}/{len(matched)}] {scheme_name}")
            try:
                self._download_single_file(entry, download_folder)
                downloaded_count += 1
            except Exception as dl_err:
                logger.error(f"    [FAIL] Download error for {entry.get('filename')}: {dl_err}")

        return downloaded_count

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES.get(month, f"Month {month}")

        logger.info("=" * 60)
        logger.info("HELIOS MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # Idempotency check
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Helios: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"HELIOS: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, target_dir)

                if files_downloaded == 0:
                    logger.warning(f"HELIOS: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("HELIOS", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success marker
                self._create_success_marker(target_dir, year, month, files_downloaded)

                # Consolidate raw files into merged excel
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success(
                    "HELIOS", year, month, files_downloaded=files_downloaded, duration=duration
                )
                logger.success(f"[SUCCESS] HELIOS download completed: {files_downloaded} files")
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
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("HELIOS", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = HeliosDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success("[SUCCESS] Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info("[INFO] Info: Month not yet published")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
