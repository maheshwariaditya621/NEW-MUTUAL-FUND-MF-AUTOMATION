# src/downloaders/tata_downloader.py

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


class TataDownloader(BaseDownloader):
    """
    Tata Mutual Fund - Monthly Portfolio Downloader.

    Downloads the official monthly consolidated portfolio spreadsheet via direct REST API
    without browser automation (Playwright/Selenium).

    API: GET https://prod-dist-api.tatamfdev.com/cms-data/api/CMSDATA_portfolio?type=monthly
    Webpage: https://www.tatamutualfund.com/schemes-related/portfolio
    """

    AMC_NAME = "tata"
    API_URL = "https://prod-dist-api.tatamfdev.com/cms-data/api/CMSDATA_portfolio?type=monthly"
    PAGE_URL = "https://www.tatamutualfund.com/schemes-related/portfolio"
    BASE_DOMAIN = "https://www.tatamutualfund.com"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.tatamutualfund.com",
        "Referer": "https://www.tatamutualfund.com/schemes-related/portfolio",
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

    def __init__(self, timeout: int = 30):
        super().__init__("Tata Mutual Fund")
        self.notifier = get_notifier()
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)
        self._cached_catalog: Optional[List[Dict[str, Any]]] = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "TATA",
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

        logger.warning(f"TATA: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("TATA", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _parse_month_year(self, item: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
        """Extracts month (1-12) and 4-digit year from API item."""
        doc_title = item.get("field_document_title", "")
        card_title = item.get("field_card_title_", "") or item.get("field_card_title", "")
        media_url = item.get("field_media_document", "")
        order_1 = item.get("field_order_1", "")

        # Year
        year = None
        if card_title.strip().isdigit() and len(card_title.strip()) == 4:
            year = int(card_title.strip())
        else:
            m = re.search(r'\b(20\d\d)\b', f"{doc_title} {media_url}")
            if m:
                year = int(m.group(1))

        # Month
        month = None
        text_to_search = f"{doc_title} {urllib.parse.unquote(media_url)}".lower()
        for m_name, m_num in self.MONTH_MAP.items():
            if re.search(r'\b' + m_name + r'\b', text_to_search):
                month = m_num
                break

        # Fallback to order_1 if valid 1-12
        if month is None and str(order_1).isdigit():
            o1 = int(order_1)
            if 1 <= o1 <= 12:
                month = o1

        return month, year

    def _normalize_document_url(self, raw_url: str) -> str:
        """Replaces betacms host with primary tatamutualfund.com domain to avoid redirect latency."""
        url = raw_url.strip()
        if url.startswith("https://betacms.tatamutualfund.com"):
            url = url.replace("https://betacms.tatamutualfund.com", self.BASE_DOMAIN)
        return url

    def fetch_catalog(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        Fetches the complete portfolio disclosure catalog from Tata CMS API.
        """
        if self._cached_catalog is not None and not force_refresh:
            return self._cached_catalog

        logger.info(f"Fetching Tata portfolio catalog from API: {self.API_URL}...")
        resp = self.session.get(self.API_URL, timeout=self.timeout)
        resp.raise_for_status()

        raw_items = resp.json()
        if not isinstance(raw_items, list):
            raise ValueError(f"Expected JSON list from API, got {type(raw_items)}")

        parsed_items: List[Dict[str, Any]] = []
        for item in raw_items:
            month, year = self._parse_month_year(item)
            raw_url = item.get("field_media_document", "").strip()
            if not raw_url:
                continue

            clean_url = self._normalize_document_url(raw_url)
            filename = os.path.basename(urllib.parse.urlparse(clean_url).path)
            decoded_filename = urllib.parse.unquote(filename)

            parsed_items.append({
                "year": year,
                "month": month,
                "month_name": self.MONTH_NAMES.get(month, "Unknown"),
                "document_title": item.get("field_document_title", ""),
                "section_flag": item.get("field_section_flag", ""),
                "order": item.get("field_order", ""),
                "order_1": item.get("field_order_1", ""),
                "raw_url": raw_url,
                "url": clean_url,
                "filename": decoded_filename,
            })

        logger.info(f"Discovered {len(parsed_items)} monthly portfolio entries across all historical years")
        self._cached_catalog = parsed_items
        return parsed_items

    def _run_download_flow(
        self, target_year: int, target_month: int, month_name: str, download_folder: Path
    ) -> Optional[Path]:
        """Discovers and downloads the consolidated monthly portfolio spreadsheet."""
        catalog = self.fetch_catalog()

        target_record = None
        for item in catalog:
            if item.get("year") == target_year and item.get("month") == target_month:
                target_record = item
                break

        if not target_record:
            logger.warning(f"TATA: No portfolio record found for {month_name} {target_year}")
            return None

        url = target_record["url"]
        clean_filename = re.sub(r'[\\/*?:"<>|]', "_", target_record["filename"])
        save_path = download_folder / clean_filename

        logger.info(f"Downloading Tata monthly portfolio for {month_name} {target_year}...")
        logger.info(f"  Title: {target_record['document_title']}")
        logger.info(f"  URL:   {url}")

        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()

        content = resp.content
        size = len(content)
        if size == 0:
            raise ValueError(f"Downloaded 0 bytes from {url}")

        is_xlsx = content.startswith(b"PK\x03\x04")
        is_xls = content.startswith(b"\xd0\xcf\x11\xe0")
        if not (is_xlsx or is_xls):
            raise ValueError(f"Invalid spreadsheet magic bytes: {content[:8]}")

        with open(save_path, "wb") as f:
            f.write(content)

        # Inspect workbook sheets
        sheet_names = []
        try:
            wb = openpyxl.load_workbook(save_path, read_only=True)
            sheet_names = wb.sheetnames
            wb.close()
        except Exception as e:
            logger.warning(f"openpyxl inspection note: {e}")

        logger.info(f"  [OK] Saved: {save_path.name} ({size:,} bytes, {len(sheet_names)} sheets)")
        return save_path

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES.get(month, f"Month {month}")

        logger.info("=" * 60)
        logger.info("TATA MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # Idempotency check
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Tata: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"TATA: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)

                if not downloaded_path:
                    logger.warning(f"TATA: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("TATA", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success marker
                self._create_success_marker(target_dir, year, month, 1)

                # Consolidate raw files into merged excel
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success(
                    "TATA", year, month, files_downloaded=1, duration=duration
                )
                logger.success(f"[SUCCESS] TATA download completed: {downloaded_path.name}")
                return {
                    "status": "success",
                    "files_downloaded": 1,
                    "duration": duration
                }

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("TATA", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = TataDownloader()
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
