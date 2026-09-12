# src/downloaders/motilal_downloader.py

import os
import time
import json
import shutil
import zipfile
import sys
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List
from urllib.parse import quote

# Add project root to sys.path for direct CLI execution
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

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


# ---------------------------------------------------------------------------
# Motilal API Constants & Helpers
# ---------------------------------------------------------------------------

API_URL  = "https://www.motilaloswalmf.com/content/aem-cloud-dept-backend-motilal-oswal/api/search-documents.json"
BASE_URL = "https://www.motilaloswalmf.com"

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}

MONTH_ALIASES = {
    1:  ["january", "jan"],
    2:  ["february", "feb"],
    3:  ["march", "mar"],
    4:  ["april", "apr"],
    5:  ["may"],
    6:  ["june", "jun"],
    7:  ["july", "jul"],
    8:  ["august", "aug"],
    9:  ["september", "sep", "sept"],
    10: ["october", "oct"],
    11: ["november", "nov"],
    12: ["december", "dec"],
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, */*",
    "Referer": "https://www.motilaloswalmf.com/",
}

REQUEST_TIMEOUT  = 30
DOWNLOAD_TIMEOUT = 120


def _is_month_end_portfolio(title: str) -> bool:
    """
    Return True if document title represents a month-end portfolio.
    Excludes fortnightly, half-yearly, and performance documents.
    """
    t = title.lower().strip()

    if "fortnightly" in t or "forthnightly" in t:
        return False
    if "half yearly" in t or "half-yearly" in t:
        return False
    if "performance" in t and "portfolio" not in t:
        return False

    if "scheme portfolio details" in t:
        return True
    if "month end" in t and "portfolio" in t:
        return True

    return False


def _title_matches_month_year(title: str, year: int, month: int) -> bool:
    """Check if document title references target portfolio month and year."""
    t = title.lower().strip()
    if str(year) not in t:
        return False
    aliases = MONTH_ALIASES.get(month, [])
    return any(alias in t for alias in aliases)


def _build_download_url(path: str) -> str:
    """Convert API path to absolute download URL with percent-encoded special characters."""
    encoded = quote(path, safe="/:.-_")
    return BASE_URL + encoded


def _is_valid_excel(content: bytes) -> bool:
    """Validate Excel magic bytes (XLSX, XLS, or valid binary data)."""
    if len(content) < 4:
        return False
    if content[:2] == b"PK":              # XLSX / ZIP
        return True
    if content[:4] == b"\xd0\xcf\x11\xe0": # XLS OLE2
        return True
    try:
        preview = content[:50].decode("utf-8", errors="ignore")
        if "<html" in preview.lower() or "<!doctype" in preview.lower():
            return False
    except Exception:
        pass
    return len(content) > 1000


# ---------------------------------------------------------------------------
# Downloader Class
# ---------------------------------------------------------------------------

class MotilalDownloader(BaseDownloader):
    """
    Motilal Oswal Mutual Fund - Portfolio Downloader

    Directly queries Motilal Oswal's AEM JSON backend API to discover
    and download month-end portfolio disclosure Excel files.
    No browser automation / Playwright required.
    """

    MONTH_NAMES = MONTH_NAMES

    def __init__(self):
        super().__init__("Motilal Oswal Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "motilal"
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "MOTILAL",
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

        logger.warning(f"MOTILAL: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("MOTILAL", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _call_api(self, year: int) -> List[Dict]:
        """Query the Motilal AEM documents API for a specific publication year."""
        params = {
            "year": str(year),
            "category": "month end portfolio",
            "month": "",
            "type": "mf",
        }
        last_exc = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = self.session.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                count = data.get("count", 0)
                logger.info(f"MOTILAL: API year={year} -> {count} records found")
                return data.get("results", [])
            except Exception as e:
                last_exc = e
                logger.warning(f"MOTILAL: API query attempt {attempt+1} failed: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF)-1)])
        if last_exc:
            raise last_exc
        raise RuntimeError(f"Failed to query Motilal API for year {year}")

    def _get_portfolio_candidates(self, target_year: int, target_month: int) -> List[Dict]:
        """Search API results for month-end portfolio documents matching year and month."""
        query_years = [target_year]
        # December disclosures publish in January of target_year + 1
        if target_month == 12:
            query_years.append(target_year + 1)

        candidates = []
        seen_paths = set()

        for qyear in query_years:
            records = self._call_api(qyear)
            for rec in records:
                path = rec.get("path", "")
                if path in seen_paths:
                    continue
                title = rec.get("title", "")
                if _is_month_end_portfolio(title) and _title_matches_month_year(title, target_year, target_month):
                    seen_paths.add(path)
                    candidates.append({
                        "title": title.strip(),
                        "path": path,
                        "url": _build_download_url(path),
                        "publish_date": rec.get("publishDate", ""),
                    })

        return candidates

    def _process_downloaded_file(self, temp_path: Path, month_name: str, year: int, download_folder: Path, original_suggested_name: str) -> Optional[Path]:
        """Process the downloaded file - extract if ZIP, preserve original name."""
        try:
            if temp_path.suffix.lower() == '.zip':
                logger.info("ZIP file detected, extracting...")
                temp_extract_dir = download_folder / f"temp_extract_{int(time.time())}"
                temp_extract_dir.mkdir(exist_ok=True)

                with zipfile.ZipFile(temp_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_extract_dir)

                found_file = None
                for file in temp_extract_dir.rglob('*'):
                    if file.is_file() and file.suffix.lower() in ['.xlsx', '.xls', '.xlsb']:
                        found_file = file
                        break

                if found_file:
                    final_path = download_folder / found_file.name
                    if final_path.exists():
                        final_path = download_folder / f"{month_name}_{year}_{found_file.name}"

                    shutil.move(str(found_file), str(final_path))
                    temp_path.unlink(missing_ok=True)
                    shutil.rmtree(temp_extract_dir, ignore_errors=True)
                    logger.info(f"MOTILAL: Extracted & saved: {final_path.name}")
                    return final_path
                else:
                    temp_path.unlink(missing_ok=True)
                    shutil.rmtree(temp_extract_dir, ignore_errors=True)
                    return None
            else:
                final_path = download_folder / original_suggested_name
                shutil.move(str(temp_path), str(final_path))
                logger.info(f"MOTILAL: Saved: {final_path.name}")
                return final_path

        except Exception as e:
            logger.error(f"Error processing file: {e}")
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            return None

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        """Discover portfolio via JSON API and download Excel file."""
        candidates = self._get_portfolio_candidates(target_year, target_month)
        if not candidates:
            return None

        # Select target document (first matching month-end portfolio)
        doc = candidates[0]
        url = doc["url"]
        raw_filename = Path(doc["path"]).name
        logger.info(f"MOTILAL: Discovered file: {doc['title']}")
        logger.info(f"MOTILAL: Download URL: {url}")

        temp_path = download_folder / f"temp_{raw_filename}"

        resp = self.session.get(url, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        content = resp.content

        if not _is_valid_excel(content):
            preview = content[:100].decode("utf-8", errors="replace")
            raise ValueError(f"Downloaded content is not valid Excel. Preview: {preview}")

        with open(temp_path, "wb") as f:
            f.write(content)

        final_path = self._process_downloaded_file(
            temp_path=temp_path,
            month_name=month_name,
            year=target_year,
            download_folder=download_folder,
            original_suggested_name=raw_filename
        )
        return final_path

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]

        logger.info("=" * 60)
        logger.info("MOTILAL OSWAL MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Motilal: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete - UPDATED")
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
                    logger.info(f"MOTILAL: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)

                if not downloaded_path:
                    logger.warning(f"MOTILAL: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("MOTILAL", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)

                # Consolidate downloads
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("MOTILAL", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] MOTILAL download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF)-1)])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("MOTILAL", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = MotilalDownloader()
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
