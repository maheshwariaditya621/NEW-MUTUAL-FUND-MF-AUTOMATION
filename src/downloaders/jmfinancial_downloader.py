# src/downloaders/jmfinancial_downloader.py

import os
import re
import time
import json
import shutil
import base64
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Any
import requests
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding

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


class JMFinancialDownloader(BaseDownloader):
    """
    JM Financial Mutual Fund - Monthly Portfolio Downloader.

    Downloads monthly portfolio disclosures via the official reverse-engineered
    AES-encrypted API without browser automation (Playwright/Selenium).

    API:
        POST https://jmmfapi.jmfinancialmf.com/api/GetDownloadNew
        Payload: {"IICategoryID": "2", "IISubCategoryID": "4", "IVSearch": ""}
    """

    AMC_NAME = "jmfinancial"
    API_BASE_URL = "https://jmmfapi.jmfinancialmf.com/api/"
    API_GET_DOWNLOAD_NEW = f"{API_BASE_URL}GetDownloadNew"
    WEB_BASE_URL = "https://www.jmfinancialmf.com/"

    # Hardcoded frontend crypto keys from React ServiceProvider context
    AES_KEY = "6fa979f20126cb08aa645a8f495f6d85"
    AES_IV = "I8zyA4lVhMCaJ5Kg"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Origin": "https://www.jmfinancialmf.com",
        "Referer": "https://www.jmfinancialmf.com/downloads/Portfolio-Disclosure",
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    MONTH_PATTERNS = {
        1: r"(?:january|jan)",
        2: r"(?:february|feb)",
        3: r"(?:march|mar)",
        4: r"(?:april|apr)",
        5: r"(?:may)",
        6: r"(?:june|jun)",
        7: r"(?:july|jul)",
        8: r"(?:august|aug)",
        9: r"(?:september|sept|sep)",
        10: r"(?:october|oct)",
        11: r"(?:november|nov)",
        12: r"(?:december|dec)",
    }

    def __init__(self, timeout: int = 30):
        super().__init__("JM Financial Mutual Fund")
        self.notifier = get_notifier()
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)
        self._cached_catalog: Optional[List[Dict[str, Any]]] = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "JM_FINANCIAL",
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

        logger.warning(f"JM_FINANCIAL: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("JM_FINANCIAL", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _decrypt_payload(self, encrypted_b64: str) -> Any:
        """Decrypts base64 AES-256-CBC ciphertext into Python data structure."""
        key_bytes = self.AES_KEY.encode("utf-8")
        iv_bytes = self.AES_IV.encode("utf-8")
        ciphertext = base64.b64decode(encrypted_b64)

        cipher = Cipher(algorithms.AES(key_bytes), modes.CBC(iv_bytes))
        decryptor = cipher.decryptor()
        padded = decryptor.update(ciphertext) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        decrypted = unpadder.update(padded) + unpadder.finalize()

        try:
            text = decrypted.decode("utf-8")
        except UnicodeDecodeError:
            text = decrypted.decode("latin-1")

        return json.loads(text)

    def _fetch_catalog(self) -> List[Dict[str, Any]]:
        """Fetches and decrypts the monthly portfolio disclosure catalog."""
        if self._cached_catalog is not None:
            return self._cached_catalog

        payload = {
            "IICategoryID": "2",
            "IISubCategoryID": "4",
            "IVSearch": "",
        }
        logger.info(f"Querying catalog from {self.API_GET_DOWNLOAD_NEW}...")
        resp = self.session.post(
            self.API_GET_DOWNLOAD_NEW,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )
        resp.raise_for_status()

        resp_json = resp.json()
        raw_data = resp_json.get("data")
        if not raw_data:
            raise ValueError("No encrypted data received from JM Financial catalog API")

        records = self._decrypt_payload(raw_data)
        if not isinstance(records, list):
            raise ValueError(f"Expected list of records, got {type(records)}")

        logger.info(f"Successfully decrypted catalog with {len(records)} records")
        self._cached_catalog = records
        return records

    def _filter_records_by_period(
        self, records: List[Dict[str, Any]], year: int, month: int
    ) -> List[Dict[str, Any]]:
        """Filters portfolio records for target year and month."""
        if month not in self.MONTH_PATTERNS:
            raise ValueError(f"Invalid month: {month}. Must be 1-12.")

        pattern = self.MONTH_PATTERNS[month]
        year_str = str(year)
        matched = []

        for item in records:
            title = item.get("Title", "")
            fn = item.get("FileName", "")
            combined = f"{title} {fn}".lower()

            if year_str not in combined:
                continue

            # Check month pattern followed by optional day digits or word boundary
            if re.search(r"\b" + pattern + r"(?:\d{1,2}|\b)", combined):
                matched.append(item)

        return matched

    def _run_download_flow(
        self, target_year: int, target_month: int, month_name: str, download_folder: Path
    ) -> int:
        catalog = self._fetch_catalog()
        matched = self._filter_records_by_period(catalog, target_year, target_month)

        if not matched:
            logger.warning(f"No portfolio records found in catalog for {month_name} {target_year}")
            return 0

        # Check if consolidated file exists; otherwise use scheme files
        consolidated = [
            r for r in matched
            if any(k in r.get("Title", "").lower() for k in ["consolidated", "all fund", "all-fund", "all schemes"])
        ]

        to_download = consolidated if consolidated else matched
        logger.info(f"Identified {len(to_download)} files to download for {month_name} {target_year}")

        count = 0
        for idx, rec in enumerate(to_download):
            title = rec.get("Title", "").strip()
            file_name_rel = rec.get("FileName", "").strip()

            if not file_name_rel:
                continue

            file_url = self.WEB_BASE_URL + file_name_rel.lstrip("/")
            base_name = os.path.basename(file_name_rel)
            clean_name = re.sub(r'[\\/*?:"<>|]', "_", base_name)
            save_path = download_folder / clean_name

            logger.info(f"  [{idx+1}/{len(to_download)}] Downloading: {title}")
            try:
                r = self.session.get(file_url, timeout=60)
                r.raise_for_status()

                content = r.content
                if len(content) == 0:
                    logger.error(f"    0 bytes received for {title}")
                    continue

                # Validate magic bytes
                is_xlsx = content.startswith(b"PK\x03\x04")
                is_xls = content.startswith(b"\xd0\xcf\x11\xe0")
                if not (is_xlsx or is_xls):
                    logger.error(f"    Invalid spreadsheet format for {title}: {content[:16]}")
                    continue

                with open(save_path, "wb") as f:
                    f.write(content)

                count += 1
                logger.info(f"    Saved: {clean_name} ({len(content):,} bytes)")

            except Exception as e:
                logger.error(f"    Failed to download {title}: {e}")

        return count

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        month_name = self.MONTH_NAMES.get(month, f"Month {month}")

        logger.info("=" * 60)
        logger.info("JM FINANCIAL MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"JM Financial: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"JM_FINANCIAL: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                file_count = self._run_download_flow(year, month, month_name, target_dir)

                if file_count == 0:
                    logger.warning(f"JM_FINANCIAL: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("JM_FINANCIAL", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, file_count)

                # Consolidate individual scheme downloads into single merged workbook
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("JM_FINANCIAL", year, month, files_downloaded=file_count, duration=duration)
                logger.success(f"[SUCCESS] JM_FINANCIAL download completed. Total files: {file_count}")
                return {"status": "success", "files_downloaded": file_count, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("JM_FINANCIAL", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = JMFinancialDownloader()
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
