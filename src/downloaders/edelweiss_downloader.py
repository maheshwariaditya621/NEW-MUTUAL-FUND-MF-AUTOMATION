# src/downloaders/edelweiss_downloader.py

import os
import re
import time
import json
import shutil
import base64
import hmac
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
from curl_cffi import requests as c_requests
import requests
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
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


class EdelweissDownloader(BaseDownloader):
    """
    Edelweiss Mutual Fund - Monthly Portfolio Downloader.

    Fetches the official monthly consolidated portfolio disclosures via direct REST API
    and AES decryption without browser automation (Playwright/Selenium).

    API: GET https://api.edelweissmf.com/edelweissmf/api/v1/mf/statutory-menus/single
    Page: https://www.edelweissmf.com/statutory/portfolio-of-schemes
    """

    AMC_NAME = "edelweiss"
    SECRET = "5b6714126d3149fbab994747b2633287"
    HASH_KEY = "r4vcos0ejvndsow95n"
    STATIC_IP = "103.0.123.175"

    PORTFOLIO_API = "https://api.edelweissmf.com/edelweissmf/api/v1/mf/statutory-menus/single"
    BASE_URL = "https://www.edelweissmf.com"

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
        super().__init__("Edelweiss Mutual Fund")
        self.notifier = get_notifier()
        self.timeout = timeout
        self._cached_catalog: Optional[List[Dict[str, Any]]] = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "EDELWEISS",
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

        logger.warning(f"EDELWEISS: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("EDELWEISS", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _generate_api_headers(self) -> Tuple[Dict[str, str], str]:
        """Generates dynamic HMAC-SHA256 key and request headers matching Angular HTTP interceptor."""
        timestamp = str(int(time.time() * 1000))
        message = f"{self.SECRET}{self.STATIC_IP}{timestamp}"
        dynamic_key = hmac.new(self.HASH_KEY.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.edelweissmf.com",
            "Referer": "https://www.edelweissmf.com/statutory/portfolio-of-schemes",
            "x-timestamp": timestamp,
            "x-ip-address": self.STATIC_IP,
        }
        return headers, dynamic_key

    def _decrypt_cryptojs_aes(self, ciphertext_b64: str, passphrase: str) -> str:
        """
        Decrypts OpenSSL / CryptoJS AES-CBC ciphertext using passphrase.
        Derives 32-byte key and 16-byte IV via OpenSSL EVP_BytesToKey (MD5).
        """
        raw = base64.b64decode(ciphertext_b64)
        if not raw.startswith(b"Salted__"):
            raise ValueError("Not an OpenSSL salted ciphertext")
        salt = raw[8:16]
        ciphertext = raw[16:]

        dt = b""
        d = b""
        while len(dt) < 48:
            d = hashlib.md5(d + passphrase.encode("utf-8") + salt).digest()
            dt += d
        key = dt[:32]
        iv = dt[32:48]

        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_plain = decryptor.update(ciphertext) + decryptor.finalize()

        pad_len = padded_plain[-1]
        plaintext = padded_plain[:-pad_len]
        return plaintext.decode("utf-8")

    def fetch_catalog(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        Queries the Edelweiss Statutory Disclosures API and decrypts the document catalog.
        """
        if self._cached_catalog is not None and not force_refresh:
            return self._cached_catalog

        logger.info(f"Querying Edelweiss Statutory API: {self.PORTFOLIO_API}...")
        headers, dynamic_key = self._generate_api_headers()
        params = {
            "type": "Statutory",
            "fundType": "MF",
            "menuName": "Portfolio of scheme(s)"
        }

        resp = c_requests.get(
            self.PORTFOLIO_API,
            params=params,
            headers=headers,
            impersonate="chrome124",
            timeout=self.timeout
        )
        resp.raise_for_status()

        ciphertext_b64 = resp.json().get("body", "")
        decrypted_json = self._decrypt_cryptojs_aes(ciphertext_b64, dynamic_key)
        data = json.loads(decrypted_json)
        files = data.get("files", [])

        # Filter for 'Monthly Portfolio and Risk-o-Meter'
        monthly_files = [
            f for f in files
            if f.get("subMenuName") == "Monthly Portfolio and Risk-o-Meter"
        ]

        parsed_items: List[Dict[str, Any]] = []
        for f in monthly_files:
            rel_path = f.get("filePath", "").lstrip("/")
            full_url = f"{self.BASE_URL}/{rel_path}"
            filename = os.path.basename(rel_path)

            # Month extraction
            m_str = str(f.get("month", "")).strip().lower()
            month_num = self.MONTH_MAP.get(m_str)
            if not month_num:
                for k, v in self.MONTH_MAP.items():
                    if k in f.get("fileTitle", "").lower():
                        month_num = v
                        break

            yr_str = str(f.get("year", "")).strip()
            year_num = int(yr_str) if yr_str.isdigit() else None

            parsed_items.append({
                "year": year_num,
                "month": month_num,
                "file_title": f.get("fileTitle", ""),
                "rel_path": rel_path,
                "url": full_url,
                "filename": filename,
                "raw_record": f,
            })

        logger.info(f"Discovered {len(parsed_items)} monthly consolidated portfolio records")
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
            logger.warning(f"EDELWEISS: No portfolio record found for {month_name} {target_year}")
            return None

        url = target_record["url"]
        clean_filename = re.sub(r'[\\/*?:"<>|]', "_", target_record["filename"])
        save_path = download_folder / clean_filename

        logger.info(f"Downloading Edelweiss monthly portfolio for {month_name} {target_year}...")
        logger.info(f"  Title: {target_record['file_title']}")
        logger.info(f"  URL:   {url}")

        dl_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Referer": "https://www.edelweissmf.com/statutory/portfolio-of-schemes",
        }

        resp = requests.get(url, headers=dl_headers, timeout=60)
        resp.raise_for_status()

        content = resp.content
        size = len(content)
        if size == 0:
            raise ValueError(f"Downloaded 0 bytes from {url}")

        is_xlsx = content.startswith(b"PK\x03\x04")
        if not is_xlsx:
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
        logger.info("EDELWEISS MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # Idempotency check
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Edelweiss: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"EDELWEISS: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)

                if not downloaded_path:
                    logger.warning(f"EDELWEISS: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("EDELWEISS", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success marker
                self._create_success_marker(target_dir, year, month, 1)

                # Consolidate raw files into merged excel
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success(
                    "EDELWEISS", year, month, files_downloaded=1, duration=duration
                )
                logger.success(f"[SUCCESS] EDELWEISS download completed: {downloaded_path.name}")
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
        self.notifier.notify_error("EDELWEISS", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = EdelweissDownloader()
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
