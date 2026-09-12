# src/downloaders/mahindra_downloader.py

import os
import re
import time
import json
import uuid
import base64
import shutil
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Any
import requests
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding

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


class MahindraDownloader(BaseDownloader):
    """
    Mahindra Manulife Mutual Fund - Monthly Portfolio Downloader.
    
    Extracts monthly portfolio disclosure spreadsheets directly from the official
    preLogin/downloads API using pure HTTP requests and client-side AES-256-CBC
    decryption, matching the public website's frontend logic without Playwright.
    """

    AMC_NAME = "mahindra"
    API_URL = "https://investorapi.mahindramanulife.com/api/v1/web/preLogin/downloads"

    # Confirmed static cryptographic parameters from public JavaScript bundle
    AES_KEY = b"mahindra2024mahindra2024mahindra"  # 32 bytes = 256 bits
    AES_IV = b"hasnainsheikh202"                  # 16 bytes = 128 bits

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.mahindramanulife.com",
        "Referer": "https://www.mahindramanulife.com/",
        "Platform": "web",
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self, timeout: int = 30):
        super().__init__("Mahindra Manulife Mutual Fund")
        self.notifier = get_notifier()
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)
        self._cached_structure: Optional[Dict[str, Any]] = None

    @classmethod
    def _decrypt_payload(cls, encrypted_b64: str) -> Dict[str, Any]:
        """
        Decrypts Base64 AES-256-CBC encrypted payload with PKCS#7 unpadding.
        """
        ciphertext = base64.b64decode(encrypted_b64)
        cipher = Cipher(algorithms.AES(cls.AES_KEY), modes.CBC(cls.AES_IV))
        decryptor = cipher.decryptor()
        padded_data = decryptor.update(ciphertext) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded_data) + unpadder.finalize()
        return json.loads(plaintext.decode("utf-8"))

    def _fetch_downloads_structure(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Query preLogin/downloads API and return the decrypted data structure.
        """
        if self._cached_structure and not force_refresh:
            return self._cached_structure

        headers = {"X-Client-Id": str(uuid.uuid4())}

        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = self.session.get(self.API_URL, headers=headers, timeout=self.timeout)
                if resp.status_code == 200:
                    data = resp.json()
                    payload = data.get("payload")
                    if not payload:
                        raise ValueError("API response missing 'payload' field")
                    self._cached_structure = self._decrypt_payload(payload)
                    return self._cached_structure
                else:
                    raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)

            except (requests.Timeout, requests.RequestException) as e:
                if attempt < MAX_RETRIES:
                    backoff = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                    logger.warning(f"Mahindra API attempt {attempt + 1} failed: {e}. Retrying in {backoff}s...")
                    time.sleep(backoff)
                else:
                    raise RuntimeError(f"Failed calling Mahindra API after {MAX_RETRIES + 1} attempts: {e}") from e

        raise RuntimeError("Failed fetching Mahindra downloads structure")

    def _get_monthly_portfolio_documents(self, year: int, month: int) -> List[Dict[str, Any]]:
        """
        Locate monthly portfolio documents for target year and month.
        
        Hierarchy:
        MANDATORY DISCLOSURES (23) -> Portfolio Disclosure (1751) -> Monthly Portfolio Disclosure (52)
        -> Year (e.g. 2026) -> Month file record.
        """
        month_name = self.MONTH_NAMES.get(month)
        if not month_name:
            raise ValueError(f"Invalid month: {month}")

        structure = self._fetch_downloads_structure()
        categories = structure.get("data", [])

        # Step 1: Find Mandatory Disclosures
        mandatory_cat = None
        for cat in categories:
            if cat.get("categoryId") == 23 or "mandatory" in (cat.get("categoryName") or "").lower():
                mandatory_cat = cat
                break
        if not mandatory_cat:
            return []

        # Step 2: Find Portfolio Disclosure
        portfolio_cat = None
        for sub in mandatory_cat.get("subcategories", []):
            if sub.get("categoryId") == 1751 or sub.get("categoryName") == "Portfolio Disclosure":
                portfolio_cat = sub
                break
        if not portfolio_cat:
            return []

        # Step 3: Find Monthly Portfolio Disclosure
        monthly_cat = None
        for sub in portfolio_cat.get("subcategories", []):
            if sub.get("categoryId") == 52 or "monthly portfolio" in (sub.get("categoryName") or "").lower():
                monthly_cat = sub
                break
        if not monthly_cat:
            return []

        # Step 4: Find Year
        year_cat = None
        for y in monthly_cat.get("subcategories", []):
            if str(y.get("categoryName", "")).strip() == str(year):
                year_cat = y
                break
        if not year_cat:
            return []

        # Step 5: Match Month
        matched_files: List[Dict[str, Any]] = []
        for f in year_cat.get("files", []):
            title = (f.get("title") or "").strip()
            file_url = (f.get("fileUrl") or "").strip()
            download_order = f.get("downloadOrder")

            title_lower = title.lower()
            is_match = (month_name.lower() in title_lower) or (download_order == month)

            if is_match and file_url:
                path_part = urllib.parse.urlparse(file_url).path
                base_name = os.path.basename(path_part)
                clean_title = re.sub(r'[^a-zA-Z0-9_\-]', '_', title).strip('_')
                filename = f"{clean_title}.xlsx" if not base_name.endswith((".xlsx", ".xls")) else base_name

                matched_files.append({
                    "title": title,
                    "file_url": file_url,
                    "download_id": f.get("downloadId"),
                    "download_order": download_order,
                    "year": year,
                    "month": month,
                    "month_name": month_name,
                    "filename": filename,
                })

        return matched_files

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        tmp_marker_path = target_dir / "_SUCCESS.json.tmp"
        
        marker_data = {
            "amc": "MAHINDRA",
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
        
        logger.warning(f"MAHINDRA: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("MAHINDRA", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES.get(month, str(month))
        
        logger.info("=" * 60)
        logger.info("MAHINDRA MANULIFE MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency check
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Mahindra: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {
                    "amc": "mahindra",
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
            logger.info(f"MAHINDRA: [DRY RUN] Would download {month_name} {year}")
            duration = time.time() - start_time
            return {
                "amc": "mahindra",
                "year": year,
                "month": month,
                "files_downloaded": 0,
                "status": "dry_run",
                "duration": duration
            }

        try:
            logger.info(f"Querying Mahindra preLogin/downloads API for {month_name} {year}...")
            matched_files = self._get_monthly_portfolio_documents(year, month)

            if not matched_files:
                logger.warning(f"MAHINDRA: No monthly portfolio found for {month_name} {year}")
                self.notifier.notify_not_published("MAHINDRA", year, month)
                if target_dir.exists():
                    shutil.rmtree(target_dir, ignore_errors=True)
                duration = time.time() - start_time
                return {
                    "amc": "mahindra",
                    "year": year,
                    "month": month,
                    "files_downloaded": 0,
                    "status": "not_published",
                    "duration": duration
                }

            saved_files: List[Path] = []
            for i, doc in enumerate(matched_files, 1):
                file_url = doc["file_url"]
                filename = doc["filename"]
                save_path = target_dir / filename

                logger.info(f"Downloading {i}/{len(matched_files)}: {doc['title']} -> {filename}")

                dl_ok = False
                for attempt in range(MAX_RETRIES + 1):
                    try:
                        resp = self.session.get(file_url, stream=True, timeout=60)
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

                # Validate format & fix extension
                validated_path = validate_and_fix_extension(save_path)
                if validated_path != save_path:
                    save_path = validated_path

                file_size = save_path.stat().st_size
                if file_size == 0:
                    raise ValueError(f"Downloaded file is 0 bytes: {save_path.name}")

                with open(save_path, "rb") as f:
                    magic = f.read(8)
                is_xlsx = magic.startswith(b"PK\x03\x04")
                is_xls = magic.startswith(b"\xd0\xcf\x11\xe0")
                if not (is_xlsx or is_xls):
                    logger.warning(f"File {save_path.name} has unexpected magic bytes: {magic[:4]!r}")

                saved_files.append(save_path)
                logger.success(f"Saved ({file_size:,} bytes): {save_path.name}")

            # Atomic success marker
            self._create_success_marker(target_dir, year, month, len(saved_files))

            # Consolidate downloads into merged excels
            self.consolidate_downloads(year, month)

            duration = time.time() - start_time
            self.notifier.notify_success("MAHINDRA", year, month, files_downloaded=len(saved_files), duration=duration)
            logger.success(f"[SUCCESS] MAHINDRA download completed ({len(saved_files)} file(s))")
            return {
                "amc": "mahindra",
                "year": year,
                "month": month,
                "files_downloaded": len(saved_files),
                "status": "success",
                "duration": duration
            }

        except requests.HTTPError as e:
            error_msg = f"API request failed: HTTP {e.response.status_code if e.response is not None else e}"
            logger.error(error_msg)
            if target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
            self.notifier.notify_error("MAHINDRA", year, month, "HTTP Error", error_msg)
            duration = time.time() - start_time
            return {
                "amc": "mahindra",
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
            self.notifier.notify_error("MAHINDRA", year, month, "Download Exception", error_msg)
            duration = time.time() - start_time
            return {
                "amc": "mahindra",
                "year": year,
                "month": month,
                "status": "failed",
                "reason": error_msg,
                "duration": duration
            }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Mahindra Manulife Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g. 2026)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    args = parser.parse_args()

    if args.month < 1 or args.month > 12:
        logger.error("Invalid month. Must be between 1 and 12.")
        raise SystemExit(1)

    downloader = MahindraDownloader()
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
