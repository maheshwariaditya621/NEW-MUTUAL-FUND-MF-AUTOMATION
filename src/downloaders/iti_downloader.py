# src/downloaders/iti_downloader.py

import os
import time
import json
import uuid
import shutil
import re
import calendar
import base64
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import requests
import openpyxl

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend

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


BASE_URL = "https://itiamc.com"
CATALOG_API_URL = f"{BASE_URL}/jeeth/api/v1/catalog/getPartnerDocumentByType"

# Reverse-engineered AES-128-CBC constants from Angular main bundle (module 5312)
AES_KEY = b"aar6tzij8o1snaar"
AES_IV = b"0123456789ABCDEF"

MONTH_NAMES = {
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


def encrypt_data(plaintext: str) -> str:
    """Encrypt plaintext string using AES-128-CBC + PKCS7 padding and return Base64 string."""
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(plaintext.encode("utf-8")) + padder.finalize()

    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(AES_IV), backend=default_backend())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(padded_data) + encryptor.finalize()

    return base64.b64encode(ciphertext).decode("utf-8")


def decrypt_data(ciphertext_b64: str) -> str:
    """Decrypt Base64 ciphertext using AES-128-CBC + PKCS7 unpadding and return plaintext string."""
    ciphertext = base64.b64decode(ciphertext_b64)

    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(AES_IV), backend=default_backend())
    decryptor = cipher.decryptor()
    padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

    unpadder = padding.PKCS7(128).unpadder()
    plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()

    return plaintext.decode("utf-8")


class ITIDownloader(BaseDownloader):
    """
    ITI Mutual Fund - Portfolio Downloader
    
    Direct requests-based scraper using the reverse-engineered encrypted catalog API:
    POST https://itiamc.com/jeeth/api/v1/catalog/getPartnerDocumentByType
    
    Downloads consolidated monthly portfolio disclosure Excel files.
    Accurately extracts statutory reporting periods from filenames and workbook headers
    to avoid the CMS upload month mismatch.
    """

    def __init__(self):
        super().__init__("ITI Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "iti"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": "https://www.itiamc.com",
            "Referer": "https://www.itiamc.com/statuory-disclosure?type=Portfolio%20Disclosures",
        })
        logger.info("ITIDownloader initialized (Requests + AES Crypto API Version)")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "ITI",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        with open(marker_path, "w", encoding="utf-8") as f:
            json.dump(marker_data, f, indent=2)
        logger.info(f"Created completion marker: {marker_path.name}")

    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str):
        corrupt_base = Path(f"data/raw/{self.AMC_NAME}/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)
        corrupt_target = corrupt_base / f"{year}_{month:02d}"
        if corrupt_target.exists():
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            corrupt_target = corrupt_target.parent / f"{corrupt_target.name}__{ts}"
        
        logger.warning(f"ITI: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("ITI", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def fetch_decrypted_catalog(self) -> dict:
        """Send encrypted request to catalog API and return decrypted JSON."""
        guid = uuid.uuid4().hex
        ts = int(time.time() * 1000)
        payload = {
            "type": "Disclosure",
            "guid": guid,
            "timeStamp": ts,
        }
        plaintext_req = json.dumps(payload)
        encrypted_req = encrypt_data(plaintext_req)

        resp = self.session.post(
            CATALOG_API_URL,
            json={"eData": encrypted_req},
            timeout=60
        )
        resp.raise_for_status()

        resp_json = resp.json()
        if "eData" not in resp_json:
            raise ValueError(f"No eData in API response: {resp_json}")

        decrypted_text = decrypt_data(resp_json["eData"])
        return json.loads(decrypted_text)

    def parse_actual_period_from_filename(self, filename: str, cms_year: Optional[str] = None) -> Tuple[Optional[int], Optional[int]]:
        """
        Extract the actual statutory reporting month (1-12) and year (YYYY) from the filename.
        Handles numeric date patterns like 31.07.2026 or month names like AUGUST_2026.
        """
        # 1. Check for numeric date formats like 31.07.2026 or 30-06-2026
        numeric_match = re.search(r"(\d{1,2})[\._-](\d{1,2})[\._-](20\d{2})", filename)
        if numeric_match:
            month = int(numeric_match.group(2))
            year = int(numeric_match.group(3))
            if 1 <= month <= 12 and 2000 <= year <= 2099:
                return month, year

        # 2. Check for month name and year in filename
        fname_clean = filename.lower().replace("-", " ").replace("_", " ").replace(".", " ")
        year_match = re.search(r"\b(20\d{2})\b", fname_clean)
        year = int(year_match.group(1)) if year_match else None
        if not year and cms_year and cms_year.isdigit():
            year = int(cms_year)

        month = None
        for name, num in MONTH_NAMES.items():
            if re.search(rf"\b{name}\b", fname_clean):
                month = num
                break

        return month, year

    def extract_monthly_portfolio_records(self, catalog_data: Optional[dict] = None) -> List[Dict]:
        """Extract all Monthly Portfolio documents from the catalog, resolving actual reporting periods."""
        if not catalog_data:
            catalog_data = self.fetch_decrypted_catalog()

        monthly_records = []
        for item in catalog_data.get("data", {}).get("typeList", []):
            if item.get("subType") == "Portfolio Disclosures":
                for cat in item.get("subTypesList", []):
                    if cat.get("topic") == "Monthly":
                        monthly_records = cat.get("topicsList", [])
                        break

        results = []
        for d in monthly_records:
            url = d.get("url", "")
            filename = url.split("/")[-1] if url else ""
            cms_month = str(d.get("month", "")).strip()
            cms_year = str(d.get("year", "")).strip()

            actual_month, actual_year = self.parse_actual_period_from_filename(filename, cms_year)

            results.append({
                "id": d.get("id"),
                "url": url,
                "filename": filename,
                "cms_month": cms_month,
                "cms_year": cms_year,
                "actual_month": actual_month,
                "actual_year": actual_year,
            })

        return results

    def _validate_excel_file(self, file_path: Path) -> bool:
        """Validate ZIP signature and openpyxl readable workbook."""
        if not file_path.exists() or file_path.stat().st_size == 0:
            return False

        with open(file_path, "rb") as f:
            magic = f.read(4)
        if magic != b"PK\x03\x04" and magic != b"\xd0\xcf\x11\xe0":
            logger.error(f"ITI: Invalid magic bytes for {file_path.name}: {magic.hex()}")
            return False

        if magic == b"PK\x03\x04":
            try:
                wb = openpyxl.load_workbook(str(file_path), read_only=True, data_only=True)
                _ = wb.sheetnames
                wb.close()
                return True
            except Exception as e:
                logger.error(f"ITI: openpyxl validation failed for {file_path.name}: {e}")
                return False

        return True

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        logger.info("ITI: Fetching and decrypting document catalog...")
        catalog_data = self.fetch_decrypted_catalog()
        records = self.extract_monthly_portfolio_records(catalog_data)
        logger.info(f"ITI: Found {len(records)} monthly portfolio records in catalog.")

        target_record = None
        for r in records:
            if r["actual_year"] == target_year and r["actual_month"] == target_month:
                target_record = r
                break

        if not target_record:
            logger.warning(f"ITI: No portfolio record found for {month_name} {target_year}.")
            return None

        download_url = target_record.get("url", "")
        if not download_url:
            logger.error(f"ITI: Empty download URL in record {target_record}")
            return None

        filename = target_record.get("filename") or os.path.basename(download_url)
        target_path = download_folder / filename

        logger.info(f"ITI: Downloading {filename} from {download_url}...")
        download_resp = self.session.get(download_url, stream=True, timeout=60)
        if download_resp.status_code != 200:
            logger.error(f"ITI: Download failed with status {download_resp.status_code}")
            return None

        temp_path = target_path.with_name(f"{target_path.stem}.tmp{target_path.suffix}")
        with open(temp_path, "wb") as f:
            for chunk in download_resp.iter_content(chunk_size=16384):
                if chunk:
                    f.write(chunk)

        if self._validate_excel_file(temp_path):
            temp_path.replace(target_path)
            logger.info(f"  [OK] Saved and validated: {target_path.name} ({target_path.stat().st_size:,} bytes)")
            return target_path
        else:
            if temp_path.exists():
                temp_path.unlink()
            return None

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        month_name = calendar.month_name[month]
        
        logger.info("=" * 60)
        logger.info("ITI MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"ITI: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"ITI: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"ITI: No portfolio file found for {month_name} {year}")
                    self.notifier.notify_not_published("ITI", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads into merged excels
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("ITI", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] ITI download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration, "file_path": str(downloaded_path)}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("ITI", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ITI Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    args = parser.parse_args()

    downloader = ITIDownloader()
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
