# src/downloaders/bandhan_downloader.py

import os
import time
import json
import shutil
import re
import random
import base64
import hashlib
import hmac
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
import requests

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding, hashes
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from cryptography.hazmat.primitives.serialization import load_pem_public_key

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


class BandhanDownloader(BaseDownloader):
    """
    Bandhan Mutual Fund - Portfolio Downloader
    
    Uses direct encrypted CMS API (AES-256-CBC + RSA-OAEP + HMAC-SHA256)
    and downloads Excel portfolio workbooks directly from Google Cloud Storage.
    No Playwright required.
    """

    API_URL = "https://pnservices.bandhanmutual.com/internal/investorservices/encdec/investor/v1/dashboard/cms-call"
    API_KEY = "WtUbdIoA2i54d3q0zdm2ZUGMxrTuh57kzxOSeAoTInObKvAV"
    STATIC_IV = b"fa1Z8M4DgI4BDQ=="  # 16 bytes

    PUBLIC_KEY_PEM = b"""-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxZFiJZ5D9pLJcxq/1QJQ
k4xxPLs6d5dtLzeago9iogRfgugKdAhPgS1fGNw0yNXzOm8bQeB/cZIq8yoo4sEre
5EKehFeAGDlTLJjQo9yL1LrrYcmRPx9pYClTn9H2nVnOoRJ+ih9SGaf/6LrpYMx+
sb8OMLOYyn5uoIf0sRNQcp+M/VadUuyJPU2/ohSHWvuKEcs6LVLvROG1knxxvfzIy
RdzN1YDkIDlhpBrWx9IAF8zmjeibaUKuim7ogC5KuAQ3SyZMgZ98R9KtyfWsV8ht
zem4mvaEf/ZwP1yQlJfIVCn0Fb8eA3Cp8+gc3YNM+j7t1awrWAf8TSol9C1i3sjw
IDAQAB
-----END PUBLIC KEY-----"""

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self, timeout: int = 60):
        super().__init__("Bandhan Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "bandhan"
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        })
        self._cached_disclosures: Optional[Dict[str, Any]] = None

    # -----------------------------------------------------------------------
    # Cryptographic Methods (AES-256-CBC, RSA-OAEP, HMAC-SHA256)
    # -----------------------------------------------------------------------

    @staticmethod
    def _generate_aes_key(length: int = 32) -> str:
        charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+"
        return "".join(random.choice(charset) for _ in range(length))

    @staticmethod
    def _generate_visitor_id(length: int = 32) -> str:
        return "".join(random.choice("0123456789abcdef") for _ in range(length))

    def _aes_encrypt(self, plaintext: str, key_str: str) -> str:
        key_bytes = key_str.encode("utf-8")
        data_bytes = plaintext.encode("utf-8")

        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(data_bytes) + padder.finalize()

        cipher = Cipher(algorithms.AES(key_bytes), modes.CBC(self.STATIC_IV))
        encryptor = cipher.encryptor()
        ct = encryptor.update(padded_data) + encryptor.finalize()

        b64 = base64.b64encode(ct).decode("ascii")
        return b64.replace("+", "-").replace("/", "_").rstrip("=")

    def _aes_decrypt(self, ciphertext: str, key_str: str) -> str:
        key_bytes = key_str.encode("utf-8")
        clean_text = ciphertext.strip().strip('"').strip("'")
        part = clean_text.split("::")[0].replace("-", "+").replace("_", "/")
        while len(part) % 4:
            part += "="
        raw = base64.b64decode(part)

        cipher = Cipher(algorithms.AES(key_bytes), modes.CBC(self.STATIC_IV))
        decryptor = cipher.decryptor()
        padded = decryptor.update(raw) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        return (unpadder.update(padded) + unpadder.finalize()).decode("utf-8")

    def _rsa_encrypt(self, message: str) -> str:
        pub_key = load_pem_public_key(self.PUBLIC_KEY_PEM)
        ct = pub_key.encrypt(
            message.encode("utf-8"),
            asym_padding.OAEP(
                mgf=asym_padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return base64.b64encode(ct).decode("ascii")

    def _hmac_sign(self, payload_json: str, visitor_id: str) -> str:
        return hmac.new(
            visitor_id.encode("utf-8"),
            payload_json.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    def call_cms_api(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encrypts and sends a payload to the Bandhan CMS endpoint, decrypts and parses response.
        """
        aes_key = self._generate_aes_key(32)
        visitor_id = self._generate_visitor_id(32)
        timestamp = str(int(time.time() * 1000))

        payload_json = json.dumps(payload, separators=(",", ":"))
        encrypted_body = self._aes_encrypt(payload_json, aes_key)

        rsa_envelope = f"{aes_key}::{encrypted_body[-32:]}"
        client_signature = self._rsa_encrypt(rsa_envelope)
        bandhan_signature = self._hmac_sign(payload_json, visitor_id)
        sign_key_enc = self._aes_encrypt(json.dumps(visitor_id), aes_key)

        headers = {
            "Content-Type": "text/plain",
            "x-api-key": self.API_KEY,
            "x-custom-timestamp": timestamp,
            "dt-platform": "web",
            "x-client-signature": client_signature,
            "x-bandhan-signature": bandhan_signature,
            "x-bandhan-sign-key": sign_key_enc,
            "x-internal-proxy": "True",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Origin": "https://bandhanmutual.com",
            "Referer": "https://bandhanmutual.com/",
        }

        response = self.session.post(
            self.API_URL,
            data=encrypted_body,
            headers=headers,
            timeout=self.timeout
        )

        if response.status_code != 200:
            raise RuntimeError(f"Bandhan API returned HTTP {response.status_code}: {response.text[:200]}")

        decrypted_text = self._aes_decrypt(response.text, aes_key)
        return json.loads(decrypted_text)

    # -----------------------------------------------------------------------
    # Portfolio Discovery Methods
    # -----------------------------------------------------------------------

    def get_statutory_portfolios(self, year: int, month: int) -> List[Dict[str, Any]]:
        """
        Queries statutory scheme portfolios for a given year and month.
        Endpoint: https://bandhanmutual.com/statutory-disclosures/scheme-portfolios/monthly-half-yearly
        """
        month_name = self.MONTH_NAMES.get(month, "")
        payload = {
            "type": "SCHEME_PORTFOLIOS",
            "data": {
                "subcategory": "monthly-and-half-yearly",
                "page": 1,
                "financial_year": str(year),
                "month": month_name,
                "posts_per_page": 100
            }
        }
        res = self.call_cms_api(payload)
        items = res.get("data", [])
        files = []
        for item in items:
            acf = item.get("acf_fields", {})
            doc_name = acf.get("document_name", "")
            scheme_info = acf.get("funds_mapping", {})
            scheme_name = scheme_info.get("post_title", "") if isinstance(scheme_info, dict) else ""

            for df in acf.get("disclosure_files", []):
                url = df.get("url", "")
                if url:
                    files.append({
                        "scheme_name": scheme_name,
                        "document_name": doc_name,
                        "filename": df.get("filename", "") or os.path.basename(url),
                        "url": url,
                        "id": df.get("id")
                    })
        return files

    def get_archive_portfolios(self, year: int, month: int) -> List[Dict[str, Any]]:
        """
        Queries consolidated disclosure archives for historical months.
        """
        if self._cached_disclosures is None:
            self._cached_disclosures = self.call_cms_api({"type": "ADDITIONAL_DISCLOSOURES"})

        month_name = self.MONTH_NAMES.get(month, "").lower()
        short_month = month_name[:3]
        year_str = str(year)
        short_year = year_str[-2:]

        matched = []
        containers = self._cached_disclosures.get("data", [])
        for c in containers:
            acf = c.get("acf_fields", {})
            if not isinstance(acf, dict):
                continue

            for df in acf.get("disclosure_files", []):
                doc_name = df.get("document_name", "")
                link_obj = df.get("document_link") or {}
                url = link_obj.get("url", "")
                filename = link_obj.get("filename", "")

                if not url:
                    continue

                combined = f"{doc_name.lower()} {filename.lower()} {url.lower()}"
                
                # Check for month match
                has_month = (month_name in combined) or (short_month in combined) or (f"-{month:02d}-" in combined)
                # Check for year match
                has_year = (year_str in combined) or (f"-{short_year}." in combined)

                if has_month and has_year:
                    matched.append({
                        "scheme_name": doc_name,
                        "document_name": doc_name,
                        "filename": filename or os.path.basename(url),
                        "url": url
                    })

        return matched

    # -----------------------------------------------------------------------
    # Downloader Lifecycle & Execution Flow
    # -----------------------------------------------------------------------

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "BANDHAN",
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
        
        logger.warning(f"BANDHAN: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("BANDHAN", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("BANDHAN MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Bandhan: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"BANDHAN: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                total_downloaded = self._run_download_flow(year, month, target_dir)
                
                if total_downloaded == 0:
                    logger.warning(f"BANDHAN: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("BANDHAN", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success marker
                self._create_success_marker(target_dir, year, month, total_downloaded)
                
                # Consolidate downloads into merged file
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("BANDHAN", year, month, files_downloaded=total_downloaded, duration=duration)
                logger.success(f"[SUCCESS] BANDHAN download completed. Total files: {total_downloaded}")
                return {"status": "success", "files_downloaded": total_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("BANDHAN", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, download_folder: Path) -> int:
        """
        Fetches portfolio files via statutory scheme portfolios API,
        falling back to consolidated disclosure archives if needed,
        and downloads all workbooks directly from Google Cloud Storage.
        """
        month_name = self.MONTH_NAMES[target_month]
        logger.info(f"Querying Bandhan portfolios for {month_name} {target_year}...")

        # 1. Try statutory scheme portfolios first (covers latest/active months e.g. July 2026)
        files_to_download = []
        try:
            statutory_files = self.get_statutory_portfolios(target_year, target_month)
            if statutory_files:
                logger.info(f"Found {len(statutory_files)} statutory scheme portfolio files for {month_name} {target_year}.")
                files_to_download = statutory_files
        except Exception as e:
            logger.warning(f"Statutory query encountered an issue: {e}")

        # 2. Fall back to archived consolidated portfolios if statutory returned none
        if not files_to_download:
            try:
                archive_files = self.get_archive_portfolios(target_year, target_month)
                if archive_files:
                    logger.info(f"Found {len(archive_files)} archive portfolio files for {month_name} {target_year}.")
                    files_to_download = archive_files
            except Exception as e:
                logger.warning(f"Archive query encountered an issue: {e}")

        if not files_to_download:
            logger.warning(f"No portfolio disclosures found for {month_name} {target_year}.")
            return 0

        # 3. Download files directly via HTTP GET
        total_downloaded = 0
        for i, item in enumerate(files_to_download):
            url = item["url"]
            filename = item["filename"]
            scheme_name = item.get("scheme_name", "")
            save_path = download_folder / filename

            # Deduplicate filename if necessary
            if save_path.exists():
                stem, ext = os.path.splitext(filename)
                safe_name = re.sub(r'[\\/*?:"<>|]', "", scheme_name[:25]).strip()
                save_path = download_folder / f"{stem}_{safe_name}{ext}"

            logger.info(f"  [{i+1}/{len(files_to_download)}] Downloading: {filename} ({scheme_name})")
            try:
                res = self.session.get(url, stream=True, timeout=60)
                res.raise_for_status()

                with open(save_path, "wb") as f:
                    for chunk in res.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)

                total_downloaded += 1
            except Exception as e:
                logger.error(f"    [FAIL] Failed to download {url}: {e}")

        return total_downloaded


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Bandhan Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument("--redo", action="store_true", help="Redo mode")
    args = parser.parse_args()

    downloader = BandhanDownloader()
    result = downloader.download(args.year, args.month)
    print(json.dumps(result))

    status = result["status"]
    if status == "success":
        logger.success(f"[SUCCESS] Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success(f"[SUCCESS] Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"[INFO] Month not yet published")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
