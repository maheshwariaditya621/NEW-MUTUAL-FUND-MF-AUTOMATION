# src/downloaders/axis_downloader.py

import os
import re
import time
import json
import uuid
import shutil
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import requests

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


class AxisDownloader(BaseDownloader):
    """
    Axis Mutual Fund - Monthly Portfolio Downloader.
    
    Downloads monthly scheme portfolio disclosure files using Axis Mutual Fund's
    public CMS API (/cms/token + /cms/get-scheme-documents) without requiring
    Playwright or browser automation.
    """

    AMC_NAME = "axis"
    BASE_URL = "https://www.axismf.com"
    TOKEN_ENDPOINT = "https://www.axismf.com/cms/token"
    DOCUMENTS_ENDPOINT = "https://www.axismf.com/cms/get-scheme-documents"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.axismf.com",
        "Referer": "https://www.axismf.com/statutory-disclosures",
    }

    # Month name mapping
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self, timeout: int = 30):
        """Initialize Axis downloader."""
        super().__init__("Axis Mutual Fund")
        self.notifier = get_notifier()
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)
        self.browser_id = str(uuid.uuid4())
        self.session.headers["browser-id"] = self.browser_id
        self._cached_token: Optional[str] = None

    def _get_axis_token(self, force_refresh: bool = False) -> str:
        """
        Obtain a fresh Bearer token from the public Axis CMS token endpoint.
        Does not log or expose raw token secrets.
        """
        if self._cached_token and not force_refresh:
            return self._cached_token

        headers = {
            "browser-id": self.browser_id,
            "Content-Type": "application/json",
        }

        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = self.session.post(
                    self.TOKEN_ENDPOINT,
                    headers=headers,
                    json={},
                    timeout=self.timeout,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    token_raw = data.get("data", {}).get("token")
                    if not token_raw:
                        raise ValueError("Token missing in response payload")

                    auth_token = token_raw if token_raw.startswith("Bearer ") else f"Bearer {token_raw}"
                    self._cached_token = auth_token
                    logger.debug("Successfully acquired fresh Axis session token")
                    return self._cached_token
                else:
                    raise requests.HTTPError(f"Token endpoint returned HTTP {resp.status_code}", response=resp)

            except (requests.Timeout, requests.RequestException) as e:
                if attempt < MAX_RETRIES:
                    backoff = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                    logger.warning(f"Axis token request error on attempt {attempt + 1}, retrying in {backoff}s...")
                    time.sleep(backoff)
                else:
                    raise RuntimeError(f"Failed to obtain Axis token after {MAX_RETRIES + 1} attempts: {e}") from e

        raise RuntimeError("Failed to obtain Axis token")

    def _fetch_monthly_portfolio_documents(
        self,
        year: int,
        month: int,
        scheme_code: str = "Consolidated",
    ) -> List[Dict[str, Any]]:
        """
        Query get-scheme-documents API for the given year, month, and schemeCode.
        Filters out weekly and adhoc files to return genuine monthly portfolio records.
        """
        month_name = self.MONTH_NAMES.get(month)
        if not month_name:
            raise ValueError(f"Invalid month: {month}")

        payload = {
            "month": month_name,
            "schemeCode": scheme_code,
            "sdID": "sdMonthSchemePortfolio",
            "sdType": "yearMonthSchemeDocs",
            "year": str(year),
        }

        # Attempt request with automatic token refresh on 401
        resp_data = None
        for attempt in range(2):
            token = self._get_axis_token(force_refresh=(attempt > 0))
            headers = {
                "Authorization": token,
                "browser-id": self.browser_id,
                "Content-Type": "application/json",
            }

            try:
                resp = self.session.post(
                    self.DOCUMENTS_ENDPOINT,
                    headers=headers,
                    json=payload,
                    timeout=self.timeout,
                )
            except requests.exceptions.Timeout as e:
                raise RuntimeError(f"Documents API request timed out: {e}") from e
            except requests.exceptions.RequestException as e:
                raise RuntimeError(f"Network error calling documents API: {e}") from e

            if resp.status_code == 401 and attempt == 0:
                self._cached_token = None
                continue

            if resp.status_code != 200:
                raise RuntimeError(
                    f"Documents API returned HTTP {resp.status_code}: {resp.text[:300]}"
                )

            try:
                resp_data = resp.json()
                break
            except Exception as e:
                raise ValueError(f"Failed parsing documents JSON: {e}") from e

        if not resp_data:
            return []

        data_obj = resp_data.get("data", {})
        doc_list = data_obj.get("documentList", []) if isinstance(data_obj, dict) else []

        portfolios: List[Dict[str, Any]] = []

        for item in doc_list:
            if not isinstance(item, dict):
                continue

            doc_name = (item.get("documentName") or "").strip()
            # Note: API response uses the key 'docuementURL' with fallback to 'documentURL'
            raw_url = (item.get("docuementURL") or item.get("documentURL") or "").strip()
            posted_date = (item.get("documentPostedDate") or "").strip()
            order_no = item.get("orderNo")
            doc_id = item.get("documentID")

            if not doc_name or not raw_url:
                continue

            lower_name = doc_name.lower()

            # Filter rule: Must be monthly, not weekly or adhoc
            is_monthly = "monthly" in lower_name and "weekly" not in lower_name and "adhoc" not in lower_name
            if not is_monthly:
                continue

            full_url = urllib.parse.urljoin(self.BASE_URL, raw_url)
            path_part = urllib.parse.urlparse(full_url).path
            filename = os.path.basename(path_part)
            if not filename or filename.startswith("?"):
                filename = f"axis_portfolio_{year}_{month:02d}.xlsx"

            portfolios.append({
                "amc": "Axis Mutual Fund",
                "year": int(year),
                "month": month,
                "month_name": month_name,
                "scheme_code": scheme_code,
                "document_name": doc_name,
                "date": posted_date,
                "url": full_url,
                "filename": filename,
                "document_id": doc_id,
                "order_no": order_no,
            })

        return portfolios

    def _check_file_count(self, file_count: int, year: int, month: int):
        """
        Sanity check file count (logging only, never fails).
        """
        if file_count < 1:
            logger.warning(f"File count ({file_count}) is 0 for {year}-{month:02d}")
        else:
            logger.info(f"File count: {file_count} file(s) for {year}-{month:02d}")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        """
        Create atomic completion marker.
        """
        marker_path = target_dir / "_SUCCESS.json"
        tmp_marker_path = target_dir / "_SUCCESS.json.tmp"
        
        marker_data = {
            "amc": "axis",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        
        # Atomic write: write to tmp, then rename
        with open(tmp_marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        
        tmp_marker_path.rename(marker_path)
        logger.info(f"Created completion marker: {marker_path.name}")

    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str):
        """
        Move incomplete/corrupt folder to quarantine.
        """
        corrupt_base = Path("data/raw/axis/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)
        
        corrupt_target = corrupt_base / f"{year}_{month:02d}"
        
        # If corrupt target already exists, append timestamp to preserve it
        if corrupt_target.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            corrupt_target = corrupt_base / f"{year}_{month:02d}_{timestamp}"
        
        # Move to corrupt
        shutil.move(str(source_dir), str(corrupt_target))
        logger.warning(f"Moved incomplete folder to _corrupt: {corrupt_target.name} (Reason: {reason})")
        self.notifier.notify_error("Axis", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        """
        Download Axis monthly portfolio files using official CMS API.
        
        Args:
            year: Calendar year (e.g., 2026)
            month: Month (1-12)
            
        Returns:
            Download metadata dictionary
        """
        start_time = time.time()
        
        logger.info("=" * 60)
        logger.info("AXIS MUTUAL FUND API DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Check for incomplete month (folder exists but no _SUCCESS.json)
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if not success_marker.exists():
                logger.warning(f"Incomplete month detected: {year}-{month:02d}")
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json marker")
            else:
                # Month already complete - verify consolidation
                logger.info(f"Axis: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
                
                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: Axis")
                logger.info(f"Mode: SKIPPED")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Status: COMPLETE")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info("=" * 60)
                
                return {
                    "amc": "axis",
                    "year": year,
                    "month": month,
                    "files_downloaded": 0,
                    "status": "skipped",
                    "duration": duration
                }
        
        # Create directory
        self.ensure_directory(str(target_dir))

        # DRY RUN MODE
        if DRY_RUN:
            logger.info("[DRY RUN] Would call API and download files")
            logger.info("[DRY RUN] Skipping actual network calls")
            duration = time.time() - start_time
            
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: Axis")
            logger.info(f"Mode: DRY RUN")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Status: SIMULATED")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info("=" * 60)
            
            return {
                "amc": "axis",
                "year": year,
                "month": month,
                "files_downloaded": 0,
                "status": "dry_run"
            }

        try:
            # Step 1: Fetch documents via CMS API
            logger.info(f"Querying Axis documents API for {year}-{month:02d} (schemeCode='Consolidated')...")
            matching_docs = self._fetch_monthly_portfolio_documents(year, month, scheme_code="Consolidated")
            
            # Step 2: Handle not published
            if not matching_docs:
                logger.warning(f"No matching monthly consolidated portfolio found for {year}-{month:02d}")
                logger.warning("Month not yet published")
                
                duration = time.time() - start_time
                
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: Axis")
                logger.info(f"Mode: AUTO")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: 0")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: NOT PUBLISHED")
                logger.info("=" * 60)
                
                return {
                    "amc": "axis",
                    "year": year,
                    "month": month,
                    "files_downloaded": 0,
                    "status": "not_published",
                    "duration": duration
                }
            
            logger.info(f"Found {len(matching_docs)} matching monthly document(s)")
            
            # Step 3: Download files
            saved_files: List[str] = []
            
            for i, doc in enumerate(matching_docs, 1):
                file_url = doc["url"]
                filename = doc["filename"]
                # Clean filename
                filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
                file_dest = target_dir / filename
                
                logger.info(f"Downloading {i}/{len(matching_docs)}: {doc['document_name']} -> {filename}")
                
                # Stream download with retry
                download_ok = False
                for attempt in range(MAX_RETRIES + 1):
                    try:
                        r = self.session.get(file_url, stream=True, timeout=60)
                        r.raise_for_status()
                        
                        with open(file_dest, "wb") as fp:
                            for chunk in r.iter_content(chunk_size=65536):
                                if chunk:
                                    fp.write(chunk)
                        
                        download_ok = True
                        break
                    except (requests.Timeout, requests.RequestException) as err:
                        if attempt < MAX_RETRIES:
                            backoff = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                            logger.warning(f"Download attempt {attempt + 1} failed: {err}. Retrying in {backoff}s...")
                            time.sleep(backoff)
                        else:
                            raise
                
                if not download_ok:
                    raise RuntimeError(f"Failed downloading {file_url}")

                # Validate file format and fix extension if needed
                validated_path = validate_and_fix_extension(file_dest)
                if validated_path != file_dest:
                    file_dest = validated_path
                
                # Check magic bytes
                if file_dest.stat().st_size == 0:
                    raise ValueError(f"Downloaded file is 0 bytes: {file_dest.name}")
                
                with open(file_dest, "rb") as f:
                    magic = f.read(8)
                is_xlsx = magic.startswith(b"PK\x03\x04")
                is_xls = magic.startswith(b"\xd0\xcf\x11\xe0")
                if not (is_xlsx or is_xls):
                    logger.warning(f"File {file_dest.name} has unexpected magic bytes: {magic[:4]!r}")

                saved_files.append(str(file_dest))
                logger.success(f"Saved ({file_dest.stat().st_size:,} bytes): {file_dest.name}")
            
            # Sanity check file count (logging only)
            self._check_file_count(len(saved_files), year, month)
            
            # Create atomic completion marker
            self._create_success_marker(target_dir, year, month, len(saved_files))
            
            # Consolidate downloads into merged excels
            self.consolidate_downloads(year, month)
            
            duration = time.time() - start_time
            
            logger.success("[SUCCESS] AXIS download completed")
            logger.info("=" * 60)
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: Axis")
            logger.info(f"Mode: AUTO")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Files downloaded: {len(saved_files)}")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Status: SUCCESS")
            logger.info("=" * 60)

            return {
                "amc": "axis",
                "year": year,
                "month": month,
                "files_downloaded": len(saved_files),
                "status": "success",
                "duration": duration
            }

        except requests.HTTPError as e:
            error_msg = f"API request failed: HTTP {e.response.status_code if e.response is not None else e}"
            logger.error(error_msg)
            self.notifier.notify_error("Axis", year, month, "HTTP Error", error_msg)
            duration = time.time() - start_time
            
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: Axis")
            logger.info(f"Mode: AUTO")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Files downloaded: 0")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Status: FAILED")
            logger.info("=" * 60)
            
            return {
                "amc": "axis",
                "year": year,
                "month": month,
                "status": "failed",
                "reason": error_msg,
                "duration": duration
            }
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Download failed: {error_msg}")
            self.notifier.notify_error("Axis", year, month, "Download Exception", error_msg)
            duration = time.time() - start_time
            
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: Axis")
            logger.info(f"Mode: AUTO")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Files downloaded: 0")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Status: FAILED")
            logger.info("=" * 60)
            
            return {
                "amc": "axis",
                "year": year,
                "month": month,
                "status": "failed",
                "reason": error_msg,
                "duration": duration
            }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Axis Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Calendar year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    
    args = parser.parse_args()
    
    if args.month < 1 or args.month > 12:
        logger.error("Invalid month. Must be between 1 and 12.")
        exit(1)
    
    downloader = AxisDownloader()
    result = downloader.download(year=args.year, month=args.month)
    
    status = result["status"]
    if status == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"[INFO] Info: Month not yet published")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        exit(1)
