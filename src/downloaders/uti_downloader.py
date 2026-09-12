# src/downloaders/uti_downloader.py

import os
import time
import json
import shutil
import zipfile
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Any

import requests

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


class UTIDownloader(BaseDownloader):
    """
    UTI Mutual Fund - Monthly Consolidated Portfolio Downloader

    URL: https://www.utimf.com/downloads/consolidate-all-portfolio-disclosure
    API: GET https://www.utimf.com/api/get-consolidate-portfolio-disclosure
    Features:
    - Pure requests (no browser/Playwright required).
    - Downloads the official consolidated ZIP archive.
    - Extracts the master SEBI Exposure portfolio workbook (.xlsx / .xls).
    - Idempotency via _SUCCESS.json and automatic consolidation into merged workbook.
    """

    API_URL = "https://www.utimf.com/api/get-consolidate-portfolio-disclosure"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("UTI Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "uti"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "UTI",
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

        logger.warning(f"UTI: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("UTI", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]

        logger.info("=" * 60)
        logger.info("UTI MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"UTI: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"UTI: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)

                if not downloaded_path:
                    logger.warning(f"UTI: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("UTI", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("UTI", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] UTI download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("UTI", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _discover_consolidated_record(self, session: requests.Session, year: int, month_name: str) -> Optional[Dict[str, Any]]:
        api_url = f"{self.API_URL}?year={year}&month={month_name}"
        logger.info(f"Calling UTI Consolidated Portfolio API: {api_url}")

        resp = session.get(api_url, headers=self.HEADERS, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        rows = data.get("rows", [])
        logger.info(f"API returned {len(rows)} row(s)")

        for row in rows:
            cat = row.get("category", "")
            rtype = row.get("type", "")
            if cat == "Consolidate portfolio disclosure" and rtype.lower() == "zip":
                return row

        return None

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        session = requests.Session()
        record = self._discover_consolidated_record(session, target_year, month_name)

        if not record:
            logger.warning(f"No matching consolidated portfolio record for {month_name} {target_year}")
            return None

        download_url = record.get("url") or record.get("doc")
        name = record.get("name", f"Consolidated Portfolio {month_name} {target_year}")
        logger.info(f"Found record: '{name}'")
        logger.info(f"Download URL: {download_url}")

        temp_zip = download_folder / f"temp_{month_name}_{target_year}.zip"
        logger.info(f"Downloading ZIP archive...")

        with session.get(download_url, headers=self.HEADERS, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(temp_zip, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

        zip_size = temp_zip.stat().st_size
        logger.info(f"Downloaded ZIP size: {zip_size:,} bytes")

        # Validate ZIP archive integrity
        with zipfile.ZipFile(temp_zip, "r") as z:
            corrupt = z.testzip()
            if corrupt is not None:
                temp_zip.unlink(missing_ok=True)
                raise ValueError(f"Corrupt file inside downloaded ZIP archive: {corrupt}")

        # Extract master SEBI Exposure file
        logger.info("Extracting SEBI Exposure file from ZIP...")
        final_path = self._extract_sebi_file(temp_zip, download_folder, month_name, target_year)

        if not final_path:
            raise ValueError("SEBI Exposure file not found inside ZIP archive")

        return final_path

    def _extract_sebi_file(self, zip_path: Path, target_folder: Path, month_name: str, year: int) -> Optional[Path]:
        """Extract SEBI Exposure file from ZIP and clean up archive."""
        temp_extract = target_folder / "temp_extract"
        temp_extract.mkdir(exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(temp_extract)

            # Look for SEBI Exposure file
            found_file = None
            for root, dirs, files in os.walk(temp_extract):
                for file in files:
                    normalized_name = file.lower().replace(" ", "")

                    if (normalized_name.startswith("sebiexposure") or
                        normalized_name.startswith("sebi_exposure")) and \
                       (file.endswith(".xlsx") or file.endswith(".xls")):
                        found_file = os.path.join(root, file)
                        break
                if found_file:
                    break

            if found_file:
                original_name = Path(found_file).name
                final_path = target_folder / original_name

                # Check for collision
                if final_path.exists():
                    final_path = target_folder / f"{month_name}_{year}_{original_name}"

                shutil.move(found_file, final_path)
                logger.info(f"  [OK] Extracted: {final_path.name}")
                return final_path
            else:
                logger.warning("  [FAIL] SEBI Exposure file not found in ZIP")
                return None

        finally:
            # Cleanup temporary archive and folder
            if zip_path.exists():
                zip_path.unlink()
            if temp_extract.exists():
                shutil.rmtree(temp_extract, ignore_errors=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = UTIDownloader()
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
