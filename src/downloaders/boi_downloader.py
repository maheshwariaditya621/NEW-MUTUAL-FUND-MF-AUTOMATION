# src/downloaders/boi_downloader.py

import os
import time
import json
import shutil
import re
import socket
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Any
import requests
import urllib3.util.connection as urllib_conn

# Force urllib3 to use IPv4 only (bypasses Azure Front Door IPv6 RST bug)
urllib_conn.allowed_gai_family = lambda: socket.AF_INET

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

# Import downloader config
try:
    from src.config.downloader_config import (
        DRY_RUN, MAX_RETRIES, RETRY_BACKOFF, HEADLESS
    )
except ImportError:
    DRY_RUN = False
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]
    HEADLESS = True


class BOIDownloader(BaseDownloader):
    """
    Bank of India Mutual Fund - Portfolio Downloader
    
    Uses AjaxService.asmx/GetDocuments API to fetch and download consolidated monthly portfolios.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Bank of India Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "boi"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "BOI",
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
        
        logger.warning(f"BOI: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("BOI", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")


    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("BOI MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"BOI: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"🕒 Duration: {duration:.2f}s")
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
                    logger.info(f"BOI: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"BOI: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("BOI", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("BOI", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] BOI download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("BOI", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        api_url = "https://www.boimf.in/AjaxService.asmx/GetDocuments"
        logger.info(f"Querying BOI GetDocuments API for {month_name} {target_year}...")

        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
        })

        headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }
        payload = {
            "pagno": 0,
            "category": None,
            "fromDate": None,
            "toDate": None,
            "LibraryName": "InvestorCorner",
            "CategoryValue": "no",
            "folderName": "MONTHLY PORTFOLIO"
        }

        resp = session.post(api_url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()

        outer_json = resp.json()
        raw_d = outer_json.get("d", "")
        if not raw_d:
            logger.warning("Empty 'd' response from GetDocuments API")
            return None

        nested_json = json.loads(raw_d)
        documents = nested_json.get("Documents", [])
        if not documents:
            logger.warning("No documents returned in GetDocuments catalog")
            return None

        month_upper = month_name.upper()
        pattern = f"{month_upper}-{target_year}"

        target_doc = None
        for doc in documents:
            doc_name = (doc.get("DocName") or "").upper()
            folder_url = (doc.get("FolderUrl") or "").upper()
            if pattern in doc_name or pattern in folder_url:
                target_doc = doc
                break

        if not target_doc:
            logger.warning(f"No monthly portfolio document found matching {pattern}")
            return None

        dl_url = target_doc.get("FolderUrl", "").strip()
        if not dl_url:
            logger.warning(f"Document {target_doc.get('DocName')} has empty FolderUrl")
            return None

        clean_filename = dl_url.split("/")[-1].split("?")[0]
        if not clean_filename.endswith(".xlsx"):
            clean_filename = f"monthly-portfolio---{month_name.lower()}-{target_year}.xlsx"

        save_path = download_folder / clean_filename
        logger.info(f"Downloading: {clean_filename} from {dl_url}...")

        dl_resp = session.get(dl_url, stream=True, timeout=60)
        if dl_resp.status_code != 200:
            logger.error(f"Failed to download {dl_url}: HTTP {dl_resp.status_code}")
            return None

        with open(save_path, "wb") as f:
            for chunk in dl_resp.iter_content(chunk_size=8192):
                f.write(chunk)

        # Validate magic bytes
        with open(save_path, "rb") as f:
            magic = f.read(8)

        if not magic.startswith(b"PK\x03\x04"):
            logger.error(f"Invalid XLSX signature for {clean_filename}, deleting...")
            save_path.unlink(missing_ok=True)
            return None

        logger.info(f"Downloaded and verified successfully: {clean_filename} ({save_path.stat().st_size:,} bytes)")
        return save_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = BOIDownloader()
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
