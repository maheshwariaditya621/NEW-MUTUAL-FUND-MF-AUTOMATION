# src/downloaders/franklin_downloader.py

import os
import time
import json
import shutil
import re
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple
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


BASE_URL = "https://www.franklintempletonindia.com"
API_URL = f"{BASE_URL}/api/literature/v1/responseLitJson?type=report"


class FranklinDownloader(BaseDownloader):
    """
    Franklin Templeton Mutual Fund - Portfolio Downloader
    
    Direct requests-based scraper using the literature API:
    https://www.franklintempletonindia.com/api/literature/v1/responseLitJson?type=report
    Downloads consolidated monthly portfolio disclosure Excel files.
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Franklin Templeton Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "franklin"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": f"{BASE_URL}/reports",
        })
        logger.info("FranklinDownloader initialized (Requests API Version)")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "FRANKLIN",
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
        
        logger.warning(f"FRANKLIN: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("FRANKLIN", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def parse_month_year(self, record: dict) -> Tuple[Optional[int], Optional[int]]:
        """Extract month and year from reference date or title."""
        ref_date = record.get("frkReferenceDate", "")
        if ref_date and re.match(r"^\d{4}-\d{2}-\d{2}$", ref_date):
            parts = ref_date.split("-")
            return int(parts[1]), int(parts[0])

        title = record.get("dctermsTitle", "")
        href = record.get("literatureHref", "")
        text = f"{title} {href}".lower()

        year_match = re.search(r"\b(20\d{2})\b", text)
        year = int(year_match.group(1)) if year_match else None

        month = None
        for name in calendar.month_name:
            if name and re.search(rf"\b{name.lower()}\b", text):
                month = list(calendar.month_name).index(name)
                break
        if not month:
            for abbr in calendar.month_abbr:
                if abbr and re.search(rf"\b{abbr.lower()}\b", text):
                    month = list(calendar.month_abbr).index(abbr)
                    break

        return month, year

    def build_download_url(self, literature_href: str) -> str:
        """Construct final download URL with /download prefix."""
        if not literature_href:
            return ""
        if literature_href.startswith("http"):
            return literature_href
        cleaned_href = literature_href.lstrip("/")
        return f"{BASE_URL}/download/{cleaned_href}"

    def _validate_excel_file(self, file_path: Path) -> bool:
        """Validate ZIP signature and openpyxl readable workbook."""
        if not file_path.exists() or file_path.stat().st_size == 0:
            return False

        with open(file_path, "rb") as f:
            magic = f.read(4)
        if magic != b"PK\x03\x04":
            logger.error(f"FRANKLIN: Invalid magic bytes for {file_path.name}: {magic.hex()}")
            return False

        try:
            wb = openpyxl.load_workbook(str(file_path), read_only=True, data_only=True)
            _ = wb.sheetnames
            wb.close()
            return True
        except Exception as e:
            logger.error(f"FRANKLIN: openpyxl validation failed for {file_path.name}: {e}")
            return False

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        logger.info(f"Fetching catalog from {API_URL}...")
        resp = self.session.get(API_URL, timeout=60)
        resp.raise_for_status()
        catalog_data = resp.json()

        categories = catalog_data.get("FirstDropDown", [])
        monthly_category = None
        for cat in categories:
            if cat.get("id") == "MONTHLY-PORTFOLIO-DSCLR":
                monthly_category = cat
                break

        if not monthly_category:
            logger.error("FRANKLIN: 'MONTHLY-PORTFOLIO-DSCLR' category not found in API response.")
            return None

        raw_records = monthly_category.get("dataRecords", {}).get("linkdata", [])
        logger.info(f"FRANKLIN: Found {len(raw_records)} records in MONTHLY-PORTFOLIO-DSCLR.")

        target_record = None
        for r in raw_records:
            m, y = self.parse_month_year(r)
            if m == target_month and y == target_year:
                target_record = r
                break

        if not target_record:
            logger.warning(f"FRANKLIN: No portfolio record found for {month_name} {target_year}.")
            return None

        lit_href = target_record.get("literatureHref", "")
        download_url = self.build_download_url(lit_href)
        filename = os.path.basename(lit_href.split("?")[0])
        target_path = download_folder / filename

        logger.info(f"Downloading {filename} from {download_url}...")
        download_resp = self.session.get(download_url, stream=True, timeout=60)
        if download_resp.status_code != 200:
            logger.error(f"FRANKLIN: Download failed with status {download_resp.status_code}")
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

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("FRANKLIN TEMPLETON MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Franklin: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"FRANKLIN: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"FRANKLIN: No portfolio file found for {month_name} {year}")
                    self.notifier.notify_not_published("FRANKLIN", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("FRANKLIN", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] FRANKLIN download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("FRANKLIN", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = FranklinDownloader()
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
