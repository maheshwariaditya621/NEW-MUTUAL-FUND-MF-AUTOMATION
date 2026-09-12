import os
import time
import json
import shutil
import re
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
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


API_URL = "https://www.pgimindia.com/api/v1/brochure/published/disclosure"


class PGIMIndiaDownloader(BaseDownloader):
    """
    PGIM India Mutual Fund - Portfolio Downloader
    
    URL: https://www.pgimindia.com/mutual-funds/disclosures/Portfolios/Monthly-Portfolio
    Pure Python requests implementation using the official published disclosure API:
    POST https://www.pgimindia.com/api/v1/brochure/published/disclosure
    """
    
    MONTH_ABBR = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    MONTH_FULL = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("PGIM India Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "pgim_india"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": "https://www.pgimindia.com",
            "Referer": "https://www.pgimindia.com/mutual-funds/disclosures/Portfolios/Monthly-Portfolio",
        })
        logger.info("PGIMIndiaDownloader initialized (Pure requests REST API version)")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "PGIM India",
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
        
        logger.warning(f"{self.AMC_NAME}: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("PGIM India", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _validate_excel_file(self, file_path: Path) -> bool:
        """Validate ZIP/XLS signature and openpyxl readable workbook."""
        if not file_path.exists() or file_path.stat().st_size == 0:
            return False

        with open(file_path, "rb") as f:
            magic = f.read(4)
        if magic != b"PK\x03\x04" and magic != b"\xd0\xcf\x11\xe0":
            logger.error(f"PGIM India: Invalid magic bytes for {file_path.name}: {magic.hex()}")
            return False

        if magic == b"PK\x03\x04":
            try:
                wb = openpyxl.load_workbook(str(file_path), read_only=True, data_only=True)
                _ = wb.sheetnames
                wb.close()
                return True
            except Exception as e:
                logger.error(f"PGIM India: openpyxl validation failed for {file_path.name}: {e}")
                return False

        return True

    def _fetch_catalog(self) -> Dict[str, Any]:
        """Calls the PGIM published disclosure API."""
        payload = {
            "headerId": 2,
            "sectionId": "SECTION_747960037",
            "source": "W",
            "branchCode": None
        }
        logger.info(f"PGIM India: Requesting published disclosure catalog: {API_URL}")
        resp = self.session.post(API_URL, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _discover_links(self, catalog: Dict[str, Any], year: int, month: int) -> List[Dict[str, Any]]:
        """Filters catalog records matching target reporting year and month."""
        month_full = self.MONTH_FULL[month].lower()
        month_abbr = self.MONTH_ABBR[month].lower()
        year_str = str(year)
        
        tabs = catalog.get("data", [])
        discovered = []
        seen_ids = set()
        seen_urls = set()

        for tab in tabs:
            tab_name = tab.get("tabName", "")
            for item in tab.get("content", []):
                disc_id = item.get("disclosureId")
                title = item.get("title", "").strip()
                pdf_path = item.get("pdfPath", "").strip()
                item_month = str(item.get("month", "")).strip().lower()
                item_year = str(item.get("year", "")).strip()
                dmy = str(item.get("dateMonthYear", "")).strip().lower()
                
                # Exclude unwanted disclosures
                combined_meta = f"{title} {pdf_path}".lower()
                if any(ex in combined_meta for ex in ["fortnightly", "daily", "overlap", "financial statement", "notice"]):
                    continue
                
                month_match = (
                    item_month == month_full or
                    f" {month_full} " in f" {dmy} " or
                    f"{month_abbr} {year_str}" in combined_meta or
                    f"{month_abbr}{year_str}" in combined_meta
                )
                year_match = (
                    item_year == year_str or
                    year_str in dmy or
                    year_str in combined_meta
                )
                
                if month_match and year_match:
                    if disc_id in seen_ids or pdf_path in seen_urls:
                        continue
                    seen_ids.add(disc_id)
                    seen_urls.add(pdf_path)
                    
                    scheme_clean = re.sub(
                        rf"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*{year_str}\b",
                        "",
                        title,
                        flags=re.IGNORECASE
                    ).strip()
                    
                    discovered.append({
                        "scheme": scheme_clean or title,
                        "title": title,
                        "category_tab": tab_name,
                        "disclosure_id": disc_id,
                        "url": pdf_path
                    })

        return discovered

    def _run_download_flow(self, target_year: int, target_month: int, month_abbr: str, month_full: str, download_folder: Path) -> int:
        catalog = self._fetch_catalog()
        records = self._discover_links(catalog, target_year, target_month)
        
        if not records:
            logger.warning(f"PGIM India: No portfolio records found for {month_full} {target_year}")
            return 0
            
        logger.info(f"PGIM India: Found {len(records)} portfolio record(s) for {month_full} {target_year}.")
        success_count = 0
        
        for idx, item in enumerate(records, 1):
            url = item["url"]
            title = item["title"]
            scheme = item["scheme"]
            
            parsed_url = urllib.parse.urlparse(url)
            raw_name = urllib.parse.unquote(os.path.basename(parsed_url.path))
            if not raw_name or not (raw_name.lower().endswith(".xlsx") or raw_name.lower().endswith(".xls")):
                clean_title = re.sub(r"[^\w\-_.]", "_", title)
                raw_name = f"{clean_title}.xlsx"
                
            target_path = download_folder / raw_name
            temp_path = target_path.with_name(target_path.stem + ".tmp.xlsx")
            
            logger.info(f"  [{idx}/{len(records)}] Downloading: {title}...")
            try:
                resp = self.session.get(url, stream=True, timeout=60)
                if resp.status_code != 200:
                    logger.error(f"    [FAIL] HTTP {resp.status_code} for {title}")
                    continue
                
                with open(temp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=16384):
                        if chunk:
                            f.write(chunk)
                
                if self._validate_excel_file(temp_path):
                    if target_path.exists():
                        target_path.unlink()
                    temp_path.rename(target_path)
                    logger.info(f"    [OK] Saved: {target_path.name} ({target_path.stat().st_size:,} bytes)")
                    success_count += 1
                else:
                    if temp_path.exists():
                        temp_path.unlink()
                    logger.error(f"    [FAIL] Validation failed for {title}")
            except Exception as e:
                if temp_path.exists():
                    temp_path.unlink()
                logger.error(f"    [FAIL] Error downloading {title}: {e}")
                
        return success_count

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        month_abbr = self.MONTH_ABBR[month]
        month_full = self.MONTH_FULL[month]
        
        logger.info("=" * 60)
        logger.info(f"PGIM INDIA MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_abbr})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"PGIM India: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"{self.AMC_NAME}: [DRY RUN] Would download {month_abbr} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_abbr, month_full, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"{self.AMC_NAME}: No portfolios found for {month_abbr} {year}")
                    self.notifier.notify_not_published("PGIM India", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads into merged excels
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("PGIM India", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("PGIM India", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = PGIMIndiaDownloader()
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
