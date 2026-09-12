import os
import time
import json
import shutil
import re
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import openpyxl
from curl_cffi import requests as cffi_requests

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


class ThreeSixtyOneDownloader(BaseDownloader):
    """
    360 ONE Mutual Fund (formerly IIFL) - Portfolio Downloader
    
    Downloads official monthly consolidated portfolio workbooks from 360 ONE's Next.js application
    and AWS S3 storage without browser automation.
    
    URL: https://www.360.one/asset/mutual-funds/downloads/
    """
    
    PAGE_URL = "https://www.360.one/asset/mutual-funds/downloads/"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.360.one/",
    }

    DOWNLOAD_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self, impersonate: str = "chrome124"):
        super().__init__("360 ONE Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "threesixtyone"
        self.impersonate = impersonate
        self._session = None

    @property
    def session(self) -> cffi_requests.Session:
        if self._session is None:
            self._session = cffi_requests.Session(impersonate=self.impersonate)
        return self._session

    def _fetch_disclosures_catalog(self) -> Dict[str, Any]:
        """Fetch the downloads page and extract the complete disclosures JSON catalog."""
        resp = self.session.get(self.PAGE_URL, headers=self.DEFAULT_HEADERS, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to fetch 360 ONE downloads page: HTTP {resp.status_code}")

        html = resp.text

        # Extract Next.js App Router RSC payload chunks
        pushes = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html)
        if not pushes:
            raise ValueError("No Next.js RSC payload found in page HTML.")

        full_payload = ""
        for p in pushes:
            unescaped = p.encode("utf-8").decode("unicode_escape", errors="ignore")
            full_payload += unescaped

        # Locate the "disclosures":{ object
        start_key = '"disclosures":{'
        start_idx = full_payload.find(start_key)
        if start_idx == -1:
            raise ValueError("Could not find 'disclosures' key in Next.js payload.")

        start_brace = start_idx + len('"disclosures":')
        brace_count = 0
        end_idx = -1

        for i in range(start_brace, len(full_payload)):
            if full_payload[i] == "{":
                brace_count += 1
            elif full_payload[i] == "}":
                brace_count -= 1
                if brace_count == 0:
                    end_idx = i + 1
                    break

        if end_idx == -1:
            raise ValueError("Failed to parse balanced JSON for 'disclosures'.")

        disclosures_str = full_payload[start_brace:end_idx]
        return json.loads(disclosures_str)

    def _find_monthly_portfolio_document(
        self, year: int, month: int, catalog: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, str]]:
        """Locate the monthly portfolio document record for a specific year and month."""
        if catalog is None:
            catalog = self._fetch_disclosures_catalog()

        # Locate "Monthly Portfolio" subcategory
        monthly_subcategory = None
        for sub in catalog.get("subcategories", []):
            if sub.get("title", "").strip().lower() == "monthly portfolio":
                monthly_subcategory = sub
                break

        if not monthly_subcategory:
            logger.warning("Monthly Portfolio subcategory not found in disclosures catalog.")
            return None

        target_month_name = self.MONTH_NAMES[month].lower()
        target_year_str = str(year)

        yearly_data = monthly_subcategory.get("yearlyData", [])
        for y_entry in yearly_data:
            y_label = str(y_entry.get("year", "")).lower()
            if target_year_str not in y_label:
                continue

            monthly_data = y_entry.get("monthlyData", [])
            for m_entry in monthly_data:
                m_label = str(m_entry.get("month", "")).lower()
                doc_groups = m_entry.get("documentGroups", [])

                for dg in doc_groups:
                    for doc in dg.get("documents", []):
                        file_name = str(doc.get("fileName", "")).lower()
                        file_url = doc.get("fileUrl", "")

                        # Match by month label or filename
                        if target_month_name in m_label or target_month_name in file_name or target_month_name in file_url.lower():
                            return {
                                "year": str(year),
                                "month": target_month_name.capitalize(),
                                "fileName": doc.get("fileName", ""),
                                "fileUrl": file_url,
                            }

        return None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "360ONE",
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
        
        logger.warning(f"360ONE: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("360ONE", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("360 ONE MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                # Month already complete - check for missing consolidation
                logger.info(f"360 ONE: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"360ONE: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"360ONE: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("360ONE", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("360ONE", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] 360 ONE download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("360ONE", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, download_folder: Path) -> Optional[Path]:
        """Direct REST API + S3 download flow for 360 ONE without browser automation."""
        doc = self._find_monthly_portfolio_document(target_year, target_month)
        if not doc:
            return None

        file_url = doc["fileUrl"]
        url_filename = file_url.split("/")[-1]
        stem = Path(url_filename).stem
        ext = Path(url_filename).suffix or ".xlsx"
        target_filename = f"{stem}{ext}"
        dest_path = download_folder / target_filename

        logger.info(f"360ONE: Downloading from {file_url} to {dest_path.name}...")
        resp = self.session.get(file_url, headers=self.DOWNLOAD_HEADERS, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"Download failed with HTTP {resp.status_code}: {file_url}")

        content = resp.content
        is_zip = content.startswith(b"PK\x03\x04")
        is_ole = content.startswith(b"\xd0\xcf\x11\xe0")

        if not (is_zip or is_ole):
            raise ValueError(f"Downloaded content is not valid Excel/ZIP. Magic: {content[:8]}")

        # Normalize extension to .xlsx if OpenXML zip container
        if is_zip and dest_path.suffix.lower() == ".xls":
            dest_path = dest_path.with_suffix(".xlsx")

        with open(dest_path, "wb") as f:
            f.write(content)

        # Validate readability
        if is_zip:
            wb = openpyxl.load_workbook(dest_path, read_only=True)
            sheet_count = len(wb.sheetnames)
            wb.close()
            logger.info(f"360ONE: Downloaded & verified OpenXML portfolio ({len(content):,} bytes, {sheet_count} sheets): {dest_path.name}")
        else:
            import xlrd
            wb = xlrd.open_workbook(dest_path)
            sheet_count = len(wb.sheet_names())
            logger.info(f"360ONE: Downloaded & verified OLE portfolio ({len(content):,} bytes, {sheet_count} sheets): {dest_path.name}")

        return dest_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = ThreeSixtyOneDownloader()
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
