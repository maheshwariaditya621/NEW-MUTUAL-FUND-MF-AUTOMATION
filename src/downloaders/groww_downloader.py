# src/downloaders/groww_downloader.py

import os
import time
import json
import shutil
import re
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple, List
import requests
from bs4 import BeautifulSoup
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


BASE_URL = "https://growwmf.in"
PORTFOLIO_PAGE_URL = f"{BASE_URL}/statutory-disclosure/portfolio"

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


class GrowwDownloader(BaseDownloader):
    """
    Groww Mutual Fund - Portfolio Downloader
    
    Direct requests-based scraper using Next.js statutory disclosure data:
    https://growwmf.in/statutory-disclosure/portfolio
    Downloads consolidated monthly portfolio disclosure Excel files.
    """
    
    MONTH_MAP = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Groww Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "groww"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        logger.info("GrowwDownloader initialized (Requests + Next.js API Version)")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "GROWW",
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
        
        logger.warning(f"GROWW: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("GROWW", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def parse_month_year(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        """Extract month (1-12) and year (YYYY) from text or filename."""
        text_clean = text.replace("-", " ").replace("_", " ").lower()

        year_match = re.search(r"\b(20\d{2})\b", text_clean)
        year = int(year_match.group(1)) if year_match else None

        month = None
        for name, num in MONTH_NAMES.items():
            if re.search(rf"\b{name}\b", text_clean):
                month = num
                break

        return month, year

    def fetch_page_and_files_data(self) -> dict:
        """Fetch statutory disclosure page and extract Next.js filesData."""
        resp = self.session.get(PORTFOLIO_PAGE_URL, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        tag = soup.find("script", id="__NEXT_DATA__")
        if not tag or not tag.string:
            raise ValueError("Could not find __NEXT_DATA__ script tag on Groww portfolio page.")

        next_data = json.loads(tag.string)
        files_data = next_data.get("props", {}).get("pageProps", {}).get("filesData", {})

        # If filesData not embedded in pageProps, query _next/data endpoint
        if not files_data:
            build_id = next_data.get("buildId", "")
            if build_id:
                api_url = f"{BASE_URL}/_next/data/{build_id}/statutory-disclosure/portfolio.json?pageName=portfolio"
                api_resp = self.session.get(
                    api_url,
                    headers={**self.session.headers, "Accept": "application/json"},
                    timeout=30
                )
                if api_resp.status_code == 200:
                    files_data = api_resp.json().get("pageProps", {}).get("filesData", {})

        return files_data

    def extract_monthly_portfolios(self, files_data: dict) -> List[Dict]:
        """
        Traverse filesData and return all consolidated monthly portfolio records.
        """
        monthly_portfolios = []

        def traverse(node: dict, current_path: str = ""):
            name = node.get("name", "")
            new_path = f"{current_path} / {name}" if current_path else name

            for f in node.get("files", []):
                fname = f.get("name", "")
                fname_lower = fname.lower()
                path_lower = new_path.lower()

                # Must be under Portfolio (and not Exposure / Tracking / AUM)
                if "portfolio" in path_lower and not any(x in path_lower for x in ["exposure", "tracking", "aum", "money market"]):
                    if "fortnightly" not in fname_lower and ("monthly" in fname_lower or "monthly portfolio" in path_lower):
                        m, y = self.parse_month_year(fname)
                        if not y and current_path:
                            _, y2 = self.parse_month_year(current_path)
                            y = y or y2

                        monthly_portfolios.append({
                            "path": new_path,
                            "filename": fname,
                            "publicUrl": f.get("publicUrl", ""),
                            "month": m,
                            "year": y,
                        })

            for sub in node.get("folders", []):
                traverse(sub, new_path)

        traverse(files_data)
        return monthly_portfolios

    def _validate_excel_file(self, file_path: Path) -> bool:
        """Validate ZIP signature and workbook opening via openpyxl."""
        if not file_path.exists() or file_path.stat().st_size == 0:
            return False

        with open(file_path, "rb") as f:
            magic = f.read(4)

        if magic == b"PK\x03\x04":
            try:
                wb = openpyxl.load_workbook(str(file_path), read_only=True, data_only=True)
                _ = wb.sheetnames
                wb.close()
                return True
            except Exception as e:
                logger.error(f"GROWW: openpyxl validation failed for {file_path.name}: {e}")
                return False
        elif magic == b"\xd0\xcf\x11\xe0":
            return True  # Valid legacy Excel file (.xls)
        else:
            logger.error(f"GROWW: Invalid magic bytes for {file_path.name}: {magic.hex()}")
            return False

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        logger.info(f"Fetching Groww disclosure catalog...")
        files_data = self.fetch_page_and_files_data()
        all_monthly = self.extract_monthly_portfolios(files_data)

        logger.info(f"GROWW: Found {len(all_monthly)} monthly portfolio records across all financial years.")

        target_record = None
        for r in all_monthly:
            if r["year"] == target_year and r["month"] == target_month:
                target_record = r
                break

        if not target_record:
            logger.warning(f"GROWW: No monthly portfolio file found for {month_name} {target_year}.")
            return None

        filename = target_record["filename"]
        download_url = target_record["publicUrl"]
        target_path = download_folder / filename

        logger.info(f"Downloading {filename} from {download_url}...")
        resp = self.session.get(download_url, stream=True, timeout=60)
        if resp.status_code != 200:
            logger.error(f"GROWW: Download failed with status {resp.status_code}")
            return None

        temp_path = target_path.with_name(f"{target_path.stem}.tmp{target_path.suffix}")
        with open(temp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=16384):
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
        month_name = self.MONTH_MAP.get(month, f"Month {month}")
        
        logger.info("=" * 60)
        logger.info("GROWW MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Groww: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"GROWW: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"GROWW: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("GROWW", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("GROWW", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] GROWW download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("GROWW", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = GrowwDownloader()
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
