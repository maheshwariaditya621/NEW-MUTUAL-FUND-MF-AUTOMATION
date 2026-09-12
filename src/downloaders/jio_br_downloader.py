# src/downloaders/jio_br_downloader.py

import os
import time
import json
import shutil
import re
import socket
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
import requests
import openpyxl
import urllib3.util.connection as urllib_conn
from bs4 import BeautifulSoup

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


# Ensure IPv4 priority for Azure Front Door CDN dual-stack stability
urllib_conn.allowed_gai_family = lambda: socket.AF_INET

BASE_PAGE_URL = "https://www.jioblackrockamc.com/statutory-disclosure/disclosures/monthly-portfolio-disclosure"
FALLBACK_ACTION_ID = "70a185f8d6bde1bf8cd922f34944fac15c8ae1a500"


class JioBRDownloader(BaseDownloader):
    """
    Jio BlackRock Mutual Fund - Portfolio Downloader
    
    Direct requests-based scraper using Next.js Server Action / RSC endpoint:
    POST https://www.jioblackrockamc.com/statutory-disclosure/disclosures/monthly-portfolio-disclosure
    
    Downloads ONLY the single consolidated monthly portfolio workbook containing all schemes.
    Discards individual scheme-level files.
    """

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Jio BlackRock Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "jio_br"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
        })
        self._action_id: Optional[str] = None
        logger.info("JioBRDownloader initialized (Requests + Next.js Server Action Version)")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "JIO_BR",
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
        
        logger.warning(f"JIO_BR: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("JIO_BR", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def calculate_fy(self, month: int, year: int) -> str:
        """Calculate Indian FY string with FI prefix: e.g. August 2026 -> FI2026-2027"""
        if month >= 4:
            return f"FI{year}-{year + 1}"
        else:
            return f"FI{year - 1}-{year}"

    def get_reporting_date_str(self, year: int, month: int) -> str:
        """Returns DD-MM-YYYY string for the month-end reporting date."""
        last_day = calendar.monthrange(year, month)[1]
        return f"{last_day:02d}-{month:02d}-{year}"

    def discover_action_id(self) -> str:
        """
        Dynamically discover Next-Action ID from the page chunk script.
        Falls back to known static action ID if network / parsing fails.
        """
        if self._action_id:
            return self._action_id

        try:
            resp = self.session.get(BASE_PAGE_URL, timeout=15)
            soup = BeautifulSoup(resp.text, "html.parser")
            page_scripts = [
                s.get("src") for s in soup.find_all("script")
                if s.get("src") and "statutory-disclosure" in s.get("src") and "page-" in s.get("src")
            ]
            if page_scripts:
                script_url = "https://www.jioblackrockamc.com" + page_scripts[0]
                js_resp = self.session.get(script_url, timeout=15)
                match = re.search(
                    r'createServerReference\)\("([a-f0-9]+)"[^,]*,\s*[^,]*,\s*[^,]*,\s*[^,]*,\s*"getDisclosureL3Data"\)',
                    js_resp.text
                )
                if match:
                    self._action_id = match.group(1)
                    logger.info(f"JIO_BR: Discovered dynamic Next-Action ID: {self._action_id}")
                    return self._action_id
        except Exception as e:
            logger.warning(f"JIO_BR: Could not dynamically extract action ID ({e}), using fallback.")

        self._action_id = FALLBACK_ACTION_ID
        return self._action_id

    def parse_rsc_response(self, rsc_text: str) -> List[Dict]:
        """
        Parse Next.js RSC stream response:
        0:{...}
        1:{"data":[...],"meta":{"pagination":...}}
        """
        data_records = []
        for line in rsc_text.splitlines():
            line = line.strip()
            if not line:
                continue
            colon_idx = line.find(":")
            if colon_idx != -1:
                chunk_json = line[colon_idx + 1:]
                try:
                    parsed = json.loads(chunk_json)
                    if isinstance(parsed, dict) and "data" in parsed and isinstance(parsed["data"], list):
                        data_records.extend(parsed["data"])
                except Exception:
                    pass
        return data_records

    def _validate_excel_file(self, file_path: Path) -> bool:
        """Validate ZIP signature and openpyxl readable workbook."""
        if not file_path.exists() or file_path.stat().st_size == 0:
            return False

        with open(file_path, "rb") as f:
            magic = f.read(4)
        if magic != b"PK\x03\x04":
            logger.error(f"JIO_BR: Invalid magic bytes for {file_path.name}: {magic.hex()}")
            return False

        try:
            wb = openpyxl.load_workbook(str(file_path), read_only=True, data_only=True)
            _ = wb.sheetnames
            wb.close()
            return True
        except Exception as e:
            logger.error(f"JIO_BR: openpyxl validation failed for {file_path.name}: {e}")
            return False

    def _run_download_flow(
        self,
        target_year: int,
        target_month: int,
        month_name: str,
        fy_str: str,
        download_folder: Path
    ) -> Optional[Path]:
        action_id = self.discover_action_id()
        reporting_date_str = self.get_reporting_date_str(target_year, target_month)
        expected_title = f"JioBlackRock Mutual Fund-Monthly-Portfolio-{reporting_date_str}"

        payload = [
            "monthly-portfolio-disclosure",
            {
                "year": fy_str,
                "month": month_name,
                "date": "$undefined"
            },
            "MF"
        ]

        headers = {
            "Accept": "text/x-component",
            "Content-Type": "text/plain;charset=UTF-8",
            "Next-Action": action_id,
            "Referer": BASE_PAGE_URL,
        }

        logger.info(f"JIO_BR: Sending Server Action POST for {month_name} {target_year} ({fy_str})...")
        resp = self.session.post(BASE_PAGE_URL, data=json.dumps(payload), headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.error(f"JIO_BR: Server Action POST failed with status {resp.status_code}")
            return None

        records = self.parse_rsc_response(resp.text)
        logger.info(f"JIO_BR: Total records received: {len(records)}")

        # Search specifically for the consolidated record
        matched_record = None
        for r in records:
            doc_type = r.get("docType", "")
            title = str(r.get("title", "")).strip()

            if doc_type != "file":
                continue

            if title == expected_title:
                matched_record = r
                break

        if not matched_record:
            logger.warning(f"JIO_BR: No consolidated portfolio found matching '{expected_title}'")
            return None

        file_url = matched_record.get("file", {}).get("url", "")
        file_ext = matched_record.get("file", {}).get("ext", ".xlsx")

        if not file_url:
            logger.error(f"JIO_BR: Matched record missing file URL: {matched_record}")
            return None

        safe_filename = f"JioBlackRock_Mutual_Fund_Monthly_Portfolio_{reporting_date_str}{file_ext}"
        target_path = download_folder / safe_filename

        logger.info(f"JIO_BR: Downloading consolidated portfolio from {file_url}...")
        download_resp = self.session.get(file_url, stream=True, timeout=60)
        if download_resp.status_code != 200:
            logger.error(f"JIO_BR: Download failed with status {download_resp.status_code}")
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
        # Jio BlackRock started in July 2025
        if year < 2025 or (year == 2025 and month < 7):
            logger.info(f"JIO_BR: Started July 2025. No data for {year}-{month:02d}. Skipping.")
            return {"status": "skipped", "reason": "pre_launch"}

        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        fy_str = self.calculate_fy(month, year)
        
        logger.info("=" * 60)
        logger.info("JIO BLACKROCK MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name}) | FY: {fy_str}")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"JIO_BR: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"JIO_BR: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, fy_str, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"JIO_BR: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("JIO_BR", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads into merged excels
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("JIO_BR", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] JIO_BR download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration, "file_path": str(downloaded_path)}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("JIO_BR", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Jio BlackRock Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    args = parser.parse_args()

    downloader = JioBRDownloader()
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
