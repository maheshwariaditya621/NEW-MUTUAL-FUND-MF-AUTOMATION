# src/downloaders/choice_downloader.py

import os
import time
import json
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
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


class ChoiceDownloader(BaseDownloader):
    """
    Choice Mutual Fund - Portfolio Downloader
    
    Downloads all scheme-wise monthly portfolio files via direct REST API
    and CDN storage without browser automation.
    
    API: POST https://choicemf.com/api/monthly-portfolio-report/portfolio-website-list
    CDN: https://doc.choicemf.com/{file_path}
    """
    
    API_URL = "https://choicemf.com/api/monthly-portfolio-report/portfolio-website-list"
    DOC_BASE_URL = "https://doc.choicemf.com"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Content-Type": "application/json",
        "Referer": "https://choicemf.com/disclosures/monthly-portfolio",
        "Origin": "https://choicemf.com",
        "Accept": "application/json, text/plain, */*",
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Choice Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "choice"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "CHOICE",
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
        
        logger.warning(f"CHOICE: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("CHOICE", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("CHOICE MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Choice: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"CHOICE: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"CHOICE: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("CHOICE", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("CHOICE", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] CHOICE download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("CHOICE", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> int:
        """
        Queries Choice REST API and downloads all scheme workbooks directly from CDN.
        """
        session = requests.Session()
        session.headers.update(self.DEFAULT_HEADERS)

        logger.info("CHOICE: Fetching portfolio website list catalog...")
        resp = session.post(self.API_URL, json={}, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"API request failed with HTTP {resp.status_code}: {resp.text[:200]}")

        data = resp.json()
        schemes = data.get("body", {}).get("data", [])
        if not schemes:
            logger.warning("CHOICE: No schemes returned from catalog API")
            return 0

        target_period = f"{target_year}-{target_month:02d}"
        matching_reports = []

        for s in schemes:
            scheme_name = s.get("scheme_name", "").strip()
            reports = s.get("reports", [])
            for rep in reports:
                report_date = rep.get("report_date", "")
                if report_date.startswith(target_period):
                    matching_reports.append({
                        "scheme_name": scheme_name,
                        "report_date": report_date,
                        "file_path": rep.get("file_path", "")
                    })

        if not matching_reports:
            logger.warning(f"CHOICE: No report found matching period {target_period}")
            return 0

        logger.info(f"CHOICE: Found {len(matching_reports)} scheme report(s) for {target_period}. Downloading...")
        downloaded_count = 0

        for idx, item in enumerate(matching_reports, 1):
            file_path = item["file_path"].strip()
            if not file_path:
                continue

            file_url = f"{self.DOC_BASE_URL}/{file_path.lstrip('/')}"
            orig_filename = Path(file_path).name
            clean_filename = re.sub(r'^\d+-', '', orig_filename)
            dest_path = download_folder / clean_filename

            logger.info(f"  [{idx}/{len(matching_reports)}] Downloading: {dest_path.name} from {file_url}...")

            try:
                r = session.get(file_url, timeout=60, stream=True)
                if r.status_code == 200:
                    content = r.content
                    if content.startswith(b"PK\x03\x04") or content.startswith(b"\xd0\xcf\x11\xe0"):
                        with open(dest_path, "wb") as f:
                            f.write(content)
                        downloaded_count += 1
                        logger.info(f"    [OK] Saved ({len(content):,} bytes)")
                    else:
                        logger.warning(f"    [SKIP] Invalid magic bytes for {dest_path.name}")
                else:
                    logger.warning(f"    [FAIL] HTTP {r.status_code} for {file_url}")
            except Exception as e:
                logger.warning(f"    [FAIL] Download error: {e}")

            time.sleep(0.5)

        return downloaded_count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = ChoiceDownloader()
    result = downloader.download(args.year, args.month)
    print(json.dumps(result))

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
