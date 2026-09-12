# src/downloaders/zerodha_downloader.py

import os
import time
import json
import shutil
import re
import calendar
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List

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


class ZerodhaDownloader(BaseDownloader):
    """
    Zerodha Mutual Fund - Portfolio Downloader

    URL: https://www.zerodhafundhouse.com/resources/disclosures
    Downloads monthly portfolio disclosures directly using pure requests
    via pre-rendered Next.js application state (__NEXT_DATA__).
    """

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    DISCLOSURES_URL = "https://www.zerodhafundhouse.com/resources/disclosures"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    ZIP_MAGIC = b"PK\x03\x04"

    def __init__(self):
        super().__init__("Zerodha Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "zerodha"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "ZERODHA",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        with open(marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        logger.info(f"Created completion marker: {marker_path.name}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]

        logger.info("=" * 60)
        logger.info(f"ZERODHA MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # Idempotency
        if target_dir.exists() and (target_dir / "_SUCCESS.json").exists():
            logger.info(f"ZERODHA: {year}-{month:02d} already complete.")
            logger.info("Verifying consolidation/merged files...")
            self.consolidate_downloads(year, month)
            return {"status": "skipped", "reason": "already_downloaded"}

        self.ensure_directory(str(target_dir))

        last_error = ""
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"ZERODHA: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_files = self._run_download_flow(year, month, month_name, target_dir)

                if not downloaded_files:
                    logger.warning(f"ZERODHA: No files found for {month_name} {year}")
                    self.notifier.notify_not_published("ZERODHA", year, month)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, len(downloaded_files))
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("ZERODHA", year, month, files_downloaded=len(downloaded_files), duration=duration)
                logger.success(f"[SUCCESS] ZERODHA download completed: {len(downloaded_files)} files")
                return {"status": "success", "files_count": len(downloaded_files), "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists() and not (target_dir / "_SUCCESS.json").exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("ZERODHA", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _fetch_monthly_files_metadata(self, session: requests.Session) -> List[Dict]:
        """Fetch disclosures page and parse all monthly portfolio disclosure file records from __NEXT_DATA__."""
        resp = session.get(self.DISCLOSURES_URL, headers=self.HEADERS, timeout=30)
        resp.raise_for_status()

        m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text, re.DOTALL)
        if not m:
            raise ValueError("Could not find __NEXT_DATA__ in Zerodha disclosures page")

        next_data = json.loads(m.group(1))
        props = next_data.get("props", {}).get("pageProps", {})
        reports = props.get("initialReports", [])

        portfolio_disc = None
        for r in reports:
            if r.get("title") == "Portfolio Disclosures":
                portfolio_disc = r
                break

        if not portfolio_disc:
            raise ValueError("Portfolio Disclosures section not found in pageProps")

        monthly_data = None
        for section in portfolio_disc.get("data", []):
            if section.get("id") == "monthly-portfolio-disclosures":
                monthly_data = section
                break

        if not monthly_data:
            raise ValueError("monthly-portfolio-disclosures section not found in Portfolio Disclosures")

        return monthly_data.get("files", [])

    def _parse_file_info(self, file_dict: Dict) -> Dict:
        """Extract schemeCode, year, and month from the disclosure file item."""
        name = file_dict.get("name", "")
        url = file_dict.get("url", "")

        m_code = re.match(r'^([A-Z0-9]+)\s*-', name)
        scheme_code = m_code.group(1) if m_code else ""

        year = None
        m_yr = re.search(r'\b(202\d)\b', name)
        if m_yr:
            year = int(m_yr.group(1))

        month = None
        months_dict = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
        abbr_dict = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}

        for m_name, m_idx in months_dict.items():
            if re.search(rf'\b{m_name}\b', name, re.IGNORECASE):
                month = m_idx
                break
        if not month:
            for m_abbr, m_idx in abbr_dict.items():
                if re.search(rf'\b{m_abbr}\b', name, re.IGNORECASE):
                    month = m_idx
                    break

        return {
            "name": name,
            "url": url,
            "scheme_code": scheme_code,
            "year": year,
            "month": month,
            "modTs": file_dict.get("modTs")
        }

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> List[Path]:
        """Download all scheme portfolio workbooks for target year and month using pure requests."""
        session = requests.Session()
        logger.info(f"Fetching disclosures metadata from Zerodha...")
        files_metadata = self._fetch_monthly_files_metadata(session)
        logger.info(f"  Found {len(files_metadata)} historical monthly portfolio records in state")

        matching_files = []
        for f in files_metadata:
            info = self._parse_file_info(f)
            if info["year"] == target_year and info["month"] == target_month:
                matching_files.append(info)

        if not matching_files:
            logger.warning(f"  No files found matching {month_name} {target_year}")
            return []

        logger.info(f"  Discovered {len(matching_files)} scheme file(s) for {month_name} {target_year}")

        downloaded_paths = []
        for idx, item in enumerate(matching_files, 1):
            raw_filename = item["url"].split("/")[-1]
            filename = urllib.parse.unquote(raw_filename)
            save_path = download_folder / filename

            logger.info(f"  [{idx}/{len(matching_files)}] Downloading: {filename}")
            logger.info(f"      URL: {item['url']}")

            try:
                with session.get(item["url"], headers=self.HEADERS, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    with open(save_path, "wb") as f_out:
                        for chunk in r.iter_content(chunk_size=65536):
                            if chunk:
                                f_out.write(chunk)

                file_size = save_path.stat().st_size
                if file_size < 1000:
                    save_path.unlink(missing_ok=True)
                    logger.error(f"    [FAIL] File too small ({file_size} bytes)")
                    continue

                # Magic byte validation
                with open(save_path, "rb") as f_check:
                    magic = f_check.read(4)

                if magic != self.ZIP_MAGIC:
                    logger.warning(f"    [WARN] Magic bytes {magic.hex()} (expected {self.ZIP_MAGIC.hex()})")

                # Validation with openpyxl
                try:
                    wb = openpyxl.load_workbook(save_path, read_only=True)
                    sheet_count = len(wb.sheetnames)
                    wb.close()
                    logger.info(f"    [OK] Validated {filename}: {sheet_count} sheet(s), {file_size:,} bytes")
                except Exception as e:
                    logger.warning(f"    [WARN] openpyxl load check: {e} (keeping file)")

                downloaded_paths.append(save_path)

            except Exception as e:
                logger.error(f"    [FAIL] Error downloading {filename}: {e}")
                if save_path.exists():
                    save_path.unlink(missing_ok=True)

        return downloaded_paths


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Zerodha Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g. 2026)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    args = parser.parse_args()

    downloader = ZerodhaDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_count')} files")
    elif status == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete")
    elif status == "not_published":
        logger.info(f"[INFO]  Info: Month not yet published")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
