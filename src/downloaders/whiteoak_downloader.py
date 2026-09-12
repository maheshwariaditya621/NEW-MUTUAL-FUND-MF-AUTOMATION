# src/downloaders/whiteoak_downloader.py

import os
import time
import json
import shutil
import re
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Any
from urllib.parse import unquote

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


class WhiteOakDownloader(BaseDownloader):
    """
    WhiteOak Mutual Fund - Monthly Scheme Portfolio Downloader

    URL: https://mf.whiteoakamc.com/regulatory-disclosures/scheme-portfolios
    API: GET https://cms.whiteoakamc.com/api/scheme-portfolios
    Features:
    - Pure requests (no browser/Playwright required).
    - Uses Strapi CMS JSON API with efficient pagination (pageSize=100).
    - Scheme-level monthly portfolio disclosures (.xlsx).
    - Idempotency via _SUCCESS.json and automatic consolidation into merged workbook.
    """

    API_URL = "https://cms.whiteoakamc.com/api/scheme-portfolios"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
    }

    ZIP_MAGIC = b"PK\x03\x04"

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("WhiteOak Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "whiteoak"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "WHITEOAK",
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

        logger.warning(f"WHITEOAK: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("WHITEOAK", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]

        logger.info("=" * 60)
        logger.info("WHITEOAK MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"WhiteOak: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"WHITEOAK: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, target_dir)

                if files_downloaded == 0:
                    logger.warning(f"WHITEOAK: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("WHITEOAK", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("WHITEOAK", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] WHITEOAK download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("WHITEOAK", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _discover_monthly_portfolios(self, session: requests.Session, year: int, month: int, month_name: str) -> List[Dict[str, Any]]:
        logger.info(f"Querying WhiteOak CMS API for {month_name} {year} monthly disclosures...")

        month_full = calendar.month_name[month]
        month_abbr = calendar.month_abbr[month]
        date_pattern = re.compile(rf"\b({month_full}|{month_abbr})\s*{year}\b", re.IGNORECASE)

        matching_records = []
        page = 1
        page_count = 1
        max_pages = 5

        while page <= page_count and page <= max_pages:
            params = {
                "pagination[page]": page,
                "pagination[pageSize]": 100,
                "sort[0]": "published_date:desc",
                "sort[1]": "id:desc",
                "populate": "*",
                "filters[period][$eq]": "monthly"
            }

            resp = session.get(self.API_URL, params=params, headers=self.HEADERS, timeout=30)
            resp.raise_for_status()

            body = resp.json()
            items = body.get("data", [])
            pagination = body.get("meta", {}).get("pagination", {})
            page_count = pagination.get("pageCount", page_count)

            found_on_page = 0
            for item in items:
                attrs = item.get("attributes", {})
                scheme_name = (attrs.get("scheme_name") or "").strip()
                doc_name = (attrs.get("doc_name") or "").strip()
                published_date = (attrs.get("published_date") or "").strip()

                doc_file = (attrs.get("doc_file") or {}).get("data") or {}
                file_attrs = doc_file.get("attributes") or {}

                file_url = (file_attrs.get("url") or "").strip()
                file_name = (file_attrs.get("name") or "").strip()

                if not file_url:
                    continue

                text_to_search = f"{doc_name} {file_name} {file_url}"
                if date_pattern.search(text_to_search):
                    matching_records.append({
                        "scheme_name": scheme_name,
                        "doc_name": doc_name,
                        "published_date": published_date,
                        "file_url": file_url,
                        "file_name": file_name,
                    })
                    found_on_page += 1

            page += 1
            # If we found records on this page and none on next or past target, can stop
            if matching_records and found_on_page == 0:
                break

        return matching_records

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> int:
        session = requests.Session()
        records = self._discover_monthly_portfolios(session, target_year, target_month, month_name)

        logger.info(f"Discovered {len(records)} monthly portfolio record(s) for {month_name} {target_year}")
        if not records:
            return 0

        files_downloaded = 0
        for idx, rec in enumerate(records, 1):
            url = rec["file_url"]
            scheme = rec["scheme_name"]
            raw_filename = rec["file_name"] or Path(unquote(url.split("?")[0])).name

            safe_scheme = re.sub(r"[^\w\-_.]", "_", scheme)[:40]
            filename = f"{safe_scheme}_{raw_filename}"
            target_path = download_folder / filename

            logger.info(f"  [{idx}/{len(records)}] Downloading: {scheme}")
            logger.info(f"      URL: {url}")
            logger.info(f"      Saving to: {filename}")

            try:
                with session.get(url, headers=self.HEADERS, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    with open(target_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            if chunk:
                                f.write(chunk)

                file_size = target_path.stat().st_size
                if file_size < 1000:
                    target_path.unlink(missing_ok=True)
                    logger.error(f"    [FAIL] File too small ({file_size} bytes)")
                    continue

                # Validate XLSX Magic Bytes
                with open(target_path, "rb") as f:
                    magic = f.read(4)

                if magic != self.ZIP_MAGIC:
                    logger.warning(f"    [WARN] Magic bytes {magic.hex()} (expected {self.ZIP_MAGIC.hex()})")

                # Validate openability
                try:
                    wb = openpyxl.load_workbook(target_path, read_only=True)
                    sheet_count = len(wb.sheetnames)
                    wb.close()
                    logger.info(f"    [OK] Validated {filename}: {sheet_count} sheet(s), {file_size:,} bytes")
                except Exception as e:
                    logger.warning(f"    [WARN] openpyxl load check: {e} (keeping file)")

                files_downloaded += 1

            except Exception as e:
                logger.error(f"    [FAIL] Error downloading {filename}: {e}")
                if target_path.exists():
                    target_path.unlink(missing_ok=True)

        return files_downloaded


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = WhiteOakDownloader()
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
