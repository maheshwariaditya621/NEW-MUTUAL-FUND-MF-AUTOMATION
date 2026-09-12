# src/downloaders/wealth_company_downloader.py

import os
import time
import json
import shutil
import re
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
from urllib.parse import urljoin, unquote

import requests
import openpyxl

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.config.constants import AMC_WEALTH
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


class WealthCompanyDownloader(BaseDownloader):
    """
    The Wealth Company AMC - Monthly Portfolio Downloader

    URL: https://www.wealthcompanyamc.in/literature-forms/portfolio-documents/monthly/
    Features:
    - Pure requests (no browser/Playwright required).
    - Extracts embedded Next.js App Router streaming JSON payload across paginated results.
    - Scheme-level monthly portfolio disclosures (.xlsx).
    - Date Barrier: Inception is October 2025.
    - Idempotency via _SUCCESS.json and automatic consolidation into merged workbook.
    """

    BASE_URL = "https://www.wealthcompanyamc.in/literature-forms/portfolio-documents/monthly/"
    SITE_BASE = "https://www.wealthcompanyamc.in"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    ZIP_MAGIC = b"PK\x03\x04"

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
        super().__init__(AMC_WEALTH)
        self.notifier = get_notifier()
        self.AMC_NAME = "wealth_company"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": AMC_WEALTH,
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

        logger.warning(f"{self.AMC_NAME}: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error(AMC_WEALTH, year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_abbr = self.MONTH_ABBR[month]
        month_full = self.MONTH_FULL[month]

        logger.info("=" * 60)
        logger.info(f"WEALTH COMPANY DOWNLOADER: {year}-{month:02d} ({month_abbr})")
        logger.info("=" * 60)

        # Date Barrier: Wealth Company operations started in Oct 2025
        if year < 2025 or (year == 2025 and month < 10):
            logger.info(f"{self.AMC_NAME}: {year}-{month:02d} is before inception (Oct 2025). Skipping.")
            return {"status": "skipped", "reason": "before_inception"}

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"{AMC_WEALTH}: {year}-{month:02d} files already downloaded.")
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
                    self.notifier.notify_not_published(AMC_WEALTH, year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success(AMC_WEALTH, year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error(AMC_WEALTH, year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _extract_page_downloads(self, html_text: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Extracts the 'downloads' list and 'pagination' metadata from Next.js push chunk."""
        scripts = re.findall(r'self\.__next_f\.push\(\[(?:0|1),\s*"(.*?)"\]\)', html_text, re.DOTALL)
        for chunk in scripts:
            if "downloads" in chunk and "attachment" in chunk:
                try:
                    unescaped = chunk.encode("utf-8").decode("unicode_escape")
                except Exception:
                    unescaped = chunk.replace(r'\"', '"').replace(r"\\", "\\")

                pos = unescaped.find('"downloads":')
                if pos == -1:
                    continue

                start_bracket = unescaped.find("[", pos)
                depth = 0
                end_bracket = -1
                for i in range(start_bracket, len(unescaped)):
                    if unescaped[i] == "[":
                        depth += 1
                    elif unescaped[i] == "]":
                        depth -= 1
                        if depth == 0:
                            end_bracket = i
                            break

                if end_bracket != -1:
                    downloads_json = unescaped[start_bracket:end_bracket + 1]
                    items = json.loads(downloads_json)

                    page_info = {}
                    pag_pos = unescaped.find('"pagination":', end_bracket)
                    if pag_pos != -1:
                        pag_start = unescaped.find("{", pag_pos)
                        depth = 0
                        pag_end = -1
                        for i in range(pag_start, len(unescaped)):
                            if unescaped[i] == "{":
                                depth += 1
                            elif unescaped[i] == "}":
                                depth -= 1
                                if depth == 0:
                                    pag_end = i
                                    break
                        if pag_end != -1:
                            page_info = json.loads(unescaped[pag_start:pag_end + 1])
                    return items, page_info
        return [], {}

    def _discover_monthly_portfolios(self, session: requests.Session, year: int, month: int, month_abbr: str, month_full: str) -> List[Dict[str, Any]]:
        """
        Crawls paginated Next.js disclosures and filters items for the specified year and month.
        """
        all_records = []
        page_num = 1
        page_count = 1

        logger.info(f"Scanning Wealth Company monthly portfolio documents for {month_full} {year}...")
        while page_num <= page_count:
            page_url = f"{self.BASE_URL}?page={page_num}" if page_num > 1 else self.BASE_URL
            logger.info(f"  Fetching page {page_num}/{page_count}...")
            resp = session.get(page_url, headers=self.HEADERS, timeout=30)
            resp.raise_for_status()

            items, page_info = self._extract_page_downloads(resp.text)
            all_records.extend(items)
            page_count = page_info.get("pageCount", page_count)
            page_num += 1

        date_prefix = f"{year}-{month:02d}"
        pattern = re.compile(rf"\b({month_full}|{month_abbr})\b.*?\b{year}\b", re.IGNORECASE)

        discovered = []
        for item in all_records:
            name = item.get("name", "").strip()
            upload_date = (item.get("uploadDate") or "").strip()
            att = item.get("attachment") or {}
            rel_url = att.get("url", "").strip()

            if not rel_url:
                continue

            is_match = False
            if upload_date and upload_date.startswith(date_prefix):
                is_match = True
            elif pattern.search(name):
                is_match = True

            if is_match:
                full_url = urljoin(self.SITE_BASE, rel_url)
                filename = Path(unquote(full_url.split("?")[0])).name

                m = re.search(r"The Wealth Company\s+(.*?)\s+-\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", name, re.IGNORECASE)
                scheme_name = f"The Wealth Company {m.group(1).strip()}" if m else name

                discovered.append({
                    "raw_name": name,
                    "scheme_name": scheme_name,
                    "portfolio_date": upload_date or f"{month_full} {year}",
                    "url": full_url,
                    "filename": filename,
                })

        return discovered

    def _run_download_flow(self, target_year: int, target_month: int, month_abbr: str, month_full: str, download_folder: Path) -> int:
        session = requests.Session()
        records = self._discover_monthly_portfolios(session, target_year, target_month, month_abbr, month_full)

        logger.info(f"Discovered {len(records)} monthly portfolio record(s) for {month_abbr} {target_year}")
        if not records:
            return 0

        files_downloaded = 0
        for idx, rec in enumerate(records, 1):
            url = rec["url"]
            filename = rec["filename"]
            scheme_name = rec["scheme_name"]
            target_path = download_folder / filename

            logger.info(f"  [{idx}/{len(records)}] Downloading: {scheme_name}")
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

                # Validate XLSX Magic Bytes (PK\x03\x04)
                with open(target_path, "rb") as f:
                    magic = f.read(4)

                if magic != self.ZIP_MAGIC:
                    logger.warning(f"    [WARN] Magic bytes {magic.hex()} (expected {self.ZIP_MAGIC.hex()})")

                # Validate openability with openpyxl
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

        return files_downloaded


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = WealthCompanyDownloader()
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
