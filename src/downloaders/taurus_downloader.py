# src/downloaders/taurus_downloader.py

import os
import time
import json
import shutil
import re
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Any
from urllib.parse import urljoin, unquote

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


class TaurusDownloader(BaseDownloader):
    """
    Taurus Mutual Fund - Portfolio Downloader

    URL: https://taurusmutualfund.com/monthly-portfolio
    Downloads scheme-level monthly portfolio workbooks using pure requests + BeautifulSoup
    via the Drupal AJAX View endpoint.
    """

    PAGE_URL = "https://taurusmutualfund.com/monthly-portfolio"
    BASE_URL = "https://taurusmutualfund.com"
    AJAX_URL = f"{BASE_URL}/views/ajax?_wrapper_format=drupal_ajax"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": PAGE_URL,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    ZIP_MAGIC = b"PK\x03\x04"

    def __init__(self):
        super().__init__("Taurus Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "taurus"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "TAURUS",
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

        logger.warning(f"TAURUS: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("TAURUS", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]

        logger.info("=" * 60)
        logger.info(f"TAURUS MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Taurus: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")
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
                    logger.info(f"TAURUS: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, target_dir)
                if files_downloaded == 0:
                    logger.warning(f"TAURUS: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("TAURUS", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                self._create_success_marker(target_dir, year, month, files_downloaded)
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("TAURUS", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] TAURUS download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("TAURUS", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _discover_monthly_portfolios(self, session: requests.Session, year: int, month: int, month_name: str) -> List[Dict[str, Any]]:
        # 1. Fetch the base page to inspect year options, month options, and drupalSettings
        logger.info("Fetching Taurus disclosures page to extract taxonomy IDs...")
        resp_get = session.get(self.PAGE_URL, headers={"User-Agent": self.HEADERS["User-Agent"]}, timeout=30)
        resp_get.raise_for_status()

        soup = BeautifulSoup(resp_get.text, "html.parser")

        # Extract Year map
        year_select = soup.find("select", attrs={"name": "field_monthly_portfolio_target_id"})
        year_map = {}
        if year_select:
            for opt in year_select.find_all("option"):
                v = opt.get("value", "")
                t = opt.get_text(strip=True)
                if v and v != "All":
                    year_map[t] = v

        # Extract Month map
        month_select = soup.find("select", attrs={"name": "field_month_target_id"})
        month_map = {}
        if month_select:
            for opt in month_select.find_all("option"):
                v = opt.get("value", "")
                t = opt.get_text(strip=True).lower()
                if v and v != "All":
                    month_map[t] = v

        # Extract view_dom_id
        view_dom_id = ""
        settings_script = soup.find("script", attrs={"data-drupal-selector": "drupal-settings-json"})
        if settings_script and settings_script.string:
            try:
                data = json.loads(settings_script.string)
                ajax_views = data.get("views", {}).get("ajaxViews", {})
                if ajax_views:
                    view_entry = list(ajax_views.values())[0]
                    view_dom_id = view_entry.get("view_dom_id", "")
            except Exception:
                pass

        year_str = str(year)
        month_lower = month_name.lower()
        year_id = year_map.get(year_str)
        month_id = month_map.get(month_lower)

        if not year_id or not month_id:
            logger.warning(f"TAURUS: Taxonomy ID not found for Year={year_str}, Month={month_name}")
            return []

        logger.info(f"Target taxonomy IDs: Year '{year_str}' -> {year_id}, Month '{month_name}' -> {month_id}")

        # 2. Query Drupal AJAX View endpoint
        payload = {
            "field_monthly_portfolio_target_id": year_id,
            "field_month_target_id": month_id,
            "view_name": "monthly_portfolio",
            "view_display_id": "page_1",
            "view_args": "",
            "view_path": "/monthly-portfolio",
            "view_base_path": "monthly-portfolio",
            "pager_element": "0",
        }
        if view_dom_id:
            payload["view_dom_id"] = view_dom_id

        resp_ajax = session.post(self.AJAX_URL, data=payload, headers=self.HEADERS, timeout=30)
        resp_ajax.raise_for_status()

        commands = resp_ajax.json()
        insert_cmd = None
        for cmd in commands:
            if cmd.get("command") == "insert":
                selector = cmd.get("selector", "")
                if "view-dom-id" in selector or selector == "":
                    insert_cmd = cmd
                    break
        if not insert_cmd:
            insert_cmd = next((c for c in commands if c.get("command") == "insert" and "views-row" in c.get("data", "")), None)

        if not insert_cmd or not insert_cmd.get("data"):
            return []

        soup_insert = BeautifulSoup(insert_cmd["data"], "html.parser")
        discovered = []

        for a in soup_insert.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            full_url = urljoin(self.BASE_URL, href)
            title = a.get_text(strip=True)
            filename = Path(unquote(full_url.split("?")[0])).name

            discovered.append({
                "title": title,
                "url": full_url,
                "filename": filename,
            })

        return discovered

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> int:
        session = requests.Session()
        records = self._discover_monthly_portfolios(session, target_year, target_month, month_name)

        logger.info(f"Discovered {len(records)} monthly portfolio record(s) for {month_name} {target_year}")
        if not records:
            return 0

        files_downloaded = 0
        for idx, rec in enumerate(records, 1):
            url = rec["url"]
            filename = rec["filename"]
            title = rec["title"]
            target_path = download_folder / filename

            logger.info(f"  [{idx}/{len(records)}] Downloading: {title}")
            logger.info(f"      URL: {url}")
            logger.info(f"      Saving to: {filename}")

            try:
                with session.get(url, headers={"User-Agent": self.HEADERS["User-Agent"]}, stream=True, timeout=60) as r:
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
                logger.error(f"    [FAIL] Failed downloading {filename}: {e}")

        return files_downloaded


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = TaurusDownloader()
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
