# src/downloaders/shriram_downloader.py

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
import xlrd

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


class ShriramDownloader(BaseDownloader):
    """
    Shriram Mutual Fund - Portfolio Downloader

    URL: https://www.shriramamc.in/investor-statutory-disclosures
    Downloads monthly consolidated portfolio workbooks using pure requests + BeautifulSoup.
    """

    PAGE_URL = "https://www.shriramamc.in/investor-statutory-disclosures"
    BASE_URL = "https://www.shriramamc.in"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    MONTH_ABBR = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    OLE2_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"

    def __init__(self):
        super().__init__("Shriram Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "shriram"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "SHRIRAM",
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

        logger.warning(f"SHRIRAM: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("SHRIRAM", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _get_fy_label(self, year: int, month: int) -> str:
        """Indian Financial Year (April - March)"""
        if month >= 4:
            return f"{year}-{year + 1}"
        else:
            return f"{year - 1}-{year}"

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]

        logger.info("=" * 60)
        logger.info(f"SHRIRAM MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))

        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"SHRIRAM: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"SHRIRAM: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                file_path = self._run_download_flow(year, month, month_name, target_dir)

                if not file_path:
                    logger.warning(f"SHRIRAM: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("SHRIRAM", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                self.consolidate_downloads(year, month)

                duration = time.time() - start_time
                self.notifier.notify_success("SHRIRAM", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] SHRIRAM download completed: {file_path.name}")
                return {"status": "success", "file": str(file_path), "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("SHRIRAM", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _discover_monthly_portfolio(self, soup: BeautifulSoup, html: str, year: int, month: int) -> Optional[Dict[str, Any]]:
        month_full = self.MONTH_NAMES[month]
        month_abbr = self.MONTH_ABBR[month]
        fy_str = self._get_fy_label(year, month)
        year_str = str(year)

        month_pattern = re.compile(rf"\b({month_full}|{month_abbr})\b.*?\b{year_str}\b", re.IGNORECASE)

        # 1. Primary: Search DOM (Active Financial Year)
        section = None
        for cand in soup.find_all(["div", "section"]):
            cid = cand.get("id", "")
            if "Monthly--Fortnightly" in cid:
                section = cand
                break

        if section:
            tab_btn = None
            for btn in section.find_all(["button", "a"]):
                btn_text = btn.get_text(strip=True)
                if "Monthly Portfolio for the FY" in btn_text:
                    tab_btn = btn
                    break

            if tab_btn:
                panel = section.find(id=re.compile(r"accordion-panel"))
                if panel:
                    cards = panel.find_all("div", class_=lambda c: c and "rounded-lg" in c)
                    for card in cards:
                        heading_el = card.find("div", class_=re.compile(r"editor-content"))
                        heading = heading_el.get_text(strip=True) if heading_el else card.get_text(" ", strip=True)

                        if month_pattern.search(heading):
                            link_el = card.find("a", href=True)
                            if link_el and link_el.get("href"):
                                href = link_el["href"].strip()
                                full_url = urljoin(self.BASE_URL, href)
                                filename = Path(unquote(full_url.split("?")[0])).name
                                return {
                                    "month_label": heading,
                                    "url": full_url,
                                    "filename": filename,
                                    "source": "HTML DOM (Active FY)",
                                    "fy": fy_str,
                                }

        # 2. Secondary: Embedded Next.js Payload (All Financial Years)
        pos = html.find(r'Monthly Portfolio for the FY\"')
        if pos != -1:
            end = html.find(r'Fortnightly Portfolio for the FY\"', pos)
            tab_block = html[pos:end] if end != -1 else html[pos:pos + 100000]

            item_pattern = re.compile(
                r'\\"accord_answer\\":\s*\\"([^\\"]+)\\",\s*\\"download_label\\":\s*\\"[^\\"]*\\",\s*\\"download_link\\":\s*\\"([^\\"]+)\\"'
            )
            for match in item_pattern.finditer(tab_block):
                label = match.group(1).strip()
                dlink = match.group(2).strip().replace(r"\/", "/")

                if month_pattern.search(label):
                    full_url = urljoin(self.BASE_URL, dlink)
                    filename = Path(unquote(full_url.split("?")[0])).name
                    return {
                        "month_label": label,
                        "url": full_url,
                        "filename": filename,
                        "source": "Embedded Next.js Payload",
                        "fy": fy_str,
                    }

        return None

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        session = requests.Session()
        logger.info(f"Fetching SHRIRAM statutory disclosures page...")
        resp = session.get(self.PAGE_URL, headers=self.HEADERS, timeout=30)
        resp.raise_for_status()

        html_text = resp.text
        soup = BeautifulSoup(html_text, "html.parser")

        discovery = self._discover_monthly_portfolio(soup, html_text, target_year, target_month)
        if not discovery:
            logger.warning(f"SHRIRAM: Monthly portfolio not found for {month_name} {target_year}")
            return None

        url = discovery["url"]
        filename = discovery["filename"]
        target_path = download_folder / filename

        logger.info(f"Discovered monthly portfolio for {month_name} {target_year}:")
        logger.info(f"  URL: {url}")
        logger.info(f"  Source: {discovery['source']}")
        logger.info(f"  Saving to: {filename}")

        with session.get(url, headers={"User-Agent": self.HEADERS["User-Agent"]}, stream=True, timeout=45) as r:
            r.raise_for_status()
            with open(target_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

        file_size = target_path.stat().st_size
        if file_size < 1000:
            target_path.unlink(missing_ok=True)
            raise ValueError(f"Downloaded file too small ({file_size} bytes)")

        # Validate XLS OLE2 Header & Workbook structure
        with open(target_path, "rb") as f:
            magic = f.read(8)

        if magic != self.OLE2_MAGIC:
            target_path.unlink(missing_ok=True)
            raise ValueError(f"Invalid XLS magic bytes (got {magic.hex()})")

        try:
            wb = xlrd.open_workbook(str(target_path))
            sheets = wb.sheet_names()
            logger.info(f"  [OK] Validated {filename}: {len(sheets)} sheet(s), {file_size:,} bytes")
        except Exception as e:
            logger.warning(f"  [WARN] xlrd load check: {e} (keeping file)")

        return target_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Shriram Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g. 2026)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    args = parser.parse_args()

    downloader = ShriramDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"[SUCCESS] Success: Downloaded file {result.get('file', '')}")
    elif status == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"[INFO]  Info: Month not yet published")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
