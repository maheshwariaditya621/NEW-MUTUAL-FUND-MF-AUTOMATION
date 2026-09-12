# src/downloaders/capitalmind_downloader.py

import os
import time
import json
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple
from urllib.parse import urljoin
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


BASE_URL = "https://www.capitalmindmf.com"
DISCLOSURES_URL = f"{BASE_URL}/statutory-disclosures.html"

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


class CapitalMindDownloader(BaseDownloader):
    """
    CapitalMind Mutual Fund - Portfolio Downloader
    
    Direct requests + BeautifulSoup scraper.
    URL: https://www.capitalmindmf.com/statutory-disclosures.html
    Parses Monthly Portfolio tab content and downloads scheme-level XLSX files.
    """
    
    MONTH_MAP = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("CapitalMind Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "capitalmind"
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
        logger.info("CapitalMindDownloader initialized (Requests + BS4 Version)")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "CAPITALMIND",
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
        
        logger.warning(f"CAPITALMIND: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("CAPITALMIND", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def parse_month_year(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        """Extract month (1-12) and year (YYYY) from text or filename."""
        text_lower = text.lower()
        
        year_match = re.search(r'\b(20\d{2})\b', text)
        year = int(year_match.group(1)) if year_match else None
        
        month = None
        for name, num in MONTH_NAMES.items():
            if re.search(rf'\b{name}\b', text_lower):
                month = num
                break
        return month, year

    def fetch_disclosures_html(self, url: str = DISCLOSURES_URL) -> str:
        """Fetch disclosures page HTML."""
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def extract_monthly_portfolio_links(self, html_content: str) -> List[Dict]:
        """
        Parse disclosures HTML and return all monthly portfolio records.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Find Monthly Portfolio tab button dynamically
        target_panel_id = None
        for btn in soup.find_all(["button", "a"]):
            btn_text = btn.get_text(strip=True).lower()
            if "monthly portfolio" in btn_text and not ("overlap" in btn_text or "fortnightly" in btn_text):
                target_panel_id = btn.get("data-bs-target") or btn.get("href")
                if target_panel_id and target_panel_id.startswith("#"):
                    break
        
        panel = None
        if target_panel_id:
            panel = soup.select_one(target_panel_id)
        if not panel:
            panel = soup.select_one("#v-pills-tabContent2")
            
        if not panel:
            for div in soup.select(".tab-pane"):
                if "monthly portfolio" in div.get_text(strip=True).lower():
                    panel = div
                    break

        if not panel:
            logger.error("CAPITALMIND: Failed to find Monthly Portfolio tab content panel in HTML.")
            return []

        results = []
        accordion_items = panel.select(".accordion-item")
        
        for item in accordion_items:
            header_el = item.select_one(".accordion-header") or item.select_one(".accordion-button")
            scheme_name = header_el.get_text(strip=True) if header_el else "Unknown Scheme"
            
            for li in item.select("li"):
                span_el = li.select_one("span")
                displayed_title = span_el.get_text(strip=True) if span_el else ""
                
                a_el = li.select_one("a[href]")
                if not a_el:
                    continue
                
                href = a_el["href"].strip()
                if not href:
                    continue
                
                filename = os.path.basename(href.split("?")[0])
                month, year = self.parse_month_year(displayed_title)
                if not month or not year:
                    m2, y2 = self.parse_month_year(filename)
                    month = month or m2
                    year = year or y2
                
                abs_url = urljoin(BASE_URL, href)
                
                results.append({
                    "scheme_name": scheme_name,
                    "displayed_title": displayed_title,
                    "month": month,
                    "year": year,
                    "href": href,
                    "download_url": abs_url,
                    "filename": filename,
                })

        return results

    def _validate_excel_file(self, file_path: Path) -> bool:
        """Validate ZIP signature and workbook opening via openpyxl."""
        if not file_path.exists() or file_path.stat().st_size == 0:
            return False
            
        with open(file_path, "rb") as f:
            magic = f.read(4)
        if magic != b"PK\x03\x04":
            logger.error(f"CAPITALMIND: Invalid magic bytes for {file_path.name}: {magic.hex()}")
            return False
            
        try:
            wb = openpyxl.load_workbook(str(file_path), read_only=True, data_only=True)
            _ = wb.sheetnames
            wb.close()
            return True
        except Exception as e:
            logger.error(f"CAPITALMIND: openpyxl validation failed for {file_path.name}: {e}")
            return False

    def _download_file(self, url: str, target_path: Path) -> bool:
        """Download file and validate integrity."""
        try:
            resp = self.session.get(url, stream=True, timeout=60)
            if resp.status_code != 200:
                logger.error(f"CAPITALMIND: Download failed with HTTP {resp.status_code} for {url}")
                return False
                
            temp_path = target_path.with_name(f"{target_path.stem}.tmp{target_path.suffix}")
            with open(temp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=16384):
                    if chunk:
                        f.write(chunk)
                        
            if self._validate_excel_file(temp_path):
                temp_path.replace(target_path)
                logger.info(f"  [OK] Downloaded and validated: {target_path.name} ({target_path.stat().st_size:,} bytes)")
                return True
            else:
                if temp_path.exists():
                    temp_path.unlink()
                return False
        except Exception as e:
            logger.error(f"CAPITALMIND: Download error for {url}: {e}")
            return False

    def _run_download_flow(self, target_year: int, target_month: int, download_folder: Path) -> List[Path]:
        """Fetch disclosures HTML, find files for target period, and download."""
        html = self.fetch_disclosures_html()
        all_links = self.extract_monthly_portfolio_links(html)
        
        target_links = [
            item for item in all_links
            if item["year"] == target_year and item["month"] == target_month
        ]
        
        if not target_links:
            logger.warning(f"CAPITALMIND: No portfolio files found for {target_year}-{target_month:02d}")
            return []
            
        logger.info(f"CAPITALMIND: Found {len(target_links)} portfolio files for {target_year}-{target_month:02d}")
        
        downloaded_paths = []
        for item in target_links:
            target_path = download_folder / item["filename"]
            logger.info(f"Downloading {item['scheme_name']} -> {item['filename']}...")
            if self._download_file(item["download_url"], target_path):
                downloaded_paths.append(target_path)
            else:
                logger.error(f"Failed to download/validate {item['filename']}")
                
        return downloaded_paths

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_MAP.get(month, f"Month {month}")
        
        logger.info("=" * 60)
        logger.info("CAPITALMIND MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency check
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Capitalmind: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"CAPITALMIND: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_files = self._run_download_flow(year, month, target_dir)
                
                if not downloaded_files:
                    logger.warning(f"CAPITALMIND: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("CAPITALMIND", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                file_count = len(downloaded_files)
                self._create_success_marker(target_dir, year, month, file_count)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("CAPITALMIND", year, month, files_downloaded=file_count, duration=duration)
                logger.success(f"[SUCCESS] CAPITALMIND download completed: {file_count} files")
                return {"status": "success", "files_downloaded": file_count, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("CAPITALMIND", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = CapitalMindDownloader()
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
