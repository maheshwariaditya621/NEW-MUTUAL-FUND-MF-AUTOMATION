import os
import time
import json
import shutil
import re
import calendar
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Any
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


class QuantDownloader(BaseDownloader):
    """
    Quant Mutual Fund - Portfolio Downloader
    
    Uses direct ASP.NET AJAX statutory disclosures API.
    URL: https://quantmutual.com/statutory-disclosures
    """
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    MONTH_SHORT_NAMES = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    API_URL = "https://quantmutual.com/statutorydisclosures.aspx/displaydisclouser"
    BASE_URL = "https://quantmutual.com"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://quantmutual.com",
        "Referer": "https://quantmutual.com/statutory-disclosures",
    }

    def __init__(self):
        super().__init__("Quant Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "quant"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "QUANT",
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
        
        logger.warning(f"QUANT: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("QUANT", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("QUANT MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Quant: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"QUANT: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                downloaded_path = self._run_download_flow(year, month, month_name, target_dir)
                
                if not downloaded_path:
                    logger.warning(f"QUANT: No portfolio found for {month_name} {year}")
                    self.notifier.notify_not_published("QUANT", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("QUANT", year, month, files_downloaded=1, duration=duration)
                logger.success(f"[SUCCESS] QUANT download completed: {downloaded_path.name}")
                return {"status": "success", "files_downloaded": 1, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("QUANT", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _parse_documents_from_html(self, d_html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(d_html, "html.parser")
        documents = []
        seen_urls = set()
        
        for a in soup.find_all("a", href=True):
            raw_href = a["href"].strip()
            if not raw_href or raw_href.startswith("javascript:") or raw_href.startswith("#"):
                continue
                
            abs_url = urljoin(self.BASE_URL, raw_href)
            if abs_url in seen_urls:
                continue
            seen_urls.add(abs_url)
            
            visible_text = a.get_text(strip=True)
            ext = Path(abs_url.split("?")[0]).suffix.lower()
            
            combined_str = f"{visible_text} {raw_href}"
            inferred_date = None
            
            date_match = re.search(r"(\d{2})(\d{2})(\d{4})", combined_str)
            if date_match:
                d, m, y = date_match.groups()
                try:
                    d_int, m_int, y_int = int(d), int(m), int(y)
                    if 1 <= m_int <= 12 and 1 <= d_int <= 31 and 2000 <= y_int <= 2099:
                        inferred_date = f"{d_int:02d}-{m_int:02d}-{y_int}"
                except ValueError:
                    pass
                    
            if not inferred_date:
                for m_num, m_name in self.MONTH_NAMES.items():
                    if re.search(rf"\b{m_name}\b", combined_str, re.IGNORECASE):
                        year_match = re.search(r"\b(20\d\d)\b", combined_str)
                        if year_match:
                            y_int = int(year_match.group(1))
                            last_d = calendar.monthrange(y_int, m_num)[1]
                            inferred_date = f"{last_d:02d}-{m_num:02d}-{y_int}"
                            break
                            
            documents.append({
                "title": visible_text,
                "raw_href": raw_href,
                "absolute_url": abs_url,
                "extension": ext,
                "inferred_date": inferred_date
            })
            
        return documents

    def _identify_document_for_month(self, documents: List[Dict[str, Any]], year: int, month: int) -> Optional[Dict[str, Any]]:
        last_day = calendar.monthrange(year, month)[1]
        expected_dmy = f"{last_day:02d}-{month:02d}-{year}"
        compact_date = f"{last_day:02d}{month:02d}{year}"
        month_name = self.MONTH_NAMES[month].lower()
        month_short = self.MONTH_SHORT_NAMES[month].lower()
        
        # Pass 1: exact inferred date match
        for doc in documents:
            if doc.get("inferred_date") == expected_dmy:
                return doc
                
        # Pass 2: compact date in href or title
        for doc in documents:
            href_lower = doc["raw_href"].lower()
            title_lower = doc["title"].lower()
            if compact_date in href_lower or compact_date in title_lower:
                return doc
                
        # Pass 3: month name and year in href or title
        for doc in documents:
            combined = f"{doc['title'].lower()} {doc['raw_href'].lower()}"
            if str(year) in combined and (month_name in combined or month_short in combined):
                return doc
                
        return None

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> Optional[Path]:
        session = requests.Session()
        payload = {
            "id": str(target_year),
            "cat": "MONTHLY PORTFOLIO"
        }
        
        logger.info(f"Querying Quant disclosures API for {target_year}...")
        resp = session.post(self.API_URL, json=payload, headers=self.HEADERS, timeout=30)
        
        if resp.status_code != 200:
            raise Exception(f"Quant API returned HTTP {resp.status_code}: {resp.text[:200]}")
            
        data = resp.json()
        if "d" not in data or not data["d"]:
            logger.warning("Quant API returned empty or missing 'd' field")
            return None
            
        documents = self._parse_documents_from_html(data["d"])
        logger.info(f"Discovered {len(documents)} document(s) in category 'MONTHLY PORTFOLIO'")
        
        doc = self._identify_document_for_month(documents, target_year, target_month)
        if not doc:
            logger.warning(f"No document matching {month_name} {target_year} found")
            return None
            
        download_url = doc["absolute_url"]
        filename = Path(doc["raw_href"]).name
        target_path = download_folder / filename
        
        logger.info(f"Downloading {filename} from {download_url}...")
        with session.get(download_url, headers={"User-Agent": self.HEADERS["User-Agent"]}, stream=True, timeout=60) as r:
            if r.status_code != 200:
                raise Exception(f"Failed to download file from {download_url}: HTTP {r.status_code}")
                
            with open(target_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
                        
        # Validate Excel
        file_size = target_path.stat().st_size
        if file_size < 1000:
            target_path.unlink(missing_ok=True)
            raise Exception(f"Downloaded file too small ({file_size} bytes), possible error response")
            
        try:
            wb = openpyxl.load_workbook(target_path, read_only=True)
            sheet_count = len(wb.sheetnames)
            wb.close()
            logger.info(f"Validated Excel workbook: {sheet_count} sheet(s), {file_size:,} bytes")
        except Exception as e:
            target_path.unlink(missing_ok=True)
            raise Exception(f"Downloaded file is not a valid Excel workbook: {e}")
            
        return target_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = QuantDownloader()
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
