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


class SamcoDownloader(BaseDownloader):
    """
    Samco Mutual Fund - Portfolio Downloader
    
    URL: https://www.samcomf.com/StatutoryDisclosure
    Downloads scheme-level portfolio workbooks using pure requests + BeautifulSoup.
    """
    
    PAGE_URL = "https://www.samcomf.com/StatutoryDisclosure"
    BASE_URL = "https://www.samcomf.com"
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("Samco Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "samco"

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "SAMCO",
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
        
        logger.warning(f"SAMCO: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("SAMCO", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        
        logger.info("=" * 60)
        logger.info("SAMCO MUTUAL FUND DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Samco: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"SAMCO: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"SAMCO: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("SAMCO", year, month)
                    if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("SAMCO", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] SAMCO download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists(): shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("SAMCO", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}

    def _extract_monthly_records(self, soup: BeautifulSoup, year: int, month: int, month_name: str) -> List[Dict[str, Any]]:
        option2 = soup.find(id="option2")
        if not option2:
            logger.error("SAMCO: option2 section not found in HTML")
            return []
            
        monthly_toggle = None
        for a in option2.find_all("a", class_="toggle"):
            if a.get_text(strip=True).lower() == "monthly":
                monthly_toggle = a
                break
                
        if not monthly_toggle:
            logger.error("SAMCO: 'Monthly' toggle header not found")
            return []
            
        monthly_div = monthly_toggle.find_next_sibling("div", class_="main_div")
        if not monthly_div:
            logger.error("SAMCO: main_div for Monthly section not found")
            return []
            
        table = monthly_div.find("table")
        if not table:
            logger.error("SAMCO: Monthly table not found")
            return []

        last_day = calendar.monthrange(year, month)[1]
        compact_date = f"{last_day:02d}{month:02d}{year}"
        month_lower = month_name.lower()
        year_str = str(year)
        month_regex = rf"{month_lower}[_\s]*{year_str}"
        
        records = []
        seen_urls = set()
        
        for row in table.find_all("tr"):
            th_td = row.find_all(["th", "td"])
            if len(th_td) < 2:
                continue
                
            title = th_td[0].get_text(strip=True)
            link_tags = th_td[1].find_all("a", href=True)
            if not link_tags:
                continue
                
            raw_hrefs = [a["href"].strip() for a in link_tags if a["href"].strip()]
            if not raw_hrefs:
                continue
                
            combined_text = f"{title} {' '.join(raw_hrefs)}".lower()
            
            is_month_match = bool(re.search(month_regex, combined_text, re.IGNORECASE))
            is_date_match = compact_date in combined_text
            
            if not (is_month_match or is_date_match):
                continue
                
            best_url = None
            for href in raw_hrefs:
                if "media1.samco.in" in href:
                    best_url = href
                    break
            if not best_url:
                best_url = urljoin(self.BASE_URL, raw_hrefs[0])
                
            if best_url in seen_urls:
                continue
            seen_urls.add(best_url)
            
            filename = Path(unquote(best_url.split("?")[0])).name
            if not filename.endswith((".xlsx", ".xls")):
                filename = f"{title}.xlsx"
                
            records.append({
                "title": title,
                "url": best_url,
                "filename": filename
            })
            
        return records

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, download_folder: Path) -> int:
        session = requests.Session()
        logger.info(f"Fetching SAMCO statutory disclosures page...")
        resp = session.get(self.PAGE_URL, headers=self.HEADERS, timeout=30)
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.text, "html.parser")
        records = self._extract_monthly_records(soup, target_year, target_month, month_name)
        
        logger.info(f"Discovered {len(records)} monthly portfolio record(s) for {month_name} {target_year}")
        if not records:
            return 0
            
        success_count = 0
        for idx, rec in enumerate(records, 1):
            url = rec["url"]
            filename = rec["filename"]
            target_path = download_folder / filename
            
            logger.info(f"  [{idx}/{len(records)}] Downloading: {filename}")
            try:
                with session.get(url, headers={"User-Agent": self.HEADERS["User-Agent"]}, stream=True, timeout=45) as r:
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
                    
                # Validate Excel
                try:
                    wb = openpyxl.load_workbook(target_path, read_only=True)
                    sheet_count = len(wb.sheetnames)
                    wb.close()
                    logger.info(f"    [OK] Validated {filename}: {sheet_count} sheet(s), {file_size:,} bytes")
                    success_count += 1
                except Exception as e:
                    logger.warning(f"    [WARN] openpyxl load check: {e} (keeping file)")
                    success_count += 1
                    
            except Exception as e:
                logger.error(f"    [FAIL] Failed to download {filename}: {e}")
                
        return success_count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = SamcoDownloader()
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
