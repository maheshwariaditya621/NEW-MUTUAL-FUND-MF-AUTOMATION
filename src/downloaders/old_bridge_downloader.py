import os
import time
import json
import shutil
import re
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
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


BASE_PAGE_URL = "https://www.oldbridgemf.com/statutory-disclosures.html"
BASE_DOMAIN = "https://www.oldbridgemf.com"


class OldBridgeDownloader(BaseDownloader):
    """
    Old Bridge Mutual Fund - Portfolio Downloader
    
    URL: https://www.oldbridgemf.com/statutory-disclosures.html
    Pure Python requests + BeautifulSoup implementation.
    Strictly excludes Portfolio Overlap and discovers scheme-level monthly portfolios.
    """
    
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

    def __init__(self):
        super().__init__("Old Bridge Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "old_bridge"
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
        logger.info("OldBridgeDownloader initialized (Pure requests + BeautifulSoup version)")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Old Bridge",
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
        
        logger.warning(f"{self.AMC_NAME}: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))
        self.notifier.notify_error("Old Bridge", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _get_financial_year(self, year: int, month: int) -> Tuple[str, str, str]:
        """
        Calculates financial year strings.
        Apr-Dec: FY starts in year, ends in year + 1 (e.g. 2026 -> 2026-2027)
        Jan-Mar: FY starts in year - 1, ends in year (e.g. Jan 2026 -> 2025-2026)
        """
        if month >= 4:
            fy_start = str(year)
            fy_end = str(year + 1)
        else:
            fy_start = str(year - 1)
            fy_end = str(year)
        fy_end_short = fy_end[-2:]
        return fy_start, fy_end, fy_end_short

    def _validate_excel_file(self, file_path: Path) -> bool:
        """Validate ZIP/XLS signature and openpyxl readable workbook."""
        if not file_path.exists() or file_path.stat().st_size == 0:
            return False

        with open(file_path, "rb") as f:
            magic = f.read(4)
        if magic != b"PK\x03\x04" and magic != b"\xd0\xcf\x11\xe0":
            logger.error(f"Old Bridge: Invalid magic bytes for {file_path.name}: {magic.hex()}")
            return False

        if magic == b"PK\x03\x04":
            try:
                wb = openpyxl.load_workbook(str(file_path), read_only=True, data_only=True)
                _ = wb.sheetnames
                wb.close()
                return True
            except Exception as e:
                logger.error(f"Old Bridge: openpyxl validation failed for {file_path.name}: {e}")
                return False

        return True

    def _fetch_page_soup(self) -> BeautifulSoup:
        """Fetch the official statutory disclosures page."""
        logger.info(f"Old Bridge: Fetching disclosures page: {BASE_PAGE_URL}")
        resp = self.session.get(BASE_PAGE_URL, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")

    def _locate_monthly_portfolio_pane(self, soup: BeautifulSoup) -> Any:
        """Dynamically finds the Monthly Portfolio tab button and resolves its tab-pane."""
        buttons = soup.find_all("button", class_="nav-link")
        target_tab_id = None
        for btn in buttons:
            if btn.get_text(strip=True).lower() == "monthly portfolio":
                target_tab_id = (
                    btn.get("data-bs-target") or
                    btn.get("data-target") or
                    btn.get("aria-controls")
                )
                if target_tab_id:
                    target_tab_id = target_tab_id.lstrip("#")
                break
                
        if not target_tab_id:
            pane = soup.find(id="v-pills-tabContent2")
            if pane:
                return pane
            raise RuntimeError("Could not find Monthly Portfolio tab or pane.")
            
        pane = soup.find(id=target_tab_id)
        if not pane:
            raise RuntimeError(f"Tab pane id '{target_tab_id}' not found in HTML.")
        return pane

    def _discover_links(self, soup: BeautifulSoup, year: int, month: int) -> List[Dict[str, str]]:
        """Discovers all scheme portfolio links for target period under Monthly Portfolio."""
        month_name = self.MONTH_NAMES[month].lower()
        month_abbr = self.MONTH_ABBR[month].lower()
        year_str = str(year)
        fy_start, fy_end, fy_end_short = self._get_financial_year(year, month)
        
        pane = self._locate_monthly_portfolio_pane(soup)
        grey_heads = pane.find_all("div", class_="grey-head")
        
        target_fy_head = None
        for gh in grey_heads:
            gh_txt = gh.get_text(strip=True)
            if any(ex in gh_txt.lower() for ex in ["portfolio overlap", "monthly portfolio"]):
                continue
            clean_gh = re.sub(r"\s+", "", gh_txt)
            if fy_start in clean_gh and (fy_end in clean_gh or fy_end_short in clean_gh):
                target_fy_head = gh
                logger.info(f"Old Bridge: Found Financial Year section: '{gh_txt}'")
                break
                
        if not target_fy_head:
            logger.warning(f"Old Bridge: FY {fy_start}-{fy_end_short} not found under Monthly Portfolio.")
            return []

        matching_elements = []
        curr = target_fy_head.find_next_sibling()
        while curr:
            if curr.name == "div" and "grey-head" in curr.get("class", []):
                break
            matching_elements.append(curr)
            curr = curr.find_next_sibling()

        discovered = []
        seen_urls = set()

        for el in matching_elements:
            for a_tag in el.find_all("a", href=True):
                href = a_tag["href"].strip()
                if not href:
                    continue
                
                h2 = a_tag.find_previous("h2")
                title = h2.get_text(strip=True) if h2 else a_tag.get_text(strip=True)
                abs_url = urllib.parse.urljoin(BASE_DOMAIN, href)
                
                combined_meta = f"{title} {href}".lower()
                # Strict exclusion of Portfolio Overlap
                if "overlap" in combined_meta:
                    continue
                
                norm_combined = re.sub(r"[-_]+", " ", combined_meta)
                month_match = (month_name in norm_combined or month_abbr in norm_combined)
                year_match = year_str in norm_combined or f"_{year_str[-2:]}" in href
                
                if month_match and year_match:
                    if abs_url in seen_urls:
                        continue
                    seen_urls.add(abs_url)
                    
                    scheme_name = title
                    if " - " in title:
                        scheme_name = title.split(" - ")[0].strip()
                    elif "-" in title:
                        scheme_name = title.split("-")[0].strip()
                    
                    discovered.append({
                        "scheme": scheme_name,
                        "title": title,
                        "href": href,
                        "url": abs_url
                    })

        return discovered

    def _run_download_flow(self, target_year: int, target_month: int, month_name: str, month_abbr: str, download_folder: Path) -> int:
        soup = self._fetch_page_soup()
        links = self._discover_links(soup, target_year, target_month)
        
        if not links:
            logger.warning(f"Old Bridge: No portfolio links found for {month_name} {target_year}")
            return 0
            
        logger.info(f"Old Bridge: Found {len(links)} portfolio link(s) for {month_name} {target_year}.")
        success_count = 0
        
        for idx, item in enumerate(links, 1):
            url = item["url"]
            title = item["title"]
            
            parsed = urllib.parse.urlparse(url)
            raw_filename = os.path.basename(parsed.path)
            if not raw_filename or not (raw_filename.endswith(".xlsx") or raw_filename.endswith(".xls")):
                clean_title = re.sub(r"[^\w\-_.]", "_", title)
                raw_filename = f"{clean_title}.xlsx"
                
            target_path = download_folder / raw_filename
            temp_path = target_path.with_name(target_path.stem + ".tmp.xlsx")
            
            logger.info(f"  [{idx}/{len(links)}] Downloading: {title}...")
            try:
                resp = self.session.get(url, stream=True, timeout=60)
                if resp.status_code != 200:
                    logger.error(f"    [FAIL] HTTP {resp.status_code} for {title}")
                    continue
                
                with open(temp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=16384):
                        if chunk:
                            f.write(chunk)
                
                if self._validate_excel_file(temp_path):
                    if target_path.exists():
                        target_path.unlink()
                    temp_path.rename(target_path)
                    logger.info(f"    [OK] Saved: {target_path.name} ({target_path.stat().st_size:,} bytes)")
                    success_count += 1
                else:
                    if temp_path.exists():
                        temp_path.unlink()
                    logger.error(f"    [FAIL] Validation failed for {title}")
            except Exception as e:
                if temp_path.exists():
                    temp_path.unlink()
                logger.error(f"    [FAIL] Error downloading {title}: {e}")
                
        return success_count

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]
        month_abbr = self.MONTH_ABBR[month]
        
        logger.info("=" * 60)
        logger.info(f"OLD BRIDGE MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        # Idempotency
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Old Bridge: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"{self.AMC_NAME}: [DRY RUN] Would download {month_name} {year}")
                    return {"status": "success", "dry_run": True}

                files_downloaded = self._run_download_flow(year, month, month_name, month_abbr, target_dir)
                
                if files_downloaded == 0:
                    logger.warning(f"{self.AMC_NAME}: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("Old Bridge", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                # Success
                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate downloads into merged excels
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("Old Bridge", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[attempt])

        # Final Failure
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Old Bridge", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = OldBridgeDownloader()
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
