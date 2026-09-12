# src/downloaders/ppfas_downloader.py

import os
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from src.downloaders.base_downloader import BaseDownloader
import time
from src.config import logger
from src.config.constants import AMC_PPFAS
from src.alerts.telegram_notifier import get_notifier

# Import downloader config
try:
    from src.config.downloader_config import (
        DRY_RUN, MAX_RETRIES, RETRY_BACKOFF
    )
except ImportError:
    # Fallback defaults
    DRY_RUN = False
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]


class PPFASDownloader(BaseDownloader):
    AMC_NAME = "ppfas"
    """
    PPFAS Mutual Fund - Portfolio Downloader
    
    Uses Playwright to navigate the month accordion and download consolidated portfolio disclosures.
    """
    
    def __init__(self):
        super().__init__(AMC_PPFAS)
        self.notifier = get_notifier()

    def _normalize_month_name(self, month: int) -> str:
        """Convert month number to full name used by PPFAS."""
        month_names = {
            1: "January", 2: "February", 3: "March", 4: "April",
            5: "May", 6: "June", 7: "July", 8: "August",
            9: "September", 10: "October", 11: "November", 12: "December"
        }
        return month_names[month]

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int) -> None:
        """Create atomic completion marker."""
        marker_path = target_dir / "_SUCCESS.json"
        
        marker_data = {
            "amc": "PPFAS",
            "year": year,
            "month": month,
            "files_downloaded": file_count,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)
        
        logger.info(f"Created completion marker: {marker_path.name}")

    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str) -> None:
        """Move incomplete/corrupt folder to quarantine."""
        corrupt_base = Path("data/raw/ppfas/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)
        
        corrupt_target = corrupt_base / f"{year}_{month:02d}"
        
        if corrupt_target.exists():
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            corrupt_target = corrupt_target.parent / f"{corrupt_target.name}__{ts}"
        
        logger.warning(f"PPFAS: Moving incomplete folder {source_dir} to {corrupt_target} (Reason: {reason})")
        shutil.move(str(source_dir), str(corrupt_target))

        # Emit warning event
        self.notifier.notify_warning(
            amc="PPFAS",
            year=year,
            month=month,
            warning_type="Corruption Recovery",
            message=f"Incomplete download detected and moved to quarantine. Reason: {reason}"
        )

    def _download_via_requests(self, target_year: int, target_month_name: str, download_folder: Path) -> Path:
        """
        Download PPFAS consolidated portfolio file using requests + BeautifulSoup.

        The PPFAS portfolio disclosure page is fully server-rendered static HTML.
        All download links are embedded in the page source — no browser needed.

        Page:    https://amc.ppfas.com/downloads/portfolio-disclosure/
        Method:  GET page → parse HTML → find id="collapse{Month}{Year}" panel
                 → extract <a class="btn btn-success"> (Consolidated link) → GET file
        """
        import requests as _requests
        from urllib.parse import urljoin
        from bs4 import BeautifulSoup

        listing_url = "https://amc.ppfas.com/downloads/portfolio-disclosure/"
        base_url    = "https://amc.ppfas.com"

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://amc.ppfas.com/",
        }

        # 1. Fetch the portfolio disclosure page
        logger.info(f"PPFAS: Fetching portfolio disclosure page")
        resp = _requests.get(listing_url, headers=headers, timeout=30, allow_redirects=True)
        resp.raise_for_status()
        logger.info(f"PPFAS: Page fetched ({len(resp.text):,} chars)")

        # 2. Parse with BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")

        # 3. Find the month accordion panel by its stable ID
        #    id="collapse{MonthName}{Year}"  e.g. id="collapseJuly2025"
        panel_id = f"collapse{target_month_name}{target_year}"
        panel = soup.find("div", id=panel_id)

        if panel is None:
            raise Exception(
                f"PPFAS: Month panel not found for {target_month_name} {target_year} "
                f"— not yet published or not on page"
            )

        logger.info(f"PPFAS: Found month panel #{panel_id}")

        # 4. Find the Consolidated link inside the panel
        #    It is the <a class="btn btn-success"> with text containing "Consolidated"
        consolidated_href = None
        for a_tag in panel.find_all("a", href=True):
            href = a_tag.get("href", "")
            if "/downloads/portfolio-disclosure/" not in href:
                continue
            ext = Path(href.split("?")[0]).suffix.lower()
            if ext not in (".xls", ".xlsx"):
                continue
            css_classes = a_tag.get("class", [])
            link_text   = a_tag.get_text(separator=" ", strip=True)
            if "btn-success" in css_classes and "Consolidated" in link_text:
                consolidated_href = href
                break

        if consolidated_href is None:
            raise Exception(
                f"PPFAS: Consolidated link not found for {target_month_name} {target_year}"
            )

        # 5. Build absolute URL
        file_url = consolidated_href if consolidated_href.startswith("http") else urljoin(base_url, consolidated_href)
        logger.info(f"PPFAS: Consolidated URL: {file_url}")

        # 6. Download the file
        filename  = file_url.split("/")[-1].split("?")[0]
        save_path = download_folder / filename

        logger.info(f"PPFAS: Downloading: {filename}")
        r = _requests.get(file_url, headers=headers, timeout=120, allow_redirects=True)
        r.raise_for_status()
        content = r.content

        # 7. Validate Excel magic bytes (not an HTML error page)
        is_xls  = len(content) >= 4 and content[:4] == b"\xd0\xcf\x11\xe0"
        is_xlsx = len(content) >= 2 and content[:2] == b"PK"
        if not (is_xls or is_xlsx):
            preview = content[:120].decode("utf-8", errors="replace")
            raise Exception(
                f"PPFAS: Downloaded content is not a valid Excel file. Preview: {preview}"
            )

        # 8. Save to disk
        with open(save_path, "wb") as f:
            f.write(content)

        size_kb = len(content) / 1024
        logger.success(f"PPFAS: Saved: {save_path.name} ({size_kb:.1f} KB)")
        return save_path

    def _check_file_count(self, file_count: int, year: int, month: int):
        """Sanity check file count (expected 1 for PPFAS consolidated)."""
        if file_count < 1:
            logger.warning(f"PPFAS: No files downloaded for {year}-{month:02d}")
        elif file_count > 1:
            logger.warning(f"PPFAS: More than 1 file downloaded ({file_count}) for {year}-{month:02d}")
        else:
            logger.info(f"PPFAS: File count (1) within normal range")

    def download(self, year: int, month: int) -> Dict:
        """
        Download PPFAS consolidated monthly portfolio file with Gold Standard retry logic.
        """
        start_time = time.time()
        
        # 1) Validation
        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month: {month}")

        logger.info("=" * 60)
        logger.info("PPFAS MUTUAL FUND REQUESTS DOWNLOADER STARTED")
        logger.info(f"Period: {year}-{month:02d}")
        if DRY_RUN:
            logger.info("MODE: DRY RUN (no network calls)")
        logger.info("=" * 60)

        if year < 2021 or (year == 2021 and month < 9):
            logger.warning(f"PPFAS: {year}-{month:02d} is before supported period (Sep 2021)")
            duration = time.time() - start_time
            logger.info(f"[SUMMARY]")
            logger.info(f"AMC: PPFAS")
            logger.info(f"Mode: SKIPPED")
            logger.info(f"Month: {year}-{month:02d}")
            logger.info(f"Status: UNSUPPORTED PERIOD")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info("=" * 60)
            return {"amc": AMC_PPFAS, "year": year, "month": month, "status": "skipped", "reason": "before_supported_period"}
            
        # 2) Idempotency
        target_dir = Path(self.get_target_folder("ppfas", year, month))
        if target_dir.exists():
            success_marker = target_dir / "_SUCCESS.json"
            if success_marker.exists():
                # Month already complete - check for missing consolidation
                logger.info(f"PPFAS: {year}-{month:02d} files already downloaded.")
                logger.info("Verifying consolidation/merged files...")

                # Always try consolidation in case it was missed/errored previously
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                logger.info("[SUCCESS] Month already complete — UPDATED")
                logger.info(f"🕒 Duration: {duration:.2f}s")
                logger.info("=" * 60)
                return {
                    "amc": AMC_PPFAS, 
                    "year": year, 
                    "month": month, 
                    "status": "skipped", 
                    "reason": "already_downloaded",
                    "duration": duration
                }
            else:
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json marker")

        self.ensure_directory(str(target_dir))

        # 3) Download with Retries
        last_error = "Unknown error"
        month_name = self._normalize_month_name(month)
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                if DRY_RUN:
                    logger.info(f"PPFAS: [DRY RUN] Would download {month_name} {year}")
                    duration = time.time() - start_time
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: PPFAS")
                    logger.info(f"Mode: DRY RUN")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: SIMULATED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": AMC_PPFAS, "year": year, "month": month, "status": "success", "dry_run": True}
                
                file_path = self._download_via_requests(year, month_name, target_dir)
                
                # 4) Success Marker
                self._create_success_marker(target_dir, year, month, 1)
                
                # Consolidate downloads
                self.consolidate_downloads(year, month)
                
                # 5) Sanity Check
                self._check_file_count(1, year, month)
                
                # 6) Notification
                duration = time.time() - start_time
                self.notifier.notify_success("PPFAS", year, month, files_downloaded=1, duration=duration)
                
                logger.success("[SUCCESS] PPFAS download completed")
                logger.info("=" * 60)
                logger.info(f"[SUMMARY]")
                logger.info(f"AMC: PPFAS")
                logger.info(f"Mode: AUTO")
                logger.info(f"Month: {year}-{month:02d}")
                logger.info(f"Files downloaded: 1")
                logger.info(f"Duration: {duration:.2f}s")
                logger.info(f"Status: SUCCESS")
                logger.info("=" * 60)
                
                return {
                    "amc": AMC_PPFAS,
                    "year": year,
                    "month": month,
                    "status": "success",
                    "file_path": str(file_path),
                    "files_downloaded": 1,
                    "duration": duration
                }

            except Exception as e:
                last_error = str(e)
                
                # Specialized handling for "Not Published"
                if "Month header" in last_error or "not found" in last_error.lower():
                    logger.warning(f"PPFAS: {year}-{month:02d} not yet published: {last_error}")
                    self.notifier.notify_not_published("PPFAS", year, month)
                    
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                        
                    duration = time.time() - start_time
                    logger.info(f"[SUMMARY]")
                    logger.info(f"AMC: PPFAS")
                    logger.info(f"Mode: AUTO")
                    logger.info(f"Month: {year}-{month:02d}")
                    logger.info(f"Status: NOT PUBLISHED")
                    logger.info(f"Duration: {duration:.2f}s")
                    logger.info("=" * 60)
                    return {"amc": AMC_PPFAS, "year": year, "month": month, "status": "not_published", "reason": last_error}

                if attempt < MAX_RETRIES:
                    backoff = RETRY_BACKOFF[attempt]
                    logger.warning(f"PPFAS: Download attempt {attempt + 1} failed: {last_error}. Retrying in {backoff}s...")
                    time.sleep(backoff)
                else:
                    logger.error(f"PPFAS: Max retries exceeded for {year}-{month:02d}")

        # If we reached here, it's a final failure
        self.notifier.notify_error(
            amc="PPFAS",
            year=year,
            month=month,
            error_type="Download Error",
            reason=last_error[:100]
        )
        
        # Clean up partial folder
        if target_dir.exists() and not (target_dir / "_SUCCESS.json").exists():
            shutil.rmtree(target_dir, ignore_errors=True)
            
        duration = time.time() - start_time
        logger.info(f"[SUMMARY]")
        logger.info(f"AMC: PPFAS")
        logger.info(f"Mode: AUTO")
        logger.info(f"Month: {year}-{month:02d}")
        logger.info(f"Status: FAILED")
        logger.info(f"Duration: {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "amc": AMC_PPFAS,
            "year": year,
            "month": month,
            "status": "failed",
            "reason": last_error,
            "duration": duration
        }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="PPFAS Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True, help="Calendar year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    
    args = parser.parse_args()
    
    downloader = PPFASDownloader()
    result = downloader.download(year=args.year, month=args.month)
    
    if result["status"] == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif result["status"] == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete (Consolidation refreshed)")
    elif result["status"] == "not_published":
        logger.info(f"[INFO]  Info: Month not yet published: {result.get('reason')}")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        exit(1)
