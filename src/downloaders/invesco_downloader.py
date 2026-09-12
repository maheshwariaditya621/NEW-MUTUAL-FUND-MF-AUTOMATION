# src/downloaders/invesco_downloader.py

import os
import re
import time
import json
import shutil
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
import requests

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


class InvescoDownloader(BaseDownloader):
    """
    Invesco Mutual Fund - Monthly Portfolio Downloader.
    
    Extracts monthly scheme portfolio files directly from the Invesco
    CompleteMonthlyHoldings backend REST API via direct HTTP requests.
    Supports ALL scheme classifications (Equity, Fixed Income / Debt,
    Hybrid, ETF, Fund of Funds, Index Funds).
    """

    BASE_URL = "https://www.invescomutualfund.com"
    HOLDINGS_API_URL = "https://www.invescomutualfund.com/api/CompleteMonthlyHoldings"
    CLASSIFICATION_API_URL = "https://www.invescomutualfund.com/api/ClassificationCompleteMonthlyHoldings"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    MONTH_FIELD_MAP = {
        1: ("JanUrl", "JanName"),
        2: ("FebUrl", "FebName"),
        3: ("MarUrl", "MarName"),
        4: ("AprUrl", "AprName"),
        5: ("MayUrl", "MayName"),
        6: ("JunUrl", "JunName"),
        7: ("JulUrl", "JulName"),
        8: ("AugUrl", "AugName"),
        9: ("SepUrl", "SepName"),
        10: ("OctUrl", "OctName"),
        11: ("NovUrl", "NovName"),
        12: ("DecUrl", "DecName"),
    }

    FALLBACK_CLASSIFICATIONS = [
        {"name": "Equity", "value": "equity"},
        {"name": "Fixed income", "value": "fixed-income"},
        {"name": "Fund of funds", "value": "fund-of-funds"},
        {"name": "Exchange traded fund", "value": "exchange-traded-fund"},
        {"name": "Hybrid", "value": "hybrid"},
        {"name": "Fixed maturity plans", "value": "fixed-maturity-plans"},
        {"name": "Index funds", "value": "index-funds"},
    ]

    def __init__(self):
        super().__init__("Invesco Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "invesco"
        self._cached_classifications: Optional[List[Dict[str, str]]] = None

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Invesco",
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
        self.notifier.notify_error("Invesco", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _get_classifications(self, session: requests.Session) -> List[Dict[str, str]]:
        """Fetch all supported fund classifications dynamically from the backend."""
        if self._cached_classifications:
            return self._cached_classifications

        try:
            resp = session.get(self.CLASSIFICATION_API_URL, headers=self.DEFAULT_HEADERS, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    classes = []
                    for item in data:
                        val = (item.get("FunClassificationValue") or "").strip()
                        name = (item.get("FundClassification") or "").replace("&nbsp;", " ").strip()
                        if val and val.lower() != "select":
                            classes.append({"name": name, "value": val})
                    if classes:
                        self._cached_classifications = classes
                        return classes
        except Exception as e:
            logger.warning(f"Failed to fetch dynamic classifications ({e}), using fallback list.")

        return self.FALLBACK_CLASSIFICATIONS

    def _get_monthly_portfolios(
        self, session: requests.Session, year: int, month: int
    ) -> List[Dict[str, Any]]:
        """
        Query CompleteMonthlyHoldings API across ALL fund classifications for the given month/year.
        """
        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month: {month}")

        url_key, name_key = self.MONTH_FIELD_MAP[month]
        categories = self._get_classifications(session)

        all_schemes: List[Dict[str, Any]] = []
        seen_urls: Set[str] = set()

        for cat in categories:
            cat_val = cat["value"]
            cat_name = cat.get("name", cat_val.capitalize())
            params = {"year": int(year), "classification": cat_val}

            try:
                resp = session.get(
                    self.HOLDINGS_API_URL,
                    params=params,
                    headers=self.DEFAULT_HEADERS,
                    timeout=25,
                )
                if resp.status_code != 200:
                    logger.warning(f"Invesco API returned status {resp.status_code} for category {cat_val}")
                    continue

                data = resp.json()
                if not isinstance(data, list):
                    continue

                for item in data:
                    if not isinstance(item, dict):
                        continue

                    scheme_name = (item.get("Name") or "").strip()
                    raw_url = (item.get(url_key) or "").strip()
                    if not scheme_name or not raw_url:
                        continue

                    full_url = urllib.parse.urljoin(self.BASE_URL, raw_url)
                    if full_url in seen_urls:
                        continue
                    seen_urls.add(full_url)

                    all_schemes.append({
                        "scheme": scheme_name,
                        "url": full_url,
                        "month_name": (item.get(name_key) or "").strip(),
                        "classification": cat_val,
                        "classification_name": cat_name,
                    })

            except Exception as e:
                logger.error(f"Error querying Invesco API for category {cat_val}: {e}")
                continue

        return all_schemes

    def _download_file(
        self, session: requests.Session, url: str, target_path: Path
    ) -> bool:
        """Download file from URL and validate file signature."""
        try:
            resp = session.get(url, headers=self.DEFAULT_HEADERS, stream=True, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"Failed to download {url}: HTTP {resp.status_code}")
                return False

            content_type = resp.headers.get("Content-Type", "").lower()
            if "text/html" in content_type:
                logger.warning(f"Skipping HTML content received for {url}")
                return False

            with open(target_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

            file_size = target_path.stat().st_size
            if file_size == 0:
                target_path.unlink(missing_ok=True)
                return False

            # Magic bytes validation: OpenXML (PK\x03\x04) or OLE XLS (\xd0\xcf\x11\xe0)
            with open(target_path, "rb") as f:
                magic = f.read(8)

            if not (magic.startswith(b"PK\x03\x04") or magic.startswith(b"\xd0\xcf\x11\xe0")):
                logger.warning(f"Invalid magic bytes ({magic[:4]!r}) for {target_path.name}")
                target_path.unlink(missing_ok=True)
                return False

            return True

        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            if target_path.exists():
                target_path.unlink(missing_ok=True)
            return False

    def download(self, year: int, month: int) -> Dict[str, Any]:
        start_time = time.time()
        month_abbr = self.MONTH_FIELD_MAP[month][1].replace("Name", "")
        
        logger.info("=" * 60)
        logger.info(f"INVESCO MUTUAL FUND DOWNLOADER: {year}-{month:02d}")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Invesco: {year}-{month:02d} files already downloaded.")
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
                    logger.info(f"{self.AMC_NAME}: [DRY RUN] Would download {month:02d}/{year}")
                    return {"status": "success", "dry_run": True}

                session = requests.Session()
                schemes = self._get_monthly_portfolios(session, year, month)
                
                if not schemes:
                    logger.warning(f"{self.AMC_NAME}: No portfolios found for {month:02d}/{year}")
                    self.notifier.notify_not_published("Invesco", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                logger.info(f"Found {len(schemes)} scheme portfolios for Invesco {year}-{month:02d} across all categories.")

                files_downloaded = 0
                for idx, item in enumerate(schemes, 1):
                    scheme_name = item["scheme"]
                    clean_scheme = scheme_name.replace("Invesco India ", "").replace("Invesco ", "").strip()
                    clean_scheme = re.sub(r'[\\/*?:"<>|]', "_", clean_scheme).replace(" ", "_").replace("&", "and")
                    clean_category = item["classification"].replace("-", "_")

                    # Extract original extension or default to .xlsx
                    path_name = urllib.parse.urlparse(item["url"]).path
                    ext = os.path.splitext(path_name)[1].lower()
                    if ext not in (".xlsx", ".xls"):
                        ext = ".xlsx"

                    fname = f"{clean_scheme}_{clean_category}{ext}"
                    save_path = target_dir / fname

                    logger.info(f"  [{idx:2d}/{len(schemes)}] Downloading ({item['classification_name']}): {scheme_name[:40]}...")
                    if self._download_file(session, item["url"], save_path):
                        files_downloaded += 1
                        logger.info(f"       [OK] Saved: {fname} ({save_path.stat().st_size:,} bytes)")
                    else:
                        logger.warning(f"       [FAIL] Failed: {fname}")

                if files_downloaded == 0:
                    raise RuntimeError("Failed to download any valid portfolio files.")

                self._create_success_marker(target_dir, year, month, files_downloaded)
                
                # Consolidate all downloaded scheme files into merged Excel
                self.consolidate_downloads(year, month)
                
                duration = time.time() - start_time
                self.notifier.notify_success("Invesco", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)])

        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Invesco", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = InvescoDownloader()
    result = downloader.download(args.year, args.month)

    status = result["status"]
    if status == "success":
        logger.success(f"[SUCCESS] Success: Downloaded {result.get('files_downloaded', 0)} file(s)")
    elif status == "skipped":
        logger.success(f"[SUCCESS] Success: Month already complete (Consolidation refreshed)")
    elif status == "not_published":
        logger.info(f"[INFO] Info: Month not yet published")
    else:
        logger.error(f"[ERROR] Failed: {result.get('reason', 'Unknown error')}")
        raise SystemExit(1)
