# src/downloaders/union_downloader.py

import os
import re
import time
import json
import shutil
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
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


class UnionDownloader(BaseDownloader):
    """
    Union Mutual Fund - Monthly Portfolio Downloader.
    
    Extracts monthly portfolio disclosure files directly from the Union Mutual Fund
    documents REST API (https://www.unionmf.com/api/downloads/documents)
    without requiring Playwright or browser automation.
    """

    BASE_URL = "https://www.unionmf.com"
    DOCUMENTS_API_URL = "https://www.unionmf.com/api/downloads/documents"
    DOWNLOADS_PAGE_URL = "https://www.unionmf.com/about-us/downloads"

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    MONTH_STR_TO_NUM = {
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

    # Static fallback mapping of known (year, month) -> FolderId
    KNOWN_FOLDERS = {
        (2027, 3): "26291acf-5876-41cc-a3d0-24c7ed9213c4",
        (2027, 2): "876114df-9ddb-4411-8397-2beaea13d6ca",
        (2027, 1): "b6897041-3838-4a44-87ef-cab650a7af33",
        (2026, 12): "eb642f12-c2f8-4764-b26f-d29d87969b3c",
        (2026, 11): "c5a1a546-b5ef-48cb-a093-d85b1b8a4d09",
        (2026, 10): "044ae2e3-62d6-46df-b73c-958cdc61aecf",
        (2026, 9): "6c176971-0dee-4e0a-b355-f1ef43da3cb9",
        (2026, 8): "e4709461-3dcd-4a47-8a23-605a090d1eef",
        (2026, 7): "5fd732bb-03f5-435c-852b-79218a93d0f3",
        (2026, 6): "4e8d856a-158b-43a0-bc2f-6e46547ab475",
        (2026, 5): "35c05df3-b43f-4f9e-849c-538afc5814f7",
        (2026, 4): "9b297250-fb6a-438e-88f1-6433bf38f71f",
        (2026, 3): "f6590be0-e035-43c8-a984-e2cdd2370270",
        (2026, 2): "b6cafa81-47fb-4935-bc54-b752b9e7d797",
        (2026, 1): "1506abdf-6c38-428e-b7fd-f3d281a660ac",
        (2025, 12): "6b14a299-fd37-41ed-84f7-c35a54df5f21",
        (2025, 11): "2985978e-3428-418f-89b5-72fe10ad1aae",
        (2025, 10): "17904dea-eceb-4543-b762-684025234c53",
        (2025, 9): "3dd070f4-bd3c-40f0-86db-4fe4d5ff8c04",
        (2022, 9): "05c9a05d-c6b7-43ce-86c4-5c5eb077869e",
    }

    def __init__(self):
        super().__init__("Union Mutual Fund")
        self.notifier = get_notifier()
        self.AMC_NAME = "union"
        self._folder_cache: Dict[Tuple[int, int], str] = dict(self.KNOWN_FOLDERS)

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        marker_data = {
            "amc": "Union",
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
        self.notifier.notify_error("Union", year, month, "Corruption Recovery", f"Moved to quarantine: {reason}")

    def _discover_folders(self, session: requests.Session) -> Dict[Tuple[int, int], str]:
        """Dynamically extract pf_disclosure folder IDs from downloads page."""
        try:
            resp = session.get(self.DOWNLOADS_PAGE_URL, headers=self.DEFAULT_HEADERS, timeout=20)
            if resp.status_code != 200:
                return {}

            html = resp.text
            idx = html.find('"pf_disclosure"')
            if idx == -1:
                return {}

            block = html[idx:idx + 4000]
            discovered: Dict[Tuple[int, int], str] = {}

            for line in block.splitlines():
                uuid_match = re.search(
                    r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})',
                    line
                )
                comment_match = re.search(r'//\s*([A-Za-z]+)\s+(\d{4})', line)
                if uuid_match and comment_match:
                    uid = uuid_match.group(1).lower()
                    mon_name = comment_match.group(1).lower()
                    yr_val = int(comment_match.group(2))
                    mon_num = self.MONTH_STR_TO_NUM.get(mon_name)
                    if mon_num:
                        discovered[(yr_val, mon_num)] = uid

            return discovered
        except Exception as e:
            logger.warning(f"Could not dynamically discover Union folders: {e}")
            return {}

    def _get_folder_id(self, session: requests.Session, year: int, month: int) -> Optional[str]:
        key = (int(year), int(month))
        if key in self._folder_cache:
            return self._folder_cache[key]

        discovered = self._discover_folders(session)
        self._folder_cache.update(discovered)
        return self._folder_cache.get(key)

    @staticmethod
    def _parse_title(title: str) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str]]:
        if not title:
            return None, None, None, None

        title_clean = title.strip()
        m = re.search(r'(\d{1,2})[-.](\d{1,2})[-.](\d{4})\s*$', title_clean)
        if not m:
            return None, None, None, None

        day = int(m.group(1))
        mon = int(m.group(2))
        yr = int(m.group(3))
        date_str = f"{yr:04d}-{mon:02d}-{day:02d}"

        lower_title = title_clean.lower()
        prefix = "monthly portfolio report "
        if lower_title.startswith(prefix):
            scheme = title_clean[len(prefix):m.start()].strip()
        else:
            scheme = title_clean[:m.start()].strip()

        return yr, mon, date_str, scheme

    def _get_monthly_portfolios(
        self, session: requests.Session, year: int, month: int
    ) -> List[Dict[str, Any]]:
        folder_id = self._get_folder_id(session, year, month)
        if not folder_id:
            return []

        portfolios: List[Dict[str, Any]] = []
        skip = 0
        top = 100

        while True:
            params = {
                "$filter": f"FolderId eq {folder_id}",
                "$top": top,
                "$skip": skip,
            }

            resp = session.get(
                self.DOCUMENTS_API_URL,
                params=params,
                headers=self.DEFAULT_HEADERS,
                timeout=25,
            )
            if resp.status_code != 200:
                logger.warning(f"Union documents API returned status {resp.status_code}")
                break

            data = resp.json()
            items = data.get("value", []) if isinstance(data, dict) else []
            if not items:
                break

            for item in items:
                if not isinstance(item, dict):
                    continue

                title = (item.get("Title") or "").strip()
                raw_url = (item.get("Url") or "").strip()
                ext = (item.get("Extension") or "").lower()

                if not title.lower().startswith("monthly portfolio report"):
                    continue
                if ext not in (".xlsx", ".xls"):
                    continue
                if not raw_url:
                    continue

                yr, mon, date_str, scheme = self._parse_title(title)
                if yr != year or mon != month:
                    continue

                full_url = urllib.parse.urljoin(self.BASE_URL, raw_url)
                parsed_path = urllib.parse.urlparse(full_url).path
                filename = os.path.basename(parsed_path)

                portfolios.append({
                    "scheme": scheme,
                    "date": date_str,
                    "title": title,
                    "url": full_url,
                    "filename": filename,
                })

            if len(items) < top:
                break
            skip += top

        return portfolios

    def _download_file(
        self, session: requests.Session, url: str, target_path: Path
    ) -> bool:
        try:
            resp = session.get(url, headers=self.DEFAULT_HEADERS, stream=True, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"Failed to download {url}: HTTP {resp.status_code}")
                return False

            content_type = resp.headers.get("Content-Type", "").lower()
            if "text/html" in content_type:
                logger.warning(f"Skipping HTML content for {url}")
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
        month_name = self.MONTH_NAMES.get(month, f"Month-{month}")
        
        logger.info("=" * 60)
        logger.info(f"UNION MUTUAL FUND DOWNLOADER: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder(self.AMC_NAME, year, month))
        
        if target_dir.exists():
            if (target_dir / "_SUCCESS.json").exists():
                logger.info(f"Union: {year}-{month:02d} files already downloaded.")
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

                session = requests.Session()
                schemes = self._get_monthly_portfolios(session, year, month)
                
                if not schemes:
                    logger.warning(f"{self.AMC_NAME}: No portfolios found for {month_name} {year}")
                    self.notifier.notify_not_published("Union", year, month)
                    if target_dir.exists():
                        shutil.rmtree(target_dir, ignore_errors=True)
                    return {"status": "not_published"}

                logger.info(f"Found {len(schemes)} scheme portfolios for Union {year}-{month:02d}.")

                files_downloaded = 0
                for idx, item in enumerate(schemes, 1):
                    scheme_name = item["scheme"]
                    clean_scheme = scheme_name.replace("Union ", "").strip()
                    clean_scheme = re.sub(r'[\\/*?:"<>|]', "_", clean_scheme).replace(" ", "_").replace("&", "and")

                    path_name = urllib.parse.urlparse(item["url"]).path
                    ext = os.path.splitext(path_name)[1].lower()
                    if ext not in (".xlsx", ".xls"):
                        ext = ".xlsx"

                    fname = f"{clean_scheme}{ext}"
                    save_path = target_dir / fname

                    logger.info(f"  [{idx:2d}/{len(schemes)}] Downloading: {scheme_name[:40]}...")
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
                self.notifier.notify_success("Union", year, month, files_downloaded=files_downloaded, duration=duration)
                logger.success(f"[SUCCESS] {self.AMC_NAME} download completed: {files_downloaded} files")
                return {"status": "success", "files_downloaded": files_downloaded, "duration": duration}

            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt+1} failed: {last_error}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)])

        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        self.notifier.notify_error("Union", year, month, "Download Failure", last_error[:100])
        return {"status": "failed", "reason": last_error}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = UnionDownloader()
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
