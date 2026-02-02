# src/downloaders/icici_downloader.py

import requests
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

# Import downloader config
try:
    from src.config.downloader_config import (
        DRY_RUN, FILE_COUNT_MIN, FILE_COUNT_MAX,
        MAX_RETRIES, RETRY_BACKOFF
    )
except ImportError:
    DRY_RUN = False
    FILE_COUNT_MIN = 1
    FILE_COUNT_MAX = 1
    MAX_RETRIES = 2
    RETRY_BACKOFF = [5, 15]


class ICICIDownloader(BaseDownloader):
    API_URL = "https://apimf.icicipruamc.com/nms/v1/downloads/files"

    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    def __init__(self):
        super().__init__("ICICI Prudential Mutual Fund")
        self.notifier = get_notifier()

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _check_file_count(self, file_count: int, year: int, month: int):
        expected = 1
        if file_count != expected:
            logger.warning(
                f"⚠️ File count mismatch for {year}-{month:02d}: "
                f"expected {expected}, got {file_count}"
            )
        else:
            logger.info(f"✅ File count OK ({file_count})")

    def _create_success_marker(self, target_dir: Path, year: int, month: int, file_count: int):
        marker_path = target_dir / "_SUCCESS.json"
        with open(marker_path, "w") as f:
            json.dump({
                "amc": "ICICI",
                "year": year,
                "month": month,
                "files_downloaded": file_count,
                "timestamp": datetime.utcnow().isoformat()
            }, f, indent=2)

        logger.info(f"🧱 _SUCCESS.json written for {year}-{month:02d}")

    def _move_to_corrupt(self, source_dir: Path, year: int, month: int, reason: str):
        corrupt_base = Path("data/raw/icici/_corrupt")
        corrupt_base.mkdir(parents=True, exist_ok=True)

        target = corrupt_base / f"{year}_{month:02d}"
        if target.exists():
            shutil.rmtree(target)

        shutil.move(str(source_dir), str(target))
        logger.warning(
            f"🚨 Corrupt folder detected → moved to _corrupt/{year}_{month:02d} | Reason: {reason}"
        )

    def _api_call_with_retry(self, headers: dict, payload: dict, year: int, month: int):
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = requests.post(self.API_URL, headers=headers, json=payload, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.Timeout:
                if attempt < MAX_RETRIES:
                    backoff = RETRY_BACKOFF[attempt]
                    logger.warning(
                        f"⏳ Timeout {attempt+1}/{MAX_RETRIES+1} for {year}-{month:02d}, retrying in {backoff}s"
                    )
                    time.sleep(backoff)
                else:
                    raise
            except requests.HTTPError:
                raise

    # ------------------------------------------------------------------ #
    # Main download
    # ------------------------------------------------------------------ #

    def download(self, year: int, month: int) -> Dict:
        start_time = time.time()
        month_name = self.MONTH_NAMES[month]

        logger.info("=" * 60)
        logger.info("📥 ICICI PRUDENTIAL MUTUAL FUND DOWNLOADER")
        logger.info(f"🗓️  Period: {year}-{month:02d} ({month_name})")
        logger.info("=" * 60)

        target_dir = Path(self.get_target_folder("icici", year, month))

        # -------------------- IDENTITY CHECK -------------------- #
        if target_dir.exists():
            if not (target_dir / "_SUCCESS.json").exists():
                logger.warning(f"⚠️ Incomplete folder detected for {year}-{month:02d}")
                self._move_to_corrupt(target_dir, year, month, "Missing _SUCCESS.json")
            else:
                duration = time.time() - start_time
                logger.info("⏭️  Month already downloaded — SKIPPING")
                logger.info(f"🕒 Duration: {duration:.2f}s")
                logger.info("=" * 60)

                return {
                    "amc": "ICICI",
                    "year": year,
                    "month": month,
                    "status": "skipped",
                    "duration": duration
                }

        self.ensure_directory(str(target_dir))
        logger.info(f"📁 Target directory ready: {target_dir}")

        # -------------------- API CALL -------------------- #
        payload = {
            "categoryId": "2024-2025,26a073d7-08d2-4a95-95fa-f83a4ee51e40",
            "fileType": "All",
            "page": "1",
            "size": "20",
            "userType": "Investor"
        }

        headers = {
            "Content-Type": "application/json",
            "env": "api",
            "origin": "https://www.icicipruamc.com",
            "referer": "https://www.icicipruamc.com/",
        }

        logger.info("🌐 Calling ICICI API…")
        resp = self._api_call_with_retry(headers, payload, year, month)

        data = resp.json()
        files = data.get("success", {}).get("data", {}).get("files", [])
        logger.info(f"📦 API returned {len(files)} file(s)")

        if not files:
            logger.warning(f"Month not yet published: {month_name} {year}")
            logger.warning("API returned empty files list")
            
            # Emit not published event
            self.notifier.notify_not_published(
                amc="ICICI",
                year=year,
                month=month
            )
            
            # Remove empty directory
            if target_dir.exists():
                shutil.rmtree(target_dir)
            
            logger.info("=" * 60)
            return {
                "amc": "ICICI",
                "year": year,
                "month": month,
                "status": "not_published"
            }

        # -------------------- MONTH MATCH -------------------- #
        # CRITICAL: Match ONLY files for requested (year, month)
        # API returns multiple months - we must filter strictly
        matched: List[Dict] = []

        # 1. PRIMARY MATCH: Timestamp-based (Trust API "applicableMonth" if valid)
        for item in files:
            ts = item.get("applicableMonth")
            rel_url = item.get("url")

            if not ts or not rel_url:
                continue

            # Convert timestamp to UTC datetime
            dt = datetime.utcfromtimestamp(ts / 1000)

            # STRICT MATCH: Only files for THIS exact month
            if dt.year == year and dt.month == month:
                matched.append({
                    "name": item.get("title", {}).get("text", "icici_monthly_portfolio") + ".zip",
                    "url": "https://www.icicipruamc.com/blob" + rel_url,
                    "matched_year": dt.year,
                    "matched_month": dt.month
                })
                logger.info(f"✓ Matched (Timestamp): {item.get('title', {}).get('text', 'Unknown')} (timestamp: {dt.year}-{dt.month:02d})")

        # 2. SECONDARY MATCH: Fallback to filename/title parsing ONLY if timestamp match failed
        # (Handles cases where API metadata is wrong but file is correct)
        if not matched and files:
            target_month_name = self.MONTH_NAMES[month].lower()
            target_year_str = str(year)
            
            for item in files:
                rel_url = item.get("url")
                if not rel_url:
                    continue

                title_text = item.get("title", {}).get("text", "") or ""
                file_name = item.get("fileName", "") or ""
                
                # Check both title and filename for "Month Year" pattern
                # Strict check: Must contain both full month name and year
                candidates = [title_text.lower(), file_name.lower()]
                is_match = False

                for text in candidates:
                    if target_year_str in text and target_month_name in text:
                        is_match = True
                        break
                
                if is_match:
                    logger.info(f"ℹ️ Using filename-based fallback for ICICI {year}-{month:02d}")
                    
                    # Construct matching entry
                    final_name = item.get("title", {}).get("text", "")
                    if not final_name:
                         final_name = item.get("fileName", "icici_monthly_portfolio")
                    
                    if not final_name.lower().endswith(".zip"):
                        final_name += ".zip"

                    matched.append({
                        "name": final_name,
                        "url": "https://www.icicipruamc.com/blob" + rel_url,
                        "matched_year": year,   # Explicitly set to requested year
                        "matched_month": month  # Explicitly set to requested month
                    })
                    logger.info(f"✓ Matched (Fallback): {final_name}")

        if not matched:
            logger.warning(f"Month not yet published: {month_name} {year}")
            logger.warning("No matching files found in API response")
            
            # Emit not published event
            self.notifier.notify_not_published(
                amc="ICICI",
                year=year,
                month=month
            )
            
            # Remove empty directory
            if target_dir.exists():
                shutil.rmtree(target_dir)
            
            logger.info("=" * 60)
            return {
                "amc": "ICICI",
                "year": year,
                "month": month,
                "status": "not_published"
            }

        logger.info(f"📌 Found {len(matched)} file(s) for {year}-{month:02d}")

        # -------------------- DOWNLOAD -------------------- #
        saved_files = []

        for idx, f in enumerate(matched, start=1):
            logger.info(f"⬇️  Downloading {idx}/{len(matched)}: {f['name']}")
            r = requests.get(f["url"], timeout=60)
            r.raise_for_status()

            # CRITICAL FIX: Save file in its ACTUAL month folder, not requested month folder
            # Derive folder from file's matched_year and matched_month
            actual_year = f["matched_year"]
            actual_month = f["matched_month"]
            actual_folder = Path(self.get_target_folder("icici", actual_year, actual_month))
            
            # Ensure actual folder exists
            self.ensure_directory(str(actual_folder))
            
            path = actual_folder / f["name"]
            with open(path, "wb") as out:
                out.write(r.content)

            saved_files.append(str(path))
            logger.info(f"✅ Saved: {path.name} → {actual_year}_{actual_month:02d}/")


        # -------------------- FINALIZE -------------------- #
        # Create _SUCCESS.json for the folder that received files
        if len(saved_files) > 0:
            # All matched files have same year/month, so use first file's folder
            actual_folder = Path(saved_files[0]).parent
            actual_folder_name = actual_folder.name
            
            # Parse year/month from folder name (YYYY_MM format)
            import re
            match = re.match(r'^(\d{4})_(\d{2})$', actual_folder_name)
            if match:
                folder_year = int(match.group(1))
                folder_month = int(match.group(2))
                
                self._check_file_count(len(saved_files), folder_year, folder_month)
                self._create_success_marker(actual_folder, folder_year, folder_month, len(saved_files))
        
        # Clean up empty requested month folder if it wasn't used
        if target_dir.exists():
            # Check if folder is empty (no ZIP files)
            zip_files = list(target_dir.glob("*.zip"))
            if len(zip_files) == 0:
                shutil.rmtree(target_dir)
                logger.info(f"🗑️  Removed empty folder: {target_dir.name}")

        duration = time.time() - start_time
        
        # Emit success event
        self.notifier.notify_success(
            amc="ICICI",
            year=year,
            month=month,
            files_downloaded=len(saved_files),
            duration=duration
        )

        logger.info("=" * 60)
        logger.info(f"🎉 SUCCESS | ICICI {year}-{month:02d}")
        logger.info(f"📄 Files downloaded: {len(saved_files)}")
        logger.info(f"⏱️  Duration: {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "amc": "ICICI",
            "year": year,
            "month": month,
            "files_downloaded": len(saved_files),
            "files": saved_files,
            "status": "success",
            "duration": duration
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ICICI Prudential Mutual Fund Downloader")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    downloader = ICICIDownloader()
    result = downloader.download(args.year, args.month)

    if result["status"] != "success":
        raise SystemExit(1)
