"""
BSE Corporate Announcements Collector.

Polls BSE AnnSubCategoryGetData API for Master Office Watchlist scrip codes,
computes dynamic recovery lookbacks across outages/weekends, normalizes filings,
and merges into canonical announcements.
"""

from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

from src.config import logger
from src.announcements.normalizer import parse_announcement_timestamp
from src.announcements.deduplicator import AnnouncementDeduplicator
from src.announcements.sync_state import SyncStateManager

BSE_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}


class BSECollector:
    """Collector for BSE Corporate Announcements feed."""

    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def fetch_scrip_announcements(
        self,
        scrip_code: str,
        from_date: datetime,
        to_date: datetime,
        max_retries: int = 2
    ) -> List[Dict[str, Any]]:
        """
        Query BSE corporate announcements for a specific scrip code.
        Format: strPrevDate=YYYYMMDD, strToDate=YYYYMMDD.
        """
        prev_str = from_date.strftime("%Y%m%d")
        to_str = to_date.strftime("%Y%m%d")

        url = (
            f"{BSE_API_URL}?pageno=1&strCat=-1&strPrevDate={prev_str}"
            f"&strScrip={scrip_code}&strSearch=P&strToDate={to_str}&strType=C&subcategory="
        )

        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(url, timeout=8)
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        return data.get("Table", [])
                    except Exception as json_err:
                        logger.warning(f"BSE JSON parse error for scrip {scrip_code}: {json_err}")
                else:
                    logger.warning(f"BSE HTTP {resp.status_code} for scrip {scrip_code}")
            except Exception as e:
                logger.warning(f"BSE request error for scrip {scrip_code} (attempt {attempt}): {e}")

            time.sleep(attempt * 0.8)

        return []

    def _process_single_scrip(
        self,
        company: Dict[str, Any],
        now_utc: datetime,
        force_recovery: bool = False
    ) -> Dict[str, Any]:
        """Poll and ingest announcements for a single Master Watchlist company."""
        scrip_code = company.get("bse_code")
        if not scrip_code:
            return {"scrip": None, "matched": 0, "new": 0, "merged": 0}

        scrip_str = str(scrip_code).strip()

        # Dynamic recovery window:
        # min(last_checkpoint - 24h, now - 3d)
        if force_recovery:
            from_dt = now_utc - timedelta(days=5)
        else:
            from_dt = SyncStateManager.get_bse_recovery_start_date(scrip_str)

        to_dt = now_utc

        announcements = self.fetch_scrip_announcements(scrip_str, from_dt, to_dt)
        if not announcements:
            SyncStateManager.update_sync_state(
                exchange="BSE",
                scrip_code=scrip_str,
                last_checkpoint=to_dt,
                status="SUCCESS"
            )
            return {"scrip": scrip_str, "matched": 0, "new": 0, "merged": 0}

        new_count = 0
        merged_count = 0
        max_ts = None

        for item in announcements:
            news_id = item.get("NEWSID")
            if not news_id:
                continue

            raw_subject = item.get("NEWSSUB") or item.get("HEADLINE") or "Corporate Announcement"
            raw_details = item.get("MORE") or item.get("HEADLINE")
            raw_dt = item.get("DT_TM")

            dissem_dt = parse_announcement_timestamp(raw_dt) or now_utc
            if max_ts is None or dissem_dt > max_ts:
                max_ts = dissem_dt

            att_file = item.get("ATTACHMENTNAME")
            att_url = None
            if att_file and att_file.strip():
                att_file = att_file.strip()
                att_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{att_file}"

            can_id, src_id, is_new = AnnouncementDeduplicator.ingest_announcement(
                exchange="BSE",
                source_announcement_id=str(news_id),
                company_id=company["company_id"],
                isin=company["isin"],
                company_name=company["company_name"],
                exchange_symbol=company.get("nse_symbol"),
                raw_subject=raw_subject,
                raw_details=raw_details,
                raw_category=item.get("CATEGORYNAME"),
                raw_subcategory=item.get("SUBCATNAME"),
                dissemination_dt=dissem_dt,
                submission_dt=None,
                time_difference=item.get("TIMEDIFF"),
                has_xbrl=bool(item.get("NSURL") or item.get("XML_NAME")),
                raw_payload=item,
                attachment_url=att_url,
                attachment_filename=att_file
            )

            if is_new:
                new_count += 1
            else:
                merged_count += 1

        checkpoint_ts = max_ts or now_utc
        SyncStateManager.update_sync_state(
            exchange="BSE",
            scrip_code=scrip_str,
            last_checkpoint=checkpoint_ts,
            status="SUCCESS"
        )

        return {
            "scrip": scrip_str,
            "matched": len(announcements),
            "new": new_count,
            "merged": merged_count
        }

    def poll(self, watchlist: List[Dict[str, Any]], recovery: bool = False) -> Dict[str, Any]:
        """
        Poll BSE announcements for all companies in the Master Watchlist.
        Executes concurrently across thread pool to meet the 60s cycle SLA.
        """
        scrip_companies = [c for c in watchlist if c.get("bse_code")]
        if not scrip_companies:
            return {"status": "SKIPPED", "reason": "No companies with bse_code in watchlist", "matched": 0}

        now_utc = datetime.now(timezone.utc)
        total_matched = 0
        total_new = 0
        total_merged = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single_scrip, comp, now_utc, recovery): comp
                for comp in scrip_companies
            }

            for fut in as_completed(futures):
                try:
                    res = fut.result()
                    total_matched += res.get("matched", 0)
                    total_new += res.get("new", 0)
                    total_merged += res.get("merged", 0)
                except Exception as e:
                    logger.error(f"Error processing BSE scrip future: {e}")

        # Update global BSE state as well
        SyncStateManager.update_sync_state(
            exchange="BSE",
            scrip_code=None,
            last_checkpoint=now_utc,
            status="SUCCESS"
        )

        return {
            "status": "SUCCESS",
            "companies_polled": len(scrip_companies),
            "matched": total_matched,
            "new_canonical": total_new,
            "merged": total_merged
        }
