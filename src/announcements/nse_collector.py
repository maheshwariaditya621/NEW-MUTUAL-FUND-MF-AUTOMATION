"""
NSE Corporate Announcements Collector.

Polls NSE corporate announcements API for equities, filters against the Master
Office Watchlist, normalizes data, and merges into canonical announcements.
"""

from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import time
import requests

from src.config import logger
from src.announcements.normalizer import parse_announcement_timestamp
from src.announcements.deduplicator import AnnouncementDeduplicator
from src.announcements.sync_state import SyncStateManager

NSE_BASE_URL = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
NSE_API_URL = "https://www.nseindia.com/api/corporate-announcements"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
}


class NSECollector:
    """Collector for NSE Corporate Announcements feed."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.last_primed_at: Optional[float] = None

    def prime_session(self, max_retries: int = 3) -> bool:
        """Prime session cookies by visiting NSE home page."""
        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(NSE_BASE_URL, timeout=10)
                if resp.status_code == 200:
                    self.last_primed_at = time.time()
                    return True
                logger.warning(f"NSE session prime attempt {attempt} returned {resp.status_code}")
            except Exception as e:
                logger.warning(f"NSE session prime attempt {attempt} failed: {e}")
            time.sleep(attempt * 1.5)
        return False

    def fetch_announcements(
        self,
        from_date: datetime,
        to_date: datetime,
        max_retries: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Fetch equity announcements for date range from NSE API.
        Date format: dd-mm-yyyy.
        """
        # Ensure session is primed or re-primed after 10 minutes
        if not self.last_primed_at or (time.time() - self.last_primed_at > 600):
            self.prime_session()

        from_str = from_date.strftime("%d-%m-%Y")
        to_str = to_date.strftime("%d-%m-%Y")
        url = f"{NSE_API_URL}?index=equities&from_date={from_str}&to_date={to_str}"

        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(url, timeout=12)
                if resp.status_code in (401, 403):
                    logger.warning(f"NSE API returned {resp.status_code}, re-priming session...")
                    self.prime_session()
                    time.sleep(attempt * 1.5)
                    continue

                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        if isinstance(data, list):
                            return data
                        elif isinstance(data, dict) and "data" in data:
                            return data["data"]
                        return []
                    except Exception as json_err:
                        logger.warning(f"Failed to parse NSE JSON response (attempt {attempt}): {json_err}")
                else:
                    logger.warning(f"NSE API HTTP error {resp.status_code} (attempt {attempt})")

            except Exception as req_err:
                logger.warning(f"NSE API request error (attempt {attempt}): {req_err}")

            time.sleep(attempt * 1.5)

        return []

    def poll(self, watchlist: List[Dict[str, Any]], recovery: bool = False) -> Dict[str, Any]:
        """
        Poll NSE announcements and ingest matching Master Watchlist records.
        """
        if not watchlist:
            return {"status": "SKIPPED", "reason": "Empty watchlist", "matched": 0}

        now_utc = datetime.now(timezone.utc)
        # Determine query range
        if recovery:
            # Recovery: yesterday to today
            from_dt = now_utc - timedelta(days=2)
        else:
            # Normal: today
            from_dt = now_utc

        to_dt = now_utc

        # Build quick lookups for watchlist
        isin_map = {w["isin"].strip().upper(): w for w in watchlist if w.get("isin")}
        symbol_map = {w["nse_symbol"].strip().upper(): w for w in watchlist if w.get("nse_symbol")}

        announcements = self.fetch_announcements(from_dt, to_dt)
        if not announcements:
            return {"status": "EMPTY", "fetched": 0, "matched": 0}

        matched_count = 0
        new_canonicals = 0
        merged_count = 0
        max_seq_id = None
        max_timestamp = None

        for item in announcements:
            raw_isin = (item.get("sm_isin") or "").strip().upper()
            raw_symbol = (item.get("symbol") or "").strip().upper()

            matched_company = isin_map.get(raw_isin) or symbol_map.get(raw_symbol)
            if not matched_company:
                continue

            matched_count += 1

            # Extract fields
            seq_id_val = item.get("seq_id")
            try:
                seq_id = int(seq_id_val) if seq_id_val is not None else None
            except (ValueError, TypeError):
                seq_id = None

            if seq_id is not None:
                max_seq_id = max(max_seq_id or 0, seq_id)

            # Unique source announcement ID: prefer seq_id or composite
            source_id = str(seq_id) if seq_id else f"{raw_symbol}_{item.get('an_dt', '')}"

            # Timestamp parsing
            raw_dt = item.get("an_dt") or item.get("sort_date")
            dissem_dt = parse_announcement_timestamp(raw_dt) or now_utc
            if max_timestamp is None or dissem_dt > max_timestamp:
                max_timestamp = dissem_dt

            # Attachment URL
            raw_att = item.get("attchmntFile")
            att_url = None
            att_filename = None
            if raw_att:
                raw_att = raw_att.strip()
                if raw_att.startswith("http"):
                    att_url = raw_att
                    att_filename = raw_att.split("/")[-1]
                else:
                    att_url = f"https://nsearchives.nseindia.com/corporate/{raw_att}"
                    att_filename = raw_att

            raw_subject = item.get("desc") or item.get("attchmntText") or "Corporate Announcement"
            raw_details = item.get("attchmntText") if raw_att else None

            can_id, src_id, is_new = AnnouncementDeduplicator.ingest_announcement(
                exchange="NSE",
                source_announcement_id=source_id,
                company_id=matched_company["company_id"],
                isin=matched_company["isin"],
                company_name=matched_company["company_name"],
                exchange_symbol=matched_company.get("nse_symbol") or raw_symbol,
                raw_subject=raw_subject,
                raw_details=raw_details,
                raw_category=item.get("category"),
                raw_subcategory=item.get("subCategory"),
                dissemination_dt=dissem_dt,
                submission_dt=None,
                time_difference=item.get("timeDiff"),
                has_xbrl=bool(item.get("xbrl") or item.get("isXbrl")),
                raw_payload=item,
                attachment_url=att_url,
                attachment_filename=att_filename
            )

            if is_new:
                new_canonicals += 1
            else:
                merged_count += 1

        # Advance checkpoint watermarks only after successful DB processing
        checkpoint_ts = max_timestamp or now_utc
        SyncStateManager.update_sync_state(
            exchange="NSE",
            last_checkpoint=checkpoint_ts,
            last_seq_id=max_seq_id,
            status="SUCCESS"
        )

        return {
            "status": "SUCCESS",
            "fetched": len(announcements),
            "matched": matched_count,
            "new_canonical": new_canonicals,
            "merged": merged_count,
            "last_seq_id": max_seq_id
        }
