"""
NSE Corporate Disclosures Source for Board Meetings & Financial Results.

Queries NSE India corporate board meetings and announcements endpoints with automatic
session cookie priming. Can be enabled/disabled independently via environment or config.
"""

from datetime import datetime, timezone, timedelta, date
from typing import List, Dict, Any, Optional
import os
import requests
import time

from src.config import logger
from src.db.connection import get_connection
from src.earnings.sources.base import BaseEarningsSource, ScheduledEventRecord, ResultsFilingRecord, SourceHealth
from src.earnings.classifier import EarningsClassifier

NSE_BOARD_MEETINGS_URL = "https://www.nseindia.com/api/corporate-board-meetings?index=equities"
NSE_ANNOUNCEMENTS_URL = "https://www.nseindia.com/api/corporate-announcements?index=equities"
NSE_ATTACH_BASE = "https://nsearchives.nseindia.com/corporate/"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}


class NSESource(BaseEarningsSource):
    """NSE exchange provider for board meetings and published results."""

    def __init__(self, is_enabled: Optional[bool] = None):
        # Allow disabling through env var ENABLE_NSE_COLLECTOR=false
        if is_enabled is None:
            env_val = os.getenv("ENABLE_NSE_COLLECTOR", "true").lower()
            self._is_enabled = env_val in ("1", "true", "yes")
        else:
            self._is_enabled = is_enabled

        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self._session_primed = False
        self._health = SourceHealth(
            source_name="NSE",
            is_enabled=self._is_enabled,
            status="IDLE"
        )
        self._company_cache: Optional[Dict[str, Dict[str, Any]]] = None

    @property
    def source_name(self) -> str:
        return "NSE"

    @property
    def is_enabled(self) -> bool:
        return self._is_enabled

    def _prime_session(self) -> bool:
        """Prime NSE session cookies by visiting the main website."""
        try:
            resp = self.session.get("https://www.nseindia.com", timeout=8)
            if resp.status_code == 200 and resp.cookies:
                self._session_primed = True
                return True
        except Exception as e:
            logger.warning(f"NSE session priming warning: {e}")
        return False

    def _get_company_cache(self) -> Dict[str, Dict[str, Any]]:
        """Map exchange_symbol string -> company row."""
        if self._company_cache is not None:
            return self._company_cache

        conn = get_connection()
        cache = {}
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT company_id, isin, exchange_symbol, bse_code, company_name
                    FROM companies
                    WHERE exchange_symbol IS NOT NULL AND TRIM(exchange_symbol) != ''
                    """
                )
                for row in cur.fetchall():
                    cid, isin, sym, bse, name = row
                    sym_clean = str(sym).strip().upper()
                    cache[sym_clean] = {
                        "company_id": cid,
                        "isin": isin,
                        "exchange_symbol": sym_clean,
                        "bse_code": str(bse).strip() if bse else None,
                        "company_name": name
                    }
        except Exception as e:
            logger.error(f"Failed to load company cache for NSE: {e}")
        self._company_cache = cache
        return cache

    def _parse_timestamp(self, ts_str: Optional[str]) -> Optional[datetime]:
        """Parse NSE timestamp string e.g. '14-Oct-2026 19:45:00' into UTC datetime."""
        if not ts_str:
            return None
        cleaned = ts_str.strip()
        # Formats commonly returned by NSE
        formats = [
            "%d-%b-%Y %H:%M:%S",
            "%d-%b-%Y %H:%M",
            "%d-%m-%Y %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ]
        ist_tz = timezone(timedelta(hours=5, minutes=30))
        for fmt in formats:
            try:
                dt = datetime.strptime(cleaned, fmt)
                return dt.replace(tzinfo=ist_tz).astimezone(timezone.utc)
            except ValueError:
                continue
        return None

    def fetch_scheduled_events(self, lookahead_days: int = 30) -> List[ScheduledEventRecord]:
        """Fetch upcoming board meetings for financial results from NSE."""
        if not self._is_enabled:
            return []

        if not self._session_primed:
            self._prime_session()

        now = datetime.now(timezone.utc)
        self._health.last_poll_at = now
        records: List[ScheduledEventRecord] = []
        company_map = self._get_company_cache()

        try:
            resp = self.session.get(NSE_BOARD_MEETINGS_URL, timeout=10)
            if resp.status_code == 401 or resp.status_code == 403:
                # Re-prime session once
                if self._prime_session():
                    resp = self.session.get(NSE_BOARD_MEETINGS_URL, timeout=10)

            if resp.status_code != 200:
                self._health.status = "DEGRADED"
                self._health.last_error = f"NSE HTTP {resp.status_code}"
                return []

            items = resp.json()
            if isinstance(items, dict):
                items = items.get("data", []) or []

            for item in items:
                sym = str(item.get("symbol", "")).strip().upper()
                comp = company_map.get(sym)
                if not comp:
                    continue

                purpose = item.get("purpose", "") or ""
                if not EarningsClassifier.is_results_meeting(purpose):
                    continue

                # Parse meeting date e.g. "17-Oct-2026"
                m_date_str = item.get("meetingDate") or ""
                meeting_date = EarningsClassifier.extract_meeting_date_from_text(m_date_str)
                if not meeting_date:
                    continue

                fy, period = EarningsClassifier.extract_fiscal_period(purpose)
                status = EarningsClassifier.detect_meeting_status(purpose)
                event_id = f"NSE_{sym}_{meeting_date.isoformat()}"

                rec = ScheduledEventRecord(
                    company_id=comp["company_id"],
                    isin=comp["isin"],
                    exchange_symbol=sym,
                    bse_code=comp["bse_code"],
                    meeting_date=meeting_date,
                    source_name="NSE",
                    source_record_id=event_id,
                    financial_year=fy,
                    fiscal_period=period,
                    raw_purpose=purpose,
                    normalized_event_type="BOARD_MEETING_RESULTS",
                    event_status=status,
                    source_url="https://www.nseindia.com/companies-listing/corporate-filings-board-meetings",
                    verification_status="VERIFIED_EXCHANGE",
                    raw_payload=item
                )
                records.append(rec)

            self._health.status = "HEALTHY"
            self._health.last_success_at = now
            self._health.total_events_fetched += len(records)
            self._health.last_error = None
            return records

        except Exception as e:
            logger.warning(f"NSE board meetings fetch warning: {e}")
            self._health.status = "DEGRADED"
            self._health.last_error = str(e)
            return []

    def fetch_results_filings(self, lookback_days: int = 3) -> List[ResultsFilingRecord]:
        """Fetch published financial results filings from NSE."""
        if not self._is_enabled:
            return []

        if not self._session_primed:
            self._prime_session()

        now = datetime.now(timezone.utc)
        self._health.last_poll_at = now
        filings: List[ResultsFilingRecord] = []
        company_map = self._get_company_cache()

        try:
            resp = self.session.get(NSE_ANNOUNCEMENTS_URL, timeout=10)
            if resp.status_code == 401 or resp.status_code == 403:
                if self._prime_session():
                    resp = self.session.get(NSE_ANNOUNCEMENTS_URL, timeout=10)

            if resp.status_code != 200:
                self._health.status = "DEGRADED"
                self._health.last_error = f"NSE HTTP {resp.status_code}"
                return []

            items = resp.json()
            if isinstance(items, dict):
                items = items.get("data", []) or []

            for item in items:
                sym = str(item.get("symbol", "")).strip().upper()
                comp = company_map.get(sym)
                if not comp:
                    continue

                desc = item.get("desc", "") or ""
                att = item.get("attmntFile", "") or ""
                sub = item.get("an_dt", "") or ""

                # Check if filing is financial results
                cat = item.get("category", "") or ""
                combined = f"{desc} {cat}".strip()
                if not ("financial" in combined.lower() or "result" in combined.lower()):
                    continue

                fy, period = EarningsClassifier.extract_fiscal_period(combined)
                res_type = EarningsClassifier.classify_result_type(combined)
                audit_status = EarningsClassifier.classify_audit_status(combined)
                is_rev, rev_type = EarningsClassifier.detect_filing_revision(desc)

                dissem_dt = self._parse_timestamp(item.get("bcast_dt") or item.get("an_dt"))
                subm_dt = self._parse_timestamp(item.get("sub_dt"))

                filing_id = str(item.get("seq_id") or f"NSE_{sym}_{int(time.time())}")
                attach_url = f"{NSE_ATTACH_BASE}{att}" if att else None

                filing = ResultsFilingRecord(
                    company_id=comp["company_id"],
                    exchange="NSE",
                    source_filing_id=filing_id,
                    filing_title=desc or f"{sym} Financial Results",
                    dissemination_timestamp=dissem_dt,
                    submission_timestamp=subm_dt,
                    financial_year=fy,
                    fiscal_period=period,
                    result_type=res_type,
                    audit_status=audit_status,
                    filing_url=f"https://www.nseindia.com/companies-listing/corporate-filings-financial-results",
                    attachment_url=attach_url,
                    attachment_filename=att,
                    is_revision=is_rev,
                    revision_type=rev_type,
                    verification_status="VERIFIED_EXCHANGE",
                    raw_metadata=item
                )
                filings.append(filing)

            self._health.status = "HEALTHY"
            self._health.last_success_at = now
            self._health.total_filings_fetched += len(filings)
            self._health.last_error = None
            return filings

        except Exception as e:
            logger.warning(f"NSE results filings warning: {e}")
            self._health.status = "DEGRADED"
            self._health.last_error = str(e)
            return []

    def get_health(self) -> SourceHealth:
        return self._health
