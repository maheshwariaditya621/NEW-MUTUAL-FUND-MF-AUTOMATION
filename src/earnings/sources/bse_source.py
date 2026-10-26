"""
BSE Corporate Disclosures Source for Board Meetings & Financial Results.
"""

from datetime import datetime, timezone, timedelta, date
from typing import List, Dict, Any, Optional
import requests
import time

from src.config import logger
from src.db.connection import get_connection
from src.earnings.sources.base import BaseEarningsSource, ScheduledEventRecord, ResultsFilingRecord, SourceHealth
from src.earnings.classifier import EarningsClassifier

BSE_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
BSE_ATTACH_BASE = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}


class BSESource(BaseEarningsSource):
    """BSE exchange provider for board meetings and published results."""

    def __init__(self, is_enabled: bool = True):
        self._is_enabled = is_enabled
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self._health = SourceHealth(
            source_name="BSE",
            is_enabled=is_enabled,
            status="IDLE"
        )
        self._company_cache: Optional[Dict[str, Dict[str, Any]]] = None

    @property
    def source_name(self) -> str:
        return "BSE"

    @property
    def is_enabled(self) -> bool:
        return self._is_enabled

    def _get_company_cache(self) -> Dict[str, Dict[str, Any]]:
        """Map bse_code string -> company row."""
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
                    WHERE bse_code IS NOT NULL AND TRIM(bse_code) != ''
                    """
                )
                for row in cur.fetchall():
                    cid, isin, sym, bse, name = row
                    bse_clean = str(bse).strip()
                    cache[bse_clean] = {
                        "company_id": cid,
                        "isin": isin,
                        "exchange_symbol": sym,
                        "bse_code": bse_clean,
                        "company_name": name
                    }
        except Exception as e:
            logger.error(f"Failed to load company cache for BSE: {e}")
        self._company_cache = cache
        return cache

    def _parse_timestamp(self, ts_str: Optional[str]) -> Optional[datetime]:
        """Parse BSE ISO timestamp string e.g. '2026-10-14T20:32:24.58' into UTC datetime."""
        if not ts_str:
            return None
        try:
            cleaned = ts_str.strip()
            # If no timezone, interpret as Asia/Kolkata (UTC+5:30) per SEBI reporting rules
            dt = datetime.fromisoformat(cleaned)
            if dt.tzinfo is None:
                ist_tz = timezone(timedelta(hours=5, minutes=30))
                dt = dt.replace(tzinfo=ist_tz).astimezone(timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            return None

    def fetch_scheduled_events(self, lookback_days: int = 25) -> List[ScheduledEventRecord]:
        """Fetch scheduled board meetings for results from BSE."""
        if not self._is_enabled:
            return []

        now = datetime.now(timezone.utc)
        from_dt = now - timedelta(days=lookback_days)
        to_dt = now

        prev_str = from_dt.strftime("%Y%m%d")
        to_str = to_dt.strftime("%Y%m%d")

        self._health.last_poll_at = now
        records: List[ScheduledEventRecord] = []
        company_map = self._get_company_cache()

        for page_num in range(1, 35):  # Check up to 34 pages (1,700 intimations)
            url = f"{BSE_API_URL}?pageno={page_num}&strCat=Board+Meeting&strPrevDate={prev_str}&strScrip=&strSearch=P&strToDate={to_str}&strType=C&subcategory="
            table = []
            for attempt in range(2):
                try:
                    resp = self.session.get(url, timeout=12)
                    if resp.status_code == 200:
                        data = resp.json()
                        table = data.get("Table", [])
                        break
                except Exception as ex:
                    if attempt == 1:
                        logger.warning(f"BSE Board Meetings page {page_num} request failed: {ex}")
                    time.sleep(0.5)

            if not table:
                # Reached end of announcements
                break

            for item in table:
                scrip = str(item.get("SCRIP_CD", "")).strip()
                comp = company_map.get(scrip)
                if not comp:
                    continue

                sub = item.get("NEWSSUB", "") or ""
                headline = item.get("HEADLINE", "") or ""
                combined_text = f"{headline} {sub}".strip()

                # Filter: check if board meeting considers financial results
                if not EarningsClassifier.is_results_meeting(combined_text):
                    continue

                # Extract scheduled meeting date
                # Try from text first
                meeting_date = EarningsClassifier.extract_meeting_date_from_text(combined_text)
                if not meeting_date:
                    # Fallback to news date
                    news_dt = self._parse_timestamp(item.get("NEWS_DT"))
                    meeting_date = news_dt.date() if news_dt else now.date()

                status = EarningsClassifier.detect_meeting_status(headline, sub)
                fy, period = EarningsClassifier.extract_fiscal_period(combined_text)

                news_id = str(item.get("NEWSID") or item.get("BSENewsid") or f"{scrip}_{meeting_date}")
                attach_name = item.get("ATTACHMENTNAME")
                attach_url = f"{BSE_ATTACH_BASE}{attach_name}" if attach_name else None

                rec = ScheduledEventRecord(
                    company_id=comp["company_id"],
                    isin=comp["isin"],
                    exchange_symbol=comp["exchange_symbol"],
                    bse_code=scrip,
                    meeting_date=meeting_date,
                    source_name="BSE",
                    source_record_id=news_id,
                    financial_year=fy,
                    fiscal_period=period,
                    raw_purpose=combined_text,
                    normalized_event_type="BOARD_MEETING_RESULTS",
                    event_status=status,
                    source_url=attach_url or f"https://www.bseindia.com/corporates/anndet_new.aspx?newsid={news_id}",
                    verification_status="VERIFIED_EXCHANGE",
                    raw_payload=item
                )
                records.append(rec)

            time.sleep(0.15)  # Pace queries to protect connection

        self._health.status = "HEALTHY"
        self._health.last_success_at = now
        self._health.total_events_fetched += len(records)
        self._health.last_error = None
        return records

    def fetch_results_filings(self, lookback_days: int = 7) -> List[ResultsFilingRecord]:
        """Fetch actual published results filings from BSE."""
        if not self._is_enabled:
            return []

        now = datetime.now(timezone.utc)
        from_dt = now - timedelta(days=lookback_days)
        to_dt = now

        prev_str = from_dt.strftime("%Y%m%d")
        to_str = to_dt.strftime("%Y%m%d")

        self._health.last_poll_at = now
        filings: List[ResultsFilingRecord] = []
        company_map = self._get_company_cache()

        for page_num in range(1, 25):  # Check up to 24 pages (1,200 filings)
            url = f"{BSE_API_URL}?pageno={page_num}&strCat=Result&strPrevDate={prev_str}&strScrip=&strSearch=P&strToDate={to_str}&strType=C&subcategory="
            table = []
            for attempt in range(2):
                try:
                    resp = self.session.get(url, timeout=12)
                    if resp.status_code == 200:
                        data = resp.json()
                        table = data.get("Table", [])
                        break
                except Exception as ex:
                    if attempt == 1:
                        logger.warning(f"BSE Results page {page_num} request failed: {ex}")
                    time.sleep(0.5)

            if not table:
                break

            for item in table:
                scrip = str(item.get("SCRIP_CD", "")).strip()
                comp = company_map.get(scrip)
                if not comp:
                    continue

                sub = item.get("NEWSSUB", "") or ""
                headline = item.get("HEADLINE", "") or ""
                title = f"{headline} - {sub}".strip(" -")

                fy, period = EarningsClassifier.extract_fiscal_period(title)
                res_type = EarningsClassifier.classify_result_type(title)
                audit_status = EarningsClassifier.classify_audit_status(title)
                is_rev, rev_type = EarningsClassifier.detect_filing_revision(headline, sub)

                dissem_dt = self._parse_timestamp(item.get("DissemDT") or item.get("NEWS_DT"))
                subm_dt = self._parse_timestamp(item.get("News_submission_dt"))

                news_id = str(item.get("NEWSID") or item.get("BSENewsid") or f"BSE_RES_{scrip}_{int(time.time())}")
                attach_name = item.get("ATTACHMENTNAME")
                attach_url = f"{BSE_ATTACH_BASE}{attach_name}" if attach_name else None

                filing = ResultsFilingRecord(
                    company_id=comp["company_id"],
                    exchange="BSE",
                    source_filing_id=news_id,
                    filing_title=title,
                    dissemination_timestamp=dissem_dt,
                    submission_timestamp=subm_dt,
                    financial_year=fy,
                    fiscal_period=period,
                    result_type=res_type,
                    audit_status=audit_status,
                    filing_url=f"https://www.bseindia.com/corporates/anndet_new.aspx?newsid={news_id}",
                    attachment_url=attach_url,
                    attachment_filename=attach_name,
                    is_revision=is_rev,
                    revision_type=rev_type,
                    verification_status="VERIFIED_EXCHANGE",
                    raw_metadata=item
                )
                filings.append(filing)

            time.sleep(0.15)

        self._health.status = "HEALTHY"
        self._health.last_success_at = now
        self._health.total_filings_fetched += len(filings)
        self._health.last_error = None
        return filings

    def get_health(self) -> SourceHealth:
        return self._health
