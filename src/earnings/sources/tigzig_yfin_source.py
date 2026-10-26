"""
TigZig YFIN Supplementary Earnings Calendar & Consensus Estimates Source.

Queries https://yfin-h.tigzig.com/v1/get-calendar/ in rate-paced batches of up to 10 tickers.
Provides forward scheduled earnings dates and analyst consensus EPS and revenue estimates.
"""

from datetime import datetime, timezone, timedelta, date
from typing import List, Dict, Any, Optional
import requests
import time

from src.config import logger
from src.db.connection import get_connection
from src.earnings.sources.base import BaseEarningsSource, ScheduledEventRecord, ResultsFilingRecord, SourceHealth

TIGZIG_CALENDAR_URL = "https://yfin-h.tigzig.com/v1/get-calendar/"
BATCH_SIZE = 10
REQUEST_TIMEOUT = 6
REQUEST_PAUSE_SECONDS = 0.5


class TigZigYfinSource(BaseEarningsSource):
    """Supplementary earnings calendar and analyst consensus estimate provider."""

    def __init__(self, is_enabled: bool = True):
        self._is_enabled = is_enabled
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "AV-Fincorp-Earnings-Engine/1.0",
            "Accept": "application/json"
        })
        self._health = SourceHealth(
            source_name="TIGZIG_YFIN",
            is_enabled=is_enabled,
            status="IDLE"
        )
        self._ticker_map: Optional[Dict[str, Dict[str, Any]]] = None

    @property
    def source_name(self) -> str:
        return "TIGZIG"

    @property
    def is_enabled(self) -> bool:
        return self._is_enabled

    def _get_ticker_map(self) -> Dict[str, Dict[str, Any]]:
        """Map 'SYMBOL.NS' -> company row."""
        if self._ticker_map is not None:
            return self._ticker_map

        conn = get_connection()
        t_map = {}
        try:
            with conn.cursor() as cur:
                # Prioritize companies with active watchlist or scheduled meetings first
                cur.execute(
                    """
                    SELECT c.company_id, c.isin, c.exchange_symbol, c.bse_code, c.company_name
                    FROM companies c
                    WHERE c.exchange_symbol IS NOT NULL AND TRIM(c.exchange_symbol) != ''
                    ORDER BY 
                        CASE WHEN EXISTS (SELECT 1 FROM master_watchlist mw WHERE mw.company_id = c.company_id AND mw.is_active = TRUE) THEN 0
                             WHEN EXISTS (SELECT 1 FROM earnings_calendar ec WHERE ec.company_id = c.company_id) THEN 1
                             ELSE 2 END,
                        c.market_cap DESC NULLS LAST
                    """
                )
                for row in cur.fetchall():
                    cid, isin, sym, bse, name = row
                    clean_sym = str(sym).strip().upper()
                    ns_ticker = f"{clean_sym}.NS"
                    t_map[ns_ticker] = {
                        "company_id": cid,
                        "isin": isin,
                        "exchange_symbol": clean_sym,
                        "bse_code": str(bse).strip() if bse else None,
                        "company_name": name
                    }
        except Exception as e:
            logger.error(f"Failed to load ticker map for TigZig: {e}")
        self._ticker_map = t_map
        return t_map

    def fetch_scheduled_events(self, lookahead_days: int = 30, max_tickers: int = 60) -> List[ScheduledEventRecord]:
        """Fetch consensus earnings calendar for high-priority symbols."""
        if not self._is_enabled:
            return []

        ticker_map = self._get_ticker_map()
        tickers = list(ticker_map.keys())[:max_tickers]
        if not tickers:
            return []

        now = datetime.now(timezone.utc)
        self._health.last_poll_at = now
        records: List[ScheduledEventRecord] = []

        chunks = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
        logger.info(f"Polling TigZig YFIN calendar for {len(tickers)} tickers across {len(chunks)} batches...")

        success_count = 0
        error_count = 0

        for chunk in chunks:
            param_str = ",".join(chunk)
            url = f"{TIGZIG_CALENDAR_URL}?tickers={param_str}"

            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    data = resp.json()
                    success_count += 1
                    if isinstance(data, dict):
                        for ticker, info in data.items():
                            if not isinstance(info, dict):
                                continue

                            dates_list = info.get("earnings_dates") or []
                            if not dates_list:
                                continue

                            first_date_str = dates_list[0]
                            try:
                                scheduled_date = date.fromisoformat(first_date_str)
                            except ValueError:
                                continue

                            # Skip dates far in past
                            if scheduled_date < (now.date() - timedelta(days=7)):
                                continue

                            comp = ticker_map.get(ticker)
                            if not comp:
                                continue

                            eps_est = info.get("earnings_estimate") or {}
                            rev_est = info.get("revenue_estimate") or {}

                            rec = ScheduledEventRecord(
                                company_id=comp["company_id"],
                                isin=comp["isin"],
                                exchange_symbol=comp["exchange_symbol"],
                                bse_code=comp["bse_code"],
                                meeting_date=scheduled_date,
                                expected_results_date=scheduled_date,
                                source_name="TIGZIG",
                                source_record_id=f"TIGZIG_{ticker}_{first_date_str}",
                                raw_purpose=f"Consensus estimated earnings date: {first_date_str}",
                                normalized_event_type="RESULTS_EXPECTED",
                                event_status="SCHEDULED",
                                source_url="https://yfin-h.tigzig.com/v1/get-calendar/",
                                verification_status="THIRD_PARTY_CALENDAR",
                                consensus_eps=eps_est.get("average"),
                                consensus_eps_high=eps_est.get("high"),
                                consensus_eps_low=eps_est.get("low"),
                                consensus_revenue_inr=rev_est.get("average"),
                                raw_payload=info
                            )
                            records.append(rec)
                else:
                    error_count += 1
                    logger.warning(f"TigZig HTTP {resp.status_code}")
            except Exception as e:
                error_count += 1
                logger.warning(f"Error querying TigZig chunk: {e}")

            time.sleep(REQUEST_PAUSE_SECONDS)

        if error_count > 0 and success_count == 0:
            self._health.status = "DEGRADED"
            self._health.last_error = f"{error_count} batches timed out"
        else:
            self._health.status = "HEALTHY"
            self._health.last_success_at = now
            self._health.total_events_fetched += len(records)
            self._health.last_error = None

        return records

    def fetch_results_filings(self, lookback_days: int = 3) -> List[ResultsFilingRecord]:
        return []

    def get_health(self) -> SourceHealth:
        return self._health
