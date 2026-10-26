"""
Master Ingestion & Synchronization Engine for Indian Company Earnings Calendar.

Coordinates multi-source data ingestion (BSE, NSE, TigZig), idempotent database storage,
historical change tracking, schedule reconciliation, health monitoring, and alerts.
"""

from datetime import datetime, timezone, timedelta, date
from typing import Dict, Any, List, Optional
import os
import sys
import time
import psycopg2.extras

from src.config import logger
from src.db.connection import get_connection
from src.earnings.sources.base import ScheduledEventRecord, ResultsFilingRecord
from src.earnings.sources.bse_source import BSESource
from src.earnings.sources.nse_source import NSESource
from src.earnings.sources.tigzig_yfin_source import TigZigYfinSource
from src.earnings.reconciliation import EarningsReconciliationEngine
from src.alerts.telegram_client import TelegramClient


class EarningsEngine:
    """Master engine orchestrating earnings data ingestion, storage, and reconciliation."""

    def __init__(self):
        self.bse = BSESource(is_enabled=True)
        self.nse = NSESource()  # Respects ENABLE_NSE_COLLECTOR env var
        self.tigzig = TigZigYfinSource(is_enabled=True)
        self.telegram = TelegramClient()
        self.enable_alerts = os.getenv("ENABLE_EARNINGS_TELEGRAM_ALERTS", "false").lower() in ("1", "true", "yes")

    def _update_sync_state(
        self,
        source_key: str,
        status: str,
        events_count: int = 0,
        results_count: int = 0,
        error: Optional[str] = None
    ):
        """Update operational sync state table."""
        conn = get_connection()
        now = datetime.now(timezone.utc)
        try:
            with conn.cursor() as cur:
                if status == "SUCCESS":
                    cur.execute(
                        """
                        INSERT INTO earnings_sync_state (
                            source_key, last_attempted_at, last_successful_at, status,
                            events_count, results_count, last_error, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, NULL, %s)
                        ON CONFLICT (source_key) DO UPDATE SET
                            last_attempted_at = EXCLUDED.last_attempted_at,
                            last_successful_at = EXCLUDED.last_successful_at,
                            status = EXCLUDED.status,
                            events_count = earnings_sync_state.events_count + EXCLUDED.events_count,
                            results_count = earnings_sync_state.results_count + EXCLUDED.results_count,
                            last_error = NULL,
                            updated_at = EXCLUDED.updated_at
                        """,
                        (source_key, now, now, status, events_count, results_count, now)
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO earnings_sync_state (
                            source_key, last_attempted_at, status, last_error, updated_at
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (source_key) DO UPDATE SET
                            last_attempted_at = EXCLUDED.last_attempted_at,
                            status = EXCLUDED.status,
                            last_error = EXCLUDED.last_error,
                            updated_at = EXCLUDED.updated_at
                        """,
                        (source_key, now, status, error, now)
                    )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update sync state for {source_key}: {e}")

    def upsert_scheduled_events(self, events: List[ScheduledEventRecord]) -> Dict[str, int]:
        """Idempotently upsert scheduled events, logging changes to history table."""
        if not events:
            return {"inserted": 0, "updated": 0, "unchanged": 0}

        conn = get_connection()
        inserted = 0
        updated = 0
        unchanged = 0

        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                for ev in events:
                    # Check if matching event already exists
                    cur.execute(
                        """
                        SELECT event_id, meeting_date, event_status, consensus_eps, consensus_revenue_inr
                        FROM earnings_calendar
                        WHERE company_id = %s AND source_name = %s AND meeting_date = %s AND source_record_id = %s
                        """,
                        (ev.company_id, ev.source_name, ev.meeting_date, ev.source_record_id)
                    )
                    existing = cur.fetchone()

                    if existing:
                        # Event exists: check if date or estimates updated
                        ev_id = existing["event_id"]
                        old_date = existing["meeting_date"]
                        old_status = existing["event_status"]

                        # Update estimates if newly supplied
                        need_update = False
                        if ev.consensus_eps and not existing["consensus_eps"]:
                            need_update = True
                        if ev.event_status != old_status:
                            need_update = True

                        if need_update:
                            cur.execute(
                                """
                                UPDATE earnings_calendar
                                SET event_status = %s,
                                    consensus_eps = COALESCE(%s, consensus_eps),
                                    consensus_eps_high = COALESCE(%s, consensus_eps_high),
                                    consensus_eps_low = COALESCE(%s, consensus_eps_low),
                                    consensus_revenue_inr = COALESCE(%s, consensus_revenue_inr),
                                    last_verified_at = CURRENT_TIMESTAMP,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE event_id = %s
                                """,
                                (
                                    ev.event_status,
                                    ev.consensus_eps,
                                    ev.consensus_eps_high,
                                    ev.consensus_eps_low,
                                    ev.consensus_revenue_inr,
                                    ev_id
                                )
                            )
                            updated += 1
                        else:
                            # Update last_verified_at
                            cur.execute(
                                "UPDATE earnings_calendar SET last_verified_at = CURRENT_TIMESTAMP WHERE event_id = %s",
                                (ev_id,)
                            )
                            unchanged += 1
                    else:
                        # Insert new event
                        cur.execute(
                            """
                            INSERT INTO earnings_calendar (
                                company_id, isin, exchange_symbol, bse_code,
                                financial_year, fiscal_period, meeting_date, meeting_time,
                                expected_results_date, raw_purpose, normalized_event_type,
                                event_status, source_url, source_record_id, source_name,
                                verification_status, consensus_eps, consensus_eps_high,
                                consensus_eps_low, consensus_revenue_inr
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON CONFLICT (company_id, source_name, meeting_date, source_record_id) DO UPDATE SET
                                last_verified_at = CURRENT_TIMESTAMP,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            (
                                ev.company_id, ev.isin, ev.exchange_symbol, ev.bse_code,
                                ev.financial_year, ev.fiscal_period, ev.meeting_date, ev.meeting_time,
                                ev.expected_results_date, ev.raw_purpose, ev.normalized_event_type,
                                ev.event_status, ev.source_url, ev.source_record_id, ev.source_name,
                                ev.verification_status, ev.consensus_eps, ev.consensus_eps_high,
                                ev.consensus_eps_low, ev.consensus_revenue_inr
                            )
                        )
                        inserted += 1

            conn.commit()
            return {"inserted": inserted, "updated": updated, "unchanged": unchanged}
        except Exception as e:
            conn.rollback()
            logger.error(f"Error upserting scheduled events: {e}")
            return {"inserted": inserted, "updated": updated, "unchanged": unchanged, "error": str(e)}

    def upsert_results_filings(self, filings: List[ResultsFilingRecord]) -> Dict[str, int]:
        """Idempotently upsert results filings and dispatch optional alerts for new filings."""
        if not filings:
            return {"inserted": 0, "existing": 0}

        conn = get_connection()
        inserted = 0
        existing = 0
        new_filings_for_alert = []

        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                for f in filings:
                    cur.execute(
                        """
                        SELECT filing_id FROM earnings_results_filings
                        WHERE exchange = %s AND source_filing_id = %s
                        """,
                        (f.exchange, f.source_filing_id)
                    )
                    row = cur.fetchone()

                    if row:
                        existing += 1
                    else:
                        cur.execute(
                            """
                            INSERT INTO earnings_results_filings (
                                company_id, exchange, source_filing_id, financial_year,
                                fiscal_period, result_type, audit_status,
                                submission_timestamp, dissemination_timestamp,
                                filing_title, filing_url, attachment_url,
                                attachment_filename, is_revision, revision_type,
                                verification_status, raw_metadata
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON CONFLICT (exchange, source_filing_id) DO NOTHING
                            RETURNING filing_id
                            """,
                            (
                                f.company_id, f.exchange, f.source_filing_id, f.financial_year,
                                f.fiscal_period, f.result_type, f.audit_status,
                                f.submission_timestamp, f.dissemination_timestamp,
                                f.filing_title, f.filing_url, f.attachment_url,
                                f.attachment_filename, f.is_revision, f.revision_type,
                                f.verification_status,
                                psycopg2.extras.Json(f.raw_metadata) if f.raw_metadata else None
                            )
                        )
                        res = cur.fetchone()
                        if res:
                            inserted += 1
                            new_filings_for_alert.append(f)

            conn.commit()

            # Optional Telegram alerting
            if self.enable_alerts and new_filings_for_alert:
                for nf in new_filings_for_alert[:5]:  # Throttle to max 5 alerts per batch
                    self._send_results_alert(nf)

            return {"inserted": inserted, "existing": existing}
        except Exception as e:
            conn.rollback()
            logger.error(f"Error upserting results filings: {e}")
            return {"inserted": inserted, "existing": existing, "error": str(e)}

    def _send_results_alert(self, filing: ResultsFilingRecord):
        """Send formatted alert on newly detected financial results filing."""
        try:
            dissem_str = filing.dissemination_timestamp.strftime("%d-%b-%Y %H:%M:%S UTC") if filing.dissemination_timestamp else "N/A"
            text = (
                f"📊 <b>New Financial Results Disseminated</b>\n\n"
                f"<b>Exchange:</b> {filing.exchange}\n"
                f"<b>Title:</b> {filing.filing_title[:200]}\n"
                f"<b>Period:</b> {filing.financial_year or ''} {filing.fiscal_period or ''}\n"
                f"<b>Type:</b> {filing.result_type} ({filing.audit_status})\n"
                f"<b>Disseminated:</b> {dissem_str}\n"
            )
            if filing.attachment_url:
                text += f"<a href='{filing.attachment_url}'>View PDF Document</a>\n"
            self.telegram.send_message(text)
        except Exception as e:
            logger.warning(f"Error dispatching results Telegram alert: {e}")

    def run_cycle(self) -> Dict[str, Any]:
        """Execute a full ingestion, synchronization, and reconciliation cycle."""
        cycle_start = time.time()
        logger.info("=== Starting Earnings Calendar & Results Sync Cycle ===")

        summary: Dict[str, Any] = {
            "bse_events": {},
            "bse_filings": {},
            "nse_events": {},
            "nse_filings": {},
            "tigzig_events": {},
            "reconciliation": 0,
            "duration_seconds": 0.0,
            "status": "SUCCESS"
        }

        # 1. BSE Ingestion
        try:
            bse_events = self.bse.fetch_scheduled_events()
            summary["bse_events"] = self.upsert_scheduled_events(bse_events)
            self._update_sync_state("BSE_BOARD_MEETINGS", "SUCCESS", events_count=len(bse_events))
        except Exception as e:
            logger.error(f"BSE events sync error: {e}")
            summary["bse_events"] = {"error": str(e)}
            self._update_sync_state("BSE_BOARD_MEETINGS", "ERROR", error=str(e))

        try:
            bse_filings = self.bse.fetch_results_filings(lookback_days=3)
            summary["bse_filings"] = self.upsert_results_filings(bse_filings)
            self._update_sync_state("BSE_RESULTS_FEED", "SUCCESS", results_count=len(bse_filings))
        except Exception as e:
            logger.error(f"BSE filings sync error: {e}")
            summary["bse_filings"] = {"error": str(e)}
            self._update_sync_state("BSE_RESULTS_FEED", "ERROR", error=str(e))

        # 2. NSE Ingestion (if enabled)
        if self.nse.is_enabled:
            try:
                nse_events = self.nse.fetch_scheduled_events()
                summary["nse_events"] = self.upsert_scheduled_events(nse_events)
                self._update_sync_state("NSE_BOARD_MEETINGS", "SUCCESS", events_count=len(nse_events))
            except Exception as e:
                logger.warning(f"NSE events sync warning: {e}")
                summary["nse_events"] = {"error": str(e)}
                self._update_sync_state("NSE_BOARD_MEETINGS", "DEGRADED", error=str(e))

            try:
                nse_filings = self.nse.fetch_results_filings(lookback_days=3)
                summary["nse_filings"] = self.upsert_results_filings(nse_filings)
                self._update_sync_state("NSE_RESULTS_FEED", "SUCCESS", results_count=len(nse_filings))
            except Exception as e:
                logger.warning(f"NSE filings sync warning: {e}")
                summary["nse_filings"] = {"error": str(e)}
                self._update_sync_state("NSE_RESULTS_FEED", "DEGRADED", error=str(e))
        else:
            summary["nse_events"] = {"status": "DISABLED"}
            summary["nse_filings"] = {"status": "DISABLED"}

        # 3. TigZig Consensus Estimates & Calendar
        try:
            tz_events = self.tigzig.fetch_scheduled_events()
            summary["tigzig_events"] = self.upsert_scheduled_events(tz_events)
            self._update_sync_state("TIGZIG_CONSENSUS", "SUCCESS", events_count=len(tz_events))
        except Exception as e:
            logger.error(f"TigZig sync error: {e}")
            summary["tigzig_events"] = {"error": str(e)}
            self._update_sync_state("TIGZIG_CONSENSUS", "ERROR", error=str(e))

        # 4. Reconciliation
        try:
            linked = EarningsReconciliationEngine.reconcile_filings_with_events()
            summary["reconciliation"] = linked
            self._update_sync_state("RECONCILIATION_ENGINE", "SUCCESS", results_count=linked)
        except Exception as e:
            logger.error(f"Reconciliation error: {e}")
            self._update_sync_state("RECONCILIATION_ENGINE", "ERROR", error=str(e))

        duration = round(time.time() - cycle_start, 2)
        summary["duration_seconds"] = duration
        logger.info(f"=== Earnings Sync Cycle Completed in {duration}s ===")
        return summary


def run():
    """Command-line entrypoint for earnings ingestion."""
    engine = EarningsEngine()
    res = engine.run_cycle()
    print("Execution Result:", res)


if __name__ == "__main__":
    run()
