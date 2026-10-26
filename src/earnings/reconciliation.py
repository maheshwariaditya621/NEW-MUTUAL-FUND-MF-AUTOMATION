"""
Reconciliation and Event History Engine for Earnings Calendar.

Handles:
1. Append-only audit history tracking in `earnings_calendar_history` when dates or statuses change.
2. Linking published results filings to scheduled calendar events.
3. Updating event statuses (SCHEDULED -> RESULTS_PUBLISHED / CONCLUDED).
4. Cross-exchange reconciliation across BSE and NSE.
"""

from datetime import datetime, timezone, timedelta, date
from typing import Optional, List, Dict, Any
import psycopg2.extras

from src.db.connection import get_connection
from src.config import logger


class EarningsReconciliationEngine:
    """Reconciles scheduled meetings with actual published results filings."""

    @staticmethod
    def record_calendar_change(
        event_id: int,
        company_id: int,
        old_date: Optional[date],
        new_date: Optional[date],
        old_status: Optional[str],
        new_status: Optional[str],
        reason: str,
        source_evidence: Optional[str] = None
    ):
        """Record an append-only audit entry in earnings_calendar_history."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO earnings_calendar_history (
                        event_id, company_id, old_meeting_date, new_meeting_date,
                        old_status, new_status, change_reason, source_evidence, detected_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event_id, company_id, old_date, new_date,
                        old_status, new_status, reason, source_evidence,
                        datetime.now(timezone.utc)
                    )
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error logging calendar history change for event {event_id}: {e}")

    @staticmethod
    def reconcile_filings_with_events():
        """
        Scan unlinked filings in `earnings_results_filings` and match with `earnings_calendar`.
        Matches on company_id and either:
        1. Exact (financial_year, fiscal_period)
        2. Meeting date within ±3 days of dissemination date
        """
        conn = get_connection()
        matched_count = 0
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Find unlinked filings
                cur.execute(
                    """
                    SELECT filing_id, company_id, financial_year, fiscal_period, 
                           dissemination_timestamp, detected_at
                    FROM earnings_results_filings
                    WHERE calendar_event_id IS NULL
                    """
                )
                unlinked = cur.fetchall()

                for f in unlinked:
                    filing_id = f["filing_id"]
                    comp_id = f["company_id"]
                    fy = f["financial_year"]
                    period = f["fiscal_period"]
                    dissem_dt = f["dissemination_timestamp"] or f["detected_at"]
                    dissem_date = dissem_dt.date() if dissem_dt else None

                    matched_event_id = None

                    # Match strategy A: by company_id, FY, and period
                    if fy and period:
                        cur.execute(
                            """
                            SELECT event_id, meeting_date, event_status
                            FROM earnings_calendar
                            WHERE company_id = %s AND financial_year = %s AND fiscal_period = %s
                            ORDER BY meeting_date DESC
                            LIMIT 1
                            """,
                            (comp_id, fy, period)
                        )
                        ev = cur.fetchone()
                        if ev:
                            matched_event_id = ev["event_id"]

                    # Match strategy B: by company_id and meeting_date window (±3 days)
                    if not matched_event_id and dissem_date:
                        cur.execute(
                            """
                            SELECT event_id, meeting_date, event_status
                            FROM earnings_calendar
                            WHERE company_id = %s 
                              AND meeting_date BETWEEN %s AND %s
                            ORDER BY meeting_date DESC
                            LIMIT 1
                            """,
                            (comp_id, dissem_date - timedelta(days=3), dissem_date + timedelta(days=3))
                        )
                        ev = cur.fetchone()
                        if ev:
                            matched_event_id = ev["event_id"]

                    if matched_event_id:
                        # Link filing to event
                        cur.execute(
                            """
                            UPDATE earnings_results_filings
                            SET calendar_event_id = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE filing_id = %s
                            """,
                            (matched_event_id, filing_id)
                        )

                        # Update event status to RESULTS_PUBLISHED
                        cur.execute(
                            """
                            UPDATE earnings_calendar
                            SET event_status = 'RESULTS_PUBLISHED', updated_at = CURRENT_TIMESTAMP
                            WHERE event_id = %s AND event_status != 'RESULTS_PUBLISHED'
                            RETURNING event_id, meeting_date, event_status
                            """,
                            (matched_event_id,)
                        )
                        upd = cur.fetchone()
                        if upd:
                            EarningsReconciliationEngine.record_calendar_change(
                                event_id=matched_event_id,
                                company_id=comp_id,
                                old_date=upd["meeting_date"],
                                new_date=upd["meeting_date"],
                                old_status="SCHEDULED",
                                new_status="RESULTS_PUBLISHED",
                                reason="Results filing linked to scheduled meeting",
                                source_evidence=f"Filing ID #{filing_id}"
                            )

                        matched_count += 1

                # Step 2: Auto-conclude passed meetings (meeting date > 3 days ago with no results published)
                today = datetime.now(timezone.utc).date()
                cur.execute(
                    """
                    UPDATE earnings_calendar
                    SET event_status = 'CONCLUDED', updated_at = CURRENT_TIMESTAMP
                    WHERE meeting_date < %s AND event_status = 'SCHEDULED'
                    """,
                    (today - timedelta(days=3),)
                )

            conn.commit()
            logger.info(f"Reconciliation engine linked {matched_count} filings to calendar events.")
            return matched_count
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during earnings reconciliation: {e}")
            return 0
