"""
Sync State Manager for Corporate Announcements.

Tracks durable watermarks, last sequence IDs, and checkpoint timestamps
for both NSE and BSE ingestion pipelines.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import psycopg2.extras

from src.db.connection import get_connection
from src.config import logger

NSE_SYNC_KEY = "NSE_EQUITIES"


class SyncStateManager:
    """Manages persistent checkpoint watermarks in announcement_sync_state."""

    @staticmethod
    def get_sync_state(exchange: str, scrip_code: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get the current sync state for an exchange (or specific BSE scrip code).
        """
        conn = get_connection()
        sync_key = f"{exchange}_{scrip_code}" if scrip_code else exchange
        if exchange == "NSE" and not scrip_code:
            sync_key = NSE_SYNC_KEY

        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT sync_key, exchange, scrip_code, 
                           last_checkpoint_timestamp, last_seq_id, 
                           last_poll_status, last_poll_at, last_error, updated_at
                    FROM announcement_sync_state
                    WHERE sync_key = %s
                    """,
                    (sync_key,)
                )
                row = cur.fetchone()
                return dict(row) if row else None
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to fetch sync state for {sync_key}: {e}")
            raise

    @staticmethod
    def update_sync_state(
        exchange: str,
        last_checkpoint: datetime,
        last_seq_id: Optional[int] = None,
        scrip_code: Optional[str] = None,
        status: str = "SUCCESS",
        error: Optional[str] = None
    ) -> None:
        """
        Upsert the sync state checkpoint.
        CRITICAL: Only call this AFTER the corresponding announcements have been committed.
        """
        conn = get_connection()
        sync_key = f"{exchange}_{scrip_code}" if scrip_code else exchange
        if exchange == "NSE" and not scrip_code:
            sync_key = NSE_SYNC_KEY

        now = datetime.now(timezone.utc)

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO announcement_sync_state (
                        sync_key, exchange, scrip_code,
                        last_checkpoint_timestamp, last_seq_id,
                        last_poll_status, last_poll_at, last_error, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (sync_key) DO UPDATE SET
                        last_checkpoint_timestamp = EXCLUDED.last_checkpoint_timestamp,
                        last_seq_id = COALESCE(EXCLUDED.last_seq_id, announcement_sync_state.last_seq_id),
                        last_poll_status = EXCLUDED.last_poll_status,
                        last_poll_at = EXCLUDED.last_poll_at,
                        last_error = EXCLUDED.last_error,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        sync_key, exchange, scrip_code,
                        last_checkpoint, last_seq_id,
                        status, now, error, now
                    )
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update sync state for {sync_key}: {e}")
            raise

    @staticmethod
    def get_bse_recovery_start_date(scrip_code: str, max_lookback_days: int = 30) -> datetime:
        """
        Calculates dynamic recovery start date for a BSE scrip.
        Formula approved in architecture:
            - If checkpoint exists: min(last_checkpoint - 24 hours, now - 3 days)
              capped at max_lookback_days (default 30 days).
            - If no checkpoint: now - 3 days (covers weekends/holidays).
        """
        now = datetime.now(timezone.utc)
        state = SyncStateManager.get_sync_state("BSE", scrip_code)

        if state and state.get("last_checkpoint_timestamp"):
            last_ts = state["last_checkpoint_timestamp"]
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=timezone.utc)
            # Replay with 24-hour safety overlap, and at least 3-day window if stale
            window_candidate = min(last_ts - timedelta(hours=24), now - timedelta(days=3))
            max_limit = now - timedelta(days=max_lookback_days)
            return max(window_candidate, max_limit)
        else:
            # Default first-run: past 3 days (sufficient for recent context without backfilling history)
            return now - timedelta(days=3)
