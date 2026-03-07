"""
Reprocessing Worker
====================
Drains the reprocessing_queue table and calls AdjustmentEngine
for each pending ISIN.

This worker is called:
  1. By the scheduler after corporate_actions are confirmed
  2. By the portfolio_loader post-load hook when new holdings arrive
     for an ISIN that has known corporate actions
  3. Manually by admin via CLI

Usage:
    from src.corporate_actions.reprocessing_worker import ReprocessingWorker
    worker = ReprocessingWorker()
    worker.drain_queue()
"""

import logging
from typing import List, Dict, Any

from src.db.connection import get_connection, get_cursor
from src.corporate_actions.adjustment_engine import AdjustmentEngine
from src.config import logger


class ReprocessingWorker:
    """
    Drains the reprocessing_queue table one ISIN at a time,
    calling AdjustmentEngine.apply_adjustment() for each.
    """

    def __init__(self):
        self.engine = AdjustmentEngine()

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def drain_queue(self, max_items: int = 500) -> Dict[str, Any]:
        """
        Processes up to `max_items` pending entries from reprocessing_queue.

        Returns:
            {
                "processed": int,
                "succeeded": int,
                "failed": int,
                "results": [...]
            }
        """
        pending = self._fetch_pending(max_items)
        if not pending:
            logger.info("[ReprocessingWorker] Queue empty — nothing to process.")
            return {"processed": 0, "succeeded": 0, "failed": 0, "results": []}

        logger.info(f"[ReprocessingWorker] Draining {len(pending)} item(s) from queue.")
        succeeded = 0
        failed    = 0
        results   = []

        for item in pending:
            queue_id = item["queue_id"]
            isin     = item["isin"]
            reason   = item["reason"]

            self._mark_running(queue_id)

            logger.info(f"[ReprocessingWorker] Processing {isin} (reason={reason})")
            result = self.engine.apply_adjustment(isin)

            if result.get("status") == "ok":
                self._mark_done(queue_id)
                succeeded += 1
            else:
                self._mark_failed(queue_id, result.get("error", "unknown error"))
                failed += 1

            results.append({**result, "queue_id": queue_id, "reason": reason})

        summary = {
            "processed": len(pending),
            "succeeded": succeeded,
            "failed":    failed,
            "results":   results,
        }
        logger.info(
            f"[ReprocessingWorker] Done. {succeeded} succeeded, {failed} failed."
        )
        return summary

    def enqueue(self, isin: str, reason: str = "MANUAL") -> bool:
        """
        Enqueues an ISIN for reprocessing.
        Silently ignores if an identical (isin, pending) entry already exists
        (enforced by the UNIQUE DEFERRABLE constraint in the DB).

        Args:
            isin:   The ISIN to reprocess
            reason: 'NEW_ACTION' | 'NEW_HOLDINGS' | 'MANUAL'

        Returns:
            True if a new row was inserted, False if already queued.
        """
        conn = get_connection()
        cur = conn.cursor()

        try:
            # Use plain INSERT — the partial unique index on (isin) WHERE status='pending'
            # prevents duplicates. If already queued, silently do nothing.
            cur.execute("""
                INSERT INTO reprocessing_queue (isin, reason, status)
                VALUES (%s, %s, 'pending')
                ON CONFLICT (isin) WHERE status = 'pending'
                DO NOTHING
            """, (isin, reason))
            inserted = cur.rowcount
            conn.commit()
            if inserted:
                logger.info(f"[ReprocessingWorker] Enqueued {isin} ({reason})")
            return inserted > 0
        except Exception as e:
            conn.rollback()
            logger.error(f"[ReprocessingWorker] Failed to enqueue {isin}: {e}")
            return False

    def enqueue_batch(self, isins: List[str], reason: str = "NEW_HOLDINGS") -> int:
        """
        Enqueues multiple ISINs at once. Only enqueues ISINs that have
        at least one CONFIRMED corporate_action (to avoid pointless work).

        Returns:
            Number of ISINs actually enqueued
        """
        if not isins:
            return 0

        # Filter: only enqueue ISINs with confirmed corporate actions
        relevant = self._filter_isins_with_actions(isins)
        if not relevant:
            return 0

        count = 0
        for isin in relevant:
            if self.enqueue(isin, reason):
                count += 1
        return count

    # ------------------------------------------------------------------ #
    #  Internal: Queue management                                          #
    # ------------------------------------------------------------------ #

    def _fetch_pending(self, limit: int) -> List[Dict[str, Any]]:
        """Fetches up to `limit` pending queue items ordered by triggered_at."""
        cursor = get_cursor()
        cursor.execute("""
            SELECT queue_id, isin, reason, triggered_at
            FROM   reprocessing_queue
            WHERE  status = 'pending'
            ORDER  BY triggered_at ASC
            LIMIT  %s
            FOR UPDATE SKIP LOCKED
        """, (limit,))
        rows = cursor.fetchall()
        return [
            {
                "queue_id":     r[0],
                "isin":         r[1],
                "reason":       r[2],
                "triggered_at": r[3],
            }
            for r in rows
        ]

    def _mark_running(self, queue_id: int) -> None:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE reprocessing_queue
            SET    status = 'running', started_at = NOW()
            WHERE  queue_id = %s
        """, (queue_id,))
        conn.commit()

    def _mark_done(self, queue_id: int) -> None:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE reprocessing_queue
            SET    status = 'done', completed_at = NOW()
            WHERE  queue_id = %s
        """, (queue_id,))
        conn.commit()

    def _mark_failed(self, queue_id: int, error_message: str) -> None:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE reprocessing_queue
            SET    status = 'failed',
                   completed_at = NOW(),
                   error_message = %s
            WHERE  queue_id = %s
        """, (error_message[:500], queue_id))
        conn.commit()

    def _filter_isins_with_actions(self, isins: List[str]) -> List[str]:
        """Returns only those ISINs from the input list that have a CONFIRMED corporate_action."""
        if not isins:
            return []
        cursor = get_cursor()
        cursor.execute("""
            SELECT DISTINCT im.isin
            FROM   corporate_actions ca
            JOIN   isin_master im ON ca.entity_id = im.entity_id
            WHERE  im.isin = ANY(%s)
              AND  ca.status = 'CONFIRMED'
        """, (isins,))
        return [r[0] for r in cursor.fetchall()]
