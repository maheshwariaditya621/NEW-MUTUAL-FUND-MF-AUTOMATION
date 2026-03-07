"""
Corporate Actions Adjustment Engine
====================================
Computes backward-adjusted quantities for equity_holdings based on
confirmed corporate actions (splits, bonuses, etc.).

Core principle:
  - raw quantity  → what the AMC Excel file reported (never modified)
  - adj_quantity  → raw_quantity × qty_factor (used for charts & analytics)
  - market_value_inr is NOT touched (₹ value is split-invariant)
  - Angel One live price is already post-split (no touch needed)

Adjustment direction (backward adjustment):
  For a 1:5 split (Kotak example):
    qty_factor   = 5      → historical qty × 5  (e.g. 5 shares → 25)
    price_factor = 0.20   → historical price × 0.2 (e.g. ₹1600 → ₹320)
"""

import logging
from decimal import Decimal
from typing import List, Dict, Any, Optional
from datetime import date

from src.db.connection import get_connection, get_cursor
from src.config import logger


class AdjustmentEngine:
    """
    Produces and applies split/bonus adjustment factors for a given ISIN.

    Usage:
        engine = AdjustmentEngine()

        # Single ISIN
        result = engine.apply_adjustment('INE237A01028')

        # All ISINs with unapplied actions
        results = engine.apply_all_pending()
    """

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def apply_adjustment(self, isin: str) -> Dict[str, Any]:
        """
        Full recalculation pipeline for a single ISIN:
          1. Fetch all CONFIRMED corporate actions for this ISIN.
          2. Build a cumulative factor timeline (backward adjustment).
          3. Persist the timeline into adjustment_factors.
          4. Apply factors to equity_holdings.adj_quantity.
          5. Mark corporate_actions.is_applied = TRUE.

        Returns:
            dict with status, rows_updated, isin
        """
        try:
            actions = self._fetch_confirmed_actions(isin)

            if not actions:
                # No actions: adj_quantity = raw quantity (factor = 1.0)
                rows = self._apply_factor_to_holdings(isin, [])
                return {"status": "ok", "isin": isin, "rows_updated": rows, "actions_count": 0}

            # Build factor timeline: list of (effective_date, qty_factor, price_factor)
            timeline = self._build_factor_timeline(actions)

            # Persist to adjustment_factors table
            self._upsert_adjustment_factors(isin, timeline, actions)

            # Apply to holdings
            rows = self._apply_factor_to_holdings(isin, timeline)

            # Mark all used actions as applied
            self._mark_actions_applied(isin)

            logger.info(f"[AdjustmentEngine] {isin}: {len(timeline)} factor(s) applied to {rows} holding rows.")
            return {
                "status": "ok",
                "isin": isin,
                "rows_updated": rows,
                "actions_count": len(actions)
            }

        except Exception as e:
            logger.error(f"[AdjustmentEngine] Failed for ISIN {isin}: {e}")
            get_connection().rollback()
            return {"status": "error", "isin": isin, "error": str(e)}

    def apply_all_pending(self) -> List[Dict[str, Any]]:
        """
        Applies adjustments for all ISINs that have at least one
        CONFIRMED corporate_action with is_applied = FALSE.
        """
        cursor = get_cursor()
        cursor.execute("""
            SELECT DISTINCT im.isin
            FROM corporate_actions ca
            JOIN isin_master im ON ca.entity_id = im.entity_id
            WHERE ca.status = 'CONFIRMED'
              AND ca.is_applied = FALSE
              AND im.isin IS NOT NULL
        """)
        rows = cursor.fetchall()
        isins = [r[0] for r in rows]

        logger.info(f"[AdjustmentEngine] Found {len(isins)} ISIN(s) with pending adjustments.")
        results = []
        for isin in isins:
            result = self.apply_adjustment(isin)
            results.append(result)
        return results

    def recompute_isin(self, isin: str) -> Dict[str, Any]:
        """
        Force-recomputes adj_quantity for a single ISIN regardless of is_applied flag.
        Useful for manual admin reruns.
        """
        logger.info(f"[AdjustmentEngine] Force-recomputing {isin}")
        return self.apply_adjustment(isin)

    # ------------------------------------------------------------------ #
    #  Internal: Fetch actions                                             #
    # ------------------------------------------------------------------ #

    def _fetch_confirmed_actions(self, isin: str) -> List[Dict[str, Any]]:
        """
        Returns all CONFIRMED corporate actions for this ISIN, ordered by
        effective_date ascending (oldest first).

        We join via isin_master → corporate_entities → corporate_actions
        because corporate_actions is keyed on entity_id (not isin directly).
        """
        cursor = get_cursor()
        cursor.execute("""
            SELECT
                ca.id                AS ca_id,
                ca.effective_date,
                ca.action_type,
                ca.numerator,
                ca.denominator,
                ca.ratio_factor
            FROM corporate_actions ca
            JOIN isin_master im
                ON ca.entity_id = im.entity_id
            WHERE im.isin = %s
              AND ca.status = 'CONFIRMED'
            ORDER BY ca.effective_date ASC
        """, (isin,))

        rows = cursor.fetchall()
        cols = ["ca_id", "effective_date", "action_type", "numerator",
                "denominator", "ratio_factor"]
        return [dict(zip(cols, r)) for r in rows]

    # ------------------------------------------------------------------ #
    #  Internal: Factor timeline builder                                   #
    # ------------------------------------------------------------------ #

    def _individual_qty_factor(self, action: Dict[str, Any]) -> Decimal:
        """
        Computes the qty adjustment factor for a single corporate action.

        SPLIT (1:5 → 5 new shares per 1 old share):
            qty_factor = numerator / denominator = 5 / 1 = 5.0

        BONUS (1:1 bonus → 1 extra per existing → total = 2 per original):
            qty_factor = (denominator + numerator) / denominator = (1+1)/1 = 2.0

        MERGER / RIGHTS:
            qty_factor = ratio_factor (as-is, set by admin when recording the action)
        """
        action_type = (action.get("action_type") or "").upper()
        numerator   = Decimal(str(action["numerator"] or 0))
        denominator = Decimal(str(action["denominator"] or 1))
        ratio_factor = Decimal(str(action["ratio_factor"] or 1))

        if denominator == 0:
            logger.warning(f"[AdjustmentEngine] ca_id={action['ca_id']} has denominator=0, skipping.")
            return Decimal("1.0")

        if action_type == "SPLIT":
            # e.g., 1 old share → 5 new shares: numerator=5, denominator=1
            return numerator / denominator

        elif action_type == "BONUS":
            # e.g., 1:1 bonus means 1 extra share per 1 existing = 2 total
            # numerator = bonus shares, denominator = existing shares
            return (denominator + numerator) / denominator

        elif action_type == "DIVIDEND":
            # Cash dividends don't change share count
            return Decimal("1.0")

        else:
            # MERGER, RIGHTS, or custom: use ratio_factor as set by admin
            return ratio_factor if ratio_factor > 0 else Decimal("1.0")

    def _build_factor_timeline(
        self, actions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Builds cumulative backward-adjustment factors from a list of actions.

        Backward adjustment means we work from the MOST RECENT action backwards.
        All historical holdings BEFORE an action's effective_date get multiplied
        by the cumulative factor.

        Returns a list of dicts:
            [
                {
                    "effective_date": date,   # holdings ON or BEFORE this date get this factor
                    "qty_factor":   Decimal,  # multiply raw_quantity by this
                    "price_factor": Decimal,  # = 1 / qty_factor
                    "ca_ids":       [int],    # which corporate_actions contributed
                }
            ]

        Example (Kotak — one 1:5 split on 2024-09-21):
            effective_date=2024-09-20, qty_factor=5.0, price_factor=0.2

        This means: all holdings where period_end_date <= 2024-09-20
        get adj_quantity = raw_quantity × 5.

        Holdings on or after 2024-09-21 (already post-split) get factor=1.0
        (handled by the absence of a factor row — default is 1.0).
        """
        timeline = []
        cumulative_qty_factor = Decimal("1.0")

        # Walk from newest to oldest action — building cumulative product
        for action in reversed(actions):
            individual_factor = self._individual_qty_factor(action)
            cumulative_qty_factor *= individual_factor

            # The factor applies to all periods BEFORE this action's effective date
            # i.e., period_end_date < effective_date
            effective_date = action["effective_date"]

            # We use effective_date - 1 day as the boundary
            from datetime import timedelta
            boundary_date = (
                effective_date
                if isinstance(effective_date, date)
                else date.fromisoformat(str(effective_date))
            )

            # Collect ca_ids that contributed so far
            ca_ids_so_far = [
                a["ca_id"] for a in actions
                if a["effective_date"] >= effective_date
            ]

            price_factor = (
                Decimal("1.0") / cumulative_qty_factor
                if cumulative_qty_factor != 0
                else Decimal("1.0")
            )

            timeline.append({
                "effective_date": boundary_date,
                "qty_factor": cumulative_qty_factor,
                "price_factor": price_factor,
                "ca_ids": ca_ids_so_far,
            })

        # timeline is now newest-first; reverse to get chronological order
        timeline.reverse()
        return timeline

    # ------------------------------------------------------------------ #
    #  Internal: Persist adjustment_factors                                #
    # ------------------------------------------------------------------ #

    def _upsert_adjustment_factors(
        self,
        isin: str,
        timeline: List[Dict[str, Any]],
        actions: List[Dict[str, Any]]
    ) -> None:
        """Upserts the computed factor timeline into adjustment_factors."""
        conn = get_connection()
        cursor = conn.cursor()

        for entry in timeline:
            cursor.execute("""
                INSERT INTO adjustment_factors
                    (isin, effective_date, qty_factor, price_factor, source_ca_ids, computed_at)
                VALUES
                    (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (isin, effective_date)
                DO UPDATE SET
                    qty_factor    = EXCLUDED.qty_factor,
                    price_factor  = EXCLUDED.price_factor,
                    source_ca_ids = EXCLUDED.source_ca_ids,
                    computed_at   = NOW()
            """, (
                isin,
                entry["effective_date"],
                float(entry["qty_factor"]),
                float(entry["price_factor"]),
                entry["ca_ids"],
            ))

        conn.commit()
        logger.debug(f"[AdjustmentEngine] Upserted {len(timeline)} factor rows for {isin}.")

    # ------------------------------------------------------------------ #
    #  Internal: Apply factor to holdings                                  #
    # ------------------------------------------------------------------ #

    def _apply_factor_to_holdings(
        self,
        isin: str,
        timeline: List[Dict[str, Any]]
    ) -> int:
        """
        Updates equity_holdings.adj_quantity for all historical rows of this ISIN.

        Algorithm:
          For each equity_holding row of this ISIN:
            - Find the applicable factor from the timeline:
                the factor whose effective_date is >= period_end_date
                (i.e., all actions that happened AFTER this holding's month)
            - adj_quantity = raw_quantity × qty_factor

          If no factor applies (holding is post all splits) → adj_quantity = raw_quantity.

        Because we work in SQL, we do this efficiently in a single UPDATE with
        a LATERAL join against our in-memory timeline.

        For a simpler and more maintainable approach: we update in Python per
        factor bucket (one UPDATE per factor entry in the timeline + one for post-split).
        This is correct, readable, and fast for the scale of MF data.
        """
        conn = get_connection()
        cursor = conn.cursor()
        total_updated = 0

        # Step 1: Baseline — set adj_quantity = raw quantity for ALL holdings of this ISIN.
        # This handles post-split months and ISINs with no applicable factor.
        # We use a subquery to get the holding_ids for this ISIN, then UPDATE by PK.
        cursor.execute("""
            UPDATE equity_holdings
            SET    adj_quantity = quantity::NUMERIC
            WHERE  holding_id IN (
                SELECT eh.holding_id
                FROM   equity_holdings eh
                JOIN   scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                JOIN   companies c         ON c.company_id  = eh.company_id
                WHERE  c.isin = %s
            )
        """, (isin,))
        total_updated = cursor.rowcount

        # Step 2: For each factor bucket, overwrite pre-split rows with qty × qty_factor
        for entry in timeline:
            effective_date = entry["effective_date"]
            qty_factor     = float(entry["qty_factor"])

            cursor.execute("""
                UPDATE equity_holdings
                SET    adj_quantity = quantity::NUMERIC * %s
                WHERE  holding_id IN (
                    SELECT eh.holding_id
                    FROM   equity_holdings eh
                    JOIN   scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                    JOIN   periods p           ON p.period_id   = ss.period_id
                    JOIN   companies c         ON c.company_id  = eh.company_id
                    WHERE  c.isin = %s
                      AND  p.period_end_date <= %s
                )
            """, (qty_factor, isin, effective_date))

            logger.debug(
                f"[AdjustmentEngine] {isin}: factor {qty_factor} applied to "
                f"{cursor.rowcount} rows with period_end_date <= {effective_date}"
            )

        conn.commit()
        logger.info(f"[AdjustmentEngine] {isin}: adj_quantity updated for {total_updated} holding rows.")
        return total_updated

    # ------------------------------------------------------------------ #
    #  Internal: Mark actions as applied                                   #
    # ------------------------------------------------------------------ #

    def _mark_actions_applied(self, isin: str) -> None:
        """Sets is_applied=TRUE on all CONFIRMED actions for this ISIN."""
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE corporate_actions ca
            SET    is_applied = TRUE
            FROM   isin_master im
            WHERE  ca.entity_id = im.entity_id
              AND  im.isin = %s
              AND  ca.status = 'CONFIRMED'
        """, (isin,))
        conn.commit()
