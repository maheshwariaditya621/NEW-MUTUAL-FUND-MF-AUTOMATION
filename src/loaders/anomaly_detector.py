from typing import List, Dict, Any, Optional
from src.config import logger
from src.db import get_cursor, get_previous_period_id

class AnomalyDetector:
    """
    Scans for potential corporate actions (Splits/Bonuses) by comparing 
    holdings across consecutive months.
    """

    @staticmethod
    def run(period_id: int):
        """
        Executes comparison between current period and previous period.
        Flags anomalies as 'PROPOSED' corporate actions.
        """
        prev_period_id = get_previous_period_id(period_id)
        if not prev_period_id:
            logger.info("No previous period found. Skipping anomaly detection.")
            return

        logger.info(f"Running Anomaly Detection for period {period_id} vs {prev_period_id}")
        
        cursor = get_cursor()
        
        # 1. Fetch current vs previous holdings for comparison
        # We group by (scheme_id, entity_id) to see how many shares a specific fund held 
        # of a specific company.
        # We join with 'companies' to get the entity_id link.
        cursor.execute(
            """
            WITH current_holdings AS (
                SELECT 
                    s.scheme_id,
                    c.entity_id,
                    h.quantity as qty,
                    h.market_value_inr as val,
                    h.percent_of_nav as p_nav
                FROM equity_holdings h
                JOIN scheme_snapshots s ON h.snapshot_id = s.snapshot_id
                JOIN companies c ON h.company_id = c.company_id
                WHERE s.period_id = %s
            ),
            previous_holdings AS (
                SELECT 
                    s.scheme_id,
                    c.entity_id,
                    h.quantity as qty,
                    h.market_value_inr as val,
                    h.percent_of_nav as p_nav
                FROM equity_holdings h
                JOIN scheme_snapshots s ON h.snapshot_id = s.snapshot_id
                JOIN companies c ON h.company_id = c.company_id
                WHERE s.period_id = %s
            )
            SELECT 
                curr.entity_id,
                curr.qty as curr_qty,
                prev.qty as prev_qty,
                curr.val as curr_val,
                prev.val as prev_val,
                curr.p_nav as curr_pnav,
                prev.p_nav as prev_pnav,
                curr.scheme_id
            FROM current_holdings curr
            JOIN previous_holdings prev ON curr.scheme_id = prev.scheme_id AND curr.entity_id = prev.entity_id
            WHERE prev.qty > 0
            """,
            (period_id, prev_period_id)
        )
        
        comparisons = cursor.fetchall()
        detected_anomalies = [] # (entity_id, ratio, confidence)

        for row in comparisons:
            entity_id, curr_qty, prev_qty, curr_val, prev_val, curr_pnav, prev_pnav, scheme_id = row
            
            qty_ratio = float(curr_qty) / float(prev_qty)
            val_diff = abs(float(curr_val) - float(prev_val)) / float(prev_val) if prev_val > 0 else 1.0
            pnav_diff = abs(float(curr_pnav) - float(prev_pnav))
            
            # Triple-Lock Trigger Logic
            is_potential_split = False
            ratio_factor = 1.0
            
            # Check common split/bonus ratios
            for target_ratio in [2.0, 5.0, 10.0]:
                if abs(qty_ratio - target_ratio) < (target_ratio * 0.05): # 5% margin
                    if val_diff < 0.05 and pnav_diff < 1.0: # Stable value and stable %NAV
                        is_potential_split = True
                        ratio_factor = target_ratio
                        break
            
            if is_potential_split:
                detected_anomalies.append({
                    "entity_id": entity_id,
                    "ratio": ratio_factor,
                    "scheme_id": scheme_id,
                    "qty_ratio": qty_ratio
                })

        # Group anomalies by entity to ensure consistency across multiple schemes
        entity_votes = {}
        for a in detected_anomalies:
            eid = a['entity_id']
            if eid not in entity_votes:
                entity_votes[eid] = []
            entity_votes[eid].append(a['ratio'])

        for eid, ratios in entity_votes.items():
            # Consensus: If multiple schemes agree on the ratio
            from collections import Counter
            most_common_ratio, count = Counter(ratios).most_common(1)[0]
            
            # 2. Check if already recorded to prevent duplicates
            cursor.execute(
                "SELECT 1 FROM corporate_actions WHERE entity_id = %s AND effective_date = (SELECT period_end_date FROM periods WHERE period_id = %s)",
                (eid, period_id)
            )
            if cursor.fetchone():
                continue

            logger.warning(f"[ANOMALY] Detected potential {most_common_ratio}:1 split for entity_id {eid} in period {period_id} ({count} schemes agreed)")
            
            # 3. Insert PROPOSED action
            cursor.execute(
                """
                INSERT INTO corporate_actions (entity_id, action_type, ratio_factor, effective_date, status, confidence_score, source)
                VALUES (%s, 'SPLIT/BONUS', %s, (SELECT period_end_date FROM periods WHERE period_id = %s), 'PROPOSED', 0.6, 'ANOMALY_DETECTOR')
                """,
                (eid, most_common_ratio, period_id)
            )
        
        cursor.connection.commit()
