"""
Analytical insights for mutual fund portfolio data.
"""

from typing import List, Optional, Dict, Tuple
from fastapi import APIRouter, HTTPException, Query, Depends, BackgroundTasks
from psycopg2.extensions import cursor
from decimal import Decimal
from datetime import date
import time

from src.api.models.insights import (
    StockActivityResponse,
    StockActivityItem,
    SectorShiftItem,
    TimeMachineSummary,
    TimeMachineResponse
)
from src.api.dependencies import get_db_cursor, get_current_user
from src.config import logger

router = APIRouter()

# In-memory cache for fast tab-switching (TTL: 5 minutes)
_insights_cache: Dict[Tuple, Tuple[float, StockActivityResponse]] = {}
INSIGHTS_CACHE_TTL = 300


@router.get("/stock-activity", response_model=StockActivityResponse)
async def get_stock_activity(
    activity_type: str = Query("buying", pattern="^(buying|selling|entrants|exits)$", description="Type of activity to fetch"),
    mcap_category: Optional[str] = Query(None, description="Filter by market cap (Large Cap, Mid Cap, Small Cap)"),
    limit: int = Query(250, ge=1, le=1000, description="Number of top stocks to return"),
    background_tasks: BackgroundTasks = None,
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Get stocks with the highest net mutual fund buying or selling in the latest month.
    
    Compares the latest available period with the previous month's data.
    """
    try:
        cache_key = (activity_type, mcap_category, limit)
        now = time.time()
        if cache_key in _insights_cache:
            cached_time, cached_res = _insights_cache[cache_key]
            if now - cached_time < INSIGHTS_CACHE_TTL:
                return cached_res

        # 1. Get the latest periods and check coverage
        from src.api.utils.data_coverage import get_period_coverage, get_data_warning
        coverage = get_period_coverage(cur)
        data_warning = get_data_warning(coverage)

        cur.execute("SELECT period_id, year, month FROM periods ORDER BY year DESC, month DESC LIMIT 3")
        periods = cur.fetchall()
        
        if len(periods) < 2:
            raise HTTPException(
                status_code=404, 
                detail="Insufficient historical data to calculate activity (need at least 2 months)."
            )

        # If the latest period is partial, skip it and use the two previous complete months.
        # This prevents misleading insights based on only a few AMC uploads.
        if coverage.get("is_partial") and len(periods) >= 3:
            # Use periods[1] and periods[2] — the 2 most recent COMPLETE months
            p1_id, y1, m1 = periods[1]  # Previous complete month (e.g., Jan-26)
            p0_id, y0, m0 = periods[2]  # Month before that (e.g., Dec-25)
            logger.info(
                f"[Insights] Latest period {coverage['latest']['label']} is partial ({coverage['coverage_pct']}%). "
                f"Using {coverage['prev']['label']} vs previous for stock activity."
            )
        else:
            # Normal case: use the latest two periods
            p1_id, y1, m1 = periods[0]  # Latest
            p0_id, y0, m0 = periods[1]  # Previous
            data_warning = None  # No need to warn if data is complete
        
        month_label = date(y1, m1, 1).strftime("%b-%y").upper()
        prev_month_label = date(y0, m0, 1).strftime("%b-%y").upper()

        # 2. Build the query to calculate net buying/selling (Entity-Centric)
        if activity_type == "buying":
            order_col = "net_qty"
            order_direction = "DESC"
            where_conditions = ["qty_curr > qty_prev", "qty_prev > 0"] # Must have been held before to be a pure "buy" (accumulation)
        elif activity_type == "selling":
            order_col = "net_qty"
            order_direction = "ASC"
            where_conditions = ["qty_curr < qty_prev", "qty_curr > 0"] # Must still be held to be a "sell" (distribution)
        elif activity_type == "entrants":
            order_col = "qty_curr"
            order_direction = "DESC"
            where_conditions = ["qty_prev = 0", "qty_curr > 0"]
        elif activity_type == "exits":
            order_col = "qty_prev"
            order_direction = "DESC"
            where_conditions = ["qty_curr = 0", "qty_prev > 0"]
        
        if mcap_category:
            where_conditions.append("c.mcap_type = %s")
            
        where_clause = "AND " + " AND ".join(where_conditions) if where_conditions else ""
        
        cur.execute(
            f"""
            WITH splits AS (
                SELECT entity_id, ratio_factor 
                FROM corporate_actions 
                WHERE effective_date > (SELECT period_end_date FROM periods WHERE period_id = %s)
                AND effective_date <= (SELECT period_end_date FROM periods WHERE period_id = %s)
                AND status = 'CONFIRMED'
            ),
            curr_h AS (
                SELECT 
                    COALESCE(c.entity_id, c.company_id + 10000000) as uid,
                    ss.scheme_id,
                    eh.quantity,
                    COALESCE(eh.adj_quantity, eh.quantity) as adj_qty,
                    eh.market_value_inr as mval
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                JOIN companies c ON eh.company_id = c.company_id
                WHERE ss.period_id = %s
            ),
            prev_h AS (
                SELECT 
                    COALESCE(c.entity_id, c.company_id + 10000000) as uid,
                    ss.scheme_id,
                    eh.quantity,
                    COALESCE(eh.adj_quantity, eh.quantity) as adj_qty,
                    eh.market_value_inr as mval
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                JOIN companies c ON eh.company_id = c.company_id
                WHERE ss.period_id = %s
            ),
            h_agg AS (
                SELECT 
                    coalesce(curr.uid, prev.uid) as uid,
                    COUNT(DISTINCT curr.scheme_id) as num_funds_curr,
                    COUNT(DISTINCT prev.scheme_id) as num_funds_prev,
                    SUM(COALESCE(curr.adj_qty, 0)) as qty_curr,
                    -- Self-healing: if adj_qty is same as quantity, and a split happened, apply it on the fly
                    SUM(COALESCE(
                        CASE 
                            WHEN prev.adj_qty = prev.quantity AND s.ratio_factor IS NOT NULL 
                            THEN prev.quantity * s.ratio_factor
                            ELSE prev.adj_qty
                        END, 
                        0
                    )) as qty_prev,
                    SUM(COALESCE(curr.mval, 0)) as mval_curr,
                    SUM(COALESCE(prev.mval, 0)) as mval_prev,
                    -- Entrant: held now, NOT held before, but was uploaded before
                    COUNT(DISTINCT curr.scheme_id) FILTER (
                        WHERE curr.scheme_id IS NOT NULL 
                        AND prev.scheme_id IS NULL
                        AND EXISTS (SELECT 1 FROM scheme_snapshots ss WHERE ss.scheme_id = curr.scheme_id AND ss.period_id = %s)
                    ) as entrants,
                    -- Exit: held before, NOT held now, but was uploaded now
                    COUNT(DISTINCT prev.scheme_id) FILTER (
                        WHERE prev.scheme_id IS NOT NULL 
                        AND curr.scheme_id IS NULL
                        AND EXISTS (SELECT 1 FROM scheme_snapshots ss WHERE ss.scheme_id = prev.scheme_id AND ss.period_id = %s)
                    ) as exits
                FROM curr_h curr
                FULL OUTER JOIN prev_h prev ON curr.uid = prev.uid AND curr.scheme_id = prev.scheme_id
                LEFT JOIN splits s ON (COALESCE(curr.uid, prev.uid) = s.entity_id)
                GROUP BY coalesce(curr.uid, prev.uid)
            )
            SELECT DISTINCT ON ({order_col}, h_agg.uid)
                c.isin, 
                COALESCE(ce.canonical_name, c.company_name) as company_name, 
                c.sector, c.mcap_type, c.market_cap, c.nse_symbol,
                qty_curr, 
                qty_prev,
                (qty_curr - qty_prev) as net_qty,
                num_funds_curr, 
                num_funds_prev,
                CASE 
                    WHEN qty_curr > 0 THEN ((qty_curr - qty_prev) * (mval_curr / qty_curr)) / 10000000.0
                    WHEN qty_prev > 0 THEN ((qty_curr - qty_prev) * (mval_prev / qty_prev)) / 10000000.0
                    ELSE 0 
                END as buy_value_cr,
                c.shares_outstanding,
                (entrants - exits) as net_entrants,
                h_agg.uid
            FROM h_agg
            JOIN companies c ON (CASE WHEN h_agg.uid > 10000000 THEN h_agg.uid - 10000000 = c.company_id ELSE h_agg.uid = c.entity_id END)
            LEFT JOIN corporate_entities ce ON c.entity_id = ce.entity_id
            WHERE (qty_curr > 0 OR qty_prev > 0) {where_clause}
            ORDER BY {order_col} {order_direction}, h_agg.uid, c.updated_at DESC
            LIMIT %s
            """,
            [p0_id, p1_id, p1_id, p0_id, p0_id, p1_id] + ([mcap_category] if mcap_category else []) + [limit]
        )
        
        rows = cur.fetchall()
        
        results = []
        for row in rows:
            isin = row[0]
            m_cap = row[4]
            shares_out = row[12]
            live_mcap = float(m_cap) if m_cap else None
            
            
            net_qty = int(row[8])
            qty_prev = int(row[7])
            
            # 1. How much of the total company did MFs buy/sell?
            ownership_change = None
            if shares_out and shares_out > 0:
                ownership_change = round((net_qty / shares_out) * 100, 4)
                
            # 2. How much did MFs increase their existing stake by?
            holding_change = None
            if qty_prev > 0:
                holding_change = round((net_qty / qty_prev) * 100, 2)
            elif qty_prev == 0 and net_qty > 0:
                holding_change = 100.0 # 100% new
            
            results.append(StockActivityItem(
                isin=isin,
                company_name=row[1],
                sector=row[2],
                classification=row[3],
                market_cap=Decimal(str(live_mcap)).quantize(Decimal('0.01')) if live_mcap else None,
                nse_symbol=row[5],
                total_qty_curr=int(row[6]),
                total_qty_prev=qty_prev,
                net_qty_bought=net_qty,
                num_funds_curr=int(row[9]),
                num_funds_prev=int(row[10]),
                net_fund_entrants=int(row[13]),
                buy_value_crore=Decimal(str(row[11])).quantize(Decimal('0.01')),
                ownership_change_percent=ownership_change,
                holding_change_percent=holding_change
            ))
            
        response = StockActivityResponse(
            month=month_label,
            prev_month=prev_month_label,
            results=results,
            total_results=len(results),
            activity_type=activity_type,
            data_warning=data_warning
        )
        _insights_cache[cache_key] = (time.time(), response)
        return response

    except Exception as e:
        logger.error(f"Failed to fetch stock activity: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to fetch insights: {str(e)}")


_time_machine_cache: Dict[Tuple, Tuple[float, TimeMachineResponse]] = {}
TIME_MACHINE_CACHE_TTL = 300


@router.get("/time-machine", response_model=TimeMachineResponse)
async def get_time_machine_comparison(
    base_period: Optional[str] = Query(None, description="Origin/Base month label, e.g. 'OCT-25'"),
    target_period: Optional[str] = Query(None, description="Destination/Target month label, e.g. 'JAN-26'"),
    limit: int = Query(60, ge=10, le=200, description="Number of items per category"),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Time-Machine Comparison:
    Compares institutional holdings between any two historical months (Base Month vs Target Month).
    Returns net accumulations, distributions, fresh entrants, complete exits, and sector rotations.
    """
    try:
        cur.execute("SELECT period_id, year, month FROM periods ORDER BY year DESC, month DESC")
        all_period_rows = cur.fetchall()
        if len(all_period_rows) < 2:
            raise HTTPException(status_code=400, detail="Insufficient periods for historical comparison (need at least 2).")

        period_labels = [date(r[1], r[2], 1).strftime("%b-%y").upper() for r in all_period_rows]
        period_map = {date(r[1], r[2], 1).strftime("%b-%y").upper(): (r[0], r[1], r[2]) for r in all_period_rows}

        # Resolve target period (default to last complete period)
        from src.api.utils.data_coverage import get_period_coverage
        coverage = get_period_coverage(cur)

        if not target_period or target_period not in period_map:
            if coverage.get("is_partial") and len(all_period_rows) >= 2:
                target_period = date(all_period_rows[1][1], all_period_rows[1][2], 1).strftime("%b-%y").upper()
            else:
                target_period = period_labels[0]

        target_idx = period_labels.index(target_period)

        # Resolve base period (default to 3 months before target, or oldest available)
        if not base_period or base_period not in period_map:
            default_base_idx = min(target_idx + 3, len(period_labels) - 1)
            if default_base_idx == target_idx and len(period_labels) > 1:
                default_base_idx = min(target_idx + 1, len(period_labels) - 1)
            base_period = period_labels[default_base_idx]

        p1_id, y1, m1 = period_map[target_period]
        p0_id, y0, m0 = period_map[base_period]

        # Ensure base is chronologically before target
        if (y0 * 12 + m0) > (y1 * 12 + m1):
            base_period, target_period = target_period, base_period
            p0_id, p1_id = p1_id, p0_id
            y0, y1 = y1, y0
            m0, m1 = m1, m0

        months_gap = (y1 * 12 + m1) - (y0 * 12 + m0)

        cache_key = (base_period, target_period, limit)
        now = time.time()
        if cache_key in _time_machine_cache:
            t, cached_res = _time_machine_cache[cache_key]
            if now - t < TIME_MACHINE_CACHE_TTL:
                return cached_res

        cur.execute(
            """
            WITH splits AS (
                SELECT entity_id, ratio_factor 
                FROM corporate_actions 
                WHERE effective_date > (SELECT period_end_date FROM periods WHERE period_id = %s)
                AND effective_date <= (SELECT period_end_date FROM periods WHERE period_id = %s)
                AND status = 'CONFIRMED'
            ),
            curr_h AS (
                SELECT 
                    COALESCE(c.entity_id, c.company_id + 10000000) as uid,
                    ss.scheme_id,
                    eh.quantity,
                    COALESCE(eh.adj_quantity, eh.quantity) as adj_qty,
                    eh.market_value_inr as mval
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                JOIN companies c ON eh.company_id = c.company_id
                WHERE ss.period_id = %s
            ),
            prev_h AS (
                SELECT 
                    COALESCE(c.entity_id, c.company_id + 10000000) as uid,
                    ss.scheme_id,
                    eh.quantity,
                    COALESCE(eh.adj_quantity, eh.quantity) as adj_qty,
                    eh.market_value_inr as mval
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                JOIN companies c ON eh.company_id = c.company_id
                WHERE ss.period_id = %s
            ),
            h_agg AS (
                SELECT 
                    coalesce(curr.uid, prev.uid) as uid,
                    COUNT(DISTINCT curr.scheme_id) as num_funds_curr,
                    COUNT(DISTINCT prev.scheme_id) as num_funds_prev,
                    SUM(COALESCE(curr.adj_qty, 0)) as qty_curr,
                    SUM(COALESCE(
                        CASE 
                            WHEN prev.adj_qty = prev.quantity AND s.ratio_factor IS NOT NULL 
                            THEN prev.quantity * s.ratio_factor
                            ELSE prev.adj_qty
                        END, 
                        0
                    )) as qty_prev,
                    SUM(COALESCE(curr.mval, 0)) as mval_curr,
                    SUM(COALESCE(prev.mval, 0)) as mval_prev,
                    COUNT(DISTINCT curr.scheme_id) FILTER (
                        WHERE curr.scheme_id IS NOT NULL AND prev.scheme_id IS NULL
                    ) as entrants,
                    COUNT(DISTINCT prev.scheme_id) FILTER (
                        WHERE prev.scheme_id IS NOT NULL AND curr.scheme_id IS NULL
                    ) as exits
                FROM curr_h curr
                FULL OUTER JOIN prev_h prev ON curr.uid = prev.uid AND curr.scheme_id = prev.scheme_id
                LEFT JOIN splits s ON (COALESCE(curr.uid, prev.uid) = s.entity_id)
                GROUP BY coalesce(curr.uid, prev.uid)
            )
            SELECT 
                c.isin, 
                COALESCE(ce.canonical_name, c.company_name) as company_name, 
                c.sector, c.mcap_type, c.market_cap, c.nse_symbol,
                qty_curr, 
                qty_prev,
                (qty_curr - qty_prev) as net_qty,
                num_funds_curr, 
                num_funds_prev,
                CASE 
                    WHEN qty_curr > 0 THEN ((qty_curr - qty_prev) * (mval_curr / qty_curr)) / 10000000.0
                    WHEN qty_prev > 0 THEN ((qty_curr - qty_prev) * (mval_prev / qty_prev)) / 10000000.0
                    ELSE 0 
                END as buy_value_cr,
                c.shares_outstanding,
                (entrants - exits) as net_entrants,
                h_agg.uid
            FROM h_agg
            JOIN companies c ON (CASE WHEN h_agg.uid > 10000000 THEN h_agg.uid - 10000000 = c.company_id ELSE h_agg.uid = c.entity_id END)
            LEFT JOIN corporate_entities ce ON c.entity_id = ce.entity_id
            WHERE (qty_curr > 0 OR qty_prev > 0)
            ORDER BY abs(qty_curr - qty_prev) DESC
            """,
            (p0_id, p1_id, p1_id, p0_id)
        )

        rows = cur.fetchall()

        all_items = []
        for row in rows:
            isin = row[0]
            m_cap = row[4]
            shares_out = row[12]
            live_mcap = float(m_cap) if m_cap else None

            net_qty = int(row[8])
            qty_prev = int(row[7])
            qty_curr = int(row[6])

            ownership_change = None
            if shares_out and shares_out > 0:
                ownership_change = round((net_qty / shares_out) * 100, 4)

            holding_change = None
            if qty_prev > 0:
                holding_change = round((net_qty / qty_prev) * 100, 2)
            elif qty_prev == 0 and net_qty > 0:
                holding_change = 100.0

            all_items.append(StockActivityItem(
                isin=isin,
                company_name=row[1],
                sector=row[2],
                classification=row[3],
                market_cap=Decimal(str(live_mcap)).quantize(Decimal('0.01')) if live_mcap else None,
                nse_symbol=row[5],
                total_qty_curr=qty_curr,
                total_qty_prev=qty_prev,
                net_qty_bought=net_qty,
                num_funds_curr=int(row[9]),
                num_funds_prev=int(row[10]),
                net_fund_entrants=int(row[13]),
                buy_value_crore=Decimal(str(row[11])).quantize(Decimal('0.01')),
                ownership_change_percent=ownership_change,
                holding_change_percent=holding_change
            ))

        # Sector Aggregations
        sector_map = {}
        for item in all_items:
            sec = item.sector or "Diversified / Others"
            if sec not in sector_map:
                sector_map[sec] = {"base": 0, "target": 0}
            sector_map[sec]["base"] += item.total_qty_prev
            sector_map[sec]["target"] += item.total_qty_curr

        sector_shifts = []
        for sec, agg in sector_map.items():
            net = agg["target"] - agg["base"]
            pct = round((net / agg["base"] * 100), 2) if agg["base"] > 0 else (100.0 if agg["target"] > 0 else 0.0)
            sector_shifts.append(SectorShiftItem(
                sector=sec,
                base_shares=agg["base"],
                target_shares=agg["target"],
                net_shares=net,
                percent_change=pct
            ))
        sector_shifts.sort(key=lambda s: abs(s.net_shares), reverse=True)

        # Categorize
        accumulations = [r for r in all_items if r.total_qty_curr > r.total_qty_prev and r.total_qty_prev > 0]
        accumulations.sort(key=lambda r: r.net_qty_bought, reverse=True)

        distributions = [r for r in all_items if r.total_qty_curr < r.total_qty_prev and r.total_qty_curr > 0]
        distributions.sort(key=lambda r: r.net_qty_bought)

        new_entrants = [r for r in all_items if r.total_qty_prev == 0 and r.total_qty_curr > 0]
        new_entrants.sort(key=lambda r: r.total_qty_curr, reverse=True)

        complete_exits = [r for r in all_items if r.total_qty_curr == 0 and r.total_qty_prev > 0]
        complete_exits.sort(key=lambda r: r.total_qty_prev, reverse=True)

        total_shares_base = sum(r.total_qty_prev for r in all_items)
        total_shares_target = sum(r.total_qty_curr for r in all_items)
        net_shares_delta = total_shares_target - total_shares_base
        net_shares_delta_pct = round((net_shares_delta / total_shares_base * 100), 2) if total_shares_base else 0.0

        summary = TimeMachineSummary(
            base_period=base_period,
            target_period=target_period,
            months_gap=months_gap,
            total_shares_base=total_shares_base,
            total_shares_target=total_shares_target,
            net_shares_delta=net_shares_delta,
            net_shares_delta_pct=net_shares_delta_pct,
            accumulated_stocks_count=len(accumulations),
            distributed_stocks_count=len(distributions),
            new_entrants_count=len(new_entrants),
            complete_exits_count=len(complete_exits)
        )

        response = TimeMachineResponse(
            summary=summary,
            top_accumulations=accumulations[:limit],
            top_distributions=distributions[:limit],
            new_entrants=new_entrants[:limit],
            complete_exits=complete_exits[:limit],
            sector_shifts=sector_shifts[:15],
            available_periods=period_labels
        )

        _time_machine_cache[cache_key] = (time.time(), response)
        return response

    except Exception as e:
        logger.error(f"Failed to fetch time-machine comparison: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to fetch time-machine comparison: {str(e)}")
