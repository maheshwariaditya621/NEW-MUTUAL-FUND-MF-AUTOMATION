"""
Analytical insights for mutual fund portfolio data.
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends
from psycopg2.extensions import cursor
from decimal import Decimal
from datetime import date

from src.api.models.insights import StockActivityResponse, StockActivityItem
from src.api.dependencies import get_db_cursor, get_current_user
from src.config import logger

router = APIRouter()


@router.get("/stock-activity", response_model=StockActivityResponse)
async def get_stock_activity(
    activity_type: str = Query("buying", regex="^(buying|selling)$", description="Type of activity to fetch"),
    mcap_category: Optional[str] = Query(None, description="Filter by market cap (Large Cap, Mid Cap, Small Cap)"),
    limit: int = Query(50, ge=1, le=100, description="Number of top stocks to return"),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Get stocks with the highest net mutual fund buying or selling in the latest month.
    
    Compares the latest available period with the previous month's data.
    """
    try:
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

        # 2. Build the query to calculate net buying/selling
        # We aggregate by isin (or entity_id if available)
        # Using LEFT JOIN to handle new entrants vs exits
        
        order_direction = "DESC" if activity_type == "buying" else "ASC"
        
        where_conditions = []
        params = [p0_id, p1_id, p1_id, p0_id]
        
        if mcap_category:
            where_conditions.append("c.mcap_type = %s")
            params.append(mcap_category)
            
        where_clause = "AND " + " AND ".join(where_conditions) if where_conditions else ""
        
        # We calculate buy_value_crore using the latest weighted average price (mval / qty)
        # This is the industry heuristic for approximate buying pressure
        
        final_params = params + [limit]
        
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
                    eh.company_id,
                    ss.scheme_id,
                    eh.quantity as qty,
                    eh.market_value_inr as mval
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                WHERE ss.period_id = %s
            ),
            prev_h AS (
                SELECT 
                    eh.company_id,
                    ss.scheme_id,
                    eh.quantity as qty
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                WHERE ss.period_id = %s
            ),
            h_agg AS (
                SELECT 
                    coalesce(curr.company_id, prev.company_id) as comp_id,
                    COUNT(DISTINCT curr.scheme_id) as num_funds_curr,
                    COUNT(DISTINCT prev.scheme_id) as num_funds_prev,
                    SUM(COALESCE(curr.qty, 0)) as qty_curr,
                    SUM(COALESCE(prev.qty, 0)) as qty_prev,
                    SUM(COALESCE(curr.mval, 0)) as mval_curr,
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
                FULL OUTER JOIN prev_h prev ON curr.company_id = prev.company_id AND curr.scheme_id = prev.scheme_id
                GROUP BY coalesce(curr.company_id, prev.company_id)
            )
            SELECT 
                c.isin, c.company_name, c.sector, c.mcap_type, c.market_cap, c.nse_symbol,
                qty_curr, 
                qty_prev * COALESCE(s.ratio_factor, 1.0) as qty_prev_adj,
                (qty_curr - (qty_prev * COALESCE(s.ratio_factor, 1.0))) as net_qty,
                num_funds_curr, 
                num_funds_prev,
                CASE 
                    WHEN qty_curr > 0 THEN ((qty_curr - (qty_prev * COALESCE(s.ratio_factor, 1.0))) * (mval_curr / qty_curr)) / 10000000.0
                    ELSE 0 
                END as buy_value_cr,
                c.shares_outstanding,
                (entrants - exits) as net_entrants
            FROM companies c
            JOIN h_agg ON c.company_id = h_agg.comp_id
            LEFT JOIN splits s ON c.entity_id = s.entity_id
            WHERE (qty_curr > 0 OR (qty_prev * COALESCE(s.ratio_factor, 1.0)) > 0) {where_clause}
            ORDER BY net_qty {order_direction}
            LIMIT %s
            """,
            [p0_id, p1_id, p1_id, p0_id, p0_id, p1_id] + (params[4:] if len(params) > 4 else []) + [limit]
        )
        
        rows = cur.fetchall()
        
        # PREFETCH LIVE PRICES CONCURRENTLY to avoid 30sec+ loads
        unique_isins = list(set([row[0] for row in rows if row[0]]))
        if unique_isins:
            from src.services.pricing_service import pricing_service
            # This fetches all required LTPs in parallel (max 6 workers) and seeds the cache
            pricing_service.prefetch_ltps(unique_isins)
        
        results = []
        for row in rows:
            isin = row[0]
            m_cap = row[4]
            shares_out = row[12]
            
            from src.services.pricing_service import pricing_service
            live_mcap = pricing_service.get_live_market_cap(isin, float(m_cap) if m_cap else None, shares_out)
            
            results.append(StockActivityItem(
                isin=isin,
                company_name=row[1],
                sector=row[2],
                classification=row[3],
                market_cap=Decimal(str(live_mcap)).quantize(Decimal('0.01')) if live_mcap else None,
                nse_symbol=row[5],
                total_qty_curr=int(row[6]),
                total_qty_prev=int(row[7]),
                net_qty_bought=int(row[8]),
                num_funds_curr=int(row[9]),
                num_funds_prev=int(row[10]),
                net_fund_entrants=int(row[13]),
                buy_value_crore=Decimal(str(row[11])).quantize(Decimal('0.01'))
            ))
            
        return StockActivityResponse(
            month=month_label,
            prev_month=prev_month_label,
            results=results,
            total_results=len(results),
            activity_type=activity_type,
            data_warning=data_warning
        )

    except Exception as e:
        logger.error(f"Failed to fetch stock activity: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to fetch insights: {str(e)}")
