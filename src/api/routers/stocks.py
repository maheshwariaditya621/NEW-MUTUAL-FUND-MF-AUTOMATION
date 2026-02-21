"""
Stock holdings endpoints.

Provides APIs for searching stocks and viewing mutual fund holdings.
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends, Path
from psycopg2.extensions import cursor
from decimal import Decimal

from src.api.models.stocks import (
    CompanySearchResult,
    StockHoldingsSummary,
    StockSearchResponse,
    SchemeHolding,
    MonthlyHoldingData,
    HistoricalHolding
)
from datetime import date
import calendar
from src.api.dependencies import get_db_cursor
from src.config import logger

router = APIRouter()


@router.get("/search", response_model=StockSearchResponse)
async def search_stocks(
    q: str = Query(..., min_length=2, description="Search query (company name, ISIN, or NSE symbol)"),
    limit: int = Query(1000, ge=1, le=2000, description="Maximum number of results"),
    cur: cursor = Depends(get_db_cursor)
):
    """
    Search for stocks by company name, ISIN, or NSE symbol.
    
    Supports ultra-fuzzy multi-word matching on all fields.
    """
    try:
        # Split query into words and build fuzzy conditions
        words = [w.strip() for w in q.split() if w.strip()]
        if not words:
            return StockSearchResponse(query=q, results=[], total_results=0)

        # Build conditions: Each word must match at least one of the fields
        placeholders = []
        conditions = []
        for word in words:
            pattern = f"%{word}%"
            conditions.append("(c.company_name ILIKE %s OR c.isin ILIKE %s OR c.nse_symbol ILIKE %s)")
            placeholders.extend([pattern, pattern, pattern])
        
        where_clause = " AND ".join(conditions)
        
        # We need the original query for relevance sorting at the end
        placeholders.extend([q, q, q, q + '%'])
        
        cur.execute(
            f"""
            SELECT DISTINCT ON (COALESCE(c.entity_id::text, c.company_id::text))
                c.isin,
                c.company_name,
                c.sector,
                c.nse_symbol,
                c.bse_code,
                COALESCE(c.entity_id::text, c.company_id::text) as sort_id
            FROM companies c
            WHERE {where_clause}
            ORDER BY 
                COALESCE(c.entity_id::text, c.company_id::text),
                CASE 
                    WHEN UPPER(c.company_name) = UPPER(%s) THEN 1
                    WHEN c.isin = UPPER(%s) THEN 2
                    WHEN c.nse_symbol = UPPER(%s) THEN 3
                    WHEN c.company_name ILIKE %s THEN 4
                    ELSE 5
                END,
                c.company_name
            """,
            placeholders
        )

        rows = cur.fetchall()
        
        # Sort results by relevance
        def get_relevance(row):
            isin, name, sector, nse, bse, sort_id = row
            if name.upper() == q.upper(): return 1
            if isin == q.upper(): return 2
            if nse and nse.upper() == q.upper(): return 3
            if name.upper().startswith(q.upper()): return 4
            return 5

        sorted_rows = sorted(rows, key=get_relevance)
        # Apply limit to sorted results
        sorted_rows = sorted_rows[:limit]
        
        results = []
        for row in sorted_rows:
            isin = row[0]
            # Triple equity filter (mandatory in workflow.md)
            if (len(isin) == 12 and 
                isin.startswith('INE') and 
                isin[8:10] == '10'):
                results.append(CompanySearchResult(
                    isin=isin,
                    company_name=row[1].upper(),
                    sector=row[2],
                    nse_symbol=row[3],
                    bse_code=row[4]
                ))

        return StockSearchResponse(
            query=q,
            results=results,
            total_results=len(results)
        )

        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Stock search failed: {e}")
        logger.error(f"Full traceback:\n{error_trace}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")



def _resolve_company_isin(identifier: str, cur: cursor) -> tuple[str, str, Optional[str], Optional[Decimal], Optional[str], Optional[str]]:
    """
    Resolve a company identifier (ISIN, name, or symbol) to ISIN.
    
    Args:
        identifier: ISIN, company name, or NSE symbol
        cur: Database cursor
        
    Returns:
        Tuple of (isin, company_name, sector, market_cap, mcap_type, mcap_updated_at)
        
    Raises:
        HTTPException: If company not found or multiple matches found
    """
    # Try exact ISIN match first
    cur.execute(
        "SELECT isin, company_name, sector, market_cap, mcap_type, mcap_updated_at FROM companies WHERE isin = %s",
        (identifier.upper(),)
    )
    result = cur.fetchone()
    if result:
        return result
    
    # Try exact NSE symbol match
    cur.execute(
        "SELECT isin, company_name, sector, market_cap, mcap_type, mcap_updated_at FROM companies WHERE nse_symbol = %s",
        (identifier.upper(),)
    )
    result = cur.fetchone()
    if result:
        return result
    
    # Try fuzzy company name match
    search_pattern = f"%{identifier}%"
    cur.execute(
        """
        SELECT isin, company_name, sector, market_cap, mcap_type, mcap_updated_at 
        FROM companies 
        WHERE company_name ILIKE %s
        ORDER BY 
            CASE 
                WHEN UPPER(company_name) = UPPER(%s) THEN 1
                WHEN company_name ILIKE %s THEN 2
                ELSE 3
            END,
            company_name
        LIMIT 2
        """,
        (search_pattern, identifier, identifier + '%')
    )
    results = cur.fetchall()
    
    if not results:
        raise HTTPException(
            status_code=404, 
            detail=f"No company found matching '{identifier}'. Try searching first using /api/v1/stocks/search"
        )
    
    if len(results) > 1:
        # Multiple matches - return helpful error
        matches = [f"{r[1]} ({r[0]})" for r in results]
        raise HTTPException(
            status_code=400,
            detail=f"Multiple companies match '{identifier}': {', '.join(matches)}. Please use exact ISIN or NSE symbol."
        )
    
    return results[0]


@router.get("/holdings", response_model=StockHoldingsSummary)
async def get_holdings_by_identifier(
    q: str = Query(..., min_length=2, description="Company identifier (ISIN, company name, or NSE symbol)"),
    months: int = Query(4, ge=1, le=12, description="Number of months to show trend"),
    cur: cursor = Depends(get_db_cursor)
):
    """
    Get detailed holdings information for a stock by any identifier.
    
    Accepts ISIN, company name, or NSE symbol. Uses fuzzy matching for company names.
    
    Shows which mutual fund schemes hold this stock, with monthly trend data.
    
    Args:
        q: Company identifier (ISIN, company name, or NSE symbol)
        months: Number of months for trend analysis (default: 4)
    
    Returns:
        Detailed holdings summary with scheme-wise breakdown
        
    Examples:
        - /api/v1/stocks/holdings?q=INE002A01018
        - /api/v1/stocks/holdings?q=RELIANCE
        - /api/v1/stocks/holdings?q=reliance industries
    """
    try:
        # 1. Resolve identifier to ISIN and metadata
        isin, company_name, sector, market_cap, mcap_type, mcap_updated_at = _resolve_company_isin(q, cur)
        
        # 2. Check if this ISIN belongs to an Entity
        cur.execute("SELECT entity_id FROM isin_master WHERE isin = %s", (isin,))
        entity_row = cur.fetchone()
        entity_id = entity_row[0] if entity_row else None
        
        # 3. Get holdings (aggregated by entity if available)
        return await _get_stock_holdings_aggregated(isin, entity_id, company_name, sector, market_cap, mcap_type, mcap_updated_at, months, cur)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch holdings for '{q}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch holdings: {str(e)}")


@router.get("/{isin}/holdings", response_model=StockHoldingsSummary)
async def get_stock_holdings(
    isin: str = Path(..., description="12-character ISIN code"),
    months: int = Query(4, ge=1, le=12, description="Number of months to show trend"),
    cur: cursor = Depends(get_db_cursor)
):
    """
    Get detailed holdings information for a specific stock by ISIN.
    
    Shows which mutual fund schemes hold this stock, with monthly trend data.
    
    Args:
        isin: 12-character ISIN code
        months: Number of months for trend analysis (default: 4)
    
    Returns:
        Detailed holdings summary with scheme-wise breakdown
    """
    try:
        # Verify company exists and get its metadata/entity
        cur.execute("""
            SELECT c.company_name, c.sector, c.entity_id, c.market_cap, c.mcap_type, c.mcap_updated_at 
            FROM companies c 
            WHERE c.isin = %s
        """, (isin.upper(),))
        company_row = cur.fetchone()
        
        if not company_row:
            # Check isin_master if not in companies yet
            cur.execute("SELECT canonical_name, sector, entity_id, NULL as market_cap, NULL as mcap_type, NULL as mcap_updated_at FROM isin_master WHERE isin = %s", (isin.upper(),))
            company_row = cur.fetchone()
            
        if not company_row:
            raise HTTPException(status_code=404, detail=f"Company with ISIN {isin} not found")
        
        company_name, sector, entity_id, market_cap, mcap_type, mcap_updated_at = company_row
        company_name = company_name.upper()
        
        return await _get_stock_holdings_aggregated(isin.upper(), entity_id, company_name, sector, market_cap, mcap_type, mcap_updated_at, months, cur)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch holdings for {isin}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch holdings: {str(e)}")


async def _get_cumulative_multiplier(entity_id: int, year: int, month: int, cur: cursor) -> float:
    """
    Calculates the cumulative quantity multiplier for a given historical period
    based on all corporate actions effective after that period.
    """
    # Get last day of the period
    last_day = calendar.monthrange(year, month)[1]
    period_end_date = date(year, month, last_day)
    
    cur.execute(
        """
        SELECT ratio_factor 
        FROM corporate_actions 
        WHERE entity_id = %s AND effective_date > %s
        """,
        (entity_id, period_end_date)
    )
    actions = cur.fetchall()
    
    multiplier = Decimal('1.0')
    for action in actions:
        multiplier *= Decimal(str(action[0]))
    
    return multiplier


async def _get_stock_holdings_aggregated(
    isin: str,
    entity_id: Optional[int],
    company_name: str,
    sector: Optional[str],
    market_cap: Optional[Decimal],
    mcap_type: Optional[str],
    mcap_updated_at: Optional[str],
    months: int,
    cur: cursor
) -> StockHoldingsSummary:
    """
    Get holdings data aggregated by Corporate Entity (if available) or by single ISIN.
    Supports multi-month history for each scheme.
    """
    # Always use exactly 4 months window as requested by user
    # 1. Get the single latest period available in the system
    cur.execute("SELECT year, month FROM periods ORDER BY year DESC, month DESC LIMIT 1")
    latest_row = cur.fetchone()
    if not latest_row:
        raise HTTPException(status_code=404, detail="No portfolio data available")
    
    latest_yr, latest_mo = latest_row
    current_month_label = date(latest_yr, latest_mo, 1).strftime("%b-%y").upper()
    
    # 2. Generate exactly 5 months ending at latest_yr, latest_mo (+1 for trend calculation)
    # We use 5 so we can show 4 months and have the 5th for the 'trend' of the 4th month if needed
    all_target_periods = [] # list of (year, month)
    curr_yr, curr_mo = latest_yr, latest_mo
    for _ in range(5):
        all_target_periods.append((curr_yr, curr_mo))
        curr_mo -= 1
        if curr_mo == 0:
            curr_mo = 12
            curr_yr -= 1
    
    # display_periods are the first 4 (newest to oldest)
    display_periods = all_target_periods[:4]
    
    # 3. Resolve these months to period_ids if they exist
    period_ids_map = {} # (year, month) -> period_id
    for yr, mo in all_target_periods:
        cur.execute("SELECT period_id FROM periods WHERE year = %s AND month = %s", (yr, mo))
        p_row = cur.fetchone()
        if p_row:
            period_ids_map[(yr, mo)] = p_row[0]
            
    all_period_ids = list(period_ids_map.values())
    display_period_ids = [period_ids_map.get(p) for p in display_periods if period_ids_map.get(p)]
    
    # 2. Define Filter Clause
    if entity_id:
        filter_clause = "c.entity_id = %s"
        filter_val = entity_id
    else:
        filter_clause = "c.isin = %s"
        filter_val = isin

    # 3. Monthly Trend (Summary Level) - Optimized to always show exactly 4 columns
    cur.execute(
        f"""
        SELECT 
            p.period_id,
            SUM(eh.quantity) as total_shares,
            COUNT(DISTINCT ss.scheme_id) as num_funds
        FROM equity_holdings eh
        JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
        JOIN periods p ON ss.period_id = p.period_id
        JOIN companies c ON eh.company_id = c.company_id
        WHERE {filter_clause} AND p.period_id IN ({','.join(['%s']*len(all_period_ids))})
        GROUP BY p.period_id
        """,
        (filter_val, *all_period_ids)
    )
    
    summary_data_map = {row[0]: row for row in cur.fetchall()}
    monthly_trend = []
    total_shares_current = 0
    num_funds_current = 0
    
    # Pre-calculate adjusted shares for ALL 5 target periods to compute trends
    adj_shares_map = {}
    for yr, mo in all_target_periods:
        pid = period_ids_map.get((yr, mo))
        shares = 0
        if pid and pid in summary_data_map:
            shares = summary_data_map[pid][1]
        
        mult = 1.0
        if entity_id:
            mult = await _get_cumulative_multiplier(entity_id, yr, mo, cur)
        
        adj_shares_map[(yr, mo)] = int(shares * mult)

    for idx, (yr, mo) in enumerate(display_periods):
        month_str = date(yr, mo, 1).strftime("%b-%y").upper()
        pid = period_ids_map.get((yr, mo))
        
        adj_shares = adj_shares_map[(yr, mo)]
        funds = 0
        if pid and pid in summary_data_map:
            funds = summary_data_map[pid][2]
        
        if idx == 0:
            total_shares_current = adj_shares
            num_funds_current = int(funds)
        
        # Calculate trend relative to previous month in all_target_periods
        trend = None
        prev_idx = idx + 1
        if prev_idx < len(all_target_periods):
            adj_prev_shares = adj_shares_map[all_target_periods[prev_idx]]
            if adj_shares > adj_prev_shares: trend = "up"
            elif adj_shares < adj_prev_shares: trend = "down"
            elif adj_shares > 0: trend = "same"
        
        # Multiplier check for is_adjusted flag
        mult = 1.0
        if entity_id:
            mult = await _get_cumulative_multiplier(entity_id, yr, mo, cur)

        monthly_trend.append(MonthlyHoldingData(
            month=month_str,
            total_shares=adj_shares,
            num_funds=int(funds),
            trend=trend,
            is_adjusted=mult > 1.001 or mult < 0.999
        ))
    
    # 4. Detailed scheme-wise holdings history
    cur.execute(
        f"""
        SELECT 
            s.scheme_id,
            s.scheme_name,
            a.amc_name,
            s.plan_type,
            s.option_type,
            ss.total_value_inr / 10000000.0 as aum_cr,
            SUM(eh.percent_of_nav) as percent_of_nav,
            SUM(eh.quantity) as quantity,
            p.period_id,
            p.year,
            p.month
        FROM equity_holdings eh
        JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
        JOIN schemes s ON ss.scheme_id = s.scheme_id
        JOIN amcs a ON s.amc_id = a.amc_id
        JOIN periods p ON ss.period_id = p.period_id
        JOIN companies c ON eh.company_id = c.company_id
        WHERE {filter_clause} AND p.period_id IN ({','.join(['%s']*len(all_period_ids))})
        GROUP BY s.scheme_id, s.scheme_name, a.amc_name, s.plan_type, s.option_type, ss.total_value_inr, p.period_id, p.year, p.month
        ORDER BY s.scheme_id, p.year DESC, p.month DESC
        """,
        (filter_val, *all_period_ids)
    )
    
    raw_holdings = cur.fetchall()
    
    # Pre-calculate multipliers and organize data
    period_multipliers = {}
    if entity_id:
        for yr, mo in all_target_periods:
            period_multipliers[(yr, mo)] = await _get_cumulative_multiplier(entity_id, yr, mo, cur)

    schemes_data = {}
    for row in raw_holdings:
        sid, sname, amc, plan, opt, aum, pnav, qty, pid, yr, mo = row
        mult = period_multipliers.get((yr, mo), 1.0)
        adj_qty = int(qty * mult)

        if sid not in schemes_data:
            schemes_data[sid] = {
                "name": sname, "amc": amc, "plan": plan, "opt": opt,
                "aum": Decimal(str(aum)).quantize(Decimal('0.01')),
                "monthly_data": {} # (yr, mo) -> {qty, pnav}
            }
        schemes_data[sid]["monthly_data"][(yr, mo)] = {
            "qty": adj_qty, "pnav": Decimal(str(pnav)).quantize(Decimal('0.0001')),
            "is_adjusted": mult > 1.001 or mult < 0.999
        }

    holdings = []
    for sid, data in schemes_data.items():
        history = []
        for idx, (yr, mo) in enumerate(display_periods):
            month_str = date(yr, mo, 1).strftime("%b-%y").upper()
            m_data = data["monthly_data"].get((yr, mo), {"qty": 0, "pnav": Decimal('0'), "is_adjusted": False})
            
            trend = None
            prev_idx = idx + 1
            if prev_idx < len(all_target_periods):
                prev_p = all_target_periods[prev_idx]
                prev_m_data = data["monthly_data"].get(prev_p)
                if prev_m_data:
                    if m_data["qty"] > prev_m_data["qty"]: trend = "up"
                    elif m_data["qty"] < prev_m_data["qty"]: trend = "down"
                    elif m_data["qty"] > 0: trend = "same"
                else:
                    if m_data["qty"] > 0: trend = "up"

            history.append(HistoricalHolding(
                month=month_str,
                num_shares=m_data["qty"],
                percent_to_aum=m_data["pnav"],
                trend=trend,
                is_adjusted=m_data.get("is_adjusted", False)
            ))

        if history:
            # We sort by the latest month's percent to nav
            latest_nav = data["monthly_data"].get((latest_yr, latest_mo), {}).get("pnav", Decimal('0'))
            holdings.append((latest_nav, SchemeHolding(
                scheme_name=data["name"],
                amc_name=data["amc"],
                plan_type=data["plan"],
                option_type=data["opt"],
                aum_cr=data["aum"],
                history=history
            )))

    # Sort holdings by latest percent_to_nav desc
    holdings.sort(key=lambda x: x[0], reverse=True)
    final_holdings = [h[1] for h in holdings]
    
    return StockHoldingsSummary(
        isin=isin,
        company_name=company_name.upper(),
        sector=sector,
        market_cap=Decimal(str(market_cap / 10000000)).quantize(Decimal('0.01')) if market_cap else None,
        mcap_type=mcap_type,
        mcap_updated_at=mcap_updated_at,
        as_of_date=current_month_label,
        total_shares=total_shares_current,
        total_funds=num_funds_current,
        monthly_trend=monthly_trend,
        holdings=final_holdings
    )
