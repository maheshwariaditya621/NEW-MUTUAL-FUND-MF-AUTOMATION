"""
Stock holdings endpoints.

Provides APIs for searching stocks and viewing mutual fund holdings.
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends, Path, BackgroundTasks
from psycopg2.extensions import cursor
from decimal import Decimal

from src.api.models.stocks import (
    CompanySearchResult,
    StockHoldingsSummary,
    StockSearchResponse,
    SchemeHolding,
    MonthlyHoldingData,
    HistoricalHolding,
    BulkPriceRequest
)
from datetime import date, datetime
import calendar
from src.api.dependencies import get_db_cursor, get_current_user
from src.config import logger
from src.services.pricing_service import pricing_service
from src.services.symbol_mapper import symbol_mapper
from src.api.utils.data_coverage import get_period_coverage, get_data_warning

router = APIRouter()


@router.get("/search", response_model=StockSearchResponse)
async def search_stocks(
    q: str = Query(..., min_length=2, description="Search query (company name, ISIN, or NSE symbol)"),
    limit: int = Query(5000, ge=1, le=10000, description="Maximum number of results"),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Search for stocks by company name, ISIN, or NSE symbol.
    
    Supports ultra-fuzzy multi-word matching on all fields with similarity ranking.
    """
    try:
        # Clean query
        q = q.strip()
        logger.info(f"🔍 Searching for: '{q}' (limit={limit})")
        words = [w.strip() for w in q.split() if w.strip()]
        if not words:
            return StockSearchResponse(query=q, results=[], total_results=0)

        # Build conditions: Each word must match via ILIKE or 전체 query similarity
        # We use a combined approach: ILIKE for word-by-word precision + similarity for overall ranking
        word_placeholders = []
        conditions = []
        
        # Word-based ILIKE conditions (precision)
        for word in words:
            pattern = f"%{word}%"
            conditions.append("(c.company_name ILIKE %s OR c.isin ILIKE %s OR c.nse_symbol ILIKE %s)")
            word_placeholders.extend([pattern, pattern, pattern])
        
        conditions_str = " AND ".join(conditions)
        
        # Final set of placeholders for the query
        # 1. Ranking CASE (5 %s)
        # 2. WHERE word conditions (len(word_placeholders) %s)
        # 3. WHERE OR condition (3 %s)
        # 4. LIMIT (1 %s)
        query_placeholders = [
            q, q, q, q + '%', q  # CASE ranking
        ] + word_placeholders + [
            q, f'%{q}%', f'%{q}%', # WHERE OR
            limit # LIMIT
        ]

        # For ranking, use global similarity + field priority
        # Similarity priority is highest for company_name
        cur.execute(
            f"""
            SELECT DISTINCT ON (relevance_score, COALESCE(c.entity_id::text, c.company_id::text))
                c.isin,
                COALESCE(ce.canonical_name, c.company_name) as company_name,
                c.sector,
                c.nse_symbol,
                c.bse_code,
                c.entity_id,
                CASE 
                    WHEN UPPER(c.company_name) = UPPER(%s) THEN 100
                    WHEN c.isin = UPPER(%s) THEN 95
                    WHEN c.nse_symbol = UPPER(%s) THEN 90
                    WHEN c.company_name ILIKE %s THEN 85
                    ELSE (similarity(c.company_name, %s) * 80)::int
                END as relevance_score
            FROM companies c
            LEFT JOIN corporate_entities ce ON c.entity_id = ce.entity_id
            WHERE ({conditions_str}) OR (c.company_name %% %s OR c.isin ILIKE %s OR c.nse_symbol ILIKE %s)
            ORDER BY 
                relevance_score DESC,
                COALESCE(c.entity_id::text, c.company_id::text),
                c.updated_at DESC
            LIMIT %s
            """,
            query_placeholders
        )

        rows = cur.fetchall()
        
        results = []
        for row in rows:
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
                    bse_code=row[4],
                    entity_id=row[5]
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



def _resolve_company_isin(identifier: str, cur: cursor) -> tuple[str, str, Optional[str], Optional[Decimal], Optional[str], Optional[str], Optional[int], Optional[int], Optional[datetime]]:
    """
    Resolve a company identifier (ISIN, name, or symbol) to ISIN and metadata.
    
    Returns:
        Tuple of (isin, company_name, sector, market_cap, mcap_type, mcap_updated_at, shares_outstanding, shares_last_updated_at, entity_id)
    """
    # Use a common query structure to fetch metadata and entity_id
    query_base = """
        SELECT 
            c.isin, 
            COALESCE(ce.canonical_name, c.company_name) as company_name, 
            c.sector, 
            c.market_cap, 
            c.mcap_type, 
            c.mcap_updated_at, 
            c.shares_outstanding, 
            c.shares_last_updated_at,
            c.entity_id
        FROM companies c
        LEFT JOIN corporate_entities ce ON c.entity_id = ce.entity_id
    """

    # Try exact ISIN match first
    cur.execute(query_base + " WHERE c.isin = %s", (identifier.upper(),))
    result = cur.fetchone()
    if result:
        return result
    
    # Try exact NSE symbol match
    cur.execute(query_base + " WHERE c.nse_symbol = %s", (identifier.upper(),))
    result = cur.fetchone()
    if result:
        return result
    
    # Try fuzzy company name match
    search_pattern = f"%{identifier}%"
    cur.execute(
        """
        SELECT DISTINCT ON (relevance_score, COALESCE(c.entity_id::text, c.company_id::text))
            c.isin, 
            COALESCE(ce.canonical_name, c.company_name) as company_name, 
            c.sector, 
            c.market_cap, 
            c.mcap_type, 
            c.mcap_updated_at, 
            c.shares_outstanding, 
            c.shares_last_updated_at,
            c.entity_id,
            CASE 
                WHEN UPPER(c.company_name) = UPPER(%s) THEN 1
                WHEN c.company_name ILIKE %s THEN 2
                ELSE 3
            END as relevance_score
        FROM companies c
        LEFT JOIN corporate_entities ce ON c.entity_id = ce.entity_id
        WHERE c.company_name ILIKE %s
        ORDER BY 
            relevance_score,
            COALESCE(c.entity_id::text, c.company_id::text),
            c.updated_at DESC
        LIMIT 2
        """,
        (identifier, identifier + '%', search_pattern)
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


@router.get("/{isin}/price")
async def get_stock_price(
    isin: str = Path(..., description="12-character ISIN code"),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Get the latest live price (LTP) for a specific stock by ISIN.
    Designed for fast polling in the UI.
    """
    from src.services.symbol_mapper import symbol_mapper
    from src.services.pricing_service import pricing_service
    
    info = symbol_mapper.get_symbol_info(isin)
    if not info:
        # Fallback to DB if symbol mapper doesn't have it (likely not an equity or missing mapping)
        cur.execute("SELECT nse_symbol, bse_code FROM companies WHERE isin = %s", (isin,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Stock not found")
        return {"isin": isin, "price": None, "timestamp": datetime.now()}

    symbol, token, exchange = info
    ltp = pricing_service.get_ltp(exchange, symbol, token)
    
    return {
        "isin": isin,
        "price": ltp,
        "timestamp": datetime.now()
    }


@router.post("/prices")
async def get_bulk_prices(
    request: BulkPriceRequest,
    background_tasks: BackgroundTasks = None,
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Fetch live prices for a list of ISINs in bulk.
    Returns prices from cache immediately and triggers background fetch for missing ones.
    """
    from src.services.symbol_mapper import symbol_mapper
    from src.services.pricing_service import pricing_service
    
    results = {}
    missing_isins = []
    
    for isin in request.isins:
        info = symbol_mapper.get_symbol_info(isin)
        if info:
            symbol, token, exchange = info
            # Check cache directly to avoid blocking if not found
            cached = pricing_service._ltp_cache.get(symbol)
            if cached and (datetime.now().timestamp() - cached['timestamp'] < pricing_service.CACHE_TTL_SECONDS):
                results[isin] = cached['ltp']
            else:
                results[isin] = None
                missing_isins.append(isin)
        else:
            results[isin] = None

    if missing_isins and background_tasks:
        background_tasks.add_task(pricing_service.prefetch_ltps, missing_isins)
        
    return results


@router.get("/holdings", response_model=StockHoldingsSummary)
async def get_holdings_by_identifier(
    q: str = Query(..., min_length=2, description="Company identifier (ISIN, company name, or NSE symbol)"),
    months: int = Query(4, ge=1, le=12, description="Number of months to show trend"),
    end_month: Optional[str] = Query(None, description="Optional end month in MMM-YY format (e.g. 'NOV-25')"),
    background_tasks: BackgroundTasks = None,
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
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
        # 1. Resolve identifier to ISIN and metadata (already includes entity_id)
        isin, company_name, sector, mcap, mcap_type, mcap_updated, shares, shares_updated, entity_id = _resolve_company_isin(q, cur)
        
        # 3. Get holdings (aggregated by entity if available)
        return await _get_stock_holdings_aggregated(
            isin, entity_id, company_name, sector, mcap, mcap_type, mcap_updated, 
            shares, shares_updated, months, end_month, background_tasks, cur
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch holdings for '{q}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch holdings: {str(e)}")


@router.get("/{isin}/holdings", response_model=StockHoldingsSummary)
async def get_stock_holdings(
    isin: str = Path(..., description="12-character ISIN code"),
    months: int = Query(4, ge=1, le=12, description="Number of months to show trend"),
    end_month: Optional[str] = Query(None, description="Optional end month in MMM-YY format (e.g. 'NOV-25')"),
    background_tasks: BackgroundTasks = None,
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
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
        # Resolve company metadata using the helper function (already includes entity_id)
        res_isin, company_name, sector, mcap, mcap_type, mcap_updated, shares, shares_updated, entity_id = _resolve_company_isin(isin, cur)
        
        return await _get_stock_holdings_aggregated(
            res_isin, entity_id, company_name, sector, mcap, mcap_type, mcap_updated,
            shares, shares_updated, months, end_month, background_tasks, cur
        )
        
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
        WHERE entity_id = %s AND effective_date > %s AND status = 'CONFIRMED'
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
    mcap_updated_at: Optional[datetime],
    shares_outstanding: Optional[int],
    shares_last_updated_at: Optional[datetime],
    months: int,
    end_month: Optional[str],
    background_tasks: Optional[BackgroundTasks],
    cur: cursor
) -> StockHoldingsSummary:
    """
    Get holdings data aggregated by Corporate Entity (if available) or by single ISIN.
    Supports multi-month history for each scheme.
    """
    # Always use exactly 4 months window as requested by user
    # 1. Determine the anchor (latest) period, with partial data detection
    latest_yr, latest_mo = None, None
    user_specified_period = bool(end_month)  # track if user explicitly chose a month
    coverage = {"is_partial": False, "coverage_pct": 100.0}  # default
    data_warning = None

    if end_month:
        try:
            # Parse 'MMM-YY' like 'NOV-25' -> year 2025, month 11
            d = datetime.strptime(end_month, "%b-%y")
            latest_yr = d.year
            latest_mo = d.month
        except ValueError:
            pass
            
    if not latest_yr:
        # Check coverage to decide if we should use latest or fall back to prev
        coverage = get_period_coverage(cur)
        data_warning = get_data_warning(coverage)

        if coverage.get("is_partial") and coverage.get("prev"):
            # Use the previous complete period as the anchor for summary stats
            # but still show the partial latest month as a column (just with a warning)
            latest_yr = coverage["prev"]["year"]
            latest_mo = coverage["prev"]["month"]
            # We still want to show the partial latest month in the columns,
            # so we restore the actual latest as the first display period later
            actual_latest_yr = coverage["latest"]["year"]
            actual_latest_mo = coverage["latest"]["month"]
        else:
            actual_latest_yr = None
            cur.execute("SELECT year, month FROM periods ORDER BY year DESC, month DESC LIMIT 1")
            latest_row = cur.fetchone()
            if not latest_row:
                raise HTTPException(status_code=404, detail="No portfolio data available")
            latest_yr, latest_mo = latest_row
            actual_latest_yr = None  # no override needed
    else:
        actual_latest_yr = None

    current_month_label = date(latest_yr, latest_mo, 1).strftime("%b-%y").upper()
    
    # Use `months` parameter to control how many display periods to show
    # We fetch months+1 so we can compute month_change for the oldest visible month
    fetch_count = months + 1
    all_target_periods = []  # list of (year, month)
    curr_yr, curr_mo = latest_yr, latest_mo
    for _ in range(fetch_count):
        all_target_periods.append((curr_yr, curr_mo))
        curr_mo -= 1
        if curr_mo == 0:
            curr_mo = 12
            curr_yr -= 1

    # If data is partial, prepend the actual latest month so it shows as a column
    # (with warning) while summary stats use the previous complete period
    if actual_latest_yr and (actual_latest_yr, actual_latest_mo) not in all_target_periods:
        all_target_periods.insert(0, (actual_latest_yr, actual_latest_mo))

    # display_periods are the first `months` (newest to oldest)
    display_periods = all_target_periods[:months]
    
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
            SUM(COALESCE(eh.adj_quantity, eh.quantity)) as total_shares,
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
        
        # NOTE: Do NOT use idx==0 for summary stats when data is partial,
        # because idx==0 is the partial month (e.g. FEB-26).
        # We set summary stats from the anchor period (latest_yr, latest_mo)
        # which is the last COMPLETE month (e.g. JAN-26).
        if (yr, mo) == (latest_yr, latest_mo):
            total_shares_current = adj_shares
            num_funds_current = int(funds)
        
        # Calculate trend relative to previous month in all_target_periods
        trend = None
        month_change = None
        percent_change = None
        prev_idx = idx + 1
        if prev_idx < len(all_target_periods):
            adj_prev_shares = adj_shares_map[all_target_periods[prev_idx]]
            
            # Only calculate trend and changes if we have previous data (>0).
            # If prev is 0, it's likely the edge of our dataset, so we shouldn't show +100% or absolute leaps.
            if adj_prev_shares > 0:
                if adj_shares > adj_prev_shares: trend = "up"
                elif adj_shares < adj_prev_shares: trend = "down"
                else: trend = "same"
                
                month_change = adj_shares - adj_prev_shares
                percent_change = round((month_change / adj_prev_shares) * 100, 2)
        
        # Multiplier check for is_adjusted flag
        mult = 1.0
        if entity_id:
            mult = await _get_cumulative_multiplier(entity_id, yr, mo, cur)

        ownership_pct = None
        if shares_outstanding and adj_shares:
            ownership_pct = round((adj_shares / shares_outstanding) * 100, 4)

        monthly_trend.append(MonthlyHoldingData(
            month=month_str,
            total_shares=adj_shares,
            num_funds=int(funds),
            trend=trend,
            is_adjusted=mult > 1.001 or mult < 0.999,
            ownership_percent=ownership_pct,
            month_change=month_change,
            percent_change=percent_change
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
            p.year,
            p.month,
            COALESCE(eh.adj_quantity, eh.quantity) as quantity,
            eh.percent_of_nav,
            ss.total_net_assets_inr as sn_total_aum,
            ss.total_value_inr as sn_equity_aum
        FROM equity_holdings eh
        JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
        JOIN schemes s ON ss.scheme_id = s.scheme_id
        JOIN amcs a ON s.amc_id = a.amc_id
        JOIN periods p ON ss.period_id = p.period_id
        JOIN companies c ON eh.company_id = c.company_id
        WHERE {filter_clause} AND p.period_id IN ({','.join(['%s']*len(all_period_ids))})
        ORDER BY s.scheme_id, p.year DESC, p.month DESC
        """,
        (filter_val, *all_period_ids)
    )
    
    raw_holdings = cur.fetchall()
    
    # Identify which snapshots actually exist for these schemes to differentiate Missing vs zero holdings
    scheme_ids = list(set(row[0] for row in raw_holdings))
    valid_snapshots = set()
    if scheme_ids:
        cur.execute("""
            SELECT scheme_id, period_id 
            FROM scheme_snapshots 
            WHERE scheme_id = ANY(%s) AND period_id = ANY(%s)
        """, (scheme_ids, all_period_ids))
        valid_snapshots = set((row[0], row[1]) for row in cur.fetchall())
        
    # Pre-calculate multipliers and organize data
    period_multipliers = {}
    if entity_id:
        for yr, mo in all_target_periods:
            period_multipliers[(yr, mo)] = await _get_cumulative_multiplier(entity_id, yr, mo, cur)

    schemes_data = {}
    for row in raw_holdings:
        sid, sname, amc, plan, opt, eq_aum, tot_aum, pnav, qty, pid, yr, mo = row
        mult = period_multipliers.get((yr, mo), 1.0)
        adj_qty = int(qty * mult)

        eq_val = Decimal(str(eq_aum or 0)).quantize(Decimal('0.01'))
        tot_val = Decimal(str(tot_aum or 0)).quantize(Decimal('0.01'))
        if tot_val == 0 and eq_val > 0:
            tot_val = eq_val

        if sid not in schemes_data:
            schemes_data[sid] = {
                "name": sname, "amc": amc, "plan": plan, "opt": opt,
                "equity_aum_cr": eq_val,
                "total_aum_cr": tot_val,
                "monthly_data": {} # (yr, mo) -> {qty, pnav}
            }
        schemes_data[sid]["monthly_data"][(yr, mo)] = {
            "qty": adj_qty, "pnav": Decimal(str(pnav)).quantize(Decimal('0.0001')),
            "is_adjusted": mult > 1.001 or mult < 0.999
        }

    holdings = []
    for sid, data in schemes_data.items():
        # Pre-resolve qtys for all target periods
        month_qtys = {}
        for yr, mo in all_target_periods:
            pid = period_ids_map.get((yr, mo))
            has_snapshot = (sid, pid) in valid_snapshots if pid else False
            m_data = data["monthly_data"].get((yr, mo))
            
            if m_data:
                month_qtys[(yr, mo)] = m_data["qty"]
            elif has_snapshot:
                month_qtys[(yr, mo)] = 0
            else:
                month_qtys[(yr, mo)] = None

        history = []
        for idx, (yr, mo) in enumerate(display_periods):
            month_str = date(yr, mo, 1).strftime("%b-%y").upper()
            m_data = data["monthly_data"].get((yr, mo), {})
            
            curr_qty = month_qtys.get((yr, mo))
            pnav = m_data.get("pnav", Decimal('0'))
            is_adj = m_data.get("is_adjusted", False)
            
            trend = None
            month_change = None
            percent_change = None
            prev_idx = idx + 1
            if prev_idx < len(all_target_periods):
                prev_p = all_target_periods[prev_idx]
                prev_qty = month_qtys.get(prev_p)
                
                if curr_qty is not None and prev_qty is not None:
                    if curr_qty > prev_qty: trend = "up"
                    elif curr_qty < prev_qty: trend = "down"
                    elif curr_qty > 0: trend = "same"
                    
                    month_change = curr_qty - prev_qty
                    if prev_qty > 0:
                        percent_change = round((month_change / prev_qty) * 100, 2)
                    elif curr_qty > 0:
                        percent_change = None
                elif curr_qty is not None and prev_qty is None:
                    # Previous month not uploaded, so we can't show change
                    if curr_qty > 0: trend = "up"

            history.append(HistoricalHolding(
                month=month_str,
                num_shares=curr_qty,
                percent_to_aum=pnav,
                ownership_percent=round((curr_qty / shares_outstanding) * 100, 6) if shares_outstanding and curr_qty is not None else None,
                trend=trend,
                is_adjusted=is_adj,
                month_change=month_change,
                percent_change=percent_change
            ))

        if history:
            # We sort by the latest month's percent to nav
            latest_nav = data["monthly_data"].get((latest_yr, latest_mo), {}).get("pnav", Decimal('0'))
            holdings.append((latest_nav, SchemeHolding(
                scheme_name=data["name"],
                amc_name=data["amc"],
                plan_type=data["plan"],
                option_type=data["opt"],
                equity_aum_cr=data["equity_aum_cr"],
                total_aum_cr=data["total_aum_cr"],
                history=history
            )))

    # Sort holdings by latest percent_to_nav desc
    holdings.sort(key=lambda x: x[0] or Decimal('0'), reverse=True)
    final_holdings = [h[1] for h in holdings]

    # Dynamic Market Cap Calculation
    live_mcap = None
    if shares_outstanding:
        # Trigger prefetch for this ISIN in background
        if background_tasks:
            background_tasks.add_task(pricing_service.prefetch_ltps, [isin])
            
        live_mcap = pricing_service.get_live_market_cap(
            isin=isin, 
            db_mcap=float(market_cap) if market_cap else None, 
            shares_outstanding=shares_outstanding,
            sync=False
        )
    else:
        live_mcap = float(market_cap) if market_cap else None
    
    # If we got a live price, market_updated_at is now, else the db value
    if live_mcap and market_cap and float(live_mcap) != float(market_cap):
        mcap_updated_at = datetime.now()
        
    # Calculate live price data
    info = symbol_mapper.get_symbol_info(isin)
    live_price = None
    if info:
        symbol, token, exchange = info
        live_price = pricing_service.get_ltp(exchange, symbol, token, sync=False)

    return StockHoldingsSummary(
        isin=isin,
        company_name=company_name.upper(),
        sector=sector,
        market_cap=Decimal(str(live_mcap / 10000000)).quantize(Decimal('0.01')) if live_mcap else None,
        live_price=live_price,
        mcap_type=mcap_type,
        mcap_updated_at=mcap_updated_at,
        as_of_date=current_month_label,
        total_shares=total_shares_current,
        total_funds=num_funds_current,
        ownership_percent=round((total_shares_current / shares_outstanding) * 100, 4) if shares_outstanding and total_shares_current else None,
        monthly_trend=monthly_trend,
        holdings=final_holdings,
        shares_outstanding=shares_outstanding,
        shares_last_updated_at=shares_last_updated_at,
        data_warning=data_warning
    )
