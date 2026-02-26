"""
Scheme portfolio endpoints.

Provides APIs for searching schemes and viewing their equity portfolios.
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends, Path
from psycopg2.extensions import cursor
from datetime import date, datetime
import calendar
from decimal import Decimal
from collections import defaultdict

from src.api.models.schemes import (
    SchemeSearchResult,
    SchemePortfolioSummary,
    SchemeSearchResponse,
    PortfolioHolding,
    MonthlyHoldingSnapshot
)
from src.api.dependencies import get_db_cursor
from src.config import logger

print("LOADING SCHEMES ROUTER WITH FIX v2 (ISIN_MASTER JOIN) ...")
logger.info("LOADING SCHEMES ROUTER WITH FIX v2 (ISIN_MASTER JOIN) ...")

router = APIRouter()


@router.get("/search", response_model=SchemeSearchResponse)
async def search_schemes(
    q: str = Query(..., min_length=2, description="Search query (scheme name or AMC name)"),
    limit: int = Query(5000, ge=1, le=10000, description="Maximum number of results"),
    cur: cursor = Depends(get_db_cursor)
):
    """
    Search for mutual fund schemes by name or AMC.
    
    Supports ultra-fuzzy multi-word matching on scheme names and AMC names with similarity ranking.
    """
    try:
        # Clean query
        q = q.strip()
        words = [w.strip() for w in q.split() if w.strip()]
        if not words:
            return SchemeSearchResponse(query=q, results=[], total_results=0)

        # Build conditions: Each word must match via ILIKE on scheme or AMC
        placeholders = []
        conditions = []
        for word in words:
            pattern = f"%{word}%"
            conditions.append("(s.scheme_name ILIKE %s OR a.amc_name ILIKE %s)")
            placeholders.extend([pattern, pattern])
        
        conditions_str = " AND ".join(conditions)
        
        # word_placeholders contains 2 * len(words) entries
        # query_placeholders order:
        # 1. CASE ranking (3 %s)
        # 2. WHERE word conditions (len(placeholders) %s)
        # 3. WHERE OR condition (2 %s)
        # 4. LIMIT (1 %s)
        query_placeholders = [
            q, q + '%', q  # CASE ranking
        ] + placeholders + [
            q, f'%{q}%',   # WHERE OR
            limit          # LIMIT
        ]

        # Original query placeholders for CASE relevance + similarity
        cur.execute(
            f"""
            SELECT 
                s.scheme_id,
                s.scheme_name,
                a.amc_name,
                s.plan_type,
                s.option_type,
                CASE 
                    WHEN UPPER(s.scheme_name) = UPPER(%s) THEN 100
                    WHEN s.scheme_name ILIKE %s THEN 85
                    ELSE (similarity(s.scheme_name, %s) * 80)::int
                END as relevance_score
            FROM schemes s
            JOIN amcs a ON s.amc_id = a.amc_id
            WHERE ({conditions_str}) OR (s.scheme_name %% %s OR s.scheme_name ILIKE %s)
            ORDER BY 
                relevance_score DESC,
                s.scheme_name,
                s.plan_type,
                s.option_type
            LIMIT %s
            """,
            query_placeholders
        )
        
        results = []
        for row in cur.fetchall():
            results.append(SchemeSearchResult(
                scheme_id=row[0],
                scheme_name=row[1],
                amc_name=row[2],
                plan_type=row[3],
                option_type=row[4],
                category=None
            ))
        
        return SchemeSearchResponse(
            query=q,
            results=results,
            total_results=len(results)
        )
        
    except Exception as e:
        logger.error(f"Scheme search failed: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


def _resolve_scheme_id(identifier: str, cur: cursor) -> tuple[int, str, str, str, str]:
    """
    Resolve a scheme identifier (ID or name) to scheme details.
    
    Args:
        identifier: Scheme ID (as string) or scheme name
        cur: Database cursor
        
    Returns:
        Tuple of (scheme_id, scheme_name, amc_name, plan_type, option_type)
        
    Raises:
        HTTPException: If scheme not found or multiple matches found
    """
    # Try as numeric ID first
    try:
        scheme_id = int(identifier)
        cur.execute(
            """
            SELECT 
                s.scheme_id,
                s.scheme_name,
                a.amc_name,
                s.plan_type,
                s.option_type
            FROM schemes s
            JOIN amcs a ON s.amc_id = a.amc_id
            WHERE s.scheme_id = %s
            """,
            (scheme_id,)
        )
        result = cur.fetchone()
        if result:
            return result
    except ValueError:
        # Not a numeric ID, continue to name search
        pass
    
    # Try fuzzy scheme name match
    search_pattern = f"%{identifier}%"
    cur.execute(
        """
        SELECT 
            s.scheme_id,
            s.scheme_name,
            a.amc_name,
            s.plan_type,
            s.option_type
        FROM schemes s
        JOIN amcs a ON s.amc_id = a.amc_id
        WHERE s.scheme_name ILIKE %s
        ORDER BY 
            CASE 
                WHEN UPPER(s.scheme_name) = UPPER(%s) THEN 1
                WHEN s.scheme_name ILIKE %s THEN 2
                ELSE 3
            END,
            s.scheme_name,
            s.plan_type,
            s.option_type
        LIMIT 2
        """,
        (search_pattern, identifier, identifier + '%')
    )
    results = cur.fetchall()
    
    if not results:
        raise HTTPException(
            status_code=404, 
            detail=f"No scheme found matching '{identifier}'. Try searching first using /api/v1/schemes/search"
        )
    
    if len(results) > 1:
        # Multiple matches - return helpful error
        matches = [f"{r[1]} ({r[2]}, {r[3]}, {r[4]})" for r in results]
        raise HTTPException(
            status_code=400,
            detail=f"Multiple schemes match '{identifier}': {', '.join(matches[:3])}. Please use exact scheme name or scheme ID."
        )
    
    return results[0]


@router.get("/portfolio", response_model=SchemePortfolioSummary)
async def get_portfolio_by_identifier(
    q: str = Query(..., min_length=2, description="Scheme identifier (scheme ID or scheme name)"),
    months: int = Query(4, ge=1, le=12, description="Number of months to show"),
    end_month: Optional[str] = Query(None, description="Optional end month in MMM-YY format (e.g. 'NOV-25')"),
    cur: cursor = Depends(get_db_cursor)
):
    """
    Get portfolio holdings by scheme name or ID (flexible identifier).
    
    Accepts scheme ID or scheme name. Uses fuzzy matching for scheme names.
    
    Shows equity holdings across the last N months with trend data.
    
    Args:
        q: Scheme identifier (scheme ID or scheme name)
        months: Number of months for comparison (default: 4)
    
    Returns:
        Complete portfolio summary with monthly holdings comparison
        
    Examples:
        - /api/v1/schemes/portfolio?q=1724
        - /api/v1/schemes/portfolio?q=quant%20multi%20cap
        - /api/v1/schemes/portfolio?q=aditya%20birla%20liquid
    """
    try:
        # Resolve identifier to scheme details
        scheme_id, scheme_name, amc_name, plan_type, option_type = _resolve_scheme_id(q, cur)
        
        # Get portfolio using the resolved scheme_id
        return await _get_scheme_portfolio_by_id(
            scheme_id, scheme_name, amc_name, plan_type, option_type, months, end_month, cur
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch portfolio for '{q}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch portfolio: {str(e)}")


@router.get("/{scheme_id}/portfolio", response_model=SchemePortfolioSummary)
async def get_scheme_portfolio(
    scheme_id: int = Path(..., description="Scheme ID"),
    months: int = Query(4, ge=1, le=12, description="Number of months to show"),
    end_month: Optional[str] = Query(None, description="Optional end month in MMM-YY format (e.g. 'NOV-25')"),
    cur: cursor = Depends(get_db_cursor)
):
    """
    Get portfolio holdings for a specific scheme by ID with monthly comparison.
    
    Shows equity holdings across the last N months with trend data.
    
    Args:
        scheme_id: Internal scheme ID
        months: Number of months for comparison (default: 4)
    
    Returns:
        Complete portfolio summary with monthly holdings comparison
    """
    try:
        # Get scheme details
        cur.execute(
            """
            SELECT 
                s.scheme_id,
                s.scheme_name,
                a.amc_name,
                s.plan_type,
                s.option_type
            FROM schemes s
            JOIN amcs a ON s.amc_id = a.amc_id
            WHERE s.scheme_id = %s
            """,
            (scheme_id,)
        )
        scheme_row = cur.fetchone()
        
        if not scheme_row:
            raise HTTPException(status_code=404, detail=f"Scheme with ID {scheme_id} not found")
        
        scheme_id, scheme_name, amc_name, plan_type, option_type = scheme_row
        
        return await _get_scheme_portfolio_by_id(
            scheme_id, scheme_name, amc_name, plan_type, option_type, months, end_month, cur
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch portfolio for scheme {scheme_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch portfolio: {str(e)}")


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
    
    return float(multiplier)


async def _get_scheme_portfolio_by_id(
    scheme_id: int,
    scheme_name: str,
    amc_name: str,
    plan_type: str,
    option_type: str,
    months: int,
    end_month: Optional[str],
    cur: cursor
) -> SchemePortfolioSummary:
    """
    Internal function to get portfolio data by scheme ID.
    
    Args:
        scheme_id: Scheme ID
        scheme_name: Scheme name
        amc_name: AMC name
        plan_type: Plan type
        option_type: Option type
        months: Number of months for comparison
        cur: Database cursor
        
    Returns:
        SchemePortfolioSummary
    """
    # Always return exactly 4 months window as requested by user
    # 1. Determine the anchor (latest) period
    latest_yr, latest_mo = None, None
    if end_month:
        try:
            # Parse 'MMM-YY' like 'NOV-25' -> year 2025, month 11
            d = datetime.strptime(end_month, "%b-%y")
            latest_yr = d.year
            latest_mo = d.month
        except ValueError:
            pass
            
    if not latest_yr:
        cur.execute("SELECT year, month FROM periods ORDER BY year DESC, month DESC LIMIT 1")
        latest_row = cur.fetchone()
        if not latest_row:
            raise HTTPException(status_code=404, detail="No portfolio data available")
        latest_yr, latest_mo = latest_row
    
    # 2. Generate exactly 4 months ending at latest_yr, latest_mo
    target_months = [] # list of (year, month)
    curr_yr, curr_mo = latest_yr, latest_mo
    for _ in range(4):
        target_months.append((curr_yr, curr_mo))
        curr_mo -= 1
        if curr_mo == 0:
            curr_mo = 12
            curr_yr -= 1
    
    # Reverse to show oldest to newest in UI
    target_months.reverse()
    
    # 3. Resolve these months to period_ids if they exist
    period_ids_map = {} # (year, month) -> period_id
    for yr, mo in target_months:
        cur.execute("SELECT period_id FROM periods WHERE year = %s AND month = %s", (yr, mo))
        p_row = cur.fetchone()
        if p_row:
            period_ids_map[(yr, mo)] = p_row[0]
            
    period_ids_list = [pid for pid in period_ids_map.values()]
    
    # Get snapshots map for efficient lookup (and to know what truly exists in DB)
    cur.execute(
        """
        SELECT 
            period_id,
            total_value_inr / 10000000.0 as aum_cr,
            total_holdings
        FROM scheme_snapshots
        WHERE scheme_id = %s AND period_id = ANY(%s)
        """,
        (scheme_id, period_ids_list)
    )
    snapshots_map = {row[0]: row for row in cur.fetchall()}
    
    # Track valid snapshots explicitly
    valid_snapshots = set()
    for pid in snapshots_map.keys():
        valid_snapshots.add(pid)
    
    monthly_aum = []
    for yr, mo in target_months:
        month_label = date(yr, mo, 1).strftime("%b-%y").upper()
        pid = period_ids_map.get((yr, mo))
        
        if pid and pid in snapshots_map:
            sn = snapshots_map[pid]
            monthly_aum.append({
                "month": month_label,
                "aum_cr": float(Decimal(str(sn[1])).quantize(Decimal('0.01'))),
                "total_holdings": sn[2]
            })
        else:
             monthly_aum.append({
                "month": month_label,
                "aum_cr": 0.0,
                "total_holdings": 0
            })
    
    # Get scheme metadata (category)
    cur.execute(
        "SELECT scheme_category FROM schemes WHERE scheme_id = %s",
        (scheme_id,)
    )
    meta_row = cur.fetchone()
    category_str = meta_row[0] if meta_row and meta_row[0] else None

    # Get all holdings across all periods
    # We use COALESCE(c.entity_id, -c.company_id) to group by a logical stable identity
    # resolving variations across months or multiple ISINs in the same month.
    cur.execute(
        """
        SELECT 
            COALESCE(im.entity_id, c.entity_id, -c.company_id) as logical_id,
            COALESCE(ce.canonical_name, im.canonical_name) as resolved_name,
            c.company_name as raw_name,
            COALESCE(ce.sector, im.sector, c.sector) as sector,
            c.isin,
            p.year,
            p.month,
            ss.total_value_inr / 10000000.0 as aum_cr_val,
            eh.percent_of_nav,
            eh.quantity,
            c.market_cap,
            c.mcap_type,
            c.shares_outstanding
        FROM equity_holdings eh
        JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
        JOIN companies c ON eh.company_id = c.company_id
        LEFT JOIN isin_master im ON c.isin = im.isin
        LEFT JOIN corporate_entities ce ON COALESCE(im.entity_id, c.entity_id) = ce.entity_id
        JOIN periods p ON ss.period_id = p.period_id
        WHERE ss.scheme_id = %s AND p.period_id = ANY(%s)
        ORDER BY logical_id, p.year DESC, p.month DESC
        """,
        (scheme_id, period_ids_list)
    )
    rows = cur.fetchall()
    
    # PREFETCH LIVE PRICES CONCURRENTLY to avoid 30sec+ loads
    # row[4] is the isin
    unique_isins = list(set([row[4] for row in rows if row[4]]))
    if unique_isins:
        from src.services.pricing_service import pricing_service
        pricing_service.prefetch_ltps(unique_isins)
        
    # Group holdings by entity and month
    # Structure: logical_id -> { metadata, monthly_data: { month_str -> { aggregated_metrics } } }
    holdings_by_entity = defaultdict(lambda: {
        "entity_id": None,
        "isin": None,
        "company_name": None,
        "sector": None,
        "market_cap": None,
        "mcap_type": None,
        "monthly_data": defaultdict(lambda: {"percent": 0.0, "quantity": 0, "aum_cr": 0.0})
    })
    
    for row in rows:
        logical_id, resolved_name, raw_name, sector, isin, year, month, aum_cr, percent_nav, quantity, m_cap, m_type, shares_out = row
        month_str = date(year, month, 1).strftime("%b-%y").upper()
        
        entry = holdings_by_entity[logical_id]
        if not entry["company_name"]:
             entry["entity_id"] = logical_id if logical_id > 0 else None
             entry["company_name"] = (resolved_name or raw_name).upper()
             entry["isin"] = isin
             entry["sector"] = sector
             
             from src.services.pricing_service import pricing_service
             live_mcap = pricing_service.get_live_market_cap(isin, float(m_cap) if m_cap else None, shares_out)
             
             entry["market_cap"] = Decimal(str(live_mcap / 10000000)).quantize(Decimal('0.01')) if live_mcap else None
             entry["mcap_type"] = m_type
            
        # Aggregate (sum) if multiple rows for same entity in same month
        entry["monthly_data"][month_str]["percent"] += float(percent_nav)
        entry["monthly_data"][month_str]["quantity"] += int(quantity)
        entry["monthly_data"][month_str]["aum_cr"] = float(aum_cr)
    
    # Build portfolio holdings list
    holdings = []
    for entity_data in holdings_by_entity.values():
        monthly_snapshots = []
        
        # Calculate multipliers for this entity across all months in target window
        multipliers = {}
        if entity_data["entity_id"]:
            for yr, mo in target_months:
                multipliers[date(yr, mo, 1).strftime("%b-%y").upper()] = await _get_cumulative_multiplier(entity_data["entity_id"], yr, mo, cur)
        
        # Create snapshots for each month in the 4-month target window
        for year, month in target_months:
            month_str = date(year, month, 1).strftime("%b-%y").upper()
            pid = period_ids_map.get((year, month))
            
            # Scenario A: Snapshot exists, and holding data exists for it
            if month_str in entity_data["monthly_data"]:
                data = entity_data["monthly_data"][month_str]
                multiplier = multipliers.get(month_str, 1.0)
                adjusted_qty = int(data["quantity"] * multiplier)
                
                monthly_snapshots.append(MonthlyHoldingSnapshot(
                    month=month_str,
                    aum_cr=Decimal(str(data["aum_cr"])).quantize(Decimal('0.01')),
                    percent_to_aum=Decimal(str(data["percent"])).quantize(Decimal('0.0001')),
                    num_shares=adjusted_qty,
                    is_adjusted=multiplier > 1.001 or multiplier < 0.999
                ))
            # Scenario B: Snapshot exists, but holding NOT present (0 shares now)
            elif pid in valid_snapshots:
                sn = snapshots_map[pid]
                monthly_snapshots.append(MonthlyHoldingSnapshot(
                    month=month_str,
                    aum_cr=Decimal(str(sn[1])).quantize(Decimal('0.01')),
                    percent_to_aum=Decimal('0.0'),
                    num_shares=0,
                    is_adjusted=False
                ))
            # Scenario C: Snapshot DOES NOT exist (AMC data not uploaded)
            else:
                monthly_snapshots.append(MonthlyHoldingSnapshot(
                    month=month_str,
                    aum_cr=Decimal('0.0'),
                    percent_to_aum=Decimal('0.0'),
                    num_shares=None,
                    is_adjusted=False
                ))
        
        # Only include if holding exists in at least one month
        if monthly_snapshots:
            holdings.append(PortfolioHolding(
                entity_id=entity_data["entity_id"],
                isin=entity_data["isin"],
                company_name=entity_data["company_name"],
                sector=entity_data["sector"],
                market_cap=entity_data["market_cap"],
                mcap_type=entity_data["mcap_type"],
                monthly_data=monthly_snapshots
            ))
    
    # Sort by latest month's % to AUM (descending)
    holdings.sort(
        key=lambda h: h.monthly_data[-1].percent_to_aum if h.monthly_data else 0,
        reverse=True
    )
    
    return SchemePortfolioSummary(
        scheme_id=scheme_id,
        scheme_name=scheme_name.upper(),
        amc_name=amc_name.upper(),
        plan_type=plan_type.upper(),
        option_type=option_type.upper(),
        category=category_str, # Use the dynamic category string
        monthly_aum=monthly_aum,
        holdings=holdings,
        total_holdings=len(holdings)
    )

