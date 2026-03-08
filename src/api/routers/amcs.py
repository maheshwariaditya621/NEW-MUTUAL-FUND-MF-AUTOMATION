"""
AMC (Asset Management Company) explorer endpoints.

Provides APIs for browsing fund houses and their respective schemes.
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends, Path
from psycopg2.extensions import cursor
from decimal import Decimal
from datetime import date

from src.api.models.amcs import (
    AMCListResponse,
    AMCSummary,
    AMCTopHolding,
    AMCDetail,
    AMCSchemeItem
)
from src.api.dependencies import get_db_cursor, get_current_user
from src.config import logger

router = APIRouter()


@router.get("/search")
async def search_amcs(
    q: str = Query("", description="Search query for AMC name"),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Search for AMCs by name.
    """
    q = q.strip()
    if not q:
        cur.execute("SELECT amc_id, amc_name FROM amcs ORDER BY amc_name LIMIT 50")
    else:
        cur.execute(
            "SELECT amc_id, amc_name FROM amcs WHERE amc_name ILIKE %s ORDER BY amc_name LIMIT 50",
            (f"%{q}%",)
        )
    
    rows = cur.fetchall()
    return [{"amc_id": r[0], "amc_name": r[1]} for r in rows]

@router.get("", response_model=AMCListResponse)
async def list_amcs(
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    List all AMCs with aggregated AUM, scheme counts, and top holdings.
    
    Data is based on the latest available period.
    """
    try:
        # 1. Get the latest period
        cur.execute("SELECT period_id, year, month FROM periods ORDER BY year DESC, month DESC LIMIT 1")
        period_row = cur.fetchone()
        if not period_row:
            return AMCListResponse(amcs=[], total_count=0, last_updated_month="N/A")
            
        latest_period_id, year, month = period_row
        month_label = date(year, month, 1).strftime("%b-%y").upper()
        
        logger.info(f"Fetching AMC stats for period {latest_period_id} ({month_label})")

        # 2. Query AMC stats (AUM and Scheme count)
        cur.execute(
            """
            SELECT 
                a.amc_id,
                a.amc_name,
                COUNT(DISTINCT s.scheme_id) as scheme_count,
                SUM(COALESCE(ss.total_value_inr, 0)) / 10000000.0 as equity_aum_cr,
                SUM(COALESCE(ss.total_net_assets_inr, 0)) / 10000000.0 as total_aum_cr
            FROM amcs a
            LEFT JOIN schemes s ON a.amc_id = s.amc_id
            LEFT JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id AND ss.period_id = %s
            GROUP BY a.amc_id, a.amc_name
            HAVING SUM(COALESCE(ss.total_value_inr, 0)) > 0 OR SUM(COALESCE(ss.total_net_assets_inr, 0)) > 0
            ORDER BY total_aum_cr DESC
            """,
            (latest_period_id,)
        )
        amc_stats = cur.fetchall()
        logger.info(f"Found {len(amc_stats)} AMCs with data")

        # 3. Fetch top 3 holdings for EACH AMC
        cur.execute(
            """
            WITH amc_holdings AS (
                SELECT 
                    s.amc_id,
                    c.company_name,
                    SUM(eh.market_value_inr) as total_val
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                JOIN schemes s ON ss.scheme_id = s.scheme_id
                JOIN companies c ON eh.company_id = c.company_id
                WHERE ss.period_id = %s
                GROUP BY s.amc_id, c.company_name
            ),
            ranked_holdings AS (
                SELECT 
                    amc_id,
                    company_name,
                    total_val,
                    ROW_NUMBER() OVER (PARTITION BY amc_id ORDER BY total_val DESC) as rank
                FROM amc_holdings
            ),
            amc_equity_total AS (
                SELECT amc_id, SUM(total_val) as amc_total_val
                FROM amc_holdings
                GROUP BY amc_id
            )
            SELECT 
                rh.amc_id,
                rh.company_name,
                (rh.total_val / NULLIF(aet.amc_total_val, 0)) * 100 as pct
            FROM ranked_holdings rh
            JOIN amc_equity_total aet ON rh.amc_id = aet.amc_id
            WHERE rh.rank <= 3
            ORDER BY rh.amc_id, rh.total_val DESC
            """,
            (latest_period_id,)
        )
        
        top_holdings_rows = cur.fetchall()
        logger.info(f"Found {len(top_holdings_rows)} top holdings entries")
        
        top3_map = {}
        for row in top_holdings_rows:
            amc_id, comp_name, pct = row
            if amc_id not in top3_map:
                top3_map[amc_id] = []
            
            # Safe Decimal conversion
            pct_val = Decimal(str(pct or 0)).quantize(Decimal('0.01'))
            
            top3_map[amc_id].append(AMCTopHolding(
                company_name=comp_name,
                percent_of_amc_equity=pct_val
            ))

        # 4. Assemble results
        results = []
        for row in amc_stats:
            amc_id, amc_name, scount, eq_aum, tot_aum = row
            
            # Safe Decimal conversion for AUM
            eq_aum_val = Decimal(str(eq_aum or 0)).quantize(Decimal('0.01'))
            tot_aum_val = Decimal(str(tot_aum or 0)).quantize(Decimal('0.01'))
            
            # If tot_aum is null/0, fallback to eq_aum (for backward compatibility if data missing)
            if tot_aum_val == 0 and eq_aum_val > 0:
                tot_aum_val = eq_aum_val

            results.append(AMCSummary(
                amc_id=amc_id,
                amc_name=amc_name.upper(),
                scheme_count=scount,
                equity_aum_cr=eq_aum_val,
                total_aum_cr=tot_aum_val,
                top_holdings=top3_map.get(amc_id, [])
            ))

        return AMCListResponse(
            amcs=results,
            total_count=len(results),
            last_updated_month=month_label
        )

    except Exception as e:
        logger.error(f"Failed to list AMCs: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{amc_id}", response_model=AMCDetail)
async def get_amc_detail(
    amc_id: int = Path(..., description="Internal AMC ID"),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Get detailed profile for an AMC, including list of all its schemes.
    """
    logger.info(f"🏢 Fetching details for AMC ID: {amc_id}")
    try:
        # Get latest period for AUM
        cur.execute("SELECT period_id FROM periods ORDER BY year DESC, month DESC LIMIT 1")
        p_row = cur.fetchone()
        latest_period_id = p_row[0] if p_row else None

        # 1. Get AMC basic info
        cur.execute("SELECT amc_name FROM amcs WHERE amc_id = %s", (amc_id,))
        amc_row = cur.fetchone()
        if not amc_row:
            raise HTTPException(status_code=404, detail="AMC not found")
        
        amc_name = amc_row[0]

        # 2. Get all schemes for this AMC with latest AUM
        cur.execute(
            """
            SELECT 
                s.scheme_id,
                s.scheme_name,
                s.plan_type,
                s.option_type,
                s.scheme_category,
                COALESCE(ss.total_value_inr, 0) / 10000000.0 as equity_aum_cr,
                COALESCE(ss.total_net_assets_inr, 0) / 10000000.0 as total_aum_cr
            FROM schemes s
            LEFT JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id AND ss.period_id = %s
            WHERE s.amc_id = %s
            -- Filter out true dead schemes (no equity and no total AUM)
            AND (ss.total_value_inr > 0 OR ss.total_net_assets_inr > 0)
            ORDER BY total_aum_cr DESC, s.scheme_name ASC
            """,
            (latest_period_id, amc_id)
        )
        
        schemes = []
        total_eq_aum = Decimal('0.0')
        total_net_aum = Decimal('0.0')
        for row in cur.fetchall():
            eq_aum_val = Decimal(str(row[5] or 0)).quantize(Decimal('0.01'))
            tot_aum_val = Decimal(str(row[6] or 0)).quantize(Decimal('0.01'))
            
            if tot_aum_val == 0 and eq_aum_val > 0:
                tot_aum_val = eq_aum_val

            schemes.append(AMCSchemeItem(
                scheme_id=row[0],
                scheme_name=row[1],
                plan_type=row[2],
                option_type=row[3],
                category=row[4],
                equity_aum_cr=eq_aum_val,
                total_aum_cr=tot_aum_val
            ))
            total_eq_aum += eq_aum_val
            total_net_aum += tot_aum_val

        return AMCDetail(
            amc_id=amc_id,
            amc_name=amc_name.upper(),
            equity_aum_cr=total_eq_aum,
            total_aum_cr=total_net_aum,
            scheme_count=len(schemes),
            schemes=schemes
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get AMC details for {amc_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
