from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from decimal import Decimal
from datetime import date
from collections import defaultdict
import calendar
from src.db.connection import get_cursor
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/comparison", tags=["Comparison"])

def get_last_day_of_month(year: int, month: int) -> str:
    """Format the last day of the month as 'Month DD, YYYY'."""
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day).strftime("%b %d, %Y")

@router.get("/periods")
async def get_available_periods(cur = Depends(get_cursor)):
    """Fetch all periods that have at least some snapshots."""
    cur.execute("""
        SELECT DISTINCT p.period_id, p.year, p.month
        FROM periods p
        JOIN scheme_snapshots sn ON p.period_id = sn.period_id
        ORDER BY p.year DESC, p.month DESC
    """)
    rows = cur.fetchall()
    return [
        {
            "period_id": r[0],
            "year": r[1],
            "month": r[2],
            "label": date(r[1], r[2], 1).strftime("%B %Y")
        } for r in rows
    ]

@router.get("/sector-exposure")
async def get_sector_comparison(
    scheme_ids: str = Query(..., description="Comma-separated list of scheme IDs"),
    period_id: Optional[int] = Query(None, description="Specific period ID to compare"),
    cur = Depends(get_cursor)
):
    try:
        ids = [int(i.strip()) for i in scheme_ids.split(",") if i.strip()]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid scheme_ids format")

    if not ids:
        raise HTTPException(status_code=400, detail="At least one scheme_id is required")
    
    if len(ids) > 5:
        raise HTTPException(status_code=400, detail="Maximum 5 schemes can be compared")

    # 1. Get Summary Data for each scheme
    # We want the latest snapshot for each
    summary = []
    sector_data = defaultdict(lambda: {f"fund_{idx+1}": 0.0 for idx in range(len(ids))})

    for idx, sid in enumerate(ids):
        # Get Latest Snapshot and Scheme Meta
        query = """
            SELECT 
                s.scheme_name, 
                s.website_sub_category,
                sn.total_value_inr / 10000000.0 as equity_aum,
                sn.total_net_assets_inr / 10000000.0 as total_aum,
                p.year, p.month,
                sn.snapshot_id
            FROM schemes s
            JOIN scheme_snapshots sn ON s.scheme_id = sn.scheme_id
            JOIN periods p ON sn.period_id = p.period_id
            WHERE s.scheme_id = %s
        """
        params = [sid]
        
        if period_id:
            query += " AND sn.period_id = %s"
            params.append(period_id)
        
        query += " ORDER BY p.year DESC, p.month DESC LIMIT 1"
        
        cur.execute(query, tuple(params))
        row = cur.fetchone()
        if not row:
            continue
            
        s_name, s_cat, eq_aum, tot_aum, yr, mo, snap_id = row
        as_of_date = get_last_day_of_month(yr, mo)

        # Get Top 10 %
        cur.execute("""
            SELECT SUM(percent_of_nav)
            FROM (
                SELECT percent_of_nav 
                FROM equity_holdings 
                WHERE snapshot_id = %s 
                ORDER BY percent_of_nav DESC 
                LIMIT 10
            ) t10
        """, (snap_id,))
        top_10_row = cur.fetchone()
        top_10_pct = float(top_10_row[0]) if top_10_row and top_10_row[0] else 0.0

        summary.append({
            "scheme_id": sid,
            "fund_name": s_name,
            "category": s_cat,
            "top_10_pct": top_10_pct,
            "equity_aum": float(eq_aum or 0),
            "total_aum": float(tot_aum or 0),
            "date": as_of_date
        })

        # Get Sector Exposure
        cur.execute("""
            SELECT 
                COALESCE(ce.sector, im.sector, c.sector) as sector_name,
                SUM(eh.percent_of_nav) as pct
            FROM equity_holdings eh
            JOIN companies c ON eh.company_id = c.company_id
            LEFT JOIN isin_master im ON c.isin = im.isin
            LEFT JOIN corporate_entities ce ON COALESCE(im.entity_id, c.entity_id) = ce.entity_id
            WHERE eh.snapshot_id = %s
            GROUP BY sector_name
            ORDER BY pct DESC
        """, (snap_id,))
        sec_rows = cur.fetchall()
        for sec_name, pct in sec_rows:
            label = sec_name or "OTHER"
            sector_data[label][f"fund_{idx+1}"] = float(pct)

    # Convert sector_data map to sorted list
    sorted_sectors = []
    # Sort sectors by highest exposure in fund 1, then fund 2...
    keys = sorted(sector_data.keys())
    for k in keys:
        entry = {"sector": k}
        entry.update(sector_data[k])
        sorted_sectors.append(entry)

    return {
        "summary": summary,
        "sectors": sorted_sectors
    }

@router.get("/sector-companies")
async def get_sector_companies(
    scheme_ids: str = Query(..., description="Comma-separated list of scheme IDs"),
    sector_name: str = Query(..., description="Name of the sector to drill down into"),
    period_id: Optional[int] = Query(None, description="Specific period ID"),
    cur = Depends(get_cursor)
):
    try:
        ids = [int(i.strip()) for i in scheme_ids.split(",") if i.strip()]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid scheme_ids format")

    company_map = defaultdict(lambda: {f"fund_{idx+1}": 0.0 for idx in range(len(ids))})
    
    for idx, sid in enumerate(ids):
        # 1. Get Latest Snapshot ID
        query = """
            SELECT sn.snapshot_id
            FROM scheme_snapshots sn
            JOIN periods p ON sn.period_id = p.period_id
            WHERE sn.scheme_id = %s
        """
        params = [sid]
        if period_id:
            query += " AND sn.period_id = %s"
            params.append(period_id)
            
        query += " ORDER BY p.year DESC, p.month DESC LIMIT 1"
        
        cur.execute(query, tuple(params))
        snap_row = cur.fetchone()
        if not snap_row:
            continue
        
        snap_id = snap_row[0]

        # 2. Get Companies in this Sector for this Snapshot
        # We use a subquery/join that mirrors the sector detection logic in the main comparison
        cur.execute("""
            SELECT 
                c.company_name,
                SUM(eh.percent_of_nav) as pct
            FROM equity_holdings eh
            JOIN companies c ON eh.company_id = c.company_id
            LEFT JOIN isin_master im ON c.isin = im.isin
            LEFT JOIN corporate_entities ce ON COALESCE(im.entity_id, c.entity_id) = ce.entity_id
            WHERE eh.snapshot_id = %s
              AND COALESCE(ce.sector, im.sector, c.sector) = %s
            GROUP BY c.company_name
            ORDER BY pct DESC
        """, (snap_id, sector_name))
        
        comp_rows = cur.fetchall()
        for c_name, pct in comp_rows:
            company_map[c_name][f"fund_{idx+1}"] = float(pct)

    # Convert to list and sort by total weight
    results = []
    for c_name, funds in company_map.items():
        total_weight = sum(funds.values())
        entry = {"company": c_name, "total_weight": total_weight}
        entry.update(funds)
        results.append(entry)

    # Sort by total weight descending
    results.sort(key=lambda x: x["total_weight"], reverse=True)

    return results

@router.get("/amc-sector-exposure")
async def get_amc_sector_comparison(
    amc_ids: str = Query(..., description="Comma-separated list of AMC IDs"),
    period_id: Optional[int] = Query(None, description="Specific period ID"),
    cur = Depends(get_cursor)
):
    try:
        ids = [int(i.strip()) for i in amc_ids.split(",") if i.strip()]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid amc_ids format")

    if not ids:
        raise HTTPException(status_code=400, detail="At least one amc_id is required")
    
    if len(ids) > 5:
        raise HTTPException(status_code=400, detail="Maximum 5 AMCs can be compared")

    # Get Latest Period
    cur.execute("SELECT period_id, year, month FROM periods ORDER BY year DESC, month DESC LIMIT 1")
    p_row = cur.fetchone()
    if not p_row:
        raise HTTPException(status_code=404, detail="No data periods found")
    
    latest_period_id, yr, mo = p_row
    as_of_date = date(yr, mo, 1).strftime("%b %d, %Y")

    summary = []
    sector_data = defaultdict(lambda: {f"fund_{idx+1}": 0.0 for idx in range(len(ids))})

    for idx, aid in enumerate(ids):
        # 1. Get the latest period FOR THIS SPECIFIC AMC
        query = """
            SELECT sn.period_id, p.year, p.month
            FROM scheme_snapshots sn
            JOIN schemes s ON sn.scheme_id = s.scheme_id
            JOIN periods p ON sn.period_id = p.period_id
            WHERE s.amc_id = %s
        """
        params = [aid]
        if period_id:
            query += " AND sn.period_id = %s"
            params.append(period_id)
            
        query += " ORDER BY p.year DESC, p.month DESC LIMIT 1"
        
        cur.execute(query, tuple(params))
        p_row_for_amc = cur.fetchone()
        if not p_row_for_amc:
            continue
            
        aid_period_id, aid_yr, aid_mo = p_row_for_amc
        aid_as_of = get_last_day_of_month(aid_yr, aid_mo)

        # 2. Get AMC Meta and Totals for their specific period
        cur.execute("""
            SELECT 
                a.amc_name,
                COUNT(DISTINCT s.scheme_id) as scheme_count,
                SUM(sn.total_value_inr) / 10000000.0 as equity_aum,
                SUM(sn.total_net_assets_inr) / 10000000.0 as total_aum
            FROM amcs a
            LEFT JOIN schemes s ON a.amc_id = s.amc_id
            LEFT JOIN scheme_snapshots sn ON s.scheme_id = sn.scheme_id AND sn.period_id = %s
            WHERE a.amc_id = %s
            GROUP BY a.amc_name
        """, (aid_period_id, aid))
        row = cur.fetchone()
        if not row:
            continue
            
        a_name, sc_count, eq_aum, tot_aum = row
        
        # 3. Get Top 10 Percent (aggregated across all schemes of the AMC)
        cur.execute("""
            WITH amc_companies AS (
                SELECT 
                    eh.company_id,
                    SUM(eh.market_value_inr) as market_val
                FROM equity_holdings eh
                JOIN scheme_snapshots sn ON eh.snapshot_id = sn.snapshot_id
                JOIN schemes s ON sn.scheme_id = s.scheme_id
                WHERE s.amc_id = %s AND sn.period_id = %s
                GROUP BY eh.company_id
                ORDER BY market_val DESC
                LIMIT 10
            )
            SELECT SUM(market_val) FROM amc_companies
        """, (aid, aid_period_id))
        top_10_row = cur.fetchone()
        top_10_val = float(top_10_row[0]) if top_10_row and top_10_row[0] else 0.0
        total_eq_val_inr = float(eq_aum or 0) * 10000000.0
        top_10_pct = (top_10_val / total_eq_val_inr * 100.0) if total_eq_val_inr > 0 else 0.0

        summary.append({
            "amc_id": aid,
            "amc_name": a_name.upper(),
            "scheme_count": sc_count,
            "top_10_pct": top_10_pct,
            "equity_aum": float(eq_aum or 0),
            "total_aum": float(tot_aum or 0),
            "date": aid_as_of
        })

        # 4. Get Sector Exposure (Aggregated)
        cur.execute("""
            SELECT 
                COALESCE(ce.sector, im.sector, c.sector) as sector_name,
                SUM(eh.market_value_inr) as val
            FROM equity_holdings eh
            JOIN scheme_snapshots sn ON eh.snapshot_id = sn.snapshot_id
            JOIN schemes s ON sn.scheme_id = s.scheme_id
            JOIN companies c ON eh.company_id = c.company_id
            LEFT JOIN isin_master im ON c.isin = im.isin
            LEFT JOIN corporate_entities ce ON COALESCE(im.entity_id, c.entity_id) = ce.entity_id
            WHERE s.amc_id = %s AND sn.period_id = %s
            GROUP BY sector_name
            ORDER BY val DESC
        """, (aid, aid_period_id))
        sec_rows = cur.fetchall()
        for sec_name, val in sec_rows:
            label = sec_name or "OTHER"
            pct = (float(val) / total_eq_val_inr * 100.0) if total_eq_val_inr > 0 else 0.0
            sector_data[label][f"fund_{idx+1}"] = pct

    # Format result
    keys = sorted(sector_data.keys())
    sorted_sectors = []
    for k in keys:
        entry = {"sector": k}
        entry.update(sector_data[k])
        sorted_sectors.append(entry)

    return {
        "summary": summary,
        "sectors": sorted_sectors
    }

@router.get("/amc-sector-companies")
async def get_amc_sector_companies(
    amc_ids: str = Query(..., description="Comma-separated list of AMC IDs"),
    sector_name: str = Query(..., description="Sector name to drill down into"),
    period_id: Optional[int] = Query(None, description="Specific period ID"),
    cur = Depends(get_cursor)
):
    try:
        ids = [int(i.strip()) for i in amc_ids.split(",") if i.strip()]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid amc_ids format")

    company_map = defaultdict(lambda: {f"fund_{idx+1}": 0.0 for idx in range(len(ids))})
    
    for idx, aid in enumerate(ids):
        # 1. Get the latest period FOR THIS SPECIFIC AMC
        query = """
            SELECT sn.period_id
            FROM scheme_snapshots sn
            JOIN schemes s ON sn.scheme_id = s.scheme_id
            JOIN periods p ON sn.period_id = p.period_id
            WHERE s.amc_id = %s
        """
        params = [aid]
        if period_id:
            query += " AND sn.period_id = %s"
            params.append(period_id)
            
        query += " ORDER BY p.year DESC, p.month DESC LIMIT 1"
        
        cur.execute(query, tuple(params))
        p_row_for_amc = cur.fetchone()
        if not p_row_for_amc:
            continue
            
        aid_period_id = p_row_for_amc[0]

        # 2. Total equity val for normalization
        cur.execute("""
            SELECT SUM(sn.total_value_inr)
            FROM scheme_snapshots sn
            JOIN schemes s ON sn.scheme_id = s.scheme_id
            WHERE s.amc_id = %s AND sn.period_id = %s
        """, (aid, aid_period_id))
        eq_row = cur.fetchone()
        amc_total_val = float(eq_row[0]) if eq_row and eq_row[0] else 0.0

        if amc_total_val <= 0:
            continue

        # 3. Get companies in this sector for this AMC in their specific period
        cur.execute("""
            SELECT 
                c.company_name,
                SUM(eh.market_value_inr) as val
            FROM equity_holdings eh
            JOIN scheme_snapshots sn ON eh.snapshot_id = sn.snapshot_id
            JOIN schemes s ON sn.scheme_id = s.scheme_id
            JOIN companies c ON eh.company_id = c.company_id
            LEFT JOIN isin_master im ON c.isin = im.isin
            LEFT JOIN corporate_entities ce ON COALESCE(im.entity_id, c.entity_id) = ce.entity_id
            WHERE s.amc_id = %s AND sn.period_id = %s
              AND COALESCE(ce.sector, im.sector, c.sector) = %s
            GROUP BY c.company_name
            ORDER BY val DESC
        """, (aid, aid_period_id, sector_name))
        
        comp_rows = cur.fetchall()
        for c_name, val in comp_rows:
            company_map[c_name][f"fund_{idx+1}"] = (float(val) / amc_total_val * 100.0)

    results = []
    for c_name, funds in company_map.items():
        total_weight = sum(funds.values())
        entry = {"company": c_name, "total_weight": total_weight}
        entry.update(funds)
        results.append(entry)

    results.sort(key=lambda x: x["total_weight"], reverse=True)
    return results
