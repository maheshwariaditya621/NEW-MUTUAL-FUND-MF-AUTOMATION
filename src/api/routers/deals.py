"""
Bulk & Block Deals API Router.

Endpoints:
1. GET /api/v1/deals - Paginated, filtered & sorted deals list
2. GET /api/v1/deals/summary - Aggregate market statistics, institutional flow breakdown
3. GET /api/v1/deals/status - Data health, sync checkpoints, and ingestion run history
4. POST /api/v1/deals/sync-now - Admin trigger for automated refresh
5. POST /api/v1/deals/upload-csv - Manual CSV file upload with validation & dry-run preview
"""

from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form, status
from pydantic import BaseModel
import psycopg2.extras

from src.api.dependencies import get_db_cursor, get_current_user, require_permission
from src.deals.engine import DealEngine
from src.config import logger

router = APIRouter()

ALLOWED_SORT_FIELDS = {
    "trade_date": "d.trade_date",
    "trade_value_inr": "d.trade_value_inr",
    "quantity": "d.quantity",
    "trade_price": "d.trade_price",
    "pct_equity_traded": "d.pct_equity_traded",
    "symbol": "d.symbol",
    "client_name": "d.client_name"
}


class CsvImportRequest(BaseModel):
    csv_content: str
    filename: Optional[str] = "upload.csv"
    deal_type: Optional[str] = None
    dry_run: bool = False


@router.get("", summary="Get paginated Bulk & Block Deals")
async def get_deals(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    deal_type: Optional[str] = None,         # BULK, BLOCK, or ALL
    exchange: Optional[str] = None,          # NSE, BSE, or ALL
    action: Optional[str] = None,             # BUY, SELL
    investor_category: Optional[str] = None, # MUTUAL_FUND, FII_FPI, DII, PROMOTER, AIF, OTHER
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    symbol: Optional[str] = None,
    isin: Optional[str] = None,
    company_id: Optional[int] = None,
    client_name: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = Query("trade_date", pattern="^(trade_date|trade_value_inr|quantity|trade_price|pct_equity_traded|symbol|client_name)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc|ASC|DESC)$"),
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_deals"))
):
    """
    Query normalized bulk and block deals with high-performance filtering and sorting.
    """
    offset = (page - 1) * limit
    conditions = ["1=1"]
    params: List[Any] = []

    if deal_type and deal_type.upper() != "ALL":
        conditions.append("d.deal_type = %s")
        params.append(deal_type.upper())

    if exchange and exchange.upper() != "ALL":
        conditions.append("d.exchange = %s")
        params.append(exchange.upper())

    if action and action.upper() != "ALL":
        conditions.append("d.action = %s")
        params.append(action.upper())

    if investor_category and investor_category.upper() != "ALL":
        conditions.append("d.investor_category = %s")
        params.append(investor_category.upper())

    if from_date:
        conditions.append("d.trade_date >= %s")
        params.append(from_date)

    if to_date:
        conditions.append("d.trade_date <= %s")
        params.append(to_date)

    # Group stock identifier filters with OR so records with missing ISIN or alternate symbol match
    stock_filters = []
    if symbol:
        stock_filters.append("d.symbol = %s")
        params.append(symbol.strip().upper())
    if isin:
        clean_isin = isin.strip().upper()
        stock_filters.append("""(
            d.isin = %s 
            OR (d.company_id IS NOT NULL AND d.company_id = (SELECT company_id FROM companies WHERE isin = %s LIMIT 1))
            OR (d.symbol IS NOT NULL AND d.symbol = (SELECT nse_symbol FROM companies WHERE isin = %s AND nse_symbol IS NOT NULL LIMIT 1))
            OR (d.symbol IS NOT NULL AND d.symbol = (SELECT bse_code FROM companies WHERE isin = %s AND bse_code IS NOT NULL LIMIT 1))
        )""")
        params.extend([clean_isin, clean_isin, clean_isin, clean_isin])
    if company_id:
        stock_filters.append("d.company_id = %s")
        params.append(company_id)
    if stock_filters:
        conditions.append(f"({' OR '.join(stock_filters)})")

    if client_name:
        conditions.append("d.client_name ILIKE %s")
        params.append(f"%{client_name.strip()}%")

    if search and search.strip():
        s_pat = f"%{search.strip()}%"
        conditions.append(
            "(d.symbol ILIKE %s OR d.security_name ILIKE %s OR d.client_name ILIKE %s OR d.isin ILIKE %s OR c.company_name ILIKE %s)"
        )
        params.extend([s_pat, s_pat, s_pat, s_pat, s_pat])

    where_clause = " AND ".join(conditions)
    sort_column = ALLOWED_SORT_FIELDS.get(sort_by, "d.trade_date")
    sort_dir = "DESC" if sort_order.lower() == "desc" else "ASC"

    # Count total matching rows
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM market_deals d
        LEFT JOIN companies c ON d.company_id = c.company_id
        WHERE {where_clause}
        """,
        params
    )
    total_count = cur.fetchone()[0]

    # Fetch rows
    fetch_params = params + [limit, offset]
    cur.execute(
        f"""
        SELECT 
            d.deal_id,
            d.deal_type,
            d.exchange,
            d.trade_date,
            d.symbol,
            d.security_name,
            d.client_name,
            d.investor_category,
            d.action,
            d.quantity,
            d.trade_price,
            d.trade_value_inr,
            d.pct_equity_traded,
            d.isin,
            d.company_id,
            d.source_name,
            d.ingestion_mode,
            c.company_name,
            c.sector,
            c.shares_outstanding,
            c.market_cap
        FROM market_deals d
        LEFT JOIN companies c ON d.company_id = c.company_id
        WHERE {where_clause}
        ORDER BY {sort_column} {sort_dir}, d.deal_id DESC
        LIMIT %s OFFSET %s
        """,
        fetch_params
    )

    rows = cur.fetchall()
    deals_list = []
    for r in rows:
        deals_list.append({
            "deal_id": r[0],
            "deal_type": r[1],
            "exchange": r[2],
            "trade_date": r[3].isoformat() if r[3] else None,
            "symbol": r[4],
            "security_name": r[5],
            "client_name": r[6],
            "investor_category": r[7],
            "action": r[8],
            "quantity": r[9],
            "trade_price": float(r[10]) if r[10] is not None else 0.0,
            "trade_value_inr": float(r[11]) if r[11] is not None else 0.0,
            "pct_equity_traded": float(r[12]) if r[12] is not None else None,
            "isin": r[13],
            "company_id": r[14],
            "source_name": r[15],
            "ingestion_mode": r[16],
            "company_name": r[17],
            "sector": r[18],
            "shares_outstanding": r[19],
            "market_cap": r[20]
        })

    return {
        "page": page,
        "limit": limit,
        "total": total_count,
        "total_pages": (total_count + limit - 1) // limit if limit > 0 else 1,
        "deals": deals_list,
        "items": deals_list
    }


@router.get("/summary", summary="Aggregate deals summary statistics")
async def get_deals_summary(
    deal_type: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_deals"))
):
    """
    Returns aggregate stats: deal counts, total trade value, unique companies,
    buy vs sell side comparison, and institutional breakdown.
    """
    conditions = ["1=1"]
    params: List[Any] = []

    if deal_type and deal_type.upper() != "ALL":
        conditions.append("deal_type = %s")
        params.append(deal_type.upper())

    if from_date:
        conditions.append("trade_date >= %s")
        params.append(from_date)

    if to_date:
        conditions.append("trade_date <= %s")
        params.append(to_date)

    where_clause = " AND ".join(conditions)

    # Aggregates
    cur.execute(
        f"""
        SELECT 
            COUNT(*) as total_deals,
            COUNT(DISTINCT symbol) as unique_symbols,
            COALESCE(SUM(trade_value_inr), 0) as total_value_inr,
            COALESCE(SUM(CASE WHEN action = 'BUY' THEN trade_value_inr ELSE 0 END), 0) as buy_value_inr,
            COALESCE(SUM(CASE WHEN action = 'SELL' THEN trade_value_inr ELSE 0 END), 0) as sell_value_inr,
            COUNT(CASE WHEN action = 'BUY' THEN 1 END) as buy_count,
            COUNT(CASE WHEN action = 'SELL' THEN 1 END) as sell_count,
            MAX(trade_date) as latest_trade_date,
            MIN(trade_date) as earliest_trade_date
        FROM market_deals
        WHERE {where_clause}
        """,
        params
    )
    row = cur.fetchone()

    # Category breakdown
    cur.execute(
        f"""
        SELECT investor_category, COUNT(*), COALESCE(SUM(trade_value_inr), 0)
        FROM market_deals
        WHERE {where_clause}
        GROUP BY investor_category
        ORDER BY SUM(trade_value_inr) DESC
        """,
        params
    )
    categories = [
        {"category": c[0], "deals_count": c[1], "total_value_inr": float(c[2])}
        for c in cur.fetchall()
    ]

    # Deal Type breakdown
    cur.execute(
        f"""
        SELECT deal_type, COUNT(*), COALESCE(SUM(trade_value_inr), 0)
        FROM market_deals
        WHERE {where_clause}
        GROUP BY deal_type
        """,
        params
    )
    deal_types = [
        {"deal_type": dt[0], "deals_count": dt[1], "total_value_inr": float(dt[2])}
        for dt in cur.fetchall()
    ]

    return {
        "total_deals": row[0],
        "unique_symbols": row[1],
        "total_value_inr": float(row[2]),
        "total_value_crores": round(float(row[2]) / 10000000.0, 2),
        "buy_value_inr": float(row[3]),
        "buy_value_crores": round(float(row[3]) / 10000000.0, 2),
        "sell_value_inr": float(row[4]),
        "sell_value_crores": round(float(row[4]) / 10000000.0, 2),
        "buy_count": row[5],
        "sell_count": row[6],
        "latest_trade_date": row[7].isoformat() if row[7] else None,
        "earliest_trade_date": row[8].isoformat() if row[8] else None,
        "by_category": categories,
        "by_deal_type": deal_types
    }


@router.get("/status", summary="Operational sync checkpoints and health diagnostics")
async def get_deals_status(
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_deals"))
):
    """
    Returns sync state for all data sources, recent ingestion runs, and total deals in DB.
    """
    cur.execute(
        """
        SELECT source_key, status, last_attempted_at, last_successful_at,
               earliest_date_covered, latest_date_covered, total_deals_count, last_error
        FROM deal_sync_state
        ORDER BY source_key
        """
    )
    sync_states = []
    for r in cur.fetchall():
        sync_states.append({
            "source_key": r[0],
            "status": r[1],
            "last_attempted_at": r[2].isoformat() if r[2] else None,
            "last_successful_at": r[3].isoformat() if r[3] else None,
            "earliest_date_covered": r[4].isoformat() if r[4] else None,
            "latest_date_covered": r[5].isoformat() if r[5] else None,
            "total_deals_count": r[6],
            "last_error": r[7]
        })

    # Recent runs
    cur.execute(
        """
        SELECT run_id, run_mode, exchange, deal_type, from_date, to_date, start_time, end_time,
               status, records_fetched, records_inserted, records_updated, records_skipped, records_rejected, error_summary
        FROM market_deal_ingestion_runs
        ORDER BY start_time DESC
        LIMIT 10
        """
    )
    recent_runs = []
    for r in cur.fetchall():
        recent_runs.append({
            "run_id": r[0],
            "run_mode": r[1],
            "exchange": r[2],
            "deal_type": r[3],
            "from_date": r[4].isoformat() if r[4] else None,
            "to_date": r[5].isoformat() if r[5] else None,
            "start_time": r[6].isoformat() if r[6] else None,
            "end_time": r[7].isoformat() if r[7] else None,
            "status": r[8],
            "records_fetched": r[9],
            "records_inserted": r[10],
            "records_updated": r[11],
            "records_skipped": r[12],
            "records_rejected": r[13],
            "error_summary": r[14]
        })

    # Total in DB
    cur.execute("SELECT COUNT(*) FROM market_deals")
    total_db_deals = cur.fetchone()[0]

    return {
        "total_deals_in_db": total_db_deals,
        "sync_states": sync_states,
        "recent_runs": recent_runs
    }


@router.post("/sync-now", summary="Trigger manual sync cycle (Admin only)")
async def trigger_deals_sync(
    days: int = Query(5, ge=1, le=30),
    deal_type: str = Query("ALL", pattern="^(BULK|BLOCK|ALL)$"),
    current_user: dict = Depends(get_current_user),
    _is_admin=Depends(require_permission("admin"))
):
    """
    Manually triggers an immediate synchronization cycle across NSE & BSE.
    """
    logger.info(f"Admin '{current_user.get('email')}' triggered deal sync (days={days}, type={deal_type})")
    engine = DealEngine()
    results = engine.sync_recent_deals(lookback_days=days, deal_type=deal_type, dry_run=False)

    return {
        "status": "COMPLETED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "results": [
            {
                "source": r.source_name,
                "fetched": r.records_fetched,
                "inserted": r.records_inserted,
                "updated": r.records_updated,
                "skipped": r.records_skipped,
                "rejected": r.records_rejected,
                "duration_seconds": r.duration_seconds,
                "error": r.error_summary
            }
            for r in results
        ]
    }


@router.post("/upload-csv", summary="Manual CSV file upload with validation & dry-run")
async def upload_deals_csv(
    payload: CsvImportRequest,
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_deals"))
):
    """
    Imports manual deal CSV files (official NSE or BSE formats) with dry-run verification.
    """
    if not payload.csv_content or not payload.csv_content.strip():
        raise HTTPException(status_code=400, detail="CSV content cannot be empty.")

    engine = DealEngine()
    result = engine.import_manual_csv(
        csv_content=payload.csv_content,
        filename=payload.filename or "upload.csv",
        forced_deal_type=payload.deal_type,
        dry_run=payload.dry_run
    )

    return result
