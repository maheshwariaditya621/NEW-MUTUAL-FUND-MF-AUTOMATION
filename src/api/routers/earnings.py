"""
Earnings Calendar & Results Tracking API Router.

Endpoints:
1. GET /api/v1/earnings/calendar - Paginated scheduled board meetings & earnings calendar
2. GET /api/v1/earnings/today - Today's Results Desk (Awaiting vs Published)
3. GET /api/v1/earnings/results - Paginated actual published financial results filings
4. GET /api/v1/earnings/company/{company_id} - Company earnings timeline, filings & audit history
5. GET /api/v1/earnings/event/{event_id} - Single event details with audit log and linked filings
6. GET /api/v1/earnings/sync-status - Data health, sync checkpoints, and ingestion diagnostics
7. POST /api/v1/earnings/sync-now - Trigger manual sync cycle (Admin / authorized)
"""

from datetime import datetime, timezone, timedelta, date
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, status
import psycopg2.extras

from src.api.dependencies import get_db_cursor, get_current_user, require_permission
from src.earnings.engine import EarningsEngine
from src.config import logger

router = APIRouter()


@router.get("/calendar", summary="Get paginated earnings calendar events")
async def get_earnings_calendar(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    company_id: Optional[int] = None,
    event_status: Optional[str] = None,  # SCHEDULED, RESCHEDULED, POSTPONED, CANCELLED, RESULTS_PUBLISHED, CONCLUDED
    financial_year: Optional[str] = None,
    fiscal_period: Optional[str] = None,
    watchlist_only: bool = False,
    search: Optional[str] = None,
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_earnings"))
):
    """
    Query forward-looking scheduled board meetings and consensus earnings dates.
    """
    offset = (page - 1) * limit
    conditions = ["1=1"]
    params = []

    if company_id:
        conditions.append("ec.company_id = %s")
        params.append(company_id)

    if from_date:
        conditions.append("ec.meeting_date >= %s")
        params.append(from_date)

    if to_date:
        conditions.append("ec.meeting_date <= %s")
        params.append(to_date)

    if event_status and event_status != "ALL":
        conditions.append("ec.event_status = %s")
        params.append(event_status.upper())

    if financial_year:
        conditions.append("ec.financial_year = %s")
        params.append(financial_year)

    if fiscal_period:
        conditions.append("ec.fiscal_period = %s")
        params.append(fiscal_period)

    if watchlist_only:
        conditions.append("EXISTS (SELECT 1 FROM master_watchlist mw WHERE mw.company_id = ec.company_id AND mw.is_active = TRUE)")

    if search and search.strip():
        s_pat = f"%{search.strip()}%"
        conditions.append("(c.company_name ILIKE %s OR c.exchange_symbol ILIKE %s OR c.bse_code ILIKE %s OR c.isin ILIKE %s)")
        params.extend([s_pat, s_pat, s_pat, s_pat])

    where_clause = " AND ".join(conditions)

    # Count total
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM earnings_calendar ec
        JOIN companies c ON ec.company_id = c.company_id
        WHERE {where_clause}
        """,
        params
    )
    total = cur.fetchone()[0]

    # Fetch rows
    fetch_params = params + [limit, offset]
    cur.execute(
        f"""
        SELECT 
            ec.event_id,
            ec.company_id,
            c.company_name,
            c.exchange_symbol,
            c.bse_code,
            c.isin,
            c.sector,
            c.industry,
            ec.financial_year,
            ec.fiscal_period,
            ec.meeting_date,
            ec.meeting_time,
            ec.expected_results_date,
            ec.raw_purpose,
            ec.normalized_event_type,
            ec.event_status,
            ec.source_name,
            ec.source_url,
            ec.verification_status,
            ec.consensus_eps,
            ec.consensus_eps_high,
            ec.consensus_eps_low,
            ec.consensus_revenue_inr,
            ec.first_detected_at,
            ec.last_verified_at,
            EXISTS (SELECT 1 FROM master_watchlist mw WHERE mw.company_id = ec.company_id AND mw.is_active = TRUE) AS is_in_watchlist,
            (SELECT COUNT(*) FROM earnings_results_filings erf WHERE erf.calendar_event_id = ec.event_id) AS linked_filings_count
        FROM earnings_calendar ec
        JOIN companies c ON ec.company_id = c.company_id
        WHERE {where_clause}
        ORDER BY ec.meeting_date ASC, c.company_name ASC
        LIMIT %s OFFSET %s
        """,
        fetch_params
    )

    columns = [desc[0] for desc in cur.description]
    events = [dict(zip(columns, row)) for row in cur.fetchall()]

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "pages": (total + limit - 1) // limit if limit > 0 else 0,
        "events": events
    }


@router.get("/today", summary="Get Today's Results Desk (Awaiting vs Published)")
async def get_today_results_desk(
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_earnings"))
):
    """
    Returns today's active results desk:
    - Summary metrics
    - Awaiting results (board meetings scheduled today not yet published)
    - Results published today (with dissemination & detection timestamps)
    """
    # Use IST today
    ist_now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    today_ist = ist_now.date()
    week_end = today_ist + timedelta(days=7)

    # 1. Summary counts
    cur.execute(
        """
        SELECT 
            COUNT(*) FILTER (WHERE meeting_date = %s) AS scheduled_today,
            COUNT(*) FILTER (WHERE meeting_date BETWEEN %s AND %s) AS upcoming_week,
            COUNT(*) FILTER (WHERE event_status = 'SCHEDULED') AS total_scheduled_active
        FROM earnings_calendar
        """,
        (today_ist, today_ist, week_end)
    )
    row_cal = cur.fetchone()

    cur.execute(
        """
        SELECT COUNT(*)
        FROM earnings_results_filings
        WHERE (dissemination_timestamp AT TIME ZONE 'Asia/Kolkata')::date = %s
           OR (detected_at AT TIME ZONE 'Asia/Kolkata')::date = %s
        """,
        (today_ist, today_ist)
    )
    published_today_count = cur.fetchone()[0]

    # 2. Awaiting Results (Scheduled for today, not published yet)
    cur.execute(
        """
        SELECT 
            ec.event_id,
            ec.company_id,
            c.company_name,
            c.exchange_symbol,
            c.bse_code,
            c.isin,
            c.sector,
            ec.financial_year,
            ec.fiscal_period,
            ec.meeting_date,
            ec.meeting_time,
            ec.raw_purpose,
            ec.event_status,
            ec.source_name,
            ec.source_url,
            ec.verification_status,
            ec.consensus_eps,
            ec.consensus_revenue_inr,
            ec.last_verified_at
        FROM earnings_calendar ec
        JOIN companies c ON ec.company_id = c.company_id
        WHERE ec.meeting_date = %s AND ec.event_status != 'RESULTS_PUBLISHED'
        ORDER BY c.company_name ASC
        """,
        (today_ist,)
    )
    awaiting_cols = [desc[0] for desc in cur.description]
    awaiting_results = [dict(zip(awaiting_cols, r)) for r in cur.fetchall()]

    # 3. Results Published Today
    cur.execute(
        """
        SELECT 
            erf.filing_id,
            erf.company_id,
            c.company_name,
            c.exchange_symbol,
            c.bse_code,
            c.isin,
            c.sector,
            erf.exchange,
            erf.financial_year,
            erf.fiscal_period,
            erf.result_type,
            erf.audit_status,
            erf.submission_timestamp,
            erf.dissemination_timestamp,
            erf.detected_at,
            erf.filing_title,
            erf.filing_url,
            erf.attachment_url,
            erf.attachment_filename,
            erf.is_revision,
            erf.revision_type,
            erf.verification_status
        FROM earnings_results_filings erf
        JOIN companies c ON erf.company_id = c.company_id
        WHERE (erf.dissemination_timestamp AT TIME ZONE 'Asia/Kolkata')::date = %s
           OR (erf.detected_at AT TIME ZONE 'Asia/Kolkata')::date = %s
        ORDER BY erf.dissemination_timestamp DESC NULLS LAST, erf.detected_at DESC
        """,
        (today_ist, today_ist)
    )
    pub_cols = [desc[0] for desc in cur.description]
    results_published = [dict(zip(pub_cols, r)) for r in cur.fetchall()]

    return {
        "date_ist": today_ist.isoformat(),
        "summary": {
            "scheduled_today": row_cal[0] or 0,
            "published_today": published_today_count or 0,
            "upcoming_this_week": row_cal[1] or 0,
            "total_scheduled_active": row_cal[2] or 0
        },
        "awaiting_results": awaiting_results,
        "results_published": results_published
    }


@router.get("/results", summary="Get paginated published financial results filings")
async def get_published_results(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    company_id: Optional[int] = None,
    exchange: Optional[str] = None,  # NSE, BSE, BOTH
    fiscal_period: Optional[str] = None,
    financial_year: Optional[str] = None,
    result_type: Optional[str] = None,  # STANDALONE, CONSOLIDATED, BOTH
    audit_status: Optional[str] = None,  # AUDITED, UNAUDITED, LIMITED_REVIEW
    search: Optional[str] = None,
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_earnings"))
):
    """
    Query actual published financial results filings with direct PDF links and exact timestamps.
    """
    offset = (page - 1) * limit
    conditions = ["1=1"]
    params = []

    if company_id:
        conditions.append("erf.company_id = %s")
        params.append(company_id)

    if from_date:
        conditions.append("(erf.dissemination_timestamp AT TIME ZONE 'Asia/Kolkata')::date >= %s")
        params.append(from_date)

    if to_date:
        conditions.append("(erf.dissemination_timestamp AT TIME ZONE 'Asia/Kolkata')::date <= %s")
        params.append(to_date)

    if exchange and exchange != "ALL":
        conditions.append("erf.exchange = %s")
        params.append(exchange.upper())

    if fiscal_period:
        conditions.append("erf.fiscal_period = %s")
        params.append(fiscal_period)

    if financial_year:
        conditions.append("erf.financial_year = %s")
        params.append(financial_year)

    if result_type and result_type != "ALL":
        conditions.append("erf.result_type = %s")
        params.append(result_type.upper())

    if audit_status and audit_status != "ALL":
        conditions.append("erf.audit_status = %s")
        params.append(audit_status.upper())

    if search and search.strip():
        s_pat = f"%{search.strip()}%"
        conditions.append("(c.company_name ILIKE %s OR c.exchange_symbol ILIKE %s OR c.bse_code ILIKE %s OR erf.filing_title ILIKE %s)")
        params.extend([s_pat, s_pat, s_pat, s_pat])

    where_clause = " AND ".join(conditions)

    # Count
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM earnings_results_filings erf
        JOIN companies c ON erf.company_id = c.company_id
        WHERE {where_clause}
        """,
        params
    )
    total = cur.fetchone()[0]

    # Fetch
    fetch_params = params + [limit, offset]
    cur.execute(
        f"""
        SELECT 
            erf.filing_id,
            erf.company_id,
            c.company_name,
            c.exchange_symbol,
            c.bse_code,
            c.isin,
            c.sector,
            erf.calendar_event_id,
            erf.exchange,
            erf.source_filing_id,
            erf.financial_year,
            erf.fiscal_period,
            erf.result_type,
            erf.audit_status,
            erf.submission_timestamp,
            erf.dissemination_timestamp,
            erf.detected_at,
            erf.filing_title,
            erf.filing_url,
            erf.attachment_url,
            erf.attachment_filename,
            erf.is_revision,
            erf.revision_type,
            erf.verification_status
        FROM earnings_results_filings erf
        JOIN companies c ON erf.company_id = c.company_id
        WHERE {where_clause}
        ORDER BY erf.dissemination_timestamp DESC NULLS LAST, erf.detected_at DESC
        LIMIT %s OFFSET %s
        """,
        fetch_params
    )

    cols = [desc[0] for desc in cur.description]
    filings = [dict(zip(cols, r)) for r in cur.fetchall()]

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "pages": (total + limit - 1) // limit if limit > 0 else 0,
        "filings": filings
    }


@router.get("/company/{company_id}", summary="Get company earnings timeline, filings & audit history")
async def get_company_earnings_timeline(
    company_id: int,
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_earnings"))
):
    """
    Complete earnings research dossier for a single company:
    - Company details
    - Upcoming events
    - Historical events & date changes
    - All published results filings
    """
    cur.execute(
        """
        SELECT company_id, company_name, exchange_symbol, bse_code, isin, sector, industry, market_cap
        FROM companies
        WHERE company_id = %s
        """,
        (company_id,)
    )
    c_row = cur.fetchone()
    if not c_row:
        raise HTTPException(status_code=404, detail="Company not found")

    comp_cols = [desc[0] for desc in cur.description]
    company = dict(zip(comp_cols, c_row))

    today = datetime.now(timezone.utc).date()

    # Upcoming events
    cur.execute(
        """
        SELECT event_id, financial_year, fiscal_period, meeting_date, meeting_time,
               expected_results_date, raw_purpose, normalized_event_type, event_status,
               source_name, source_url, verification_status, consensus_eps, consensus_revenue_inr
        FROM earnings_calendar
        WHERE company_id = %s AND meeting_date >= %s
        ORDER BY meeting_date ASC
        """,
        (company_id, today)
    )
    up_cols = [desc[0] for desc in cur.description]
    upcoming = [dict(zip(up_cols, r)) for r in cur.fetchall()]

    # Past events
    cur.execute(
        """
        SELECT event_id, financial_year, fiscal_period, meeting_date, meeting_time,
               raw_purpose, normalized_event_type, event_status,
               source_name, source_url, verification_status
        FROM earnings_calendar
        WHERE company_id = %s AND meeting_date < %s
        ORDER BY meeting_date DESC
        """,
        (company_id, today)
    )
    past_cols = [desc[0] for desc in cur.description]
    past = [dict(zip(past_cols, r)) for r in cur.fetchall()]

    # Results filings
    cur.execute(
        """
        SELECT filing_id, exchange, financial_year, fiscal_period, result_type, audit_status,
               submission_timestamp, dissemination_timestamp, detected_at,
               filing_title, filing_url, attachment_url, is_revision, revision_type
        FROM earnings_results_filings
        WHERE company_id = %s
        ORDER BY dissemination_timestamp DESC NULLS LAST, detected_at DESC
        """,
        (company_id,)
    )
    f_cols = [desc[0] for desc in cur.description]
    filings = [dict(zip(f_cols, r)) for r in cur.fetchall()]

    # History audit log
    cur.execute(
        """
        SELECT history_id, event_id, old_meeting_date, new_meeting_date,
               old_status, new_status, change_reason, source_evidence, detected_at
        FROM earnings_calendar_history
        WHERE company_id = %s
        ORDER BY detected_at DESC
        """,
        (company_id,)
    )
    h_cols = [desc[0] for desc in cur.description]
    history = [dict(zip(h_cols, r)) for r in cur.fetchall()]

    return {
        "company": company,
        "upcoming_events": upcoming,
        "past_events": past,
        "filings": filings,
        "history": history
    }


@router.get("/event/{event_id}", summary="Get single event details with audit log and linked filings")
async def get_event_details(
    event_id: int,
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_earnings"))
):
    """
    Detailed inspector for a single calendar event.
    """
    cur.execute(
        """
        SELECT 
            ec.*,
            c.company_name, c.exchange_symbol, c.bse_code, c.sector, c.industry
        FROM earnings_calendar ec
        JOIN companies c ON ec.company_id = c.company_id
        WHERE ec.event_id = %s
        """,
        (event_id,)
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Earnings calendar event not found")

    cols = [desc[0] for desc in cur.description]
    event_data = dict(zip(cols, row))

    # Linked filings
    cur.execute(
        """
        SELECT filing_id, exchange, financial_year, fiscal_period, result_type, audit_status,
               submission_timestamp, dissemination_timestamp, detected_at,
               filing_title, filing_url, attachment_url, is_revision, revision_type
        FROM earnings_results_filings
        WHERE calendar_event_id = %s
        ORDER BY dissemination_timestamp DESC NULLS LAST
        """,
        (event_id,)
    )
    f_cols = [desc[0] for desc in cur.description]
    linked_filings = [dict(zip(f_cols, r)) for r in cur.fetchall()]

    # Change history
    cur.execute(
        """
        SELECT history_id, old_meeting_date, new_meeting_date,
               old_status, new_status, change_reason, source_evidence, detected_at
        FROM earnings_calendar_history
        WHERE event_id = %s
        ORDER BY detected_at DESC
        """,
        (event_id,)
    )
    h_cols = [desc[0] for desc in cur.description]
    history = [dict(zip(h_cols, r)) for r in cur.fetchall()]

    return {
        "event": event_data,
        "linked_filings": linked_filings,
        "history": history
    }


@router.get("/sync-status", summary="Operational diagnostics and source sync health")
async def get_earnings_sync_status(
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_earnings"))
):
    """
    Exposes operational sync state and diagnostic health for earnings ingestion.
    """
    cur.execute(
        """
        SELECT source_key, last_attempted_at, last_successful_at, status,
               events_count, results_count, last_error, updated_at
        FROM earnings_sync_state
        ORDER BY source_key ASC
        """
    )
    cols = [desc[0] for desc in cur.description]
    sources = [dict(zip(cols, r)) for r in cur.fetchall()]

    cur.execute("SELECT COUNT(*) FROM earnings_calendar")
    total_events = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM earnings_results_filings")
    total_filings = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM earnings_calendar_history")
    total_history_records = cur.fetchone()[0]

    return {
        "total_calendar_events": total_events,
        "total_results_filings": total_filings,
        "total_history_records": total_history_records,
        "sources": sources
    }


@router.post("/sync-now", summary="Trigger manual sync cycle (Admin only)")
async def trigger_sync_now(
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("admin"))
):
    """
    Triggers an immediate background sync run of BSE, NSE, TigZig, and reconciliation.
    """
    try:
        engine = EarningsEngine()
        result = engine.run_cycle()
        return {"status": "SUCCESS", "result": result}
    except Exception as e:
        logger.error(f"Manual earnings sync error: {e}")
        raise HTTPException(status_code=500, detail=f"Sync execution failed: {str(e)}")
