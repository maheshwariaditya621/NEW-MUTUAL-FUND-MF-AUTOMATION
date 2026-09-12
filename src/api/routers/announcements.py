"""
Corporate Announcements API Router.

Provides endpoints for:
1. Querying canonical announcements with multi-factor filters and search.
2. Viewing announcement details, revisions, and raw exchange sources.
3. Accessing attachments (local file streaming or 302 redirect).
4. Master Office Watchlist management (view, add, remove, search).
5. Collector sync state and health metrics.
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel
import psycopg2.extras

from src.api.dependencies import get_db_cursor, get_current_user, require_permission
from src.announcements.watchlist_service import WatchlistService
from src.announcements.pdf_manager import PDFManager
from src.announcements.sync_state import SyncStateManager
from src.announcements.classifier import CATEGORY_RULES
from src.config import logger

router = APIRouter()


class AddWatchlistRequest(BaseModel):
    company_id: int
    notes: Optional[str] = None


@router.get("", summary="Get paginated canonical corporate announcements")
async def get_announcements(
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    company_id: Optional[int] = None,
    category: Optional[str] = None,
    exchange: Optional[str] = None,  # 'NSE', 'BSE', 'BOTH'
    search: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    is_revision: Optional[bool] = None,
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_announcements"))
):
    """
    Fetch paginated corporate announcements feed with filters.
    """
    offset = (page - 1) * limit
    conditions = ["1=1"]
    params = []

    if company_id:
        conditions.append("ca.company_id = %s")
        params.append(company_id)

    if category and category != "All":
        conditions.append("%s = ANY(ca.categories)")
        params.append(category)

    if exchange:
        ex = exchange.upper()
        if ex == "NSE":
            conditions.append("ca.has_nse = TRUE")
        elif ex == "BSE":
            conditions.append("ca.has_bse = TRUE")
        elif ex == "BOTH":
            conditions.append("ca.has_nse = TRUE AND ca.has_bse = TRUE")

    if is_revision is not None:
        conditions.append("ca.is_revision = %s")
        params.append(is_revision)

    if search and search.strip():
        search_pattern = f"%{search.strip()}%"
        conditions.append("(ca.title ILIKE %s OR ca.company_name ILIKE %s OR c.exchange_symbol ILIKE %s)")
        params.extend([search_pattern, search_pattern, search_pattern])

    if from_date:
        conditions.append("ca.primary_timestamp >= %s")
        params.append(from_date)

    if to_date:
        conditions.append("ca.primary_timestamp <= %s")
        params.append(to_date)

    where_clause = " AND ".join(conditions)

    # 1. Total count
    count_sql = f"""
        SELECT COUNT(*)
        FROM canonical_announcements ca
        JOIN companies c ON ca.company_id = c.company_id
        WHERE {where_clause}
    """
    cur.execute(count_sql, tuple(params))
    total_records = cur.fetchone()[0]

    # 2. Page records
    data_sql = f"""
        SELECT 
            ca.canonical_id,
            ca.company_id,
            ca.isin,
            ca.company_name,
            c.exchange_symbol AS nse_symbol,
            c.bse_code,
            c.sector,
            ca.primary_timestamp,
            ca.title,
            ca.summary_text,
            ca.has_nse,
            ca.has_bse,
            ca.categories,
            ca.is_revision,
            ca.revision_type,
            ca.parent_canonical_id,
            ca.created_at
        FROM canonical_announcements ca
        JOIN companies c ON ca.company_id = c.company_id
        WHERE {where_clause}
        ORDER BY ca.primary_timestamp DESC
        LIMIT %s OFFSET %s
    """
    cur.execute(data_sql, tuple(params + [limit, offset]))
    rows = cur.fetchall()

    if not rows:
        return {
            "items": [],
            "total": total_records,
            "page": page,
            "limit": limit,
            "total_pages": (total_records + limit - 1) // limit if limit else 0
        }

    canonical_ids = [r[0] for r in rows]

    # 3. Batch fetch attachments for these canonical announcements
    cur.execute(
        """
        SELECT attachment_id, canonical_id, exchange, original_file_name,
               source_file_url, lifecycle_stage, is_locally_available
        FROM announcement_attachments
        WHERE canonical_id = ANY(%s)
        ORDER BY attachment_id ASC
        """,
        (canonical_ids,)
    )
    attachments_by_can = {}
    for att in cur.fetchall():
        can_id = att[1]
        attachments_by_can.setdefault(can_id, []).append({
            "attachment_id": att[0],
            "exchange": att[2],
            "original_file_name": att[3],
            "source_file_url": att[4],
            "lifecycle_stage": att[5],
            "is_locally_available": att[6]
        })

    # 4. Batch fetch sources
    cur.execute(
        """
        SELECT source_id, canonical_id, exchange, source_announcement_id,
               dissemination_timestamp, has_xbrl, time_difference
        FROM announcement_sources
        WHERE canonical_id = ANY(%s)
        ORDER BY dissemination_timestamp ASC
        """,
        (canonical_ids,)
    )
    sources_by_can = {}
    for src in cur.fetchall():
        can_id = src[1]
        sources_by_can.setdefault(can_id, []).append({
            "source_id": src[0],
            "exchange": src[2],
            "source_announcement_id": src[3],
            "dissemination_timestamp": src[4],
            "has_xbrl": src[5],
            "time_difference": src[6]
        })

    items = []
    for r in rows:
        c_id = r[0]
        items.append({
            "canonical_id": c_id,
            "company_id": r[1],
            "isin": r[2],
            "company_name": r[3],
            "nse_symbol": r[4],
            "bse_code": r[5],
            "sector": r[6],
            "primary_timestamp": r[7],
            "title": r[8],
            "summary_text": r[9],
            "has_nse": r[10],
            "has_bse": r[11],
            "categories": r[12],
            "is_revision": r[13],
            "revision_type": r[14],
            "parent_canonical_id": r[15],
            "created_at": r[16],
            "attachments": attachments_by_can.get(c_id, []),
            "sources": sources_by_can.get(c_id, [])
        })

    return {
        "items": items,
        "total": total_records,
        "page": page,
        "limit": limit,
        "total_pages": (total_records + limit - 1) // limit if limit else 0
    }


@router.get("/categories", summary="Get announcement categories list")
async def get_categories(
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_announcements"))
):
    """Return available category tags with count of announcements."""
    cur.execute(
        """
        SELECT unnest(categories) AS cat, COUNT(*) 
        FROM canonical_announcements 
        GROUP BY cat 
        ORDER BY COUNT(*) DESC
        """
    )
    counts = dict(cur.fetchall())
    defined_cats = list(CATEGORY_RULES.keys()) + ["Other"]

    result = []
    for c in defined_cats:
        result.append({
            "name": c,
            "count": counts.get(c, 0)
        })
    return result


@router.get("/attachments/{attachment_id}/view", summary="Stream or redirect announcement attachment")
async def view_attachment(
    attachment_id: int,
    token: Optional[str] = Query(None)
):
    """
    Serve attachment:
    - Days 0-7 / Days 8-60: Stream local PDF if present
    - Purged / Not local: 302 redirect to exchange original URL
    """
    loc = PDFManager.get_attachment_location(attachment_id)
    if not loc:
        raise HTTPException(status_code=404, detail="Attachment not found")

    if loc["delivery"] == "LOCAL":
        return FileResponse(
            path=loc["path"],
            filename=loc["filename"],
            media_type="application/pdf",
            content_disposition_type="inline"
        )
    else:
        return RedirectResponse(url=loc["url"], status_code=status.HTTP_302_FOUND)


@router.get("/{canonical_id}", summary="Get canonical announcement details")
async def get_announcement_details(
    canonical_id: int,
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_announcements"))
):
    """Retrieve full details, all sources, revisions, and attachments for an announcement."""
    cur.execute(
        """
        SELECT 
            ca.canonical_id, ca.company_id, ca.isin, ca.company_name,
            c.exchange_symbol, c.bse_code, c.sector, c.industry,
            ca.primary_timestamp, ca.title, ca.summary_text,
            ca.has_nse, ca.has_bse, ca.categories, ca.is_revision,
            ca.revision_type, ca.parent_canonical_id, ca.created_at
        FROM canonical_announcements ca
        JOIN companies c ON ca.company_id = c.company_id
        WHERE ca.canonical_id = %s
        """,
        (canonical_id,)
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Announcement not found")

    # Fetch sources
    cur.execute(
        """
        SELECT source_id, exchange, source_announcement_id, raw_subject,
               raw_details, raw_category, raw_subcategory, submission_timestamp,
               dissemination_timestamp, time_difference, has_xbrl, raw_payload
        FROM announcement_sources
        WHERE canonical_id = %s
        ORDER BY dissemination_timestamp ASC
        """,
        (canonical_id,)
    )
    sources = []
    for s in cur.fetchall():
        sources.append({
            "source_id": s[0],
            "exchange": s[1],
            "source_announcement_id": s[2],
            "raw_subject": s[3],
            "raw_details": s[4],
            "raw_category": s[5],
            "raw_subcategory": s[6],
            "submission_timestamp": s[7],
            "dissemination_timestamp": s[8],
            "time_difference": s[9],
            "has_xbrl": s[10],
            "raw_payload": s[11]
        })

    # Fetch attachments
    cur.execute(
        """
        SELECT attachment_id, source_id, exchange, original_file_name,
               source_file_url, file_size_bytes, lifecycle_stage, is_locally_available
        FROM announcement_attachments
        WHERE canonical_id = %s
        ORDER BY attachment_id ASC
        """,
        (canonical_id,)
    )
    attachments = []
    for a in cur.fetchall():
        attachments.append({
            "attachment_id": a[0],
            "source_id": a[1],
            "exchange": a[2],
            "original_file_name": a[3],
            "source_file_url": a[4],
            "file_size_bytes": a[5],
            "lifecycle_stage": a[6],
            "is_locally_available": a[7]
        })

    # Fetch child revisions if any
    cur.execute(
        """
        SELECT canonical_id, title, revision_type, primary_timestamp
        FROM canonical_announcements
        WHERE parent_canonical_id = %s
        ORDER BY primary_timestamp ASC
        """,
        (canonical_id,)
    )
    child_revisions = [
        {"canonical_id": r[0], "title": r[1], "revision_type": r[2], "primary_timestamp": r[3]}
        for r in cur.fetchall()
    ]

    return {
        "canonical_id": row[0],
        "company_id": row[1],
        "isin": row[2],
        "company_name": row[3],
        "exchange_symbol": row[4],
        "bse_code": row[5],
        "sector": row[6],
        "industry": row[7],
        "primary_timestamp": row[8],
        "title": row[9],
        "summary_text": row[10],
        "has_nse": row[11],
        "has_bse": row[12],
        "categories": row[13],
        "is_revision": row[14],
        "revision_type": row[15],
        "parent_canonical_id": row[16],
        "created_at": row[17],
        "sources": sources,
        "attachments": attachments,
        "child_revisions": child_revisions
    }


# ============================================================
# MASTER OFFICE WATCHLIST ENDPOINTS
# ============================================================

@router.get("/watchlist/active", summary="Get active Master Office Watchlist companies")
async def get_active_watchlist(
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_announcements"))
):
    """Retrieve all monitored stocks in the Master Office Watchlist."""
    return WatchlistService.get_active_watchlist()


@router.post("/watchlist/add", summary="Add company to Master Office Watchlist")
async def add_to_master_watchlist(
    req: AddWatchlistRequest,
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("manage_master_watchlist"))
):
    """Add a stock to the Master Office Watchlist."""
    res = WatchlistService.add_to_master_watchlist(
        company_id=req.company_id,
        notes=req.notes,
        user_id=current_user.get("id")
    )
    return {"message": "Company added to Master Watchlist", "data": res}


@router.delete("/watchlist/{company_id}", summary="Remove company from Master Office Watchlist")
async def remove_from_master_watchlist(
    company_id: int,
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("manage_master_watchlist"))
):
    """Remove a stock from the Master Office Watchlist."""
    success = WatchlistService.remove_from_master_watchlist(company_id)
    if not success:
        raise HTTPException(status_code=404, detail="Company not found in active master watchlist")
    return {"message": "Company removed from Master Watchlist", "company_id": company_id}


@router.get("/watchlist/search", summary="Search companies to add to watchlist")
async def search_companies(
    q: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=50),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_announcements"))
):
    """Search master company universe by name, symbol, BSE code, or ISIN."""
    return WatchlistService.search_companies(query=q, limit=limit)


# ============================================================
# WORKER & SYNC HEALTH ENDPOINTS
# ============================================================

@router.get("/health/sync", summary="Get announcement collector sync state")
async def get_sync_health(
    cur=Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("view_announcements"))
):
    """Retrieve latest sync timestamps, poll status, and volume stats."""
    cur.execute(
        """
        SELECT sync_key, exchange, scrip_code, last_checkpoint_timestamp,
               last_seq_id, last_poll_status, last_poll_at, last_error
        FROM announcement_sync_state
        ORDER BY updated_at DESC
        LIMIT 20
        """
    )
    states = [
        {
            "sync_key": r[0],
            "exchange": r[1],
            "scrip_code": r[2],
            "last_checkpoint_timestamp": r[3],
            "last_seq_id": r[4],
            "last_poll_status": r[5],
            "last_poll_at": r[6],
            "last_error": r[7]
        }
        for r in cur.fetchall()
    ]

    cur.execute("SELECT COUNT(*) FROM canonical_announcements")
    total_canonical = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM master_watchlist WHERE is_active = TRUE")
    active_watchlist = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM announcement_attachments WHERE is_locally_available = TRUE")
    local_attachments = cur.fetchone()[0]

    return {
        "sync_states": states,
        "total_canonical_announcements": total_canonical,
        "active_watchlist_count": active_watchlist,
        "local_attachments_stored": local_attachments
    }


@router.post("/sync-now", summary="Trigger immediate poll cycle")
async def trigger_sync_now(
    current_user: dict = Depends(get_current_user),
    _has_perm=Depends(require_permission("manage_master_watchlist"))
):
    """Manually invoke a single ingestion poll cycle."""
    from src.announcements.announcement_worker import AnnouncementWorker
    worker = AnnouncementWorker()
    res = worker.poll_once(recovery=False)
    return {"message": "Sync cycle executed", "result": res}
