"""
FastAPI Router for Fund Manager Intelligence.

Provides public endpoints for scheme fund manager lookups, manager profiles,
and assignment histories, as well as protected administrative endpoints
for monitoring synchronization health, triggering sync runs, and reviewing
the unmatched/ambiguous queue.
"""

from typing import Optional, List
from datetime import datetime, date
import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks, Header
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor

from src.config import logger
from src.db.connection import get_pool_connection, release_pool_connection
from src.api.dependencies import get_db_cursor, verify_admin
from src.services.fund_manager_sync import FundManagerSyncService

router = APIRouter()


# ── Response Models ──────────────────────────────────────────────────────────

class ManagerAssignment(BaseModel):
    manager_id: int
    name: str
    canonical_name: str
    role: Optional[str] = "Fund Manager"
    is_primary: bool = False
    tenure_start_date: Optional[date] = None
    tenure_end_date: Optional[date] = None
    is_active: bool = True
    verification_status: str
    match_method: Optional[str] = None
    source_doc_date: Optional[date] = None
    last_confirmed_at: Optional[datetime] = None


class SchemeManagersResponse(BaseModel):
    scheme_id: int
    scheme_name: str
    plan_type: str
    option_type: str
    amfi_code: Optional[str] = None
    amc_name: str
    managers: List[ManagerAssignment]
    source_url: Optional[str] = None
    last_retrieval_timestamp: Optional[datetime] = None


class ReviewApprovalRequest(BaseModel):
    queue_id: int
    action: str  # 'approve', 'reject', 'ignore'
    target_amfi_code: Optional[str] = None
    reviewer_notes: Optional[str] = None


# ── Public Endpoints ─────────────────────────────────────────────────────────

@router.get("/schemes/{scheme_id}/fund-managers", response_model=SchemeManagersResponse)
async def get_scheme_fund_managers(
    scheme_id: int,
    cur = Depends(get_db_cursor)
):
    """
    Look up current verified fund managers for a specific mutual fund scheme.
    """
    cur.execute("""
        SELECT 
            s.scheme_id, s.scheme_name, s.plan_type, s.option_type, s.amfi_code, a.amc_name
        FROM schemes s
        JOIN amcs a ON s.amc_id = a.amc_id
        WHERE s.scheme_id = %s;
    """, (scheme_id,))
    scheme = cur.fetchone()
    if not scheme:
        raise HTTPException(status_code=404, detail="Scheme not found")

    cur.execute("""
        SELECT 
            sfm.manager_id,
            sfm.source_manager_name,
            fm.canonical_name,
            sfm.role,
            sfm.is_primary,
            sfm.tenure_start_date,
            sfm.tenure_end_date,
            sfm.is_active,
            sfm.verification_status,
            sfm.match_method,
            sfm.source_doc_date,
            sfm.source_url,
            sfm.last_confirmed_at
        FROM scheme_fund_managers sfm
        JOIN fund_managers fm ON sfm.manager_id = fm.manager_id
        WHERE sfm.scheme_id = %s AND sfm.is_active = TRUE
        ORDER BY sfm.is_primary DESC, sfm.tenure_start_date ASC NULLS LAST, fm.canonical_name ASC;
    """, (scheme_id,))
    rows = cur.fetchall()

    managers = []
    source_url = None
    last_confirmed = None

    for r in rows:
        managers.append(ManagerAssignment(
            manager_id=r[0],
            name=r[1],
            canonical_name=r[2],
            role=r[3],
            is_primary=bool(r[4]),
            tenure_start_date=r[5],
            tenure_end_date=r[6],
            is_active=bool(r[7]),
            verification_status=r[8],
            match_method=r[9],
            source_doc_date=r[10],
            last_confirmed_at=r[12]
        ))
        if not source_url and r[11]:
            source_url = r[11]
        if not last_confirmed or (r[12] and r[12] > last_confirmed):
            last_confirmed = r[12]

    return SchemeManagersResponse(
        scheme_id=scheme[0],
        scheme_name=scheme[1],
        plan_type=scheme[2],
        option_type=scheme[3],
        amfi_code=scheme[4],
        amc_name=scheme[5],
        managers=managers,
        source_url=source_url,
        last_retrieval_timestamp=last_confirmed
    )


@router.get("/schemes/{scheme_id}/fund-managers/history")
async def get_scheme_fund_managers_history(
    scheme_id: int,
    cur = Depends(get_db_cursor)
):
    """
    Get full historical assignments and transition change log for a scheme.
    """
    # 1. All assignments (active and departed)
    cur.execute("""
        SELECT 
            sfm.id,
            sfm.manager_id,
            sfm.source_manager_name,
            fm.canonical_name,
            sfm.role,
            sfm.is_primary,
            sfm.tenure_start_date,
            sfm.tenure_end_date,
            sfm.is_active,
            sfm.verification_status,
            sfm.first_observed_at,
            sfm.last_confirmed_at
        FROM scheme_fund_managers sfm
        JOIN fund_managers fm ON sfm.manager_id = fm.manager_id
        WHERE sfm.scheme_id = %s
        ORDER BY sfm.tenure_start_date DESC NULLS LAST, sfm.id DESC;
    """, (scheme_id,))
    assignments = [
        {
            "assignment_id": r[0],
            "manager_id": r[1],
            "name": r[2],
            "canonical_name": r[3],
            "role": r[4],
            "is_primary": r[5],
            "tenure_start_date": r[6],
            "tenure_end_date": r[7],
            "is_active": r[8],
            "verification_status": r[9],
            "first_observed_at": r[10],
            "last_confirmed_at": r[11]
        }
        for r in cur.fetchall()
    ]

    # 2. Change log audit
    cur.execute("""
        SELECT 
            cl.log_id,
            cl.manager_id,
            fm.canonical_name,
            cl.change_type,
            cl.change_date,
            cl.details,
            cl.created_at
        FROM fund_manager_change_log cl
        JOIN fund_managers fm ON cl.manager_id = fm.manager_id
        WHERE cl.scheme_id = %s
        ORDER BY cl.created_at DESC;
    """, (scheme_id,))
    changes = [
        {
            "log_id": r[0],
            "manager_id": r[1],
            "manager_name": r[2],
            "change_type": r[3],
            "change_date": r[4],
            "details": r[5],
            "recorded_at": r[6]
        }
        for r in cur.fetchall()
    ]

    return {
        "scheme_id": scheme_id,
        "assignments": assignments,
        "change_log": changes
    }


@router.get("/fund-managers/{manager_id}")
async def get_fund_manager_profile(
    manager_id: int,
    cur = Depends(get_db_cursor)
):
    """
    Get fund manager profile with all current and historical scheme assignments.
    """
    cur.execute("""
        SELECT manager_id, canonical_name, bio, created_at, updated_at
        FROM fund_managers
        WHERE manager_id = %s;
    """, (manager_id,))
    mgr = cur.fetchone()
    if not mgr:
        raise HTTPException(status_code=404, detail="Fund Manager not found")

    cur.execute("""
        SELECT 
            s.scheme_id,
            s.scheme_name,
            s.plan_type,
            s.option_type,
            a.amc_name,
            sfm.role,
            sfm.tenure_start_date,
            sfm.tenure_end_date,
            sfm.is_active,
            sfm.verification_status,
            COALESCE(ss.total_net_assets_inr, ss.total_value_inr, 0) as aum_inr
        FROM scheme_fund_managers sfm
        JOIN schemes s ON sfm.scheme_id = s.scheme_id
        JOIN amcs a ON s.amc_id = a.amc_id
        LEFT JOIN (
            SELECT scheme_id, total_net_assets_inr, total_value_inr
            FROM scheme_snapshots
            WHERE period_id = (SELECT MAX(period_id) FROM scheme_snapshots)
        ) ss ON s.scheme_id = ss.scheme_id
        WHERE sfm.manager_id = %s
        ORDER BY sfm.is_active DESC, aum_inr DESC, s.scheme_name ASC;
    """, (manager_id,))
    rows = cur.fetchall()

    active_schemes = []
    historical_schemes = []
    total_aum = 0.0

    for r in rows:
        item = {
            "scheme_id": r[0],
            "scheme_name": r[1],
            "plan_type": r[2],
            "option_type": r[3],
            "amc_name": r[4],
            "role": r[5],
            "tenure_start_date": r[6],
            "tenure_end_date": r[7],
            "is_active": r[8],
            "verification_status": r[9],
            "latest_aum_cr": round(float(r[10]) / 1e7, 2) if r[10] else None
        }
        if r[8]:
            active_schemes.append(item)
            if r[10]:
                total_aum += float(r[10])
        else:
            historical_schemes.append(item)

    return {
        "manager_id": mgr[0],
        "name": mgr[1],
        "canonical_name": mgr[1],
        "bio": mgr[2],
        "active_schemes_count": len(active_schemes),
        "historical_schemes_count": len(historical_schemes),
        "total_active_aum_cr": round(total_aum / 1e7, 2),
        "active_schemes": active_schemes,
        "historical_schemes": historical_schemes
    }


@router.get("/fund-managers")
async def list_fund_managers(
    q: Optional[str] = Query(None, description="Search by manager name"),
    amc_id: Optional[int] = Query(None, description="Filter by AMC ID"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    cur = Depends(get_db_cursor)
):
    """
    Search and list fund managers with active schemes and AMC relationships.
    """
    query = """
        SELECT 
            fm.manager_id,
            fm.canonical_name,
            COUNT(DISTINCT sfm.scheme_id) FILTER (WHERE sfm.is_active = TRUE) as active_schemes_count,
            ARRAY_AGG(DISTINCT a.amc_name) FILTER (WHERE sfm.is_active = TRUE) as amcs
        FROM fund_managers fm
        LEFT JOIN scheme_fund_managers sfm ON fm.manager_id = sfm.manager_id
        LEFT JOIN schemes s ON sfm.scheme_id = s.scheme_id
        LEFT JOIN amcs a ON s.amc_id = a.amc_id
        WHERE 1=1
    """
    params = []

    if q and q.strip():
        query += " AND fm.canonical_name ILIKE %s"
        params.append(f"%{q.strip()}%")

    if amc_id:
        query += " AND s.amc_id = %s"
        params.append(amc_id)

    query += """
        GROUP BY fm.manager_id, fm.canonical_name
        ORDER BY active_schemes_count DESC, fm.canonical_name ASC
        LIMIT %s OFFSET %s;
    """
    params.extend([limit, offset])

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    return {
        "limit": limit,
        "offset": offset,
        "count": len(rows),
        "managers": [
            {
                "manager_id": r[0],
                "name": r[1],
                "canonical_name": r[1],
                "active_schemes_count": r[2] or 0,
                "amcs": r[3] or []
            }
            for r in rows
        ]
    }


# ── Administrative Endpoints ─────────────────────────────────────────────────

@router.get("/admin/fund-manager-sync/status")
async def get_sync_status(
    authorized: bool = Depends(verify_admin),
    cur = Depends(get_db_cursor)
):
    """
    Get synchronization status, coverage metrics, and audit ledger for admin panel.
    """
    # 1. Total schemes in DB
    cur.execute("SELECT COUNT(*) FROM schemes;")
    total_schemes = cur.fetchone()[0]

    # 2. Total matched schemes (with active verified assignments)
    cur.execute("""
        SELECT COUNT(DISTINCT scheme_id) 
        FROM scheme_fund_managers 
        WHERE is_active = TRUE AND verification_status = 'verified';
    """)
    matched_schemes = cur.fetchone()[0]

    # 3. Total unique managers
    cur.execute("SELECT COUNT(*) FROM fund_managers;")
    total_managers = cur.fetchone()[0]

    # 4. Total unmatched queue pending
    cur.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE review_status = 'pending') as pending_count,
            COUNT(*) FILTER (WHERE review_status = 'approved') as approved_count,
            COUNT(*) FILTER (WHERE review_status = 'rejected') as rejected_count
        FROM fund_manager_unmatched_queue;
    """)
    q_counts = cur.fetchone()

    # 5. Last successful sync run
    cur.execute("""
        SELECT 
            job_id, started_at, finished_at, status, source_endpoint,
            records_processed, exact_identifier_matches, verified_matches,
            unmatched_records, ambiguous_records, manager_additions,
            manager_departures, summary_json, errors
        FROM fund_manager_sync_history
        ORDER BY started_at DESC
        LIMIT 10;
    """)
    runs = [
        {
            "job_id": r[0],
            "started_at": r[1],
            "finished_at": r[2],
            "status": r[3],
            "source_endpoint": r[4],
            "records_processed": r[5],
            "exact_matches": r[6],
            "verified_matches": r[7],
            "unmatched_records": r[8],
            "ambiguous_records": r[9],
            "manager_additions": r[10],
            "manager_departures": r[11],
            "summary": r[12],
            "errors": r[13]
        }
        for r in cur.fetchall()
    ]

    last_run = runs[0] if runs else None

    return {
        "health": "healthy" if last_run and last_run["status"] == "success" else "warning",
        "total_local_schemes": total_schemes,
        "matched_schemes": matched_schemes,
        "coverage_percent": round(matched_schemes / total_schemes * 100, 2) if total_schemes else 0,
        "total_fund_managers": total_managers,
        "review_queue": {
            "pending": q_counts[0] or 0,
            "approved": q_counts[1] or 0,
            "rejected": q_counts[2] or 0
        },
        "last_run": last_run,
        "recent_runs": runs
    }


def _run_sync_task():
    """Background worker task for running synchronization."""
    logger.info("⚡ Background synchronization worker triggered.")
    try:
        service = FundManagerSyncService()
        res = service.sync(dry_run=False)
        logger.info(f"⚡ Background sync completed successfully: {res['matched_schemes']} matched.")
    except Exception as e:
        logger.error(f"⚡ Background sync failed: {e}", exc_info=True)


@router.post("/admin/fund-manager-sync/run")
async def trigger_sync_run(
    background_tasks: BackgroundTasks,
    dry_run: bool = Query(False),
    authorized: bool = Depends(verify_admin)
):
    """
    Trigger fund manager synchronization run asynchronously.
    """
    if dry_run:
        service = FundManagerSyncService()
        result = await asyncio.to_thread(service.sync, dry_run=True)
        return {
            "message": "Dry-run synchronization completed",
            "report": result
        }
    else:
        background_tasks.add_task(_run_sync_task)
        return {
            "message": "Fund Manager Synchronization triggered in the background.",
            "status": "initiated"
        }


@router.get("/admin/fund-manager-sync/unmatched")
async def list_unmatched_queue(
    status: str = Query("pending"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    authorized: bool = Depends(verify_admin),
    cur = Depends(get_db_cursor)
):
    """
    Get items from the unmatched / ambiguous review queue.
    """
    cur.execute("""
        SELECT 
            q.queue_id,
            q.scheme_id,
            s.scheme_name,
            s.plan_type,
            s.option_type,
            q.source_scheme_code,
            q.source_scheme_name,
            q.source_isin,
            q.source_amc,
            q.candidate_match_reason,
            q.confidence_score,
            q.review_status,
            q.created_at
        FROM fund_manager_unmatched_queue q
        LEFT JOIN schemes s ON q.scheme_id = s.scheme_id
        WHERE q.review_status = %s
        ORDER BY q.confidence_score DESC, q.created_at DESC
        LIMIT %s OFFSET %s;
    """, (status, limit, offset))
    rows = cur.fetchall()

    return {
        "status_filter": status,
        "count": len(rows),
        "items": [
            {
                "queue_id": r[0],
                "scheme_id": r[1],
                "local_scheme_name": r[2],
                "plan_type": r[3],
                "option_type": r[4],
                "candidate_source_code": r[5],
                "candidate_source_name": r[6],
                "candidate_isin": r[7],
                "source_amc": r[8],
                "match_reason": r[9],
                "confidence_score": float(r[10]) if r[10] else 0.0,
                "review_status": r[11],
                "queued_at": r[12]
            }
            for r in rows
        ]
    }


@router.post("/admin/fund-manager-sync/review")
async def review_unmatched_item(
    req: ReviewApprovalRequest,
    authorized: bool = Depends(verify_admin),
    cur = Depends(get_db_cursor)
):
    """
    Approve, reject, or ignore an ambiguous/unmatched scheme mapping.
    If approved with an AMFI code, automatically updates schemes.amfi_code
    and links the managers.
    """
    cur.execute("""
        SELECT queue_id, scheme_id, source_scheme_code, source_scheme_name, review_status
        FROM fund_manager_unmatched_queue
        WHERE queue_id = %s;
    """, (req.queue_id,))
    item = cur.fetchone()
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found")

    scheme_id = item[1]
    chosen_amfi_code = req.target_amfi_code or item[2]

    if req.action == "approve":
        if not chosen_amfi_code:
            raise HTTPException(status_code=400, detail="Cannot approve without a target AMFI code")

        # 1. Update schemes table with approved AMFI code
        cur.execute("""
            UPDATE schemes
            SET amfi_code = %s, updated_at = CURRENT_TIMESTAMP
            WHERE scheme_id = %s;
        """, (chosen_amfi_code, scheme_id))

        # 2. Mark queue item approved
        cur.execute("""
            UPDATE fund_manager_unmatched_queue
            SET review_status = 'approved',
                reviewed_by = 'admin',
                reviewed_at = CURRENT_TIMESTAMP,
                notes = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_id = %s;
        """, (req.reviewer_notes or "Approved by Administrator", req.queue_id))

        return {
            "status": "approved",
            "queue_id": req.queue_id,
            "scheme_id": scheme_id,
            "amfi_code_assigned": chosen_amfi_code,
            "message": "Mapping approved and canonical AMFI code recorded."
        }

    elif req.action in ("reject", "ignore"):
        cur.execute("""
            UPDATE fund_manager_unmatched_queue
            SET review_status = %s,
                reviewed_by = 'admin',
                reviewed_at = CURRENT_TIMESTAMP,
                notes = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_id = %s;
        """, (req.action, req.reviewer_notes or f"Marked as {req.action}", req.queue_id))

        return {
            "status": req.action,
            "queue_id": req.queue_id,
            "message": f"Queue item marked as {req.action}."
        }

    else:
        raise HTTPException(status_code=400, detail=f"Invalid action: {req.action}")
