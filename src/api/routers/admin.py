from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, File, UploadFile, BackgroundTasks, Query, Path
from psycopg2.extensions import cursor
from pydantic import BaseModel
import os
import uuid
from datetime import datetime, timedelta, timezone
import json
import pandas as pd
import io

from src.api.models.admin import (
    PendingMerge, NotificationLog, AdminStats,
    UserManagementRead, UserCreate, UserUpdate, InviteCreate, InviteRead
)
from src.api.dependencies import get_db_cursor, verify_admin, get_current_user
from src.api.utils.auth_utils import get_password_hash, create_access_token
from src.config import logger
from src.services.admin_file_service import admin_file_service

router = APIRouter()

# ─────────────────────────────────────────────
# In-memory job store for extraction runs
# ─────────────────────────────────────────────
_extraction_jobs: Dict[str, Any] = {}
_cancelled_jobs: set = set()

ALL_AMC_SLUGS = [
    "abakkus", "absl", "angelone", "axis", "bajaj", "bandhan", "baroda",
    "boi", "canara", "capitalmind", "choice", "dsp", "edelweiss", "franklin",
    "groww", "hdfc", "helios", "hsbc", "icici", "invesco", "iti", "jio_br",
    "jmfinancial", "kotak", "lic", "mahindra", "mirae_asset", "motilal",
    "navi", "nippon", "nj", "old_bridge", "pgim_india", "ppfas", "quant",
    "quantum", "samco", "sbi", "shriram", "sundaram", "tata", "taurus",
    "threesixtyone", "trust", "unifi", "union", "uti", "wealth_company",
    "whiteoak", "zerodha"
]

class PipelineRequest(BaseModel):
    amc_slugs: List[str]
    year: int
    month: int
    steps: List[str] = ["download", "merge", "extract"]
    dry_run: bool = True
    redo: bool = False

@router.get("/pending-merges", response_model=List[PendingMerge], dependencies=[Depends(verify_admin)])
async def get_pending_merges(cur: cursor = Depends(get_db_cursor)):
    """List all pending scheme renames for approval."""
    cur.execute("""
        SELECT 
            pm.merge_id, pm.amc_id, a.amc_name, pm.new_scheme_name, 
            pm.old_scheme_id, s.scheme_name as old_scheme_name,
            pm.plan_type, pm.option_type, pm.is_reinvest,
            pm.confidence_score, pm.detection_method, pm.metadata,
            pm.created_at, pm.status
        FROM pending_scheme_merges pm
        JOIN amcs a ON pm.amc_id = a.amc_id
        JOIN schemes s ON pm.old_scheme_id = s.scheme_id
        WHERE pm.status = 'PENDING'
        ORDER BY pm.created_at DESC
    """)
    
    results = []
    for row in cur.fetchall():
        results.append(PendingMerge(
            merge_id=row[0],
            amc_id=row[1],
            amc_name=row[2],
            new_scheme_name=row[3],
            old_scheme_id=row[4],
            old_scheme_name=row[5],
            plan_type=row[6],
            option_type=row[7],
            is_reinvest=row[8],
            confidence_score=row[9],
            detection_method=row[10],
            metadata=row[11] if isinstance(row[11], dict) else json.loads(row[11] or '{}'),
            created_at=row[12],
            status=row[13]
        ))
    return results

@router.post("/approve-merge/{merge_id}", dependencies=[Depends(verify_admin)])
async def approve_merge(merge_id: int, cur: cursor = Depends(get_db_cursor)):
    """
    Approves a scheme merge:
    1. Creates/Updates scheme_aliases.
    2. Moves snapshots from 'Pending' scheme (if any exist) to 'Canonical' scheme.
    3. Updates pending_scheme_merges status.
    """
    # 1. Get merge details
    cur.execute("SELECT amc_id, new_scheme_name, old_scheme_id, plan_type, option_type, is_reinvest FROM pending_scheme_merges WHERE merge_id = %s", (merge_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Merge request not found")
    
    amc_id, new_name, canonical_id, plan, opt, re = row
    
    # 2. Check if a 'Quarantined' scheme was created for this new name
    cur.execute("SELECT scheme_id FROM schemes WHERE amc_id = %s AND scheme_name = %s AND plan_type = %s AND option_type = %s AND is_reinvest = %s", (amc_id, new_name, plan, opt, re))
    quarantine_row = cur.fetchone()
    quarantine_id = quarantine_row[0] if quarantine_row else None
    
    # 3. Insert into aliases
    cur.execute("""
        INSERT INTO scheme_aliases (amc_id, alias_name, canonical_scheme_id, plan_type, option_type, is_reinvest, approved_by, detection_method)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (amc_id, alias_name, plan_type, option_type, is_reinvest) 
        DO UPDATE SET canonical_scheme_id = EXCLUDED.canonical_scheme_id
    """, (amc_id, new_name, canonical_id, plan, opt, re, 'admin-vault', 'MANUAL_APPROVAL'))
    
    # 4. If quarantine scheme exists, move its snapshots to canonical and DELETE the quarantine scheme
    if quarantine_id and quarantine_id != canonical_id:
        # Move snapshots (idempotently skip if exists in target)
        cur.execute("""
            UPDATE scheme_snapshots 
            SET scheme_id = %s 
            WHERE scheme_id = %s 
            AND period_id NOT IN (SELECT period_id FROM scheme_snapshots WHERE scheme_id = %s)
        """, (canonical_id, quarantine_id, canonical_id))
        
        # Cleanup remaining snapshots that couldn't be moved (duplicates)
        cur.execute("DELETE FROM scheme_snapshots WHERE scheme_id = %s", (quarantine_id,))
        
        # Finally delete the temporary scheme
        cur.execute("DELETE FROM schemes WHERE scheme_id = %s", (quarantine_id,))
        logger.info(f"Merged quarantine scheme_id {quarantine_id} into canonical_id {canonical_id}")

    # 5. Mark as approved
    cur.execute("UPDATE pending_scheme_merges SET status = 'APPROVED' WHERE merge_id = %s", (merge_id,))
    
    return {"status": "success", "message": f"Successfully merged '{new_name}' into scheme ID {canonical_id}"}

@router.post("/reject-merge/{merge_id}", dependencies=[Depends(verify_admin)])
async def reject_merge(merge_id: int, cur: cursor = Depends(get_db_cursor)):
    """Rejects the merge, keeping the schemes separate."""
    cur.execute("UPDATE pending_scheme_merges SET status = 'REJECTED' WHERE merge_id = %s", (merge_id,))
    return {"status": "success", "message": "Merge rejected. Schemes will remain separate."}

@router.get("/alerts", response_model=List[NotificationLog], dependencies=[Depends(verify_admin)])
async def get_alerts(limit: int = Query(50, ge=1, le=100), cur: cursor = Depends(get_db_cursor)):
    """View recent system notifications and alerts."""
    cur.execute("SELECT notification_id, level, category, content, status, created_at FROM notification_logs ORDER BY created_at DESC LIMIT %s", (limit,))
    results = []
    for row in cur.fetchall():
        results.append(NotificationLog(
            notification_id=row[0],
            level=row[1],
            category=row[2],
            content=row[3],
            status=row[4],
            created_at=row[5]
        ))
    return results

@router.get("/stats", response_model=AdminStats, dependencies=[Depends(verify_admin)])
async def get_admin_stats(cur: cursor = Depends(get_db_cursor)):
    """Aggregate statistics for the admin dashboard."""
    # Pending merges
    cur.execute("SELECT COUNT(*) FROM pending_scheme_merges WHERE status = 'PENDING'")
    pending_count = cur.fetchone()[0]
    
    # Total schemes
    cur.execute("SELECT COUNT(*) FROM schemes")
    total_schemes = cur.fetchone()[0]
    
    # Last extraction
    cur.execute("SELECT status FROM extraction_runs ORDER BY created_at DESC LIMIT 1")
    last_status = cur.fetchone()
    last_status = last_status[0] if last_status else "N/A"
    
    # Errors in last 24h
    cur.execute("SELECT COUNT(*) FROM notification_logs WHERE level IN ('ERROR', 'CRITICAL') AND created_at > NOW() - INTERVAL '24 hours'")
    error_count = cur.fetchone()[0]
    
    return AdminStats(
        pending_merges_count=pending_count,
        total_schemes=total_schemes,
        last_extraction_status=last_status,
        error_count_24h=error_count
    )

@router.post("/upload-ace-data", dependencies=[Depends(verify_admin)])
async def upload_ace_data(
    file: UploadFile = File(...),
    cur: cursor = Depends(get_db_cursor)
):
    """
    Ingests market cap and classification data from an Ace Equity Excel export.
    Uses ISIN as the primary key for updates.
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are supported")

    try:
        contents = await file.read()
        
        # 0. Save a copy of the file for auditing
        upload_dir = os.path.join("data", "uploads", "ace_equity")
        os.makedirs(upload_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_filename = f"ACE_INGEST_{timestamp}_{file.filename}"
        file_path = os.path.join(upload_dir, safe_filename)
        
        with open(file_path, "wb") as f:
            f.write(contents)
            
        df = pd.read_excel(io.BytesIO(contents))
        
        # 1. Smart Header Detection
        # Ace Equity files sometimes have title rows at the top (rows 0-5).
        # We look for the row that contains 'ISIN'.
        df_full = pd.read_excel(io.BytesIO(contents), header=None)
        header_row_idx = 0
        found_header = False
        
        for i, row in df_full.head(10).iterrows():
            row_str = [str(cell).upper() for cell in row]
            if any('ISIN' in s for s in row_str):
                header_row_idx = i
                found_header = True
                break
        
        if not found_header:
            # Fallback to standard reading if ISIN not found in first 10 rows
            df = pd.read_excel(io.BytesIO(contents))
        else:
            # Re-read with detected header row
            df = pd.read_excel(io.BytesIO(contents), skiprows=header_row_idx)

        # 2. Flexible Column Mapping
        cols = {str(c).upper().strip(): c for c in df.columns}
        
        isin_col = next((cols[c] for c in cols if 'ISIN' in c), None)
        mcap_col = next((cols[c] for c in cols if 'MARKET CAP' in c or 'MCAP' in c or 'VALUATION' in c), None)
        type_col = next((cols[c] for c in cols if any(k in c for k in ['CATEGORY', 'CLASSIFICATION', 'TYPE', 'CAP']) and 'MARKET' not in c), None)
        shares_col = next((cols[c] for c in cols if 'SHARES' in c or 'OUTSTANDING' in c), None)

        if not isin_col:
            raise HTTPException(status_code=400, detail="Could not find 'ISIN' column in Excel. Tried scanning first 10 rows.")

        # 3. Process and Update
        updates_count = 0
        skipped_count = 0
        now = datetime.now()

        for _, row in df.iterrows():
            isin = str(row[isin_col]).strip()
            if not isin or isin == 'nan':
                continue
            
            mcap = row[mcap_col] if mcap_col else None
            mcap_type = str(row[type_col]).strip() if type_col else None
            shares = row[shares_col] if shares_col else None

            # Clean and Normalize data
            # Handle NaN values from pandas
            try:
                mcap_val = int(float(mcap)) if pd.notnull(mcap) else None
                shares_val = int(float(shares)) if pd.notnull(shares) else None
                mcap_type_val = mcap_type if pd.notnull(mcap_type) and mcap_type != 'nan' else None
            except:
                mcap_val = None
                shares_val = None
                mcap_type_val = None

            # Update DB
            cur.execute("""
                UPDATE companies 
                SET market_cap = COALESCE(%s, market_cap),
                    mcap_type = COALESCE(%s, mcap_type),
                    shares_outstanding = COALESCE(%s, shares_outstanding),
                    mcap_updated_at = CASE WHEN %s IS NOT NULL THEN %s ELSE mcap_updated_at END,
                    shares_last_updated_at = CASE WHEN %s IS NOT NULL THEN %s ELSE shares_last_updated_at END
                WHERE isin = %s
            """, (mcap_val, mcap_type_val, shares_val, mcap_val, now, shares_val, now, isin))
            
            if cur.rowcount > 0:
                updates_count += 1
            else:
                skipped_count += 1

        cur.connection.commit()
        
        return {
            "status": "success",
            "message": f"Processed {len(df)} rows.",
            "updates_count": updates_count,
            "skipped_count": skipped_count,
            "timestamp": now.isoformat()
        }

    except Exception as e:
        logger.error(f"Ace Equity upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")

@router.post("/sync-shares", dependencies=[Depends(verify_admin)])
async def trigger_shares_sync(background_tasks: BackgroundTasks):
    """
    Triggers the automated background job to sync shares outstanding from yfinance for all companies.
    """
    from src.services.shares_service import shares_service
    
    try:
        # Pushing this to a background task prevents blocking the FastAPI event loop
        background_tasks.add_task(shares_service.sync_all_shares)
        
        return {
            "status": "success",
            "message": "Shares sync initiated in the background.",
            "updated_count": "Pending (Background execution)"
        }
    except Exception as e:
        logger.error(f"Error pushing shares sync: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to sync shares: {str(e)}")
@router.get("/files/inventory", dependencies=[Depends(verify_admin)])
async def get_file_inventory(cur: cursor = Depends(get_db_cursor)):
    """List all raw and merged files with their DB status."""
    try:
        inventory = admin_file_service.get_inventory(cur)
        stats = admin_file_service.get_storage_stats()
        return {
            "status": "success",
            "inventory": inventory,
            "stats": stats
        }
    except Exception as e:
        logger.error(f"Error getting file inventory: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/files", dependencies=[Depends(verify_admin)])
async def delete_file(
    amc_slug: str = Query(...),
    year: int = Query(...),
    month: int = Query(...),
    category: str = Query(..., pattern="^(raw|merged)$"),
    cur: cursor = Depends(get_db_cursor)
):
    """Delete a specific raw data folder or merged Excel file."""
    success = admin_file_service.delete_files(amc_slug, year, month, category)
    if not success:
        raise HTTPException(status_code=404, detail=f"File/folder not found for {amc_slug} {year}-{month} ({category})")
    
    # Log the action
    cur.execute("""
        INSERT INTO notification_logs (level, category, content)
        VALUES ('WARNING', 'FILE_MANAGEMENT', %s)
    """, (f"Admin manually DELETED {category} files for {amc_slug} period {year}-{month}",))
    cur.connection.commit()
    
    return {"status": "success", "message": f"Successfully deleted {category} for {amc_slug} {year}-{month}"}

@router.delete("/files/bulk", dependencies=[Depends(verify_admin)])
async def bulk_delete_files(
    year: int = Query(...),
    month: int = Query(...),
    category: str = Query(..., pattern="^(raw|merged)$"),
    cur: cursor = Depends(get_db_cursor)
):
    """Delete ALL files for a specific month/year across all AMCs."""
    inventory = admin_file_service.get_inventory(cur)
    deleted_count = 0
    
    for item in inventory:
        if item['year'] == year and item['month'] == month:
            # Check if requested category is present
            if (category == 'raw' and item['raw_present']) or (category == 'merged' and item['merged_present']):
                if admin_file_service.delete_files(item['amc_slug'], year, month, category):
                    deleted_count += 1
    
    if deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"No {category} files found for period {year}-{month} to delete.")

    # Log the action
    cur.execute("""
        INSERT INTO notification_logs (level, category, content)
        VALUES ('CRITICAL', 'FILE_MANAGEMENT', %s)
    """, (f"Admin triggered BULK DELETE of {category} files for period {year}-{month}. Total deleted: {deleted_count}",))
    cur.connection.commit()

    return {"status": "success", "message": f"Bulk deleted {deleted_count} {category} file sets for {year}-{month}"}


# ─────────────────────────────────────────────────────────
# Extraction Control Endpoints
# ─────────────────────────────────────────────────────────

@router.get("/amc-slugs", dependencies=[Depends(verify_admin)])
async def get_amc_slugs():
    """Return list of all valid AMC slugs."""
    return {"slugs": ALL_AMC_SLUGS}


@router.post("/trigger-pipeline", dependencies=[Depends(verify_admin)])
async def trigger_pipeline(
    request: PipelineRequest,
    background_tasks: BackgroundTasks,
):
    """Trigger the data pipeline (download, merge, extract) for one or more AMCs."""
    from src.downloaders.downloader_orchestrator import PipelineOrchestrator

    job_id = str(uuid.uuid4())[:8].upper()
    slugs = ALL_AMC_SLUGS if "all" in request.amc_slugs else request.amc_slugs

    # PREVENT FUTURE DATA RUNS
    # Data is only available after the month ends — block current and future months.
    now = datetime.now()
    if (request.year, request.month) >= (now.year, now.month):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot trigger pipeline for {request.year}-{request.month:02d}. "
                   f"Data is only available for completed past months (before {now.year}-{now.month:02d})."
        )

    _extraction_jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "amc_slugs": slugs,
        "year": request.year,
        "month": request.month,
        "steps": request.steps,
        "dry_run": request.dry_run,
        "redo": request.redo,
        "results": {},
        "started_at": datetime.utcnow().isoformat(),
        "completed_at": None,
        "total": len(slugs),
        "done": 0,
    }

    def run_pipeline():
        try:
            orchestrator = PipelineOrchestrator(cancelled_jobs=_cancelled_jobs)
            _extraction_jobs[job_id]["status"] = "running"
            for slug in slugs:
                # CHECK FOR CANCELLATION
                if job_id in _cancelled_jobs:
                    logger.warning(f"Job {job_id} cancelled by user. Halting pipeline.")
                    break

                try:
                    result = orchestrator.run_pipeline(
                        amc_slug=slug,
                        year=request.year,
                        month=request.month,
                        steps=request.steps,
                        dry_run=request.dry_run,
                        redo=request.redo,
                        job_id=job_id
                    )
                    _extraction_jobs[job_id]["results"][slug] = result
                except Exception as e:
                    logger.error(f"Pipeline error for {slug}: {e}")
                    _extraction_jobs[job_id]["results"][slug] = {
                        "status": "error", "error": str(e)[:300]
                    }
                _extraction_jobs[job_id]["done"] += 1
        finally:
            _extraction_jobs[job_id]["status"] = "completed"
            _extraction_jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()

    background_tasks.add_task(run_pipeline)
    return {"job_id": job_id, "status": "queued", "amc_count": len(slugs)}


@router.get("/extraction-jobs", dependencies=[Depends(verify_admin)])
async def get_extraction_jobs():
    """List the 20 most recent extraction jobs."""
    jobs = sorted(
        _extraction_jobs.values(),
        key=lambda x: x["started_at"],
        reverse=True
    )[:20]
    return jobs


@router.post("/cancel-job/{job_id}", dependencies=[Depends(verify_admin)])
async def cancel_job(job_id: str):
    """Mark a job as cancelled so the orchestrator stops processing further AMCs."""
    if job_id not in _extraction_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    _cancelled_jobs.add(job_id)
    _extraction_jobs[job_id]["status"] = "cancelled"
    return {"message": f"Job {job_id} cancellation requested"}


@router.get("/extraction-jobs/{job_id}", dependencies=[Depends(verify_admin)])
async def get_extraction_job(job_id: str):
    """Return status and results for a specific job."""
    if job_id not in _extraction_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return _extraction_jobs[job_id]


# ─────────────────────────────────────────────────────────
# User Management Endpoints
# ─────────────────────────────────────────────────────────

@router.get("/users", response_model=List[UserManagementRead], dependencies=[Depends(verify_admin)])
async def list_users(cur: cursor = Depends(get_db_cursor)):
    """List all users for management."""
    cur.execute("""
        SELECT id, username, email, role, is_active, permissions, expires_at, 
               last_login, created_at, failed_login_attempts, locked_until 
        FROM users 
        ORDER BY created_at DESC
    """)
    results = []
    for row in cur.fetchall():
        results.append(UserManagementRead(
            id=row[0], username=row[1], email=row[2], role=row[3],
            is_active=row[4], permissions=row[5] or [], expires_at=row[6],
            last_login=row[7], created_at=row[8], failed_login_attempts=row[9],
            locked_until=row[10]
        ))
    return results

@router.post("/users", response_model=UserManagementRead, dependencies=[Depends(verify_admin)])
async def create_user(user_in: UserCreate, cur: cursor = Depends(get_db_cursor)):
    """Create a new user manually."""
    # Check if exists
    cur.execute("SELECT id FROM users WHERE username = %s OR email = %s", (user_in.username, user_in.email))
    if cur.fetchone():
        raise HTTPException(status_code=400, detail="Username or Email already exists")
    
    password_hash = get_password_hash(user_in.password)
    
    cur.execute("""
        INSERT INTO users (username, email, password_hash, role, permissions, expires_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id, created_at
    """, (user_in.username, user_in.email, password_hash, user_in.role, 
          json.dumps(user_in.permissions), user_in.expires_at))
    
    res = cur.fetchone()
    return UserManagementRead(
        id=res[0], username=user_in.username, email=user_in.email, role=user_in.role,
        is_active=True, permissions=user_in.permissions, expires_at=user_in.expires_at,
        last_login=None, created_at=res[1], failed_login_attempts=0, locked_until=None
    )

@router.patch("/users/{user_id}", response_model=UserManagementRead, dependencies=[Depends(verify_admin)])
async def update_user(user_id: int, user_in: UserUpdate, cur: cursor = Depends(get_db_cursor)):
    """Update user details, permissions, or reset password."""
    # Build dynamic update query
    updates = []
    params = []
    
    if user_in.username is not None:
        updates.append("username = %s")
        params.append(user_in.username)
    if user_in.email is not None:
        updates.append("email = %s")
        params.append(user_in.email)
    if user_in.role is not None:
        updates.append("role = %s")
        params.append(user_in.role)
    if user_in.is_active is not None:
        updates.append("is_active = %s")
        params.append(user_in.is_active)
        # If activating, reset failed attempts
        if user_in.is_active:
            updates.append("failed_login_attempts = 0")
            updates.append("locked_until = NULL")
    if user_in.permissions is not None:
        updates.append("permissions = %s")
        params.append(json.dumps(user_in.permissions))
    if user_in.expires_at is not None:
        updates.append("expires_at = %s")
        params.append(user_in.expires_at)
    if user_in.password is not None:
        updates.append("password_hash = %s")
        params.append(get_password_hash(user_in.password))
        
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
        
    params.append(user_id)
    query = f"UPDATE users SET {', '.join(updates)} WHERE id = %s RETURNING id"
    cur.execute(query, tuple(params))
    
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="User not found")
        
    # Fetch updated user
    cur.execute("""
        SELECT id, username, email, role, is_active, permissions, expires_at, 
               last_login, created_at, failed_login_attempts, locked_until 
        FROM users WHERE id = %s
    """, (user_id,))
    row = cur.fetchone()
    return UserManagementRead(
        id=row[0], username=row[1], email=row[2], role=row[3],
        is_active=row[4], permissions=row[5] or [], expires_at=row[6],
        last_login=row[7], created_at=row[8], failed_login_attempts=row[9],
        locked_until=row[10]
    )

@router.post("/users/{user_id}/impersonate", dependencies=[Depends(verify_admin)])
async def impersonate_user(user_id: int, cur: cursor = Depends(get_db_cursor)):
    """Admin 'Masquerade' feature: Generate a token for any user."""
    cur.execute("SELECT username, role, is_active FROM users WHERE id = %s", (user_id,))
    user = cur.fetchone()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if not user[2]: # is_active
        raise HTTPException(status_code=400, detail="Cannot impersonate an inactive user")
        
    # Generate token for that user
    access_token = create_access_token(data={"sub": user[0]})
    
    logger.info(f"Admin is impersonating user: {user[0]}")
    return {"access_token": access_token, "token_type": "bearer", "username": user[0]}


# ─────────────────────────────────────────────────────────
# Invite Link System Endpoints
# ─────────────────────────────────────────────────────────

@router.post("/invites", response_model=InviteRead, dependencies=[Depends(verify_admin)])
async def create_invite(
    invite_in: InviteCreate, 
    cur: cursor = Depends(get_db_cursor)
):
    """Generate a unique invite token for a guest."""
    token = str(uuid.uuid4().hex)
    expires_at = datetime.utcnow() + timedelta(days=invite_in.days_valid)
    
    cur.execute("""
        INSERT INTO user_invites (token, email, permissions, expires_at, created_by, account_expiry_days)
        VALUES (%s, %s, %s, %s, NULL, %s)
        RETURNING invite_id, created_at
    """, (token, invite_in.email, json.dumps(invite_in.permissions), expires_at, invite_in.account_expiry_days))
    
    res = cur.fetchone()
    return InviteRead(
        invite_id=res[0], token=token, email=invite_in.email,
        permissions=invite_in.permissions, expires_at=expires_at,
        account_expiry_days=invite_in.account_expiry_days,
        is_used=False, created_at=res[1]
    )

@router.get("/invites", response_model=List[InviteRead], dependencies=[Depends(verify_admin)])
async def list_invites(cur: cursor = Depends(get_db_cursor)):
    """List all active/inactive onboarding invites."""
    cur.execute("SELECT invite_id, token, email, permissions, expires_at, is_used, created_at, account_expiry_days FROM user_invites ORDER BY created_at DESC")
    invites = []
    for row in cur.fetchall():
        invites.append(InviteRead(
            invite_id=row[0], token=row[1], email=row[2],
            permissions=row[3] or [], expires_at=row[4],
            is_used=row[5], created_at=row[6], account_expiry_days=row[7]
        ))
    return invites

@router.delete("/invites/{invite_id}", dependencies=[Depends(verify_admin)])
async def revoke_invite(invite_id: int, cur: cursor = Depends(get_db_cursor)):
    """Revoke/Delete an invite link."""
    cur.execute("DELETE FROM user_invites WHERE invite_id = %s", (invite_id,))
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Invite not found")
    return {"status": "success", "message": "Invite revoked"}


@router.get("/public/invites/{token}", response_model=InviteRead)
async def verify_invite(token: str, cur: cursor = Depends(get_db_cursor)):
    """Public endpoint to verify an invite token before registration."""
    cur.execute("""
        SELECT invite_id, token, email, permissions, expires_at, is_used, created_at 
        FROM user_invites WHERE token = %s AND is_used = FALSE
    """, (token,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Invalid or used invite token")
        
    expires_at = row[4]
    if expires_at and datetime.now(timezone.utc) > expires_at:
        raise HTTPException(status_code=400, detail="Invite link has expired")
        
    return InviteRead(
        invite_id=row[0], token=row[1], email=row[2],
        permissions=row[3] or [], expires_at=row[4],
        account_expiry_days=row[7] if len(row) > 7 else None,
        is_used=row[5], created_at=row[6]
    )

class UserRegisterRequest(BaseModel):
    token: str
    username: str
    password: str

@router.post("/public/invites/register")
async def register_from_invite(
    req: UserRegisterRequest,
    cur: cursor = Depends(get_db_cursor)
):
    """Public endpoint to register a new user via invite."""
    token = req.token
    username = req.username
    password = req.password
    # 1. Verify invite again
    cur.execute("SELECT invite_id, email, permissions, expires_at, account_expiry_days FROM user_invites WHERE token = %s AND is_used = FALSE", (token,))
    invite = cur.fetchone()
    if not invite:
        raise HTTPException(status_code=404, detail="Invalid invite")
    
    # invite indices: 0:id, 1:email, 2:permissions, 3:link_expires_at, 4:account_expiry_days
    if invite[3] and datetime.now(timezone.utc) > invite[3]:
        raise HTTPException(status_code=400, detail="Invite link has expired")
        
    # 2. Check if username/email taken
    cur.execute("SELECT id FROM users WHERE username = %s OR email = %s", (username, invite[1]))
    if cur.fetchone():
        raise HTTPException(status_code=400, detail="Username or Email already taken")
        
    # 3. Create user with optional expiry
    user_expires_at = None
    if invite[4] is not None:
        user_expires_at = datetime.now(timezone.utc) + timedelta(days=invite[4])
        # Set to end of day
        user_expires_at = user_expires_at.replace(hour=23, minute=59, second=59)

    hashed = get_password_hash(password)
    cur.execute("""
        INSERT INTO users (username, email, password_hash, role, permissions, expires_at)
        VALUES (%s, %s, %s, 'user', %s, %s)
    """, (username, invite[1], hashed, json.dumps(invite[2]), user_expires_at))
    
    # 4. Mark invite as used
    cur.execute("UPDATE user_invites SET is_used = TRUE WHERE token = %s", (token,))
    
    return {"status": "success", "message": "Registration successful. You can now login."}
