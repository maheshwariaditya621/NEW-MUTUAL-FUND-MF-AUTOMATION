from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, File, UploadFile, BackgroundTasks, Query, Path
from psycopg2.extensions import cursor
import os
from datetime import datetime
import json
import pandas as pd
import io

from src.api.models.admin import PendingMerge, NotificationLog, AdminStats
from src.api.dependencies import get_db_cursor, verify_admin
from src.config import logger
from src.services.admin_file_service import admin_file_service

router = APIRouter()

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
    category: str = Query(..., regex="^(raw|merged)$"),
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
    category: str = Query(..., regex="^(raw|merged)$"),
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
