"""
Persistent Execution Ledger Repository.

Manages pipeline_runs and pipeline_execution_ledger tables with transactional
guarantees, advisory locking, crash recovery, and lineage tracking.
"""

import os
import socket
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from src.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, logger
from src.db.connection import get_connection, get_pool_connection

MASTER_PIPELINE_LOCK_ID = 888999111


class PipelineLockManager:
    """
    Manages a persistent, dedicated PostgreSQL connection solely for holding
    a session-level advisory lock across the entire master pipeline run.
    """

    def __init__(self, lock_id: int = MASTER_PIPELINE_LOCK_ID):
        self.lock_id = lock_id
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._locked = False

    def acquire(self) -> bool:
        """
        Attempts to acquire session-level advisory lock.
        Returns True if acquired, False if already held by another process.
        """
        try:
            self._conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                options='-c client_encoding=UTF8'
            )
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s);", (self.lock_id,))
                acquired = cur.fetchone()[0]
                if acquired:
                    self._locked = True
                    logger.info(f"Acquired PostgreSQL Advisory Lock ({self.lock_id})")
                    return True
                else:
                    logger.warning(f"Advisory Lock ({self.lock_id}) is currently held by another active master run.")
                    self.release()
                    return False
        except Exception as e:
            logger.error(f"Error acquiring advisory lock {self.lock_id}: {e}")
            self.release()
            return False

    def release(self):
        """Releases the advisory lock and closes the dedicated connection."""
        if self._conn and not self._conn.closed:
            try:
                if self._locked:
                    with self._conn.cursor() as cur:
                        cur.execute("SELECT pg_advisory_unlock(%s);", (self.lock_id,))
                    logger.info(f"Released PostgreSQL Advisory Lock ({self.lock_id})")
            except Exception as e:
                logger.warning(f"Error releasing advisory lock {self.lock_id}: {e}")
            finally:
                try:
                    self._conn.close()
                except Exception:
                    pass
        self._conn = None
        self._locked = False

    def is_locked(self) -> bool:
        return self._locked


# ─────────────────────────────────────────────────────────────
# Master Pipeline Runs (Batch-level)
# ─────────────────────────────────────────────────────────────

def create_pipeline_run(
    run_id: str,
    target_year: int,
    target_month: int,
    trigger_type: str = "SCHEDULED",
    concurrency: int = 2,
    dry_run: bool = False,
    redo: bool = False,
    total_amcs: int = 0
) -> Dict[str, Any]:
    """Records a new master pipeline run."""
    pid = os.getpid()
    hostname = socket.gethostname()
    conn = get_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, target_year, target_month, trigger_type, status,
                concurrency, dry_run, redo, total_amcs, pid, hostname,
                started_at, heartbeat_at
            )
            VALUES (%s, %s, %s, %s, 'RUNNING', %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING *;
            """,
            (run_id, target_year, target_month, trigger_type, concurrency, dry_run, redo, total_amcs, pid, hostname)
        )
        row = cur.fetchone()
    conn.commit()
    logger.info(f"Pipeline Run registered: {run_id} ({target_year}-{target_month:02d}) [trigger={trigger_type}]")
    return dict(row)


def update_pipeline_run_heartbeat(run_id: str):
    """Refreshes heartbeat timestamp for a running master pipeline."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pipeline_runs SET heartbeat_at = CURRENT_TIMESTAMP WHERE run_id = %s;",
                (run_id,)
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.debug(f"Failed to update heartbeat for {run_id}: {e}")


VALID_RUN_STATUSES = {'RUNNING', 'COMPLETED', 'COMPLETED_WITH_ERRORS', 'FAILED', 'CANCELLED'}

def complete_pipeline_run(
    run_id: str,
    status: str,
    success_count: int,
    failed_count: int,
    skipped_count: int,
    summary_json: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None
):
    """Marks a master pipeline run as completed or failed."""
    if status not in VALID_RUN_STATUSES:
        status = "COMPLETED" if failed_count == 0 else "COMPLETED_WITH_ERRORS"
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
            SET status = %s,
                success_count = %s,
                failed_count = %s,
                skipped_count = %s,
                completed_at = CURRENT_TIMESTAMP,
                summary_json = %s,
                error_message = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s;
            """,
            (
                status,
                success_count,
                failed_count,
                skipped_count,
                Json(summary_json or {}),
                error_message,
                run_id
            )
        )
    conn.commit()
    logger.info(f"Pipeline Run {run_id} marked {status}: success={success_count}, failed={failed_count}, skipped={skipped_count}")


# ─────────────────────────────────────────────────────────────
# Per-AMC Ledger Entries
# ─────────────────────────────────────────────────────────────

def init_ledger_entries(run_id: str, amc_slugs: List[str], year: int, month: int):
    """Initializes PENDING ledger entries for all AMCs in a run."""
    conn = get_connection()
    with conn.cursor() as cur:
        for slug in amc_slugs:
            cur.execute(
                """
                INSERT INTO pipeline_execution_ledger (
                    run_id, amc_slug, year, month, stage, status
                )
                VALUES (%s, %s, %s, %s, 'INIT', 'PENDING')
                ON CONFLICT (run_id, amc_slug) DO UPDATE
                SET status = 'PENDING', stage = 'INIT', updated_at = CURRENT_TIMESTAMP;
                """,
                (run_id, slug, year, month)
            )
    conn.commit()


def start_amc_ledger(run_id: str, amc_slug: str, stage: str = "DOWNLOAD"):
    """Transitions an AMC entry to RUNNING with start timestamp."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_execution_ledger
            SET status = 'RUNNING',
                stage = %s,
                started_at = CURRENT_TIMESTAMP,
                heartbeat_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s AND amc_slug = %s;
            """,
            (stage, run_id, amc_slug)
        )
    conn.commit()


def update_amc_stage(run_id: str, amc_slug: str, stage: str):
    """Updates the stage of an active AMC."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_execution_ledger
                SET stage = %s,
                    heartbeat_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE run_id = %s AND amc_slug = %s;
                """,
                (stage, run_id, amc_slug)
            )
        conn.commit()
    except Exception:
        conn.rollback()


def mark_amc_success(
    run_id: str,
    amc_slug: str,
    amc_id: Optional[int],
    period_id: Optional[int],
    rows_fetched: int = 0,
    rows_extracted: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    duplicates_prevented: int = 0,
    file_hash: Optional[str] = None,
    duration_seconds: float = 0.0,
    metadata: Optional[Dict[str, Any]] = None
):
    """
    Records SUCCESS status in ledger.
    Guaranteed: Only called AFTER database transactions have committed successfully.
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_execution_ledger
            SET status = 'SUCCESS',
                stage = 'COMPLETED',
                amc_id = %s,
                period_id = %s,
                rows_fetched = %s,
                rows_extracted = %s,
                rows_inserted = %s,
                rows_updated = %s,
                duplicates_prevented = %s,
                file_hash = %s,
                duration_seconds = %s,
                completed_at = CURRENT_TIMESTAMP,
                error_message = NULL,
                metadata = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s AND amc_slug = %s;
            """,
            (
                amc_id,
                period_id,
                rows_fetched,
                rows_extracted,
                rows_inserted,
                rows_updated,
                duplicates_prevented,
                file_hash,
                duration_seconds,
                Json(metadata or {}),
                run_id,
                amc_slug
            )
        )
    conn.commit()
    logger.info(f"Ledger [{amc_slug}]: SUCCESS (inserted={rows_inserted}, extracted={rows_extracted})")


VALID_STAGES = {'INIT', 'DOWNLOAD', 'MERGE', 'EXTRACT', 'LOAD', 'COMPLETED', 'PIPELINE'}

def _normalize_stage(stage: Optional[str]) -> str:
    if stage and stage.upper() in VALID_STAGES:
        return stage.upper()
    return "PIPELINE"


def mark_amc_failed(
    run_id: str,
    amc_slug: str,
    stage: str,
    error_type: str,
    error_message: str,
    stack_trace: Optional[str] = None,
    retry_count: int = 0,
    max_retries: int = 3,
    duration_seconds: float = 0.0,
    metadata: Optional[Dict[str, Any]] = None
):
    """Records FAILED status in ledger."""
    stage = _normalize_stage(stage)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_execution_ledger
                SET status = 'FAILED',
                    stage = %s,
                    error_type = %s,
                    error_message = %s,
                    stack_trace = %s,
                    retry_count = %s,
                    max_retries = %s,
                    duration_seconds = %s,
                    completed_at = CURRENT_TIMESTAMP,
                    metadata = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE run_id = %s AND amc_slug = %s;
                """,
                (
                    stage,
                    error_type,
                    error_message,
                    stack_trace,
                    retry_count,
                    max_retries,
                    duration_seconds,
                    Json(metadata or {}),
                    run_id,
                    amc_slug
                )
            )
        conn.commit()
        logger.error(f"Ledger [{amc_slug}]: FAILED at stage {stage}: {error_message}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to record failure for {amc_slug} in ledger: {e}")


def mark_amc_retry_pending(
    run_id: str,
    amc_slug: str,
    stage: str,
    error_message: str,
    retry_count: int,
    max_retries: int = 3,
    metadata: Optional[Dict[str, Any]] = None
):
    """Marks AMC as RETRY_PENDING prior to retry backoff."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_execution_ledger
            SET status = 'RETRY_PENDING',
                stage = %s,
                error_message = %s,
                retry_count = %s,
                max_retries = %s,
                metadata = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s AND amc_slug = %s;
            """,
            (stage, error_message, retry_count, max_retries, Json(metadata or {}), run_id, amc_slug)
        )
    conn.commit()


def mark_amc_skipped(
    run_id: str,
    amc_slug: str,
    reason: str,
    duration_seconds: float = 0.0,
    metadata: Optional[Dict[str, Any]] = None
):
    """Marks AMC as SKIPPED (e.g., already successfully processed)."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_execution_ledger
            SET status = 'SKIPPED',
                stage = 'COMPLETED',
                error_message = %s,
                duration_seconds = %s,
                completed_at = CURRENT_TIMESTAMP,
                metadata = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s AND amc_slug = %s;
            """,
            (reason, duration_seconds, Json(metadata or {}), run_id, amc_slug)
        )
    conn.commit()
    logger.info(f"Ledger [{amc_slug}]: SKIPPED ({reason})")


def is_amc_period_successful(amc_slug: str, year: int, month: int) -> bool:
    """
    Double-check idempotency:
    Returns True only if:
    1. A ledger record has status = 'SUCCESS' for (amc_slug, year, month).
    2. An actual scheme snapshot exists for this AMC's schemes in that period.
    """
    conn = get_connection()
    with conn.cursor() as cur:
        # Check ledger record
        cur.execute(
            """
            SELECT amc_id, period_id FROM pipeline_execution_ledger
            WHERE amc_slug = %s AND year = %s AND month = %s AND status = 'SUCCESS'
            ORDER BY completed_at DESC LIMIT 1;
            """,
            (amc_slug, year, month)
        )
        ledger_row = cur.fetchone()
        if not ledger_row:
            return False

        amc_id, period_id = ledger_row
        if not amc_id or not period_id:
            return True

        # Verify underlying snapshot exists
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM scheme_snapshots ss
                JOIN schemes s ON ss.scheme_id = s.scheme_id
                WHERE s.amc_id = %s AND ss.period_id = %s
            );
            """,
            (amc_id, period_id)
        )
        snapshot_exists = cur.fetchone()[0]
        return snapshot_exists


def recover_orphaned_runs(current_run_id: Optional[str] = None) -> int:
    """
    Safeguard 2 Implementation:
    Recovers orphaned RUNNING records without blindly resetting active jobs.
    1. Checks all pipeline_runs where status = 'RUNNING'.
    2. If run_id is current_run_id, skips it.
    3. Checks if recorded PID is alive on current host:
       - If process is alive, do NOT recover.
       - If process is dead or heartbeat has ceased (> 30 min), marks run FAILED.
    4. For any dead runs, updates associated RUNNING ledger entries to RETRY_PENDING.
    """
    current_hostname = socket.gethostname()
    current_pid = os.getpid()
    recovered_count = 0

    conn = get_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT run_id, pid, hostname, heartbeat_at, started_at
            FROM pipeline_runs
            WHERE status = 'RUNNING'
            """
        )
        running_batches = cur.fetchall()

        for batch in running_batches:
            batch_run_id = batch["run_id"]
            if batch_run_id == current_run_id:
                continue

            batch_pid = batch["pid"]
            batch_host = batch["hostname"]
            is_dead = False

            # If on the same host, inspect OS process table
            if batch_host == current_hostname and batch_pid:
                if batch_pid == current_pid:
                    continue
                try:
                    # Signal 0 tests process existence
                    os.kill(batch_pid, 0)
                    # Process exists! Do not touch it.
                    logger.info(f"Run {batch_run_id} PID {batch_pid} is confirmed ACTIVE. Skipping recovery.")
                    continue
                except (OSError, ProcessLookupError):
                    # Process does not exist
                    is_dead = True
            else:
                # Different host or missing PID: check heartbeat age (> 30 min)
                hb = batch["heartbeat_at"] or batch["started_at"]
                if hb:
                    age_seconds = (datetime.now(timezone.utc) - hb).total_seconds()
                    if age_seconds > 1800:
                        is_dead = True

            if is_dead:
                logger.warning(f"Orphaned master run detected: {batch_run_id} (PID {batch_pid} not alive). Recovering...")
                # Mark master batch FAILED
                cur.execute(
                    """
                    UPDATE pipeline_runs
                    SET status = 'FAILED',
                        error_message = 'Interrupted: process terminated unexpectedly',
                        completed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE run_id = %s;
                    """,
                    (batch_run_id,)
                )

                # Transition interrupted RUNNING ledger entries to RETRY_PENDING
                cur.execute(
                    """
                    UPDATE pipeline_execution_ledger
                    SET status = 'RETRY_PENDING',
                        error_message = 'Interrupted by process termination/restart. Pending retry.',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE run_id = %s AND status = 'RUNNING';
                    """,
                    (batch_run_id,)
                )
                recovered_count += cur.rowcount

    conn.commit()
    if recovered_count > 0:
        logger.info(f"Safely recovered {recovered_count} orphaned AMC ledger job(s) to RETRY_PENDING.")
    return recovered_count


# ─────────────────────────────────────────────────────────────
# Queries for Admin Dashboard
# ─────────────────────────────────────────────────────────────

def get_recent_pipeline_runs(limit: int = 20) -> List[Dict[str, Any]]:
    """Fetches recent pipeline batches formatted for the Admin Dashboard."""
    conn = get_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT 
                r.run_id,
                r.target_year as year,
                r.target_month as month,
                r.trigger_type,
                r.status,
                r.concurrency,
                r.dry_run,
                r.redo,
                r.total_amcs as total,
                (r.success_count + r.failed_count + r.skipped_count) as done,
                r.success_count,
                r.failed_count,
                r.skipped_count,
                r.started_at,
                r.completed_at,
                r.error_message
            FROM pipeline_runs r
            ORDER BY r.started_at DESC
            LIMIT %s;
            """,
            (limit,)
        )
        runs = cur.fetchall()

    result = []
    for r in runs:
        item = dict(r)
        # Format timestamps as ISO strings
        if item.get("started_at"):
            item["started_at"] = item["started_at"].isoformat()
        if item.get("completed_at"):
            item["completed_at"] = item["completed_at"].isoformat()
        result.append(item)
    return result


def get_pipeline_run_details(run_id: str) -> Optional[Dict[str, Any]]:
    """Fetches full progress details of a pipeline run including per-AMC status."""
    conn = get_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT * FROM pipeline_runs WHERE run_id = %s;
            """,
            (run_id,)
        )
        run_row = cur.fetchone()
        if not run_row:
            return None

        cur.execute(
            """
            SELECT 
                amc_slug, stage, status, retry_count, max_retries,
                rows_fetched, rows_extracted, rows_inserted, rows_updated,
                duplicates_prevented, file_hash, error_type, error_message,
                duration_seconds, started_at, completed_at, metadata
            FROM pipeline_execution_ledger
            WHERE run_id = %s
            ORDER BY amc_slug;
            """,
            (run_id,)
        )
        amc_rows = cur.fetchall()

    run_dict = dict(run_row)
    results = {}
    done_count = 0
    slugs = []

    for row in amc_rows:
        slug = row["amc_slug"]
        slugs.append(slug)
        status = row["status"].lower()
        if status in ("success", "failed", "skipped", "cancelled"):
            done_count += 1

        stage_str = row["stage"] or "INIT"
        status_str = row["status"].lower()
        is_success = status_str in ("success", "skipped")
        download_status = "success" if (is_success or stage_str in ("MERGE", "EXTRACT", "LOAD", "COMPLETED")) else ("failed" if stage_str == "DOWNLOAD" and status_str == "failed" else ("running" if stage_str == "DOWNLOAD" and status_str == "running" else "pending"))
        merge_status = "success" if (is_success or stage_str in ("EXTRACT", "LOAD", "COMPLETED")) else ("failed" if stage_str == "MERGE" and status_str == "failed" else ("running" if stage_str == "MERGE" and status_str == "running" else "pending"))
        extract_status = status_str if stage_str in ("EXTRACT", "LOAD", "COMPLETED") else ("failed" if status_str == "failed" else ("running" if status_str == "running" else "pending"))

        results[slug] = {
            "status": status,
            "stage": stage_str,
            "rows_extracted": row["rows_extracted"],
            "rows_inserted": row["rows_inserted"],
            "duplicates_prevented": row["duplicates_prevented"],
            "duration": float(row["duration_seconds"] or 0),
            "retry_count": row["retry_count"],
            "error": row["error_message"],
            "steps": {
                "download": {"status": download_status},
                "merge": {"status": merge_status},
                "extract": {
                    "status": extract_status,
                    "rows_read": row["rows_extracted"],
                    "rows_inserted": row["rows_inserted"],
                    "error": row["error_message"]
                }
            }
        }

    return {
        "job_id": run_dict["run_id"],
        "status": run_dict["status"].lower(),
        "amc_slugs": slugs,
        "year": run_dict["target_year"],
        "month": run_dict["target_month"],
        "steps": ["download", "merge", "extract"],
        "dry_run": run_dict["dry_run"],
        "redo": run_dict["redo"],
        "results": results,
        "started_at": run_dict["started_at"].isoformat() if run_dict["started_at"] else None,
        "completed_at": run_dict["completed_at"].isoformat() if run_dict["completed_at"] else None,
        "total": len(slugs),
        "done": done_count,
        "success_count": run_dict["success_count"],
        "failed_count": run_dict["failed_count"],
        "skipped_count": run_dict["skipped_count"]
    }
