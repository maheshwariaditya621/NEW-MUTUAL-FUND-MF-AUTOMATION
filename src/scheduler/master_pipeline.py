"""
Master Pipeline Orchestrator.

Centralized, production-grade master scheduler for all 50 AMC integrations.
Features:
- PostgreSQL session advisory lock (overlapping run protection)
- Safe crash recovery for interrupted/orphaned jobs
- Bounded concurrency (default: 2)
- Transient error classification with exponential backoff retries
- Transaction-guaranteed ledger recording
- Pure idempotent execution (zero duplicates)
"""

import os
import sys
import time
import uuid
import socket
import argparse
import traceback
import shutil
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple

# Automatically anchor current working directory to project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import logger
from src.downloaders.downloader_orchestrator import PipelineOrchestrator
from src.downloaders.downloader_factory import DownloaderFactory
from src.alerts.telegram_notifier import get_notifier
from src.db.ledger_repository import (
    PipelineLockManager,
    create_pipeline_run,
    update_pipeline_run_heartbeat,
    complete_pipeline_run,
    init_ledger_entries,
    start_amc_ledger,
    update_amc_stage,
    mark_amc_success,
    mark_amc_failed,
    mark_amc_retry_pending,
    mark_amc_skipped,
    is_amc_period_successful,
    recover_orphaned_runs,
)

# Transient error signatures suitable for retries
TRANSIENT_KEYWORDS = [
    "timeout", "timed out", "connection reset", "connection refused",
    "429", "too many requests", "502", "bad gateway", "503", "service unavailable",
    "504", "gateway timeout", "remote disconnected", "socket", "network is unreachable"
]


def is_transient_error(error_msg: str) -> bool:
    """Classifies an error message as transient (retryable) or permanent."""
    if not error_msg:
        return False
    msg_lower = error_msg.lower()
    return any(k in msg_lower for k in TRANSIENT_KEYWORDS)


def get_applicable_period() -> Tuple[int, int]:
    """
    Computes the target SEBI disclosure month.
    SEBI mandates monthly portfolio disclosure by the 10th-15th of the following month.
    Thus, data gathered in current month M always targets calendar month M-1.
    """
    now = datetime.now()
    if now.month == 1:
        return now.year - 1, 12
    else:
        return now.year, now.month - 1


class MasterPipelineRunner:
    """
    Executes the centralized AMC master pipeline with strict concurrency,
    transaction boundaries, and persistent ledger auditing.
    """

    def __init__(
        self,
        year: Optional[int] = None,
        month: Optional[int] = None,
        amc_slugs: Optional[List[str]] = None,
        concurrency: int = 2,
        dry_run: bool = False,
        redo: bool = False,
        trigger_type: str = "SCHEDULED",
        max_retries: int = 3,
        backoff_delays: Optional[List[int]] = None
    ):
        calc_year, calc_month = get_applicable_period()
        self.year = year or calc_year
        self.month = month or calc_month
        self.amc_slugs = amc_slugs or sorted(list(DownloaderFactory.DOWNLOADER_MAP.keys()))
        self.concurrency = max(1, concurrency)
        self.dry_run = dry_run
        self.redo = redo
        self.trigger_type = trigger_type
        self.max_retries = max_retries
        self.backoff_delays = backoff_delays or [5, 15, 30]
        self.run_id = str(uuid.uuid4())[:8].upper()
        self.lock_manager = PipelineLockManager()
        try:
            self.notifier = get_notifier()
        except Exception as e:
            logger.warning(f"Could not initialize Telegram notifier: {e}")
            self.notifier = None

    def _notify(self, message: str):
        """Safely dispatches a Telegram alert without crashing execution."""
        if not self.notifier:
            return
        try:
            self.notifier.alert(message)
        except Exception as e:
            logger.debug(f"Telegram notification dispatch failed: {e}")

    def run(self) -> Dict[str, Any]:
        """
        Main execution loop of the master pipeline.
        Guarantees advisory lock holding and clean release.
        """
        start_time = time.time()
        logger.info(f"=== MASTER PIPELINE INITIALIZING ===")
        logger.info(f"Run ID: {self.run_id}")
        logger.info(f"Target Period: {self.year}-{self.month:02d}")
        logger.info(f"AMCs: {len(self.amc_slugs)} | Concurrency: {self.concurrency} | Trigger: {self.trigger_type}")
        logger.info(f"Dry Run: {self.dry_run} | Redo: {self.redo}")

        # 1. Acquire PostgreSQL Session Advisory Lock (Safeguard 3)
        if not self.lock_manager.acquire():
            logger.warning("Another master pipeline instance is currently running. Exiting cleanly to prevent overlap.")
            return {
                "status": "skipped",
                "reason": "advisory_lock_held",
                "message": "Another master pipeline instance is active."
            }

        try:
            # 2. Recover Orphaned / Interrupted Runs (Safeguard 2)
            recovered = recover_orphaned_runs(current_run_id=self.run_id)
            if recovered > 0:
                logger.info(f"Recovered {recovered} stale job(s) from previous interrupted runs.")

            # 3. Register Master Run Batch in DB
            create_pipeline_run(
                run_id=self.run_id,
                target_year=self.year,
                target_month=self.month,
                trigger_type=self.trigger_type,
                concurrency=self.concurrency,
                dry_run=self.dry_run,
                redo=self.redo,
                total_amcs=len(self.amc_slugs)
            )

            # 4. Initialize PENDING Ledger Entries
            init_ledger_entries(self.run_id, self.amc_slugs, self.year, self.month)

            # Send Telegram Run Start Alert
            self._notify(
                f"🚀 <b>Pipeline Run Started</b>\n"
                f"🆔 <b>Run ID</b>: <code>{self.run_id}</code>\n"
                f"📅 <b>Period</b>: {self.year}-{self.month:02d}\n"
                f"🏢 <b>AMCs</b>: {len(self.amc_slugs)}\n"
                f"⚙️ <b>Trigger</b>: {self.trigger_type} (Dry Run: {self.dry_run})"
            )

            # 5. Process AMCs with Bounded Concurrency
            success_count = 0
            failed_count = 0
            skipped_count = 0
            total_rows_inserted = 0
            amc_results = {}

            logger.info(f"Executing {len(self.amc_slugs)} AMCs with concurrency={self.concurrency}...")

            with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
                future_to_amc = {
                    executor.submit(self._process_single_amc, slug): slug
                    for slug in self.amc_slugs
                }

                for future in as_completed(future_to_amc):
                    slug = future_to_amc[future]
                    # Periodic heartbeat update
                    update_pipeline_run_heartbeat(self.run_id)

                    try:
                        res = future.result()
                        amc_results[slug] = res
                        st = res.get("status")
                        if st == "success":
                            success_count += 1
                            total_rows_inserted += res.get("rows_inserted", 0)
                        elif st == "skipped":
                            skipped_count += 1
                        else:
                            failed_count += 1
                    except Exception as e:
                        logger.error(f"Unexpected worker thread failure for {slug}: {e}")
                        failed_count += 1
                        amc_results[slug] = {
                            "status": "failed",
                            "stage": "WORKER_THREAD",
                            "error": str(e)
                        }
                        mark_amc_failed(
                            run_id=self.run_id,
                            amc_slug=slug,
                            stage="WORKER_THREAD",
                            error_type="THREAD_EXCEPTION",
                            error_message=str(e),
                            stack_trace=traceback.format_exc()
                        )

            total_duration = time.time() - start_time
            master_status = "COMPLETED" if failed_count == 0 else "COMPLETED_WITH_ERRORS"
            if success_count == 0 and failed_count > 0:
                master_status = "FAILED"

            summary = {
                "run_id": self.run_id,
                "target_period": f"{self.year}-{self.month:02d}",
                "status": master_status,
                "total_amcs": len(self.amc_slugs),
                "success_count": success_count,
                "failed_count": failed_count,
                "skipped_count": skipped_count,
                "rows_inserted": total_rows_inserted,
                "duration_seconds": round(total_duration, 2),
                "concurrency": self.concurrency,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            # 6. Storage Retention: Prune old merged excels (> 2 months)
            self._prune_old_merged_excels(keep_months=2)

            # 7. Complete Master Run in DB
            complete_pipeline_run(
                run_id=self.run_id,
                status=master_status,
                success_count=success_count,
                failed_count=failed_count,
                skipped_count=skipped_count,
                summary_json=summary
            )

            # 8. Print Master Run Summary Table
            self._print_summary_report(summary, amc_results)

            # Send Telegram Run Finish Summary Alert
            self._notify(
                f"🏁 <b>Pipeline Run Finished</b>\n"
                f"🆔 <b>Run ID</b>: <code>{self.run_id}</code>\n"
                f"📅 <b>Period</b>: {self.year}-{self.month:02d}\n"
                f"📊 <b>Status</b>: <b>{master_status}</b>\n"
                f"✅ <b>Success</b>: {success_count} | ⏸️ <b>Skipped</b>: {skipped_count} | ❌ <b>Failed</b>: {failed_count}\n"
                f"💾 <b>Holdings Loaded</b>: {total_rows_inserted:,}\n"
                f"⏱️ <b>Duration</b>: {total_duration/60:.1f} mins"
            )
            return summary

        finally:
            # Always release PostgreSQL advisory lock
            self.lock_manager.release()

    def _process_single_amc(self, amc_slug: str) -> Dict[str, Any]:
        """
        Executes Download -> Merge -> Extract -> Load for one AMC.
        Isolated failure domain: Exceptions here will NEVER crash the master pipeline.
        Includes retry loop for transient failures.
        """
        amc_start = time.time()

        # Idempotency check: Skip if already successfully extracted & loaded (and not redo)
        if not self.redo and not self.dry_run:
            if is_amc_period_successful(amc_slug, self.year, self.month):
                logger.info(f"[{amc_slug.upper()}] Period {self.year}-{self.month:02d} already successfully processed. Skipping.")
                mark_amc_skipped(
                    run_id=self.run_id,
                    amc_slug=amc_slug,
                    reason="already_processed",
                    duration_seconds=time.time() - amc_start
                )
                return {"status": "skipped", "reason": "already_processed"}

        # Retry loop for transient failures
        orchestrator = PipelineOrchestrator()
        current_attempt = 0

        while current_attempt <= self.max_retries:
            start_amc_ledger(self.run_id, amc_slug, stage="DOWNLOAD")

            try:
                # Run full 3-step pipeline: download -> merge -> extract/load
                result = orchestrator.run_pipeline(
                    amc_slug=amc_slug,
                    year=self.year,
                    month=self.month,
                    steps=["download", "merge", "extract"],
                    dry_run=self.dry_run,
                    redo=self.redo,
                    job_id=self.run_id
                )

                duration = time.time() - amc_start
                status = result.get("status")

                if status == "success":
                    extract_data = result.get("steps", {}).get("extract", {})
                    download_data = result.get("steps", {}).get("download", {})

                    # Safeguard 1: Ensure SUCCESS is recorded only when load succeeded
                    rows_inserted = extract_data.get("rows_inserted", 0)
                    rows_extracted = extract_data.get("rows_read", 0)
                    amc_id = extract_data.get("amc_id")
                    period_id = extract_data.get("period_id")
                    file_hash = extract_data.get("file_hash")

                    mark_amc_success(
                        run_id=self.run_id,
                        amc_slug=amc_slug,
                        amc_id=amc_id,
                        period_id=period_id,
                        rows_fetched=download_data.get("files_downloaded", 0),
                        rows_extracted=rows_extracted,
                        rows_inserted=rows_inserted,
                        file_hash=file_hash,
                        duration_seconds=round(duration, 2),
                        metadata={"dry_run": self.dry_run, "redo": self.redo}
                    )

                    # Storage Retention: Delete raw multi-sheet workbooks after DB commit
                    self._cleanup_amc_raw_files(amc_slug, self.year, self.month)

                    # Send Telegram Success Alert for AMC
                    files_dl = download_data.get("files_downloaded", 0)
                    self._notify(
                        f"✅ <b>[{amc_slug.upper()}] Processed</b>\n"
                        f"📅 <b>Period</b>: {self.year}-{self.month:02d}\n"
                        f"📥 <b>Downloaded</b>: {files_dl} file(s)\n"
                        f"📊 <b>Extracted</b>: {rows_extracted:,} rows\n"
                        f"💾 <b>DB Holdings</b>: {rows_inserted:,}\n"
                        f"⏱️ <b>Duration</b>: {duration:.1f}s"
                    )

                    return {
                        "status": "success",
                        "rows_extracted": rows_extracted,
                        "rows_inserted": rows_inserted,
                        "duration": round(duration, 2)
                    }

                elif status in ("stopped", "cancelled"):
                    reason = result.get("reason") or "Stopped"
                    mark_amc_skipped(
                        run_id=self.run_id,
                        amc_slug=amc_slug,
                        reason=reason,
                        duration_seconds=round(duration, 2)
                    )
                    if reason != "already_processed":
                        self._notify(
                            f"⏸️ <b>[{amc_slug.upper()}] Skipped</b>\n"
                            f"📅 <b>Period</b>: {self.year}-{self.month:02d}\n"
                            f"ℹ️ <b>Reason</b>: {reason}"
                        )
                    return {"status": "skipped", "reason": reason}

                else:
                    # Failed step
                    failed_stage, error_msg = self._extract_error_detail(result)

                    if current_attempt < self.max_retries and is_transient_error(error_msg):
                        backoff = self.backoff_delays[min(current_attempt, len(self.backoff_delays) - 1)]
                        current_attempt += 1
                        logger.warning(
                            f"[{amc_slug.upper()}] Transient failure at {failed_stage}: {error_msg}. "
                            f"Retrying ({current_attempt}/{self.max_retries}) in {backoff}s..."
                        )
                        mark_amc_retry_pending(
                            run_id=self.run_id,
                            amc_slug=amc_slug,
                            stage=failed_stage,
                            error_message=f"Attempt {current_attempt} failed: {error_msg}",
                            retry_count=current_attempt,
                            max_retries=self.max_retries
                        )
                        time.sleep(backoff)
                        continue
                    else:
                        # Permanent failure or retries exhausted
                        mark_amc_failed(
                            run_id=self.run_id,
                            amc_slug=amc_slug,
                            stage=failed_stage,
                            error_type="TRANSIENT_EXHAUSTED" if current_attempt > 0 else "PERMANENT_ERROR",
                            error_message=error_msg,
                            retry_count=current_attempt,
                            max_retries=self.max_retries,
                            duration_seconds=round(duration, 2)
                        )
                        self._notify(
                            f"❌ <b>[{amc_slug.upper()}] Failed</b>\n"
                            f"📅 <b>Period</b>: {self.year}-{self.month:02d}\n"
                            f"⚠️ <b>Stage</b>: {failed_stage}\n"
                            f"🛑 <b>Error</b>: {error_msg[:150]}\n"
                            f"🔄 <b>Attempts</b>: {current_attempt + 1}/{self.max_retries + 1}"
                        )
                        return {
                            "status": "failed",
                            "stage": failed_stage,
                            "error": error_msg,
                            "retry_count": current_attempt
                        }

            except Exception as e:
                duration = time.time() - amc_start
                err_str = str(e)

                if current_attempt < self.max_retries and is_transient_error(err_str):
                    backoff = self.backoff_delays[min(current_attempt, len(self.backoff_delays) - 1)]
                    current_attempt += 1
                    logger.warning(
                        f"[{amc_slug.upper()}] Exception {err_str}. "
                        f"Retrying ({current_attempt}/{self.max_retries}) in {backoff}s..."
                    )
                    mark_amc_retry_pending(
                        run_id=self.run_id,
                        amc_slug=amc_slug,
                        stage="PIPELINE",
                        error_message=f"Exception attempt {current_attempt}: {err_str}",
                        retry_count=current_attempt,
                        max_retries=self.max_retries
                    )
                    time.sleep(backoff)
                    continue
                else:
                    mark_amc_failed(
                        run_id=self.run_id,
                        amc_slug=amc_slug,
                        stage="PIPELINE",
                        error_type="EXCEPTION",
                        error_message=err_str,
                        stack_trace=traceback.format_exc(),
                        retry_count=current_attempt,
                        max_retries=self.max_retries,
                        duration_seconds=round(duration, 2)
                    )
                    return {
                        "status": "failed",
                        "stage": "PIPELINE",
                        "error": err_str,
                        "retry_count": current_attempt
                    }

        return {"status": "failed", "stage": "UNKNOWN", "error": "Exceeded retry loop"}

    def _extract_error_detail(self, result: Dict[str, Any]) -> Tuple[str, str]:
        """Identifies which pipeline step failed and extracts the error message."""
        steps = result.get("steps", {})
        for stage in ("download", "merge", "extract"):
            step_data = steps.get(stage, {})
            if step_data.get("status") in ("failed", "error"):
                err = step_data.get("error") or step_data.get("reason") or "Unknown step error"
                return stage.upper(), str(err)
    def _cleanup_amc_raw_files(self, amc_slug: str, year: int, month: int):
        """
        Safely removes downloaded raw multi-sheet workbooks for this AMC and period
        once its data has been successfully committed to the database.
        Saves ~95% of disk space while keeping database snapshots intact.
        """
        if self.dry_run:
            return
        candidate_dirs = [
            PROJECT_ROOT / "data" / "raw" / amc_slug / f"{year}_{month:02d}",
            PROJECT_ROOT / "data" / "raw" / amc_slug / f"{year}_{month}",
            PROJECT_ROOT / "data" / "raw" / amc_slug / str(year) / str(month)
        ]
        for raw_dir in candidate_dirs:
            if raw_dir.exists() and raw_dir.is_dir():
                try:
                    shutil.rmtree(raw_dir)
                    logger.info(f"[{amc_slug.upper()}] Storage Retention: Auto-purged raw download files from {raw_dir.relative_to(PROJECT_ROOT)}")
                except Exception as e:
                    logger.warning(f"[{amc_slug.upper()}] Storage Retention: Could not delete raw directory {raw_dir}: {e}")

    def _prune_old_merged_excels(self, keep_months: int = 2):
        """
        Retains merged consolidated workbooks for the last `keep_months` (e.g. current + previous).
        Safely deletes older consolidated Excels (>2 months old) because all holdings 
        are permanently queryable in PostgreSQL.
        """
        if self.dry_run:
            return
        merged_base = PROJECT_ROOT / "data" / "output" / "merged excels"
        if not merged_base.exists():
            return

        cutoff_year = self.year
        cutoff_month = self.month - (keep_months - 1)
        while cutoff_month <= 0:
            cutoff_month += 12
            cutoff_year -= 1

        cutoff_period = (cutoff_year, cutoff_month)
        pruned_count = 0

        try:
            for amc_dir in merged_base.iterdir():
                if not amc_dir.is_dir():
                    continue
                for year_dir in amc_dir.iterdir():
                    if not year_dir.is_dir() or not year_dir.name.isdigit():
                        continue
                    file_year = int(year_dir.name)
                    for file in year_dir.glob("CONSOLIDATED_*.xlsx"):
                        parts = file.stem.split("_")
                        try:
                            f_year = int(parts[-2])
                            f_month = int(parts[-1])
                            if (f_year, f_month) < cutoff_period:
                                file.unlink()
                                pruned_count += 1
                                logger.debug(f"Storage Retention: Pruned old merged file {file.name}")
                        except (ValueError, IndexError):
                            continue
            if pruned_count > 0:
                logger.info(f"Storage Retention: Pruned {pruned_count} consolidated Excel(s) older than {cutoff_year}-{cutoff_month:02d}.")
        except Exception as e:
            logger.warning(f"Storage Retention: Error during merged Excel pruning: {e}")

    @staticmethod
    def purge_all_loaded_raw_files() -> int:
        """
        Scans data/raw/ and purges raw multi-file folders for any AMC/period
        that has already been consolidated into a merged Excel workbook
        or confirmed loaded in PostgreSQL ledger.
        """
        raw_base = PROJECT_ROOT / "data" / "raw"
        merged_base = PROJECT_ROOT / "data" / "output" / "merged excels"
        if not raw_base.exists():
            return 0

        from src.db.ledger_repository import get_connection
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT amc_slug, year, month FROM pipeline_execution_ledger WHERE status = 'SUCCESS';")
            ledger_success = set((row[0], row[1], row[2]) for row in cur.fetchall())

        purged_count = 0
        for amc_dir in raw_base.iterdir():
            if not amc_dir.is_dir():
                continue
            slug = amc_dir.name
            for entry in list(amc_dir.iterdir()):
                if not entry.is_dir():
                    continue
                year, month = None, None
                if "_" in entry.name:
                    parts = entry.name.split("_")
                    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                        year, month = int(parts[0]), int(parts[1])
                elif entry.name.isdigit():
                    for m_dir in list(entry.iterdir()):
                        if m_dir.is_dir() and m_dir.name.isdigit():
                            y, m = int(entry.name), int(m_dir.name)
                            merged_dir = merged_base / slug / str(y)
                            has_merged = merged_dir.exists() and (
                                any(merged_dir.glob(f"CONSOLIDATED_{slug.upper()}_{y}_{m:02d}*.xlsx")) or
                                any(merged_dir.glob(f"CONSOLIDATED_{slug.upper()}_{y}_{m}*.xlsx"))
                            )
                            if has_merged or (slug, y, m) in ledger_success:
                                try:
                                    shutil.rmtree(m_dir)
                                    purged_count += 1
                                except Exception as e:
                                    logger.warning(f"Failed to delete {m_dir}: {e}")

                if year and month:
                    merged_dir = merged_base / slug / str(year)
                    has_merged = merged_dir.exists() and (
                        any(merged_dir.glob(f"CONSOLIDATED_{slug.upper()}_{year}_{month:02d}*.xlsx")) or
                        any(merged_dir.glob(f"CONSOLIDATED_{slug.upper()}_{year}_{month}*.xlsx"))
                    )
                    if has_merged or (slug, year, month) in ledger_success:
                        try:
                            shutil.rmtree(entry)
                            purged_count += 1
                        except Exception as e:
                            logger.warning(f"Failed to delete {entry}: {e}")
        return purged_count

    def _print_summary_report(self, summary: Dict[str, Any], results: Dict[str, Any]):
        """Logs a clear, tabular execution summary."""
        lines = [
            "",
            "================================================================================",
            f"                       MASTER PIPELINE EXECUTION REPORT",
            "================================================================================",
            f" Run ID       : {summary['run_id']}",
            f" Target Period: {summary['target_period']}",
            f" Trigger Type : {self.trigger_type} | Concurrency: {self.concurrency}",
            f" Started At   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (IST)",
            "--------------------------------------------------------------------------------",
            f" {'AMC SLUG':<16} | {'STATUS':<10} | {'ROWS INSERTED':<14} | {'DURATION':<10} | {'ERROR / REASON'}",
            "--------------------------------------------------------------------------------"
        ]

        for slug, res in sorted(results.items()):
            status = res.get("status", "unknown").upper()
            rows = str(res.get("rows_inserted", "-"))
            dur = f"{res.get('duration', 0):.1f}s" if res.get('duration') else "-"
            err = res.get("error") or res.get("reason") or ""
            if len(err) > 35:
                err = err[:32] + "..."
            lines.append(f" {slug:<16} | {status:<10} | {rows:<14} | {dur:<10} | {err}")

        lines.extend([
            "================================================================================",
            f" Total AMCs : {summary['total_amcs']}",
            f" Success    : {summary['success_count']}",
            f" Failed     : {summary['failed_count']}",
            f" Skipped    : {summary['skipped_count']}",
            f" Rows Loaded: {summary['rows_inserted']}",
            f" Duration   : {summary['duration_seconds']}s",
            "================================================================================",
            ""
        ])

        report_str = "\n".join(lines)
        for line in lines:
            if line:
                logger.info(line)
        print(report_str)


def main():
    """CLI entry point for manual and systemd invocations."""
    parser = argparse.ArgumentParser(description="Mutual Fund AMC Master Pipeline")
    parser.add_argument("--mode", choices=["auto", "manual"], default="auto", help="auto computes last closed month")
    parser.add_argument("--year", type=int, default=None, help="Target 4-digit year (e.g. 2026)")
    parser.add_argument("--month", type=int, default=None, help="Target month (1-12)")
    parser.add_argument("--amcs", type=str, default="ALL", help="Comma-separated AMC slugs or 'ALL'")
    parser.add_argument("--concurrency", type=int, default=2, help="Max parallel AMC workers (default: 2)")
    parser.add_argument("--dry-run", action="store_true", help="Extract and validate without writing to DB")
    parser.add_argument("--redo", action="store_true", help="Purge and re-extract already loaded data")
    parser.add_argument("--trigger-type", choices=["SCHEDULED", "MANUAL", "CLI"], default="CLI")
    parser.add_argument("--purge-raw", action="store_true", help="Purge historical raw folders for AMCs already loaded in DB")

    args = parser.parse_args()

    if args.purge_raw:
        count = MasterPipelineRunner.purge_all_loaded_raw_files()
        print(f"Purged {count} historical raw AMC period folder(s) whose snapshots exist in DB.")
        sys.exit(0)

    slugs = None
    if args.amcs and args.amcs.upper() != "ALL":
        slugs = [s.strip().lower() for s in args.amcs.split(",") if s.strip()]

    runner = MasterPipelineRunner(
        year=args.year,
        month=args.month,
        amc_slugs=slugs,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
        redo=args.redo,
        trigger_type=args.trigger_type
    )

    result = runner.run()
    if result.get("status") == "FAILED":
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
