import os
import sys
import json
import subprocess
from datetime import datetime
from typing import Dict, Any, List, Optional, Set
from src.downloaders.downloader_factory import DownloaderFactory
from src.utils.excel_merger import consolidate_amc_downloads
from src.extractors.orchestrator import ExtractionOrchestrator
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

class PipelineOrchestrator:
    """
    Orchestrates the full data pipeline: Download -> Merge -> Extract -> Load.
    """
    
    def __init__(self, cancelled_jobs: Optional[Set[str]] = None):
        self.extraction_orchestrator = ExtractionOrchestrator()
        self.notifier = get_notifier()
        self.cancelled_jobs = cancelled_jobs or set()

    def run_pipeline(
        self, 
        amc_slug: str, 
        year: int, 
        month: int, 
        steps: List[str], 
        dry_run: bool = False, 
        redo: bool = False,
        job_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run the specified pipeline steps for an AMC/month.
        """
        if job_id and job_id in self.cancelled_jobs:
            logger.warning(f"Job {job_id} is cancelled. Skipping {amc_slug}.")
            return {"status": "cancelled", "reason": "Job stopped by user"}

        results = {
            "amc": amc_slug,
            "year": year,
            "month": month,
            "steps": {},
            "status": "success"
        }
        
        # 1. DOWNLOAD
        if "download" in steps:
            logger.info(f"Starting DOWNLOAD step for {amc_slug} ({year}-{month:02d})")
            
            # Find the downloader module path
            downloader_info = DownloaderFactory.DOWNLOADER_MAP.get(amc_slug)
            if not downloader_info:
                results["steps"]["download"] = {"status": "error", "error": "Downloader not found"}
                results["status"] = "failed"
                return results
            
            module_path, class_name = downloader_info
            # Convert module path to file path
            file_path = module_path.replace(".", "/") + ".py"
            
            try:
                # Redo logic: purge folder
                if redo:
                    target_v = os.path.join("data", "raw", amc_slug, f"{year}_{month:02d}")
                    if os.path.exists(target_v):
                        import shutil
                        logger.warning(f"Redo requested: Deleting existing raw folder {target_v}")
                        shutil.rmtree(target_v)

                # Prepare command
                cmd = [sys.executable, file_path, "--year", str(year), "--month", str(month)]
                
                # Check if script supports dry-run/redo (simple heuristic)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()
                        if "--dry-run" in content:
                            if dry_run: cmd.append("--dry-run")
                        if "--redo" in content:
                            if redo: cmd.append("--redo")
                except Exception:
                    logger.debug(f"Could not check CLI flags for {file_path}")

                logger.info(f"Running downloader in subprocess: {' '.join(cmd)}")
                
                # Setup environment
                env = os.environ.copy()
                env["PYTHONPATH"] = "."

                process = subprocess.run(cmd, capture_output=True, text=True, timeout=300, env=env)
                
                # Parse JSON result from stdout (even on failure)
                download_res = {}
                try:
                    # Look for the last JSON block in stdout
                    lines = process.stdout.strip().split("\n")
                    json_str = next((l for l in reversed(lines) if l.strip().startswith("{") and l.strip().endswith("}")), None)
                    if json_str:
                        download_res = json.loads(json_str)
                except Exception as e:
                    logger.debug(f"Could not parse JSON from downloader: {e}")

                if process.returncode != 0:
                    error_out = process.stderr or process.stdout
                    logger.error(f"Downloader subprocess failed: {error_out}")
                    
                    # If we got a structured reason from JSON, use it. Otherwise fallback to generic.
                    reason = download_res.get("reason") or f"Subprocess exit {process.returncode}"
                    results["steps"]["download"] = {"status": "failed", "reason": reason}
                    results["status"] = "failed"
                    
                    # Downloader subprocess already sent its own Telegram notification.
                    # Orchestrator stays silent on Telegram for download step.
                    return results
                else:
                    # Fallback if success but no JSON found
                    if not download_res:
                        download_res = {"status": "success", "files_downloaded": 0, "reason": "No JSON output captured"}
                    
                    results["steps"]["download"] = download_res
                    
                    # Check if we should proceed to next steps
                    status = download_res.get("status")
                    if status not in ["success", "skipped"]:
                        logger.warning(f"Download for {amc_slug} returned status: {status}. Stopping pipeline.")
                        results["status"] = "stopped"
                        results["reason"] = download_res.get("reason") or f"Download status: {status}"
                        return results
                    
                    # If success but no files, we should also probably stop
                    if status == "success" and download_res.get("files_downloaded", 0) == 0:
                        logger.warning(f"No files were downloaded for {amc_slug}. Stopping pipeline.")
                        results["status"] = "stopped"
                        results["reason"] = "No files downloaded"
                        return results


            except Exception as e:
                logger.error(f"Error during download for {amc_slug}: {e}")
                results["steps"]["download"] = {"status": "error", "error": str(e)}
                results["status"] = "failed"
                return results

        # 2. MERGE
        if "merge" in steps:
            # Check for cancellation again
            if job_id and job_id in self.cancelled_jobs:
                results["status"] = "cancelled"
                return results

            logger.info(f"Starting MERGE step for {amc_slug} ({year}-{month:02d})")
            try:
                if redo:
                    output_folder = f"data/output/merged excels/{amc_slug}/{year}"
                    output_file = f"{output_folder}/CONSOLIDATED_{amc_slug.upper()}_{year}_{month:02d}.xlsx"
                    if os.path.exists(output_file):
                        logger.warning(f"Redo requested: Deleting existing merged file {output_file}")
                        os.remove(output_file)

                merge_res = consolidate_amc_downloads(amc_slug, year, month)
                if merge_res:
                    results["steps"]["merge"] = {"status": "success", "file_path": str(merge_res)}
                else:
                    results["steps"]["merge"] = {"status": "failed", "reason": "Consolidation failed (likely no valid sheets)"}
                    results["status"] = "failed"
                    return results
            except Exception as e:
                logger.error(f"Error during merge for {amc_slug}: {e}")
                results["steps"]["merge"] = {"status": "error", "error": str(e)}
                results["status"] = "failed"
                return results

        # 3. EXTRACT / LOAD
        if "extract" in steps:
            # Check for cancellation again
            if job_id and job_id in self.cancelled_jobs:
                results["status"] = "cancelled"
                return results

            logger.info(f"Starting EXTRACT/LOAD step for {amc_slug} ({year}-{month:02d})")
            try:
                # Telegram Notification (Start)
                self.notifier.alert(f"⚡ <b>[{amc_slug.upper()}]</b> Extraction/DB Loading started...")

                extract_res = self.extraction_orchestrator.process_amc_month(
                    amc_slug=amc_slug,
                    year=year,
                    month=month,
                    dry_run=dry_run,
                    redo=redo
                )
                results["steps"]["extract"] = extract_res
                
                rows_read = extract_res.get("rows_read", 0)
                loaded = extract_res.get("rows_inserted", 0)
                self.notifier.alert(f"🎯 <b>[{amc_slug.upper()}]</b> Extracted {rows_read} rows. {'(Dry Run)' if dry_run else f'Loaded {loaded} rows.'}")

            except Exception as e:
                logger.error(f"Error during extraction for {amc_slug}: {e}")
                results["steps"]["extract"] = {"status": "error", "error": str(e)}
                results["status"] = "failed"

        return results
