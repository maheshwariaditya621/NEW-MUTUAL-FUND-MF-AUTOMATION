import hashlib
import os
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from src.config import logger
from src.extractors.extractor_factory import ExtractorFactory
from src.loaders.portfolio_loader import PortfolioLoader
from src.utils.db_backup import backup_database, prune_old_backups
from src.extractors.drift_detector import DriftDetector
from src.db import (
    upsert_amc, upsert_period, record_extraction_run, 
    check_file_already_extracted, TransactionContext,
    delete_extraction_run_and_holdings, check_period_locked
)
import subprocess

class ExtractionOrchestrator:
    """
    Manages the end-to-end extraction process:
    - File discovery
    - Idempotency (Hashing)
    - Versioned Extractor Selection
    - Data Lineage Logging
    """

    def __init__(self, base_dir: str = "data/output/merged excels"):
        self.base_dir = Path(base_dir)
        self.drift_detector = DriftDetector()

    def _get_git_commit(self) -> str:
        """Fetch current git commit hash."""
        try:
            return subprocess.check_output(['git', 'rev-parse', 'HEAD'], stderr=subprocess.STDOUT).decode('ascii').strip()
        except Exception:
            return "UNKNOWN"

    def compute_file_hash(self, file_path: Path) -> str:
        """Computes SHA-256 hash of a file for high-integrity idempotency."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def process_amc_month(self, amc_slug: str, year: int, month: int, redo: bool = False, dry_run: bool = False) -> Dict[str, Any]:
        """
        Orchestrates extraction for a specific AMC and month.
        """
        file_path = self.base_dir / amc_slug / str(year) / f"CONSOLIDATED_{amc_slug.upper()}_{year}_{month:02d}.xlsx"
        
        if not file_path.exists():
            logger.error(f"Merged file not found: {file_path}")
            return {"status": "failed", "reason": "file_not_found"}

        # 0. Check Period Lock
        if not dry_run and not redo:
            if check_period_locked(year, month):
                logger.warning(f"Period {year}-{month:02d} is FINAL/LOCKED. Skipping processing.")
                return {"status": "skipped", "reason": "period_locked"}

        # 1. Setup Extractor (Moved up for correct AMC Name)
        extractor = ExtractorFactory.get_extractor(amc_slug, year, month)
        if not extractor:
            logger.error(f"No extractor found for {amc_slug} in {year}_{month}")
            return {"status": "failed", "reason": "no_extractor"}

        file_hash = self.compute_file_hash(file_path)
        
        # 2. Redo / Idempotency Check
        if not dry_run:
            if redo:
                logger.warning(f"REDO requested for {amc_slug} {year}_{month:02d}. Purging existing data.")
                # We need IDs for purge
                with TransactionContext():
                    # Use extractor.amc_name (Canonical) instead of slug
                    amc_id = upsert_amc(extractor.amc_name) 
                    from datetime import date, timedelta
                    next_month = date(year, month, 1) + timedelta(days=32)
                    period_end = next_month.replace(day=1) - timedelta(days=1)
                    period_id = upsert_period(year, month, period_end)
                    delete_extraction_run_and_holdings(amc_id, period_id)
            elif check_file_already_extracted(file_hash):
                logger.info(f"File {file_path.name} already processed (matched hash). Skipping.")
                return {"status": "skipped", "reason": "already_extracted"}

        # 3. Extraction with Drift Detection
        start_time = time.time()
        try:
            # Note: We need a sample sheet to check drift
            import pandas as pd
            xls = pd.ExcelFile(file_path, engine='openpyxl')
            header_fingerprint = "UNKNOWN"
            if xls.sheet_names:
                sample_df = pd.read_excel(xls, sheet_name=xls.sheet_names[0], nrows=20)
                _, header_fingerprint = self.drift_detector.check_drift(amc_slug, extractor.version, sample_df.columns.tolist())

            holdings = extractor.extract(str(file_path))
            rows_read = len(holdings)
            
            if dry_run:
                duration = time.time() - start_time
                logger.info(f"DRY RUN: Extracted {rows_read} holdings in {duration:.2f}s. Skipping DB load.")
                return {"status": "success", "rows_read": rows_read, "dry_run": True}

            # 4. Persistence (Database Setup)
            with TransactionContext() as ctx:
                amc_id = upsert_amc(extractor.amc_name)
                from datetime import date, timedelta
                next_month = date(year, month, 1) + timedelta(days=32)
                period_end = next_month.replace(day=1) - timedelta(days=1)
                period_id = upsert_period(year, month, period_end)

                loader_result = PortfolioLoader.load_holdings(amc_id, period_id, holdings)
                rows_inserted = loader_result["rows_inserted"]
                duration = time.time() - start_time

                record_extraction_run(
                    amc_id=amc_id,
                    period_id=period_id,
                    file_name=file_path.name,
                    file_hash=file_hash,
                    extractor_version=extractor.version,
                    header_fingerprint=header_fingerprint,
                    rows_read=rows_read,
                    rows_inserted=rows_inserted,
                    rows_filtered=0, # TODO: Track filtered rows in extractor
                    total_value=sum(h['market_value_inr'] for h in holdings),
                    processing_time_seconds=duration,
                    status="SUCCESS",
                    git_commit_hash=self._get_git_commit()
                )

                # 5. Generate Reconciliation Report
                self._generate_reconciliation_report(amc_slug, year, month, holdings)

                # 6. Automated Backup (Harden)
                backup_path = backup_database()
                if backup_path:
                    prune_old_backups(keep_count=6)
            
            return {"status": "success", "rows_inserted": rows_inserted, "duration": duration}

        except Exception as e:
            logger.exception(f"Unexpected error extracting {file_path}: {e}")
            return {"status": "failed", "error": str(e)}

    def _generate_reconciliation_report(self, amc: str, year: int, month: int, holdings: List[Dict[str, Any]]):
        """Generates a CSV reconciliation report in /reports/."""
        import csv
        report_dir = Path("reports")
        report_dir.mkdir(exist_ok=True)
        
        report_path = report_dir / f"reconciliation_{amc}_{year}_{month:02d}.csv"
        
        # Group by scheme
        schemes = {}
        for h in holdings:
            s_name = h['scheme_name']
            if s_name not in schemes:
                schemes[s_name] = {"rows": 0, "value": 0.0}
            schemes[s_name]["rows"] += 1
            schemes[s_name]["value"] += h['market_value_inr']
            
        with open(report_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["AMC", "Scheme", "Rows Extracted", "Total Value (INR)"])
            for s_name, data in schemes.items():
                writer.writerow([holdings[0]['amc_name'] if holdings else amc.upper(), s_name, data["rows"], f"{data['value']:.2f}"])
        
        logger.info(f"Reconciliation report generated: {report_path}")
