import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from src.downloaders.downloader_factory import DownloaderFactory
from src.utils.excel_merger import consolidate_amc_downloads
from src.extractors.orchestrator import ExtractionOrchestrator
from src.config import logger

class PipelineOrchestrator:
    """
    Orchestrates the full data pipeline: Download -> Merge -> Extract -> Load.
    """
    
    def __init__(self):
        self.extraction_orchestrator = ExtractionOrchestrator()

    def run_pipeline(
        self, 
        amc_slug: str, 
        year: int, 
        month: int, 
        steps: List[str], 
        dry_run: bool = False, 
        redo: bool = False
    ) -> Dict[str, Any]:
        """
        Run the specified pipeline steps for an AMC/month.
        
        Args:
            amc_slug: AMC identifier
            year: Year (YYYY)
            month: Month (1-12)
            steps: List of steps to run ["download", "merge", "extract"]
            dry_run: If True, don't write to DB (only affects extract/load)
            redo: If True, overwrite existing files/data
            
        Returns:
            Dict containing status and results for each step.
        """
        results = {
            "amc": amc_slug,
            "year": year,
            "month": month,
            "steps": {}
        }
        
        # 1. DOWNLOAD
        if "download" in steps:
            logger.info(f"Starting DOWNLOAD step for {amc_slug} ({year}-{month:02d})")
            downloader = DownloaderFactory.get_downloader(amc_slug)
            if not downloader:
                results["steps"]["download"] = {"status": "error", "error": "Downloader not found"}
                return results
                
            try:
                # Note: Most base downloaders don't support 'redo' natively yet, 
                # but we can implement it by deleting the folder if it exists.
                if redo:
                    target_v = downloader.get_target_folder(amc_slug, year, month)
                    if os.path.exists(target_v):
                        import shutil
                        logger.warning(f"Redo requested: Deleting existing raw folder {target_v}")
                        shutil.rmtree(target_v)
                
                download_res = downloader.download(year, month)
                results["steps"]["download"] = download_res
                
                if download_res.get("status") == "failed":
                    logger.error(f"Download failed for {amc_slug}: {download_res.get('reason')}")
                    return results
            except Exception as e:
                logger.error(f"Error during download for {amc_slug}: {e}")
                results["steps"]["download"] = {"status": "error", "error": str(e)}
                return results

        # 2. MERGE
        if "merge" in steps:
            logger.info(f"Starting MERGE step for {amc_slug} ({year}-{month:02d})")
            try:
                # consolidate_amc_downloads handles its own check for existing file vs redo
                # But it doesn't take a 'redo' flag, it checks mtimes.
                # If we want to force it, we should delete the existing merged file.
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
                    results["steps"]["merge"] = {"status": "error", "error": "Consolidation failed"}
                    return results
            except Exception as e:
                logger.error(f"Error during merge for {amc_slug}: {e}")
                results["steps"]["merge"] = {"status": "error", "error": str(e)}
                return results

        # 3. EXTRACT / LOAD
        if "extract" in steps:
            logger.info(f"Starting EXTRACT/LOAD step for {amc_slug} ({year}-{month:02d})")
            try:
                extract_res = self.extraction_orchestrator.process_amc_month(
                    amc_slug=amc_slug,
                    year=year,
                    month=month,
                    dry_run=dry_run,
                    redo=redo
                )
                results["steps"]["extract"] = extract_res
            except Exception as e:
                logger.error(f"Error during extraction for {amc_slug}: {e}")
                results["steps"]["extract"] = {"status": "error", "error": str(e)}

        return results
