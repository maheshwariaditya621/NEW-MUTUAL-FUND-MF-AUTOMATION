from datetime import datetime
import sys
import pkgutil
import importlib
import inspect
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger

def main() -> None:
    year, month = 2026, 1
    results = []

    started = datetime.now()
    logger.info(f"Bulk Download Batch for {year}-{month:02d} started at {started}")

    # Discover all downloaders in src.downloaders
    downloaders_pkg = importlib.import_module("src.downloaders")
    for _, module_name, is_pkg in pkgutil.iter_modules(downloaders_pkg.__path__, "src.downloaders."):
        if is_pkg:
            continue
        
        try:
            module = importlib.import_module(module_name)
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, BaseDownloader) and obj is not BaseDownloader:
                    amc_slug = getattr(obj, "AMC_NAME", name.lower().replace("downloader", ""))
                    
                    logger.info(f"Running downloader: {name} for {amc_slug}")
                    
                    row = {
                        "amc": amc_slug,
                        "downloader": name,
                        "status": "failed",
                        "reason": "",
                        "files": 0
                    }
                    
                    try:
                        downloader = obj()
                        result = downloader.download(year=year, month=month)
                        row["status"] = result.get("status", "unknown")
                        row["reason"] = result.get("reason", "") or result.get("message", "")
                        row["files"] = result.get("files_downloaded", 0)
                    except Exception as e:
                        row["reason"] = str(e)
                        logger.error(f"Error running {name}: {e}")
                    
                    results.append(row)
                    logger.info(f"{amc_slug} -> {row['status']} ({row['files']} files) {row['reason']}")
                    
        except Exception as e:
            logger.error(f"Error loading module {module_name}: {e}")

    ended = datetime.now()
    logger.info("\n=== BULK DOWNLOAD SUMMARY ===")
    logger.info(f"Started: {started}")
    logger.info(f"Ended:   {ended}")
    logger.info(f"Duration: {ended - started}")

    success_count = sum(1 for r in results if r["status"] == "success")
    skipped_count = sum(1 for r in results if r["status"] == "skipped")
    failed_count = sum(1 for r in results if r["status"] == "failed")
    not_published_count = sum(1 for r in results if r["status"] == "not_published")

    logger.info(f"Total: {len(results)}")
    logger.info(f"Success: {success_count}")
    logger.info(f"Skipped: {skipped_count}")
    logger.info(f"Failed: {failed_count}")
    logger.info(f"Not Published: {not_published_count}")

if __name__ == "__main__":
    main()
