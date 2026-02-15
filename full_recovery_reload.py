
import os
from src.db.connection import get_connection
from src.extractors.orchestrator import ExtractionOrchestrator
from datetime import datetime

def full_recovery():
    base_path = "data/output/merged excels"
    orchestrator = ExtractionOrchestrator()
    
    # Discovery
    discovery = []
    for amc_slug in sorted(os.listdir(base_path)):
        slug_dir = os.path.join(base_path, amc_slug)
        if not os.path.isdir(slug_dir): continue
        
        for year_str in os.listdir(slug_dir):
            year_dir = os.path.join(slug_dir, year_str)
            if not os.path.isdir(year_dir): continue
            try:
                year = int(year_str)
            except: continue
            
            for file_name in os.listdir(year_dir):
                if file_name.startswith("CONSOLIDATED_") and file_name.endswith(".xlsx"):
                    parts = file_name.replace(".xlsx", "").split("_")
                    if len(parts) >= 4:
                        try:
                            month = int(parts[-1])
                            discovery.append((amc_slug, year, month))
                        except: continue
    
    print(f"Discovered {len(discovery)} extraction targets on filesystem.")
    
    # 1. Backup (Optional but safe)
    # We rely on the orchestrator's redo logic which purges before load.
    
    # 2. Process
    results = []
    for slug, year, month in discovery:
        print(f"\n>>> PROCESSING: {slug} | {year}-{month:02d}")
        try:
            # redo=True purges and re-processes
            res = orchestrator.process_amc_month(
                amc_slug=slug,
                year=year,
                month=month,
                redo=True,
                dry_run=False
            )
            print(f"    Result: {res.get('status')} | Rows: {res.get('rows_inserted')}")
            results.append((slug, year, month, res.get('status'), res.get('rows_inserted')))
        except Exception as e:
            print(f"    ERROR: {e}")
            results.append((slug, year, month, "FAILED", str(e)))
            
    print("\n" + "="*60)
    print(f"{'AMC Slug':<20} | {'Period':<8} | {'Status':<10} | {'Rows'}")
    print("-" * 60)
    for slug, y, m, status, info in results:
        print(f"{slug:<20} | {y}-{m:02d} | {status:<10} | {info}")

if __name__ == "__main__":
    full_recovery()
