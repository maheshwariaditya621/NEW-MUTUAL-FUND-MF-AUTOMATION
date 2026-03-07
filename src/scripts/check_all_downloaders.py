"""
check_all_downloaders.py
------------------------
Checks all AMCs for a given month/year to verify downloaders
resolve and respond correctly.

Usage:
    python src/scripts/check_all_downloaders.py --year 2025 --month 11

What it does:
    - Runs each downloader in dry_run=True mode (no files downloaded)
    - Reports OK / FAILED / SKIPPED per AMC
    - Prints a final summary
"""

import argparse
import sys
from src.downloaders.downloader_factory import DownloaderFactory
from src.downloaders.downloader_orchestrator import PipelineOrchestrator

OK_STATUSES = {"success", "skipped", "dry_run", "not_published", "before_inception", "already_downloaded"}

def main():
    parser = argparse.ArgumentParser(description="Check all AMC downloaders for a given month")
    parser.add_argument("--year",  type=int, required=True,  help="Year  (e.g. 2025)")
    parser.add_argument("--month", type=int, required=True,  help="Month (1-12)")
    parser.add_argument("--amc",   type=str, default=None,   help="Test a single AMC slug only")
    args = parser.parse_args()

    all_amcs = list(DownloaderFactory.DOWNLOADER_MAP.keys())
    if args.amc:
        if args.amc not in DownloaderFactory.DOWNLOADER_MAP:
            print(f"ERROR: Unknown AMC slug '{args.amc}'")
            sys.exit(1)
        all_amcs = [args.amc]

    print(f"\nChecking {len(all_amcs)} AMC(s) for {args.year}-{args.month:02d}  [dry_run=True]")
    print("=" * 70)

    ok_list     = []
    failed_list = []

    for amc in all_amcs:
        try:
            o = PipelineOrchestrator()
            r = o.run_pipeline(amc, args.year, args.month, steps=["download"], dry_run=True, redo=False)
            step   = r.get("steps", {}).get("download", {})
            status = step.get("status") or r.get("status", "unknown")

            if status in OK_STATUSES:
                ok_list.append((amc, status))
                print(f"  OK   [{status:<18}] {amc}")
            else:
                reason = step.get("reason") or step.get("error") or "unknown"
                failed_list.append((amc, status, reason))
                print(f"  FAIL [{status:<18}] {amc}  -> {reason}")

        except Exception as e:
            failed_list.append((amc, "exception", str(e)))
            print(f"  ERR  [exception         ] {amc}  -> {e}")

    print("=" * 70)
    print(f"SUMMARY:  {len(ok_list)} OK   |   {len(failed_list)} FAILED")

    if failed_list:
        print("\nFAILED AMCs:")
        for amc, status, reason in failed_list:
            print(f"  {amc:<22} [{status}]  {reason}")
        sys.exit(1)
    else:
        print("\nAll downloaders responded successfully.")

if __name__ == "__main__":
    main()
