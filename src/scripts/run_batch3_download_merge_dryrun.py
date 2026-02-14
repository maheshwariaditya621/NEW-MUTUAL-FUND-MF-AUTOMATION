from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.downloaders.nippon_downloader import NipponDownloader
from src.downloaders.uti_downloader import UTIDownloader
from src.downloaders.mirae_asset_downloader import MiraeAssetDownloader
from src.extractors.orchestrator import ExtractionOrchestrator


def run() -> int:
    amc_downloaders = [
        ("nippon", NipponDownloader),
        ("uti", UTIDownloader),
        ("mirae_asset", MiraeAssetDownloader),
    ]
    months = [(2025, 11), (2025, 12), (2026, 1)]

    print("=== BATCH-3 DOWNLOAD+MERGE ===")
    for amc_slug, downloader_cls in amc_downloaders:
        for year, month in months:
            try:
                downloader = downloader_cls()
                result = downloader.download(year=year, month=month)
                status = "success" if result.get("status") == "success" else result.get("status", "unknown")
                print(f"download {amc_slug} {year}-{month:02d} -> {status}")
            except Exception as error:
                print(f"download {amc_slug} {year}-{month:02d} -> failed ({error})")

    print("\n=== BATCH-3 DRY-RUN ===")
    orchestrator = ExtractionOrchestrator()
    for amc_slug, _ in amc_downloaders:
        for year, month in months:
            try:
                result = orchestrator.process_amc_month(
                    amc_slug=amc_slug,
                    year=year,
                    month=month,
                    dry_run=True,
                )
                status = result.get("status", "unknown")
                rows = result.get("rows_read", "")
                reason = result.get("reason") or result.get("error") or ""
                print(f"dry-run {amc_slug} {year}-{month:02d} -> {status} rows={rows} {reason}")
            except Exception as error:
                print(f"dry-run {amc_slug} {year}-{month:02d} -> failed ({error})")

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
