from datetime import datetime
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.downloaders.hdfc_downloader import HDFCDownloader
from src.downloaders.sbi_downloader import SBIDownloader
from src.downloaders.icici_downloader import ICICIDownloader
from src.downloaders.hsbc_downloader import HSBCDownloader
from src.downloaders.kotak_downloader import KotakDownloader
from src.downloaders.ppfas_downloader import PPFASDownloader
from src.downloaders.axis_downloader import AxisDownloader
from src.downloaders.bajaj_downloader import BajajDownloader
from src.downloaders.absl_downloader import ABSLDownloader
from src.downloaders.angelone_downloader import AngelOneDownloader


def main() -> None:
    amc_downloaders = [
        ("hdfc", HDFCDownloader),
        ("sbi", SBIDownloader),
        ("icici", ICICIDownloader),
        ("hsbc", HSBCDownloader),
        ("kotak", KotakDownloader),
        ("ppfas", PPFASDownloader),
        ("axis", AxisDownloader),
        ("bajaj", BajajDownloader),
        ("absl", ABSLDownloader),
        ("angelone", AngelOneDownloader),
    ]

    periods = [(2025, 12), (2026, 1)]
    results = []

    started = datetime.now()
    print(f"Batch started at {started}")

    for amc_slug, downloader_cls in amc_downloaders:
        for year, month in periods:
            row = {
                "amc": amc_slug,
                "year": year,
                "month": month,
                "status": "failed",
                "reason": "",
                "files": "",
            }
            try:
                downloader = downloader_cls()
                result = downloader.download(year=year, month=month)
                row["status"] = result.get("status", "unknown")
                row["reason"] = result.get("reason", "") or result.get("message", "")
                if "files_downloaded" in result:
                    row["files"] = str(result.get("files_downloaded"))
            except Exception as error:
                row["reason"] = str(error)

            results.append(row)
            print(f"{amc_slug} {year}-{month:02d} -> {row['status']} {row['reason']}")

    ended = datetime.now()
    print("\n=== SUMMARY ===")
    print(f"Started: {started}")
    print(f"Ended:   {ended}")

    for result in results:
        print(
            f"{result['amc']},{result['year']}-{result['month']:02d},"
            f"{result['status']},{result['files']},{result['reason']}"
        )


if __name__ == "__main__":
    main()
