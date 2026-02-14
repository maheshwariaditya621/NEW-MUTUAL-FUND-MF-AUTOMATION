import sys
from pathlib import Path
from datetime import datetime
import urllib3

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def run_hdfc(periods, results):
    import src.downloaders.hdfc_downloader as hdfc_mod
    from src.config.constants import AMC_HDFC

    hdfc_mod.AMC_HDFC = AMC_HDFC

    for year, month in periods:
        entry = {"amc": "hdfc", "year": year, "month": month, "status": "failed", "reason": "", "files": ""}
        try:
            downloader = hdfc_mod.HDFCDownloader()
            result = downloader.download(year=year, month=month)
            entry["status"] = result.get("status", "unknown")
            entry["reason"] = result.get("reason", "") or result.get("message", "")
            if "files_downloaded" in result:
                entry["files"] = str(result.get("files_downloaded"))
        except Exception as error:
            entry["reason"] = str(error)
        results.append(entry)
        print(f"hdfc {year}-{month:02d} -> {entry['status']} {entry['reason']}")


def run_absl(periods, results):
    from src.downloaders.absl_downloader import ABSLDownloader

    for year, month in periods:
        entry = {"amc": "absl", "year": year, "month": month, "status": "failed", "reason": "", "files": ""}
        try:
            downloader = ABSLDownloader()
            result = downloader.download(year=year, month=month)
            entry["status"] = result.get("status", "unknown")
            entry["reason"] = result.get("reason", "") or result.get("message", "")
            if "files_downloaded" in result:
                entry["files"] = str(result.get("files_downloaded"))
        except Exception as error:
            entry["reason"] = str(error)
        results.append(entry)
        print(f"absl {year}-{month:02d} -> {entry['status']} {entry['reason']}")


def run_angelone(periods, results):
    import src.downloaders.angelone_downloader as angel_mod

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    original_get = angel_mod.requests.get

    def patched_get(*args, **kwargs):
        kwargs.setdefault("verify", False)
        return original_get(*args, **kwargs)

    angel_mod.requests.get = patched_get

    try:
        for year, month in periods:
            entry = {"amc": "angelone", "year": year, "month": month, "status": "failed", "reason": "", "files": ""}
            try:
                downloader = angel_mod.AngelOneDownloader()
                result = downloader.download(year=year, month=month)
                entry["status"] = result.get("status", "unknown")
                entry["reason"] = result.get("reason", "") or result.get("message", "")
                if "files_downloaded" in result:
                    entry["files"] = str(result.get("files_downloaded"))
            except Exception as error:
                entry["reason"] = str(error)
            results.append(entry)
            print(f"angelone {year}-{month:02d} -> {entry['status']} {entry['reason']}")
    finally:
        angel_mod.requests.get = original_get


def main() -> None:
    periods = [(2025, 12), (2026, 1)]
    results = []

    started = datetime.now()
    print(f"Recovery batch started: {started}")

    run_hdfc(periods, results)
    run_absl(periods, results)
    run_angelone(periods, results)

    ended = datetime.now()
    print("\n=== RECOVERY SUMMARY ===")
    print(f"Started: {started}")
    print(f"Ended:   {ended}")
    for result in results:
        print(
            f"{result['amc']},{result['year']}-{result['month']:02d},"
            f"{result['status']},{result['files']},{result['reason']}"
        )


if __name__ == "__main__":
    main()
