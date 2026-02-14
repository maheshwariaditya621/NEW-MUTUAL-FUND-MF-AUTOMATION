import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from src.extractors.extractor_factory import ADDITIONAL_AMC_NAMES  # pylint: disable=import-outside-toplevel
except Exception:  # broad catch to keep script resilient if imports fail in partial environments
    ADDITIONAL_AMC_NAMES = {}

IMPLEMENTED_AMCS = [
    "hdfc",
    "sbi",
    "icici",
    "hsbc",
    "kotak",
    "ppfas",
    "axis",
    "bajaj",
    "absl",
    "angelone",
] + sorted(ADDITIONAL_AMC_NAMES.keys())

CSV_HEADERS = [
    "amc_slug",
    "test_year",
    "test_month",
    "merged_file_found",
    "dry_run_status",
    "dry_run_rows_read",
    "dry_run_error",
    "loader_status",
    "loader_rows_inserted",
    "loader_error",
    "reconciliation_report_found",
    "extraction_run_status",
    "notes",
]

FILE_PATTERN = re.compile(r"CONSOLIDATED_[A-Z0-9_]+_(\d{4})_(\d{2})\.xlsx$", re.IGNORECASE)


def discover_latest_period(base_dir: Path, amc_slug: str) -> Tuple[Optional[int], Optional[int], Optional[Path]]:
    amc_dir = base_dir / amc_slug
    if not amc_dir.exists() or not amc_dir.is_dir():
        return None, None, None

    best: Tuple[int, int, Path] = (0, 0, Path())

    for year_dir in amc_dir.iterdir():
        if not year_dir.is_dir():
            continue
        for file_path in year_dir.glob("CONSOLIDATED_*.xlsx"):
            match = FILE_PATTERN.match(file_path.name)
            if not match:
                continue

            year = int(match.group(1))
            month = int(match.group(2))

            if (year, month) > (best[0], best[1]):
                best = (year, month, file_path)

    if best[0] == 0:
        return None, None, None

    return best[0], best[1], best[2]


def reconciliation_report_exists(project_root: Path, amc_slug: str, year: int, month: int) -> bool:
    report_path = project_root / "reports" / f"reconciliation_{amc_slug}_{year}_{month:02d}.csv"
    return report_path.exists()


def get_orchestrator(base_dir: Path) -> Tuple[Optional[Any], Optional[str]]:
    try:
        from src.extractors.orchestrator import ExtractionOrchestrator  # pylint: disable=import-outside-toplevel

        return ExtractionOrchestrator(base_dir=str(base_dir)), None
    except Exception as error:  # broad catch to keep report generation resilient
        return None, str(error)


def resolve_test_period(
    base_dir: Path,
    amc_slug: str,
    year: Optional[int],
    month: Optional[int],
) -> Tuple[Optional[int], Optional[int], Optional[Path]]:
    if year is None or month is None:
        return discover_latest_period(base_dir, amc_slug)

    file_candidate = base_dir / amc_slug / str(year) / f"CONSOLIDATED_{amc_slug.upper()}_{year}_{month:02d}.xlsx"
    if file_candidate.exists():
        return year, month, file_candidate

    return year, month, None


def run_dry_verification(
    orchestrator: Any,
    amc_slug: str,
    year: int,
    month: int,
    row: Dict[str, str],
) -> None:
    try:
        dry_result = orchestrator.process_amc_month(
            amc_slug=amc_slug,
            year=year,
            month=month,
            dry_run=True,
        )
        row["dry_run_status"] = dry_result.get("status", "unknown")
        row["dry_run_rows_read"] = str(dry_result.get("rows_read", ""))
        row["dry_run_error"] = str(dry_result.get("error", dry_result.get("reason", "")))
    except Exception as error:
        row["dry_run_status"] = "failed"
        row["dry_run_error"] = str(error)


def run_loader_verification(
    orchestrator: Any,
    project_root: Path,
    amc_slug: str,
    year: int,
    month: int,
    redo_load: bool,
    row: Dict[str, str],
) -> None:
    try:
        load_result = orchestrator.process_amc_month(
            amc_slug=amc_slug,
            year=year,
            month=month,
            redo=redo_load,
            dry_run=False,
        )
        row["loader_status"] = load_result.get("status", "unknown")
        row["loader_rows_inserted"] = str(load_result.get("rows_inserted", ""))
        row["loader_error"] = str(load_result.get("error", load_result.get("reason", "")))
        row["extraction_run_status"] = row["loader_status"]
        row["reconciliation_report_found"] = "YES" if reconciliation_report_exists(project_root, amc_slug, year, month) else "NO"
    except Exception as error:
        row["loader_status"] = "failed"
        row["loader_error"] = str(error)
        row["extraction_run_status"] = "failed"
        row["reconciliation_report_found"] = "NO"


def execute_selected_modes(
    mode: str,
    orchestrator: Optional[Any],
    orchestrator_error: Optional[str],
    project_root: Path,
    amc_slug: str,
    year: int,
    month: int,
    redo_load: bool,
    row: Dict[str, str],
) -> None:
    if mode in ("dry-run", "both"):
        if orchestrator is None:
            row["dry_run_status"] = "failed"
            row["dry_run_error"] = f"orchestrator_import_failed: {orchestrator_error}"
        else:
            run_dry_verification(
                orchestrator=orchestrator,
                amc_slug=amc_slug,
                year=year,
                month=month,
                row=row,
            )

    if mode in ("load", "both"):
        if orchestrator is None:
            row["loader_status"] = "failed"
            row["loader_error"] = f"orchestrator_import_failed: {orchestrator_error}"
            row["extraction_run_status"] = "failed"
            row["reconciliation_report_found"] = "NO"
        else:
            run_loader_verification(
                orchestrator=orchestrator,
                project_root=project_root,
                amc_slug=amc_slug,
                year=year,
                month=month,
                redo_load=redo_load,
                row=row,
            )


def run_verification(
    project_root: Path,
    base_dir: Path,
    output_file: Path,
    mode: str,
    year: Optional[int],
    month: Optional[int],
    redo_load: bool,
) -> None:
    orchestrator, orchestrator_error = get_orchestrator(base_dir=base_dir)
    rows: List[Dict[str, str]] = []

    for amc_slug in IMPLEMENTED_AMCS:
        row: Dict[str, str] = dict.fromkeys(CSV_HEADERS, "")
        row["amc_slug"] = amc_slug

        selected_year, selected_month, selected_file = resolve_test_period(
            base_dir=base_dir,
            amc_slug=amc_slug,
            year=year,
            month=month,
        )

        if selected_year is None or selected_month is None or selected_file is None:
            row["merged_file_found"] = "NO"
            row["notes"] = "No merged consolidated file found for this AMC"
            rows.append(row)
            continue

        row["test_year"] = str(selected_year)
        row["test_month"] = f"{selected_month:02d}"
        row["merged_file_found"] = "YES"

        execute_selected_modes(
            mode=mode,
            orchestrator=orchestrator,
            orchestrator_error=orchestrator_error,
            project_root=project_root,
            amc_slug=amc_slug,
            year=selected_year,
            month=selected_month,
            redo_load=redo_load,
            row=row,
        )

        rows.append(row)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"AMC verification report generated: {output_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate AMC extractor/loader verification CSV report.")
    parser.add_argument(
        "--base-dir",
        default="data/output/merged excels",
        help="Base directory containing merged consolidated AMC Excel files.",
    )
    parser.add_argument(
        "--output",
        default="docs/AMC_VERIFICATION_RESULTS_AUTO.csv",
        help="Output CSV file path.",
    )
    parser.add_argument(
        "--mode",
        choices=["dry-run", "load", "both"],
        default="dry-run",
        help="Verification mode: dry-run only, load only, or both.",
    )
    parser.add_argument("--year", type=int, help="Optional fixed year to test for all AMCs.")
    parser.add_argument("--month", type=int, help="Optional fixed month (1-12) to test for all AMCs.")
    parser.add_argument(
        "--redo-load",
        action="store_true",
        help="When mode includes load, run with redo=True.",
    )

    args = parser.parse_args()

    if (args.year is None) ^ (args.month is None):
        raise ValueError("Provide both --year and --month together, or omit both for auto-detection.")

    project_root = PROJECT_ROOT
    base_dir = project_root / args.base_dir
    output_file = project_root / args.output

    run_verification(
        project_root=project_root,
        base_dir=base_dir,
        output_file=output_file,
        mode=args.mode,
        year=args.year,
        month=args.month,
        redo_load=args.redo_load,
    )


if __name__ == "__main__":
    main()
