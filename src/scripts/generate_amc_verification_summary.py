import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List


def load_rows(csv_path: Path) -> List[Dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        return list(reader)


def classify_row(row: Dict[str, str]) -> str:
    merged_file_found = (row.get("merged_file_found") or "").strip().upper()
    dry_run_status = (row.get("dry_run_status") or "").strip().lower()
    loader_status = (row.get("loader_status") or "").strip().lower()

    if merged_file_found != "YES":
        return "missing-input"

    has_loader_data = loader_status != ""

    if has_loader_data:
        if loader_status == "success":
            return "pass"
        if loader_status == "skipped":
            return "skipped"
        return "fail"

    if dry_run_status == "success":
        return "pass"
    if dry_run_status == "skipped":
        return "skipped"
    if dry_run_status == "":
        return "unknown"
    return "fail"


def summarize(rows: List[Dict[str, str]]) -> Dict[str, int]:
    counts = {
        "total": len(rows),
        "pass": 0,
        "fail": 0,
        "skipped": 0,
        "missing-input": 0,
        "unknown": 0,
    }

    for row in rows:
        bucket = classify_row(row)
        counts[bucket] += 1

    return counts


def row_status_text(row: Dict[str, str]) -> str:
    bucket = classify_row(row)
    mapping = {
        "pass": "PASS",
        "fail": "FAIL",
        "skipped": "SKIPPED",
        "missing-input": "MISSING_INPUT",
        "unknown": "UNKNOWN",
    }
    return mapping[bucket]


def row_primary_error(row: Dict[str, str]) -> str:
    for key in ["loader_error", "dry_run_error", "notes"]:
        value = (row.get(key) or "").strip()
        if value:
            return value
    return "-"


def build_overall_section(lines: List[str], counts: Dict[str, int]) -> None:
    lines.append("## Overall")
    lines.append("")
    lines.append(f"- Total AMCs: {counts['total']}")
    lines.append(f"- Pass: {counts['pass']}")
    lines.append(f"- Fail: {counts['fail']}")
    lines.append(f"- Skipped: {counts['skipped']}")
    lines.append(f"- Missing Input: {counts['missing-input']}")
    lines.append(f"- Unknown: {counts['unknown']}")
    lines.append("")


def build_status_table(lines: List[str], rows: List[Dict[str, str]]) -> None:
    lines.append("## AMC Status Table")
    lines.append("")
    lines.append("| AMC | Year | Month | Merged File | Dry Run | Loader | Final Status | Primary Error/Note |")
    lines.append("|---|---:|---:|---|---|---|---|---|")

    for row in rows:
        amc = (row.get("amc_slug") or "").strip()
        year = (row.get("test_year") or "").strip() or "-"
        month = (row.get("test_month") or "").strip() or "-"
        merged = (row.get("merged_file_found") or "").strip() or "-"
        dry = (row.get("dry_run_status") or "").strip() or "-"
        loader = (row.get("loader_status") or "").strip() or "-"
        final_status = row_status_text(row)
        error_text = row_primary_error(row).replace("|", "/")

        lines.append(
            f"| {amc} | {year} | {month} | {merged} | {dry} | {loader} | {final_status} | {error_text} |"
        )

    lines.append("")


def build_action_queue(lines: List[str], rows: List[Dict[str, str]]) -> None:
    lines.append("## Action Queue")
    lines.append("")

    missing = [r for r in rows if classify_row(r) == "missing-input"]
    failed = [r for r in rows if classify_row(r) == "fail"]

    if missing:
        lines.append("### Missing Merged Inputs")
        for row in missing:
            lines.append(f"- {row.get('amc_slug', '').strip()}: consolidated merged file not found")
        lines.append("")

    if failed:
        lines.append("### Failed Runs")
        for row in failed:
            lines.append(f"- {row.get('amc_slug', '').strip()}: {row_primary_error(row)}")
        lines.append("")

    if not missing and not failed:
        lines.append("- No immediate blockers detected.")


def generate_markdown(rows: List[Dict[str, str]], source_csv: Path) -> str:
    counts = summarize(rows)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines: List[str] = []
    lines.append("# AMC Verification Summary Dashboard")
    lines.append("")
    lines.append(f"Generated at: {generated_at}")
    lines.append(f"Source CSV: {source_csv}")
    lines.append("")
    build_overall_section(lines, counts)
    build_status_table(lines, rows)
    build_action_queue(lines, rows)

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate markdown summary dashboard from AMC verification CSV.")
    parser.add_argument(
        "--input",
        default="docs/AMC_VERIFICATION_RESULTS_AUTO.csv",
        help="Input verification CSV path.",
    )
    parser.add_argument(
        "--output",
        default="docs/AMC_VERIFICATION_SUMMARY.md",
        help="Output markdown file path.",
    )

    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[2]
    input_csv = project_root / args.input
    output_md = project_root / args.output

    rows = load_rows(input_csv)
    markdown = generate_markdown(rows, input_csv)

    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(markdown, encoding="utf-8")

    print(f"AMC summary dashboard generated: {output_md}")


if __name__ == "__main__":
    main()
