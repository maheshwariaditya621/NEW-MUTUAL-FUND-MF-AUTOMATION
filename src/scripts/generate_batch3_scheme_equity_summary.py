from collections import defaultdict
from pathlib import Path
import csv
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.extractors.extractor_factory import ExtractorFactory

AMCS = ["nippon", "uti", "mirae_asset"]
MONTHS = [(2025, 11), (2025, 12), (2026, 1)]


def main() -> None:
    aggregate = defaultdict(lambda: {
        "equity_holdings_count": 0,
        "unique_isins": set(),
        "total_market_value_inr": 0.0,
    })

    for amc in AMCS:
        for year, month in MONTHS:
            merged_path = Path(f"data/output/merged excels/{amc}/{year}/CONSOLIDATED_{amc.upper()}_{year}_{month:02d}.xlsx")
            if not merged_path.exists():
                continue

            extractor = ExtractorFactory.get_extractor(amc, year, month)
            if extractor is None:
                continue

            rows = extractor.extract(str(merged_path))

            for row in rows:
                scheme_name = str(row.get("scheme_name") or "UNKNOWN_SCHEME").strip()
                key = (amc, year, month, scheme_name)
                aggregate[key]["equity_holdings_count"] += 1

                isin = (row.get("isin") or "").strip() if isinstance(row.get("isin"), str) else row.get("isin")
                if isin:
                    aggregate[key]["unique_isins"].add(isin)

                try:
                    aggregate[key]["total_market_value_inr"] += float(row.get("market_value_inr") or 0)
                except Exception:
                    pass

    output_path = Path("docs/BATCH3_SCHEME_EQUITY_SUMMARY_2025_11_2026_01.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "amc",
            "year",
            "month",
            "scheme_name",
            "equity_holdings_count",
            "unique_isins",
            "total_market_value_inr",
        ])

        for (amc, year, month, scheme_name), data in sorted(
            aggregate.items(), key=lambda item: (item[0][0], item[0][1], item[0][2], item[0][3])
        ):
            writer.writerow([
                amc,
                year,
                f"{month:02d}",
                scheme_name,
                data["equity_holdings_count"],
                len(data["unique_isins"]),
                f"{data['total_market_value_inr']:.2f}",
            ])

    print(f"written={output_path}")
    print(f"rows={len(aggregate)}")


if __name__ == "__main__":
    main()
