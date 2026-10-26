"""
Manual CSV Import Adapter for Bulk and Block Deals.

Validates official NSE and BSE deal CSVs, reports malformed rows with line numbers,
performs dry-run previews, and extracts normalized deal records.
"""

import csv
import io
from datetime import date
from decimal import Decimal
from typing import List, Dict, Any, Tuple, Optional

from src.deals.models import NormalizedDeal
from src.deals.normalizer import DealNormalizer


class CSVDealSource:
    """Parses and validates manual CSV uploads for Bulk and Block Deals."""

    @classmethod
    def parse_and_validate(
        cls,
        csv_content: str,
        filename: str = "upload.csv",
        forced_deal_type: Optional[str] = None
    ) -> Tuple[List[NormalizedDeal], List[Dict[str, Any]]]:
        """
        Parses CSV content, returning (valid_deals, rejected_rows).
        Each rejected row contains: {"line": int, "row": list, "reason": str}
        """
        valid_deals: List[NormalizedDeal] = []
        rejected_rows: List[Dict[str, Any]] = []

        reader = csv.reader(io.StringIO(csv_content))
        header_map: Optional[Dict[str, int]] = None
        detected_exchange: str = "NSE"
        detected_deal_type: str = forced_deal_type.upper() if forced_deal_type else "BULK"

        # Check filename hints
        fn_upper = filename.upper()
        if "BLOCK" in fn_upper:
            detected_deal_type = "BLOCK"
        if "BSE" in fn_upper:
            detected_exchange = "BSE"

        line_num = 0
        for row in reader:
            line_num += 1
            if not row or not any(c.strip() for c in row):
                continue

            clean_cells = [c.strip().upper() for c in row]

            # Detect Header Row (only once before data rows)
            if not header_map:
                if any(c in ("SYMBOL", "SCRIP CD", "SCRIP_CD", "SECURITY CODE") for c in clean_cells) and any("CLIENT" in c for c in clean_cells):
                    header_map = {clean_cells[i]: i for i in range(len(clean_cells))}
                    if any("SCRIP" in c for c in clean_cells):
                        detected_exchange = "BSE"
                    continue
                continue

            if not header_map:
                continue

            # Row extraction
            try:
                # 1. Date
                date_idx = cls._find_idx(header_map, ["DATE", "DEAL DATE", "DEAL_DATE", "TRADE DATE"])
                if date_idx is None or date_idx >= len(row):
                    rejected_rows.append({"line": line_num, "row": row, "reason": "Missing Date column"})
                    continue
                trade_date = DealNormalizer.parse_date(row[date_idx])
                if not trade_date:
                    rejected_rows.append({"line": line_num, "row": row, "reason": f"Invalid date: '{row[date_idx]}'"})
                    continue

                # 2. Symbol / Scrip Code
                sym_idx = cls._find_idx(header_map, ["SYMBOL", "SCRIP CD", "SCRIP_CD", "SECURITY CODE"])
                if sym_idx is None or sym_idx >= len(row) or not row[sym_idx].strip():
                    rejected_rows.append({"line": line_num, "row": row, "reason": "Missing symbol/code"})
                    continue
                symbol = row[sym_idx].strip().upper()

                # 3. Security Name
                sec_idx = cls._find_idx(header_map, ["SECURITY NAME", "SCRIP NAME", "COMPANY NAME", "NAME OF THE COMPANY"])
                sec_name = row[sec_idx].strip() if (sec_idx is not None and sec_idx < len(row)) else symbol

                # 4. Client Name
                client_idx = cls._find_idx(header_map, ["CLIENT NAME", "CLIENT_NAME", "NAME OF CLIENT", "CLIENT"])
                if client_idx is None or client_idx >= len(row) or not row[client_idx].strip():
                    rejected_rows.append({"line": line_num, "row": row, "reason": "Missing client name"})
                    continue
                client_name = row[client_idx].strip()

                # 5. Buy / Sell Side
                side_idx = cls._find_idx(header_map, ["BUY/SELL", "BUY / SELL", "SIDE", "DEAL TYPE", "TYPE"])
                action = "BUY"
                if side_idx is not None and side_idx < len(row):
                    action = DealNormalizer.parse_action(row[side_idx])

                # 6. Quantity
                qty_idx = cls._find_idx(header_map, ["QUANTITY TRADED", "QUANTITY", "QTY", "SHARES"])
                if qty_idx is None or qty_idx >= len(row):
                    rejected_rows.append({"line": line_num, "row": row, "reason": "Missing quantity column"})
                    continue
                qty = int(DealNormalizer.parse_numeric(row[qty_idx]))
                if qty <= 0:
                    rejected_rows.append({"line": line_num, "row": row, "reason": f"Non-positive quantity: {qty}"})
                    continue

                # 7. Price
                price_idx = cls._find_idx(header_map, ["TRADE PRICE / WGT. AVG. PRICE", "TRADE PRICE", "PRICE", "RATE", "AVG. PRICE"])
                if price_idx is None or price_idx >= len(row):
                    rejected_rows.append({"line": line_num, "row": row, "reason": "Missing trade price column"})
                    continue
                price = DealNormalizer.parse_numeric(row[price_idx])
                if price <= Decimal("0"):
                    rejected_rows.append({"line": line_num, "row": row, "reason": f"Non-positive price: {price}"})
                    continue

                val_inr = (price * Decimal(qty)).quantize(Decimal("0.01"))

                # Resolve company
                company_id, resolved_isin, shares = DealNormalizer.resolve_company(
                    symbol=symbol,
                    exchange=detected_exchange
                )
                pct_equity = None
                if shares and shares > Decimal("0"):
                    pct_equity = ((Decimal(qty) / shares) * Decimal("100")).quantize(Decimal("0.0001"))

                inv_cat = DealNormalizer.categorize_investor(client_name)

                deal = NormalizedDeal(
                    deal_type=detected_deal_type,
                    exchange=detected_exchange,
                    trade_date=trade_date,
                    symbol=symbol,
                    security_name=sec_name,
                    client_name=client_name,
                    investor_category=inv_cat,
                    action=action,
                    quantity=qty,
                    trade_price=price,
                    trade_value_inr=val_inr,
                    source_name=f"MANUAL_CSV_{detected_exchange}",
                    ingestion_mode="MANUAL_CSV",
                    company_id=company_id,
                    isin=resolved_isin,
                    pct_equity_traded=pct_equity
                )
                valid_deals.append(deal)

            except Exception as row_err:
                rejected_rows.append({"line": line_num, "row": row, "reason": str(row_err)})

        return valid_deals, rejected_rows

    @staticmethod
    def _find_idx(header_map: Dict[str, int], possible_names: List[str]) -> Optional[int]:
        """Find the matching column index for a set of known header names."""
        for name in possible_names:
            if name in header_map:
                return header_map[name]
            # Try partial substring match
            for h_key, idx in header_map.items():
                if name in h_key:
                    return idx
        return None
