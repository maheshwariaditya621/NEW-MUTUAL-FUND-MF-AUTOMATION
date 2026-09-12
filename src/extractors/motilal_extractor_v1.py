from typing import Dict, Any, List, Optional
import pandas as pd
import re

from src.config import logger
from src.extractors.common_extractor_v1 import CommonExtractorV1


class MotilalExtractorV1(CommonExtractorV1):
    """
    Dedicated extractor for Motilal Oswal Mutual Fund.

    July 2026 format change notes:
    - Sheet structure changed: rows 0-5 now contain company boilerplate
      (company name, address, CIN, monthly statement date), rows 6 blank,
      row 7 = actual scheme name, row 8 = scheme description in parens,
      row 9 = header row.
    - Old format had scheme name in rows 0-2 before the header.
    - Column name change: 'Market Value (In Lakhs)' → 'Market/Fair Value (Rs. in Lakhs)'
    - Column name change: '% to Net Asset' → '% to Net Assets'
    - Sector sidebar columns ('Sector / Rating', 'Percent') still present.
    - Each sheet represents one fund with combined holdings for all plans.
    """

    # Rows to skip from top of sheet before looking for scheme name
    # In July format, scheme name is always on row 7 (0-indexed) = 2 rows before header
    _SCHEME_NAME_ROW = 7

    # Rows that contain company boilerplate, not scheme names
    _BOILERPLATE_MARKERS = [
        "INVESTMENT MANAGER",
        "REGISTERED OFFICE",
        "ASSET MANAGEMENT COMPANY",
        "MONTHLY PORTFOLIO STATEMENT",
        "CIN:",
        "EMAIL:",
        "SEBI",
    ]

    def __init__(self):
        super().__init__(amc_slug="motilal", amc_name="Motilal Oswal Mutual Fund")

    def _is_boilerplate(self, text: str) -> bool:
        """Returns True if the given text is a boilerplate/company header line."""
        t = text.upper()
        return any(marker in t for marker in self._BOILERPLATE_MARKERS)

    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        Find header row by looking for ISIN + a secondary descriptor keyword.
        Searches first 25 rows (override of base class to keep Motilal-specific limit).
        """
        secondary_keywords = ["INSTRUMENT", "ISSUER", "COMPANY", "NAME OF THE"]
        for i in range(min(len(df), 25)):
            row = [str(v).upper().strip() for v in df.iloc[i].values if pd.notna(v)]
            has_isin = any("ISIN" in v for v in row)
            has_secondary = any(any(sk in v for sk in secondary_keywords) for v in row)
            if has_isin and has_secondary:
                return i
        return super().find_header_row(df)

    def _extract_scheme_name(self, df: pd.DataFrame, header_idx: int, sheet_name: str) -> str:
        """
        Extract the scheme name from the sheet.

        Strategy (handles both old and new July 2026 format):
        1. First try: look at exactly (header_idx - 2) and (header_idx - 1) rows
           These are the scheme name and description rows in the new July format.
        2. Fallback: scan all rows before the header for 'MOTILAL OSWAL' text
           that is NOT company boilerplate.
        3. Final fallback: return sheet name title-cased.
        """
        # Strategy 1: New July 2026 format — scheme name is 2 rows before header
        candidate_rows = []
        for offset in [2, 1]:
            row_idx = header_idx - offset
            if row_idx >= 0:
                candidate_rows.append(row_idx)

        for row_idx in candidate_rows:
            row_vals = [str(v).strip() for v in df.iloc[row_idx].values
                        if pd.notna(v) and str(v).strip() and str(v).strip() != "nan"]
            for val in row_vals:
                # Must mention Motilal Oswal, must not be parenthetical description,
                # must not be boilerplate company info
                if (
                    "MOTILAL OSWAL" in val.upper()
                    and not val.strip().startswith("(")
                    and not self._is_boilerplate(val)
                    and len(val) > 10
                ):
                    # Strip embedded newline + description if cell merges scheme name
                    # and description (e.g. "Motilal Oswal Nifty 500 ETF\n(An open-ended...)")
                    clean = val.strip().split("\n")[0].strip()
                    return clean

        # Strategy 2: Fallback scan (handles old format where header was at row 2-3)
        for i in range(min(header_idx, 15)):
            row_vals = [str(v).strip() for v in df.iloc[i].values
                        if pd.notna(v) and str(v).strip() and str(v).strip() != "nan"]
            for val in row_vals:
                if (
                    "MOTILAL OSWAL" in val.upper()
                    and not val.strip().startswith("(")
                    and not self._is_boilerplate(val)
                    and len(val) > 10
                ):
                    clean = val.strip().split("\n")[0].strip()
                    return clean

        # Final fallback
        return sheet_name.title()

    def _deduplicate_sector_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        After _map_columns, both 'Industry*' and 'Sector / Rating' map to 'sector'.
        This creates duplicate column names in pandas which corrupts row iteration.
        Keep only the first occurrence of each canonical column name.
        """
        seen = {}
        new_cols = []
        for i, col in enumerate(df.columns):
            if col not in seen:
                seen[col] = i
                new_cols.append(col)
            else:
                # Rename duplicate to a throwaway name
                new_cols.append(f"_dup_{col}_{i}")
        df.columns = new_cols
        # Drop all throwaway columns
        drop_cols = [c for c in df.columns if c.startswith("_dup_")]
        if drop_cols:
            df = df.drop(columns=drop_cols)
        return df

    def _detect_percent_unit(self, equity_df: pd.DataFrame) -> str:
        """
        Detect whether the percent_of_nav column uses:
        - "PERCENT" format: values already in percentage form (e.g. 5.71, 0.93, 0.01)
          Identified when the max value in the column is > 1.5.
        - "RATIO" format: values as decimal ratios (e.g. 0.0571, 0.0093, 0.0001)
          Identified when the max value in the column is <= 1.0.

        In June 2026: format was RATIO (max value typically 0.02-0.10).
        In July 2026: format changed to PERCENT (max value = 5.71, 5.12, etc.).

        This matters because parse_percentage() treats values <= 1 as ratios
        and multiplies by 100 — breaking July format for stocks < 1% weight.
        """
        if equity_df.empty or "percent_of_nav" not in equity_df.columns:
            return "PERCENT"  # safe default
        max_pct = equity_df["percent_of_nav"].apply(self.safe_float).max()
        return "PERCENT" if max_pct > 1.5 else "RATIO"

    def _parse_percent_value(self, value, percent_unit: str) -> float:
        """
        Parse a percent_of_nav value using the detected unit:
        - "PERCENT": value is already a percentage → use as-is (safe_float only).
        - "RATIO": value is a decimal ratio → multiply by 100 (standard parse_percentage).
        """
        if percent_unit == "PERCENT":
            return self.safe_float(value)
        # RATIO format: delegate to the standard parser
        return self.parse_percentage(value)

    def _get_sector_from_row(self, row: pd.Series) -> str:
        """
        Safely extract sector value. Handles the case where pandas returns
        a Series when there are duplicate column names (pre-dedup safety net).
        """
        val = row.get("sector", "N/A")
        if isinstance(val, pd.Series):
            # Take the first non-null value
            non_null = val.dropna()
            val = non_null.iloc[0] if not non_null.empty else "N/A"
        return self.clean_company_name(val)

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Motilal Oswal file: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            if "INDEX" in str(sheet_name).upper():
                continue

            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_raw.empty:
                continue

            # Detect Header
            header_idx = self.find_header_row(df_raw)
            if header_idx == -1:
                logger.debug(f"[{sheet_name}] No valid header row found. Skipping.")
                continue

            # Extract Full Scheme Name (uses df_raw so we can look before header)
            full_scheme_name = self._extract_scheme_name(df_raw, header_idx, str(sheet_name))

            # Read data starting from header row
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            raw_columns = df.columns.tolist()

            # Determine global units from the header area (before mapping)
            global_unit = self.scan_sheet_for_global_units(df)

            # Map columns to canonical names
            df = self._map_columns(df)

            # CRITICAL FIX: Remove duplicate column names caused by both
            # 'Industry*' and 'Sector / Rating' mapping to 'sector'.
            df = self._deduplicate_sector_columns(df)

            if "isin" not in df.columns:
                logger.debug(f"[{sheet_name}] No 'isin' column after mapping. Skipping.")
                continue

            value_unit = self._resolve_value_unit(raw_columns, global_unit)
            scheme_info = self.parse_verbose_scheme_name(full_scheme_name)

            # Extract Total Net Assets (AUM) from the raw sheet footer.
            # Use df_raw (not the mapped df) to avoid duplicate-column corruption.
            raw_net_assets = self._extract_net_assets_from_raw(df_raw, header_idx)

            normalized_net_assets: Optional[float] = None
            if raw_net_assets is not None:
                normalized_net_assets = self.normalize_currency(raw_net_assets, value_unit)

            # Filter equity rows
            equity_df = pd.DataFrame()
            if "isin" in df.columns:
                equity_df = self.filter_equity_isins(df, "isin")

            # Detect whether % to Net Assets is already in percent form (July 2026+)
            # or decimal ratio form (June 2026 and earlier) — critical for correct parsing.
            percent_unit = self._detect_percent_unit(equity_df)

            sheet_holdings: List[Dict[str, Any]] = []

            if not equity_df.empty:
                for _, row in equity_df.iterrows():
                    sheet_holdings.append(
                        {
                            "amc_name": self.amc_name,
                            "scheme_name": scheme_info["scheme_name"],
                            "scheme_description": scheme_info["description"],
                            "plan_type": scheme_info["plan_type"],
                            "option_type": scheme_info["option_type"],
                            "is_reinvest": scheme_info["is_reinvest"],
                            "isin": row.get("isin"),
                            "company_name": self.clean_company_name(row.get("company_name")),
                            "quantity": int(self.normalize_currency(row.get("quantity", 0), "RUPEES")),
                            "market_value_inr": self.normalize_currency(
                                row.get("market_value_inr", 0), value_unit
                            ),
                            "percent_of_nav": self._parse_percent_value(
                                row.get("percent_of_nav", 0), percent_unit
                            ),
                            "sector": self._get_sector_from_row(row),
                            "total_net_assets": normalized_net_assets,
                        }
                    )
            elif normalized_net_assets:
                # Ghost Holding for Non-Equity / Debt / Liquid funds
                sheet_holdings.append(
                    {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info["scheme_name"],
                        "scheme_description": scheme_info["description"],
                        "plan_type": scheme_info["plan_type"],
                        "option_type": scheme_info["option_type"],
                        "is_reinvest": scheme_info["is_reinvest"],
                        "isin": None,
                        "company_name": "N/A",
                        "quantity": 0,
                        "market_value_inr": 0,
                        "percent_of_nav": 0,
                        "sector": "N/A",
                        "total_net_assets": normalized_net_assets,
                    }
                )

            if sheet_holdings:
                self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
            all_holdings.extend(sheet_holdings)

        logger.info(
            f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}"
        )
        return all_holdings

    def _extract_net_assets_from_raw(
        self, df_raw: pd.DataFrame, header_idx: int
    ) -> Optional[float]:
        """
        Scan df_raw rows AFTER the header for GRAND TOTAL / NET ASSETS keywords
        and return the raw AUM value (before currency normalisation).

        Using df_raw avoids the duplicate-column corruption that arises when
        the mapped df has two 'sector' columns.
        """
        for i in range(header_idx + 1, len(df_raw)):
            row = df_raw.iloc[i]
            row_text = " ".join(
                str(val).upper() for val in row.values if pd.notna(val)
            )
            if "GRAND TOTAL" in row_text or "TOTAL AUM" in row_text:
                candidates = []
                for val in row.values:
                    f_val = self.safe_float(val)
                    # Must be > 105 to avoid picking up 100.00% as AUM
                    if f_val is not None and f_val > 105:
                        candidates.append(f_val)

                if candidates:
                    # If last candidate is ~100, take the one before it
                    if len(candidates) > 1 and abs(candidates[-1] - 100.0) < 0.05:
                        return candidates[-2]
                    return candidates[-1]
        return None
