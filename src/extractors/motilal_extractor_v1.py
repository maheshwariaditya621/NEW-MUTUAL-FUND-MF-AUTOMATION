from typing import Dict, Any, List
import pandas as pd
import re

from src.config import logger
from src.extractors.common_extractor_v1 import CommonExtractorV1


class MotilalExtractorV1(CommonExtractorV1):
    """
    Dedicated extractor for Motilal Oswal Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_slug="motilal", amc_name="Motilal Oswal Mutual Fund")

    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        Headers in Motilal files are typically in Row 3 (index 2).
        """
        for i in range(min(len(df), 10)):
            row = [str(v).upper().strip() for v in df.iloc[i].values if pd.notna(v)]
            if "ISIN" in row:
                return i
        return super().find_header_row(df)

    def _extract_scheme_name(self, df: pd.DataFrame, header_idx: int, sheet_name: str) -> str:
        """
        Extract the full descriptive scheme name from Row 1 (index 0).
        Typical Row 1: "Motilal Oswal focused fund"
        """
        for i in range(min(header_idx, 3)):
            row_vals = [str(v).strip() for v in df.iloc[i].values if pd.notna(v) and str(v).strip()]
            for val in row_vals:
                if "MOTILAL OSWAL" in val.upper() and len(val) > 10:
                    cleaned = val.strip()
                    # Normalize: if all caps, all lower, or specific keywords
                    if cleaned.isupper() or cleaned.islower() or "focused fund" in cleaned.lower():
                        cleaned = cleaned.title()
                    return cleaned
        
        return sheet_name.title()

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
                continue

            # Extract Full Scheme Name
            full_scheme_name = self._extract_scheme_name(df_raw, header_idx, str(sheet_name))
            
            # Read data
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            raw_columns = df.columns.tolist()
            global_unit = self.scan_sheet_for_global_units(df)

            df = self._map_columns(df)

            if "isin" not in df.columns:
                continue

            value_unit = self._resolve_value_unit(raw_columns, global_unit)
            equity_df = self.filter_equity_isins(df, "isin")
            
            if equity_df.empty:
                continue

            scheme_info = self.parse_verbose_scheme_name(full_scheme_name)
            
            sheet_holdings: List[Dict[str, Any]] = []

            for _, row in equity_df.iterrows():
                # Add check for stop marker in first col
                # Note: filter_equity_isins already filtered ISINs, 
                # but we want to ensure we don't pick up junk.
                # Actually BaseExtractor stops by default on markers.
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
                        "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), value_unit),
                        "percent_of_nav": self.parse_percentage(row.get("percent_of_nav", 0)),
                        "sector": self.clean_company_name(row.get("sector", "N/A")),
                    }
                )

            self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
            all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings
