from typing import Dict, Any, List
import pandas as pd
import re

from src.config import logger
from src.extractors.common_extractor_v1 import CommonExtractorV1


class MiraeExtractorV1(CommonExtractorV1):
    """
    Dedicated extractor for Mirae Asset Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_slug="mirae_asset", amc_name="Mirae Asset Mutual Fund")

    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        Headers in Mirae files are typically in Row 3 (index 2 or 3).
        """
        for i in range(min(len(df), 10)):
            row = [str(v).upper().strip() for v in df.iloc[i].values if pd.notna(v)]
            if "ISIN" in row:
                return i
        return super().find_header_row(df)

    def _extract_scheme_name(self, df: pd.DataFrame, header_idx: int, sheet_name: str) -> str:
        """
        Extract the full descriptive scheme name from Row 2 (index 1 or 2).
        Typical Row 2: "Mirae Asset Large Cap Fund"
        """
        search_range = min(header_idx, 5)
        for i in range(search_range):
            row_vals = [str(v).strip() for v in df.iloc[i].values if pd.notna(v) and str(v).strip()]
            for val in row_vals:
                if "MIRAE ASSET" in val.upper() and len(val) > 10:
                    # Clean up common suffixes like "(An Open Ended...)"
                    cleaned = re.split(r'\(', val)[0].strip()
                    return cleaned
        
        return sheet_name

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Mirae Asset Mutual Fund file: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
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
            scheme_info = self.parse_verbose_scheme_name(full_scheme_name)

            # Extract Total Net Assets (AUM) from the sheet's footer using df (raw read)
            raw_net_assets = None
            for idx, row in df.iterrows():
                row_vals = [str(val).upper() if pd.notna(val) else "" for val in row.values]
                row_text = " ".join(row_vals)
                if "GRAND TOTAL" in row_text or "NET ASSETS" in row_text or "TOTAL AUM" in row_text:
                    candidates = []
                    for val in row.values:
                        f_val = self.safe_float(val)
                        if f_val is not None and f_val > 105: # Avoid 100.00%
                            candidates.append(f_val)
                    
                    if candidates:
                        if len(candidates) > 1:
                             if abs(candidates[-1] - 100.0) < 0.05:
                                 raw_net_assets = candidates[-2]
                             else:
                                 raw_net_assets = candidates[-1]
                        else:
                            raw_net_assets = candidates[0]
                        
                    if raw_net_assets:
                        break
                        
            normalized_net_assets = None
            if raw_net_assets:
                normalized_net_assets = self.normalize_currency(raw_net_assets, value_unit)

            equity_df = pd.DataFrame()
            if "isin" in df.columns:
                equity_df = self.filter_equity_isins(df, "isin")
            
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
                            "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), value_unit),
                            "percent_of_nav": self.parse_percentage(row.get("percent_of_nav", 0)),
                            "sector": self.clean_company_name(row.get("sector", "N/A")),
                            "total_net_assets": normalized_net_assets
                        }
                    )
            elif normalized_net_assets:
                # Ghost Holding for Non-Equity funds
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
                        "total_net_assets": normalized_net_assets
                    }
                )

            self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
            all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings
