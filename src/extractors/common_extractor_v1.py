from typing import Dict, Any, List

import pandas as pd

from src.config import logger
from src.extractors.base_extractor import BaseExtractor


class CommonExtractorV1(BaseExtractor):
    """
    Reusable extractor for AMCs that follow the standard merged-excel pattern.
    """

    def __init__(self, amc_slug: str, amc_name: str):
        self.amc_slug = amc_slug
        super().__init__(amc_name=amc_name, version="v1")
        self.column_mapping = {
            "ISIN": "isin",
            "COMPANY": "company_name",
            "INSTRUMENT": "company_name",
            "ISSUER": "company_name",
            "NAME OF THE INSTRUMENT": "company_name",
            "QUANTITY": "quantity",
            "UNITS": "quantity",
            "MARKET VALUE": "market_value_inr",
            "FAIR VALUE": "market_value_inr",
            "MARKET/ FAIR VALUE": "market_value_inr",
            "MARKET/Fair Value": "market_value_inr",
            "VALUE": "market_value_inr",
            "% TO NAV": "percent_of_nav",
            "NAV": "percent_of_nav",
            "% TO NET ASSET": "percent_of_nav",
            "% OF NET ASSET": "percent_of_nav",
            "% OF NAV": "percent_of_nav",
            "WEIGHTAGE": "percent_of_nav",
            "INDUSTRY": "sector",
            "SECTOR": "sector",
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from {self.amc_name} file: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_raw.empty:
                continue

            header_idx = self.find_header_row(df_raw)
            if header_idx == -1:
                continue

            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            raw_columns = df.columns.tolist()
            global_unit = self.scan_sheet_for_global_units(df)

            df = self._map_columns(df)

            value_unit = self._resolve_value_unit(raw_columns, global_unit)

            # Extract Total Net Assets (AUM) from the sheet's footer
            raw_net_assets = None
            for idx, row in df_raw.iterrows():
                row_vals = [str(val).upper() if pd.notna(val) else "" for val in row.values]
                row_text = " ".join(row_vals)
                if "GRAND TOTAL" in row_text or "NET ASSETS" in row_text:
                    # Filter for numbers and pick the best candidate
                    candidates = []
                    for val in row.values:
                        f_val = self.safe_float(val)
                        if f_val is not None and f_val > 0:
                            candidates.append(f_val)
                    
                    if candidates:
                        # Logic: usually [Value, 100]. We want the one that isn't exactly 100.
                        if len(candidates) > 1:
                            # If the last one is ~100.0, take the previous one
                            if abs(candidates[-1] - 100.0) < 0.01:
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

            equity_df = self.filter_equity_isins(df, "isin")
            # ALL FUNDS RULE: Do not continue if empty, we still want to record the scheme + total AUM

            scheme_info = self.parse_verbose_scheme_name(str(sheet_name).strip())
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
                            "total_net_assets": normalized_net_assets,
                        }
                    )
            else:
                # Ghost Holding to carry Scheme metadata + Total AUM for non-equity funds
                # The Loader will handle cases where isin is None (just creates snapshot)
                sheet_holdings.append({
                    "amc_name": self.amc_name,
                    "scheme_name": scheme_info["scheme_name"],
                    "scheme_description": scheme_info["description"],
                    "plan_type": scheme_info["plan_type"],
                    "option_type": scheme_info["option_type"],
                    "is_reinvest": scheme_info["is_reinvest"],
                    "isin": None,
                    "total_net_assets": normalized_net_assets,
                    "market_value_inr": 0, # No equity value
                    "percent_of_nav": 0
                })

            if not equity_df.empty:
                self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
            
            all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings

    def _resolve_value_unit(self, raw_columns: List[Any], default_unit: str) -> str:
        # Use the inherited method from BaseExtractor
        return super()._resolve_value_unit(raw_columns, default_unit)
