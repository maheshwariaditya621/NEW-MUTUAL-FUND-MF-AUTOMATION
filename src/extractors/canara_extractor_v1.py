from typing import Dict, Any, List
import pandas as pd
import re

from src.config import logger
from src.extractors.base_extractor import BaseExtractor


class CanaraExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Canara Robeco Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_name="Canara Robeco Mutual Fund", version="v1")
        self.column_mapping = {
            "NAME OF THE INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY / RATING": "sector",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE": "market_value_inr",
            "% TO NET ASSETS": "percent_of_nav",
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Canara file: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            # Skip empty or summary sheets if any
            if any(kw in str(sheet_name).upper() for kw in ["SUMMARY", "TOTAL", "INDEX"]):
                continue

            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_raw.empty:
                continue

            # Header detection
            header_idx = self.find_header_row(df_raw)
            if header_idx == -1:
                logger.warning(f"Header not found in sheet '{sheet_name}'")
                continue

            # Units detection (lakhs is default for Canara but let's scan)
            global_unit = self.scan_sheet_for_global_units(df_raw)

            # Re-read with correct header or slice
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            raw_columns = df.columns.tolist()

            df = self._map_columns(df)

            if "isin" not in df.columns:
                logger.warning(f"ISIN column not found in sheet '{sheet_name}' after mapping")
                continue

            # Filter for equity
            equity_df = self.filter_equity_isins(df, "isin")
            if equity_df.empty:
                continue

            # Scheme info parsing from Row 0 of df_raw or sheet_name
            # Canara usually has full scheme name in row 0
            raw_scheme_text = str(df_raw.iloc[0, 1]).strip() if df_raw.shape[1] > 1 else str(sheet_name)
            if pd.isna(raw_scheme_text) or raw_scheme_text.lower() == 'nan':
                 raw_scheme_text = str(sheet_name)
            
            scheme_info = self.parse_verbose_scheme_name(raw_scheme_text)

            # Resolve value unit from header if possible
            value_unit = self._resolve_value_unit(raw_columns, global_unit)

            sheet_holdings: List[Dict[str, Any]] = []

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
                        "percent_of_nav": self.safe_float(row.get("percent_of_nav", 0)),
                        "sector": row.get("sector", "Other"),
                    }
                )

            sheet_total_nav = sum(h.get('percent_of_nav', 0.0) for h in sheet_holdings)
            logger.info(f"[{sheet_name}] Extracted {len(sheet_holdings)} holdings. Total NAV: {sheet_total_nav:.2f}%")
            
            if self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"]):
                all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        new_cols = {}
        for col in df.columns:
            # Normalize whitespace: replace newlines/tabs with space, then consolidate
            col_norm = re.sub(r'\s+', ' ', str(col)).strip().upper()

            # Special case for Canara: avoid picking up derivative exposure as main NAV %
            if "DERIVATIVE" in col_norm:
                continue

            for pattern, canonical in self.column_mapping.items():
                if pattern.upper() in col_norm:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)

    def _resolve_value_unit(self, raw_columns: List[Any], default_unit: str) -> str:
        # If any column header has "Lacs" or "Lakhs", return LAKHS
        for col in raw_columns:
            if any(kw in str(col).upper() for kw in ["LACS", "LAKHS"]):
                return "LAKHS"
            if any(kw in str(col).upper() for kw in ["CRORE", "CR."]):
                return "CRORES"
        return default_unit
