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
            "% TO NAV": "percent_to_nav",
            "NAV": "percent_to_nav",
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

            if "isin" not in df.columns:
                continue

            value_unit = self._resolve_value_unit(raw_columns, global_unit)
            equity_df = self.filter_equity_isins(df, "isin")
            if equity_df.empty:
                continue

            scheme_info = self.parse_verbose_scheme_name(str(sheet_name).strip())
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
                        "company_name": row.get("company_name"),
                        "quantity": int(self.normalize_currency(row.get("quantity", 0), "RUPEES")),
                        "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), value_unit),
                        "percent_to_nav": self.safe_float(row.get("percent_to_nav", 0)),
                        "sector": row.get("sector", None),
                    }
                )

            self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
            all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        new_cols = {}
        for col in df.columns:
            col_upper = str(col).upper()
            for pattern, canonical in self.column_mapping.items():
                if pattern.upper() in col_upper:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)

    def _resolve_value_unit(self, raw_columns: List[Any], default_unit: str) -> str:
        value_unit = default_unit
        for col in raw_columns:
            col_upper = str(col).upper()
            is_mv_col = False
            for pattern, canonical in self.column_mapping.items():
                if pattern.upper() in col_upper and canonical == "market_value_inr":
                    is_mv_col = True
                    break

            if is_mv_col:
                header_unit = self.detect_units(col)
                if header_unit != "RUPEES":
                    return header_unit

        return value_unit
