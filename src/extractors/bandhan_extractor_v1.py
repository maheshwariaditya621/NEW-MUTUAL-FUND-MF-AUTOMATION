from typing import List, Dict, Any
import pandas as pd
import re
from src.config import logger
from src.extractors.base_extractor import BaseExtractor

class BandhanExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Bandhan AMC.
    Scheme names are found in Row 1, Col 1.
    Headers are typically at Row 3.
    """

    def __init__(self):
        super().__init__(amc_name="Bandhan Mutual Fund", version="v1")
        self.column_mapping = {
            "ISIN": "isin",
            "NAME OF THE INSTRUMENT": "company_name",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE": "market_value_inr",
            "MARKET VALUE": "market_value_inr",
            "INDUSTRY / RATING": "sector",
            "RATINGS / INDUSTRY": "sector",
            "% TO NAV": "percent_of_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Bandhan file: {file_path}")
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            # Skip only real summary/index-of-sheets pages, not index FUNDS
            if any(kw in sheet_name.upper() for kw in ["SUMMARY", "INDEX OF"]):
                continue

            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_raw.empty or len(df_raw) < 10:
                continue

            # 1. Extract Scheme Name
            # Bandhan ELSS Tax Saver Fund is at Row 1, Col 1
            scheme_name_raw = ""
            if len(df_raw) > 1 and df_raw.shape[1] > 1:
                val = df_raw.iloc[1, 1]
                if pd.notna(val) and len(str(val).strip()) > 5:
                    scheme_name_raw = str(val).strip()
            
            if not scheme_name_raw:
                # Fallback to sheet name if Row 1 is empty
                scheme_name_raw = sheet_name.strip()

            scheme_info = self.parse_verbose_scheme_name(scheme_name_raw)

            # 2. Find Header
            header_idx = self.find_header_row(df_raw)
            if header_idx == -1:
                logger.warning(f"Could not find header for sheet: {sheet_name}")
                continue

            # 3. Read Data
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            raw_columns = df.columns.tolist()
            global_unit = self.scan_sheet_for_global_units(df_raw.head(15)) # Scan first 15 rows of raw

            df = self._map_columns(df)
            if "isin" not in df.columns:
                continue

            value_unit = self._resolve_value_unit(raw_columns, global_unit)
            equity_df = self.filter_equity_isins(df, "isin")
            
            if equity_df.empty:
                continue

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
                        "percent_of_nav": self.parse_percentage(row.get("percent_of_nav", 0)),
                        "sector": row.get("sector", None),
                    }
                )

            # Dedup within sheet
            merged_holdings = self.merge_duplicate_isins(sheet_holdings)
            
            if self.validate_nav_completeness(merged_holdings, scheme_info["scheme_name"]):
                all_holdings.extend(merged_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings

    def validate_nav_completeness(self, holdings: List[Dict[str, Any]], scheme_name: str) -> bool:
        """
        Bandhan has thematic/hybrid funds with varying thresholds.
        Following user policy: Low Nav% is a warning, not a failure.
        """
        if not holdings:
            logger.error(f"[{scheme_name}] FAILED: No holdings extracted.")
            return False
            
        total_nav_pct = sum(h.get('percent_of_nav', 0.0) for h in holdings)
        isin_count = len(holdings)
        
        # 1. NAV Guard (Warning Only per user policy)
        if not (90.0 <= total_nav_pct <= 105.0):
            logger.warning(f"[{scheme_name}] NAV GUARD WARNING: {total_nav_pct:.2f}% (Range 90-105%).")

        # 2. Holdings Count Guard
        # Small Cap/Total Market Index can have 200-800 items. 
        max_limit = 200
        upper_name = scheme_name.upper()
        if "SMALL CAP" in upper_name or "INDEX" in upper_name or "TOTAL MARKET" in upper_name:
            max_limit = 1000
            
        if isin_count > max_limit:
            logger.error(f"[{scheme_name}] FAILED COUNT GUARD: {isin_count} (Max {max_limit}).")
            return False
            
        return True

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        new_cols = {}
        for col in df.columns:
            col_upper = str(col).upper()
            for pattern, canonical in self.column_mapping.items():
                if pattern in col_upper:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)

    def _resolve_value_unit(self, raw_columns: List[Any], default_unit: str) -> str:
        # Bandhan usually says "( Rs. in Lacs)" in header
        for col in raw_columns:
            if "MARKET" in str(col).upper() and "VALUE" in str(col).upper():
                detected = self.detect_units(str(col))
                if detected != "RUPEES":
                    return detected
        return default_unit

    def merge_duplicate_isins(self, holdings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not holdings: return []
        merged = {}
        for h in holdings:
            isin = h['isin']
            if isin not in merged:
                merged[isin] = h
            else:
                merged[isin]['quantity'] += h['quantity']
                merged[isin]['market_value_inr'] += h['market_value_inr']
                merged[isin]['percent_of_nav'] += h['percent_of_nav']
        return list(merged.values())
