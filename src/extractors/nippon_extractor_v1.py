from typing import Dict, Any, List
import pandas as pd
import re

from src.config import logger
from src.extractors.common_extractor_v1 import CommonExtractorV1


class NipponExtractorV1(CommonExtractorV1):
    """
    Dedicated extractor for Nippon India Mutual Fund.
    Overrides CommonExtractorV1 to extract full scheme names from the top rows.
    """

    def __init__(self):
        super().__init__(amc_slug="nippon", amc_name="Nippon India Mutual Fund")

    def parse_verbose_scheme_name(self, raw_name: str) -> Dict[str, Any]:
        """
        Nippon-specific smart parsing.
        Only splits on ' - ' if the subsequent part contains Plan/Option keywords.
        This prevents merging "Income Generation" and "Wealth Creation" schemes.
        """
        name_clean = raw_name.replace("_", " ").strip()
        
        # Check if there is a hyphen
        if " - " in name_clean:
            parts = name_clean.split(" - ")
            main_name = parts[0].strip()
            remaining = " - ".join(parts[1:]).strip()
            
            # Smart check: Does the remaining part look like a Plan/Option?
            plan_keywords = ["DIRECT", "REGULAR", "GROWTH", "IDCW", "DIVIDEND", "PAYOUT", "REINVEST", "PLAN", "OPTION"]
            upper_rem = remaining.upper()
            
            if any(kw in upper_rem for kw in plan_keywords):
                # It's a plan/option suffix (e.g., "Direct - Growth")
                scheme_name = main_name
                description = remaining
            else:
                # It's a sub-scheme name (e.g., "Income Generation")
                # Keep it as part of the scheme name
                scheme_name = name_clean
                description = ""
        else:
            scheme_name = name_clean
            description = ""

        upper_scheme = scheme_name.upper()
        
        # Plan Detection
        plan_type = "Regular"
        if "DIRECT" in upper_scheme or "DIRECT" in raw_name.upper():
            plan_type = "Direct"
            
        # Option Detection
        option_type = "Growth"
        if any(kw in upper_scheme for kw in ["IDCW", "DIVIDEND", "PAYOUT", "INCOME"]) or \
           any(kw in raw_name.upper() for kw in ["IDCW", "DIVIDEND", "PAYOUT", "INCOME"]):
            option_type = "IDCW"
        
        # Reinvestment Flag
        is_reinvest = False
        if "REINVEST" in upper_scheme or "REINVEST" in raw_name.upper():
            is_reinvest = True
            
        return {
            "scheme_name": scheme_name,
            "description": description,
            "plan_type": plan_type,
            "option_type": option_type,
            "is_reinvest": is_reinvest
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Nippon India Mutual Fund file: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            if "Index" in str(sheet_name):
                continue

            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_raw.empty:
                continue

            # Detect Header
            header_idx = self.find_header_row(df_raw)
            if header_idx == -1:
                logger.warning(f"Header not found in sheet: {sheet_name}")
                continue

            # Extract Full Scheme Name from top rows
            full_scheme_name = self._extract_full_name(df_raw, header_idx, str(sheet_name))
            
            # Read data using discovered header
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            raw_columns = df.columns.tolist()
            global_unit = self.scan_sheet_for_global_units(df)

            df = self._map_columns(df)

            if "isin" not in df.columns:
                logger.warning(f"ISIN column not found in sheet: {sheet_name}")
                continue

            value_unit = self._resolve_value_unit(raw_columns, global_unit)
            equity_df = self.filter_equity_isins(df, "isin")
            
            if equity_df.empty:
                continue

            # Parse metadata once for the sheet (plan/option/reinvest)
            # We use the full name for parsing these details
            scheme_info = self.parse_verbose_scheme_name(full_scheme_name)
            
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
                        "sector": self.clean_company_name(row.get("sector", "N/A")),
                    }
                )

            self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
            all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings

    def _extract_full_name(self, df: pd.DataFrame, header_idx: int, default_name: str) -> str:
        """
        Search top rows for the full descriptive scheme name.
        Typically found in Row 1 or 2.
        """
        search_range = min(header_idx, 5)
        logger.debug(f"Sheet {default_name}: header_idx={header_idx}, search_range={search_range}")
        for i in range(search_range):
            row_vals = [str(v).strip() for v in df.iloc[i].values if pd.notna(v) and str(v).strip()]
            
            # Pattern 1: Row starts with RLMF code. The name is usually the next non-empty cell.
            if row_vals and any(re.match(r'^RLMF\d+', v, re.I) for v in row_vals):
                for val in row_vals:
                    if not re.match(r'^RLMF\d+', val, re.I) and len(val) > 10 and val.upper() != "INDEX":
                        cleaned = re.split(r'\(', val)[0].strip()
                        logger.debug(f"  MATCH FOUND (RLMF Pattern): {cleaned}")
                        return cleaned

            # Pattern 2: Fallback to "NIPPON INDIA" or "ETF" search
            for val in row_vals:
                if ("NIPPON INDIA" in val.upper() or " ETF" in val.upper()) and len(val) > 10:
                    cleaned = re.split(r'\(', val)[0].strip()
                    cleaned = re.sub(r'^RLMF\d+\s+', '', cleaned, flags=re.IGNORECASE)
                    logger.debug(f"  MATCH FOUND (Keyword Pattern): {cleaned}")
                    return cleaned
        
        logger.warning(f"Full name not found for {default_name}. Range: {search_range}")
        return default_name
