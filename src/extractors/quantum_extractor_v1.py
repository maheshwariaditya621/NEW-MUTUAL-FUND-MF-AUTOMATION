import pandas as pd
import re
from typing import List, Dict, Any, Optional
from src.extractors.common_extractor_v1 import CommonExtractorV1
from src.config import logger

class QuantumExtractorV1(CommonExtractorV1):
    """
    Dedicated extractor for Quantum Mutual Fund.
    Excel Structure:
    - Header Row: Typically at index 6 (Row 7).
    - Scheme Name: Typically in Row 0.
    - Units: Values are in Lakhs ("Rupees Lacs").
    """

    def __init__(self):
        super().__init__(amc_slug="quantum", amc_name="Quantum Mutual Fund")

    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        Quantum headers are often in Row 15 (index 14).
        Searching for ISIN + Industry/Rating.
        """
        for i in range(min(len(df), 25)):
            row_str = " ".join([str(v).upper() for v in df.iloc[i].values if pd.notna(v)])
            if "ISIN" in row_str and any(k in row_str for k in ["NAME OF THE INSTRUMENT", "QUANTITY", "INDUSTRY"]):
                return i
        return 14 # Default fallback

    def _extract_scheme_name(self, df: pd.DataFrame, header_idx: int, sheet_name: str) -> str:
        """
        Extract scheme name from the top rows.
        Quantums Fund names are usually between Row 0 and Header.
        """
        for i in range(header_idx):
            row_vals = [str(v).strip() for v in df.iloc[i].values if pd.notna(v) and len(str(v).strip()) > 10]
            for val in row_vals:
                v_up = val.upper()
                # Must contain Quantum and (Fund/ETF/FOF) and not be the generic AMC name
                if "QUANTUM" in v_up and any(k in v_up for k in ["FUND", "ETF", "FOF"]) and "MANAGEMENT" not in v_up and "MUTUAL FUND" not in v_up:
                    name = val
                    # Aggressively strip everything before "Quantum"
                    name = re.sub(r"(?i)^.*?\b(Quantum)\b", r"\1", name).strip()
                    # Strip everything starting from "(" or " for the period"
                    name = re.split(r"(?i)\(| for the period", name)[0].strip()
                    return name.title()
        
        return sheet_name.replace("Combine ", "").title()

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Specific mapping for Quantum columns.
        """
        col_map = {
            "ISIN": "isin",
            "NAME OF THE INSTRUMENT": "company_name",
            "QUANTITY": "quantity",
            "MARKET VALUE (IN RUPEES LACS)": "market_value_inr",
            "MARKET VALUE (IN RUPEES LAKHS)": "market_value_inr",
            "% TO NAV": "percent_to_nav",
            "INDUSTRY": "sector"
        }
        
        new_cols = {}
        for col in df.columns:
            clean_col = str(col).strip().upper()
            if clean_col in col_map:
                new_cols[col] = col_map[clean_col]
        
        return df.rename(columns=new_cols)

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Quantum file: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            if any(k in str(sheet_name).upper() for k in ["INDEX", "SUMMARY", "COMBINE INDEX"]):
                continue

            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_raw.empty:
                continue

            # Detect Header
            header_idx = self.find_header_row(df_raw)
            
            # Extract Full Scheme Name
            full_scheme_name = self._extract_scheme_name(df_raw, header_idx, str(sheet_name))
            
            # Read data
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            raw_columns = df.columns.tolist()
            
            # Quantum is in LAKHS
            value_unit = "LAKHS"
            
            df = self._map_columns(df)

            if "isin" not in df.columns:
                continue

            equity_df = self.filter_equity_isins(df, "isin")
            
            if equity_df.empty:
                continue

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
                        "percent_to_nav": self.parse_percentage(row.get("percent_to_nav", 0)),
                        "sector": row.get("sector", None),
                    }
                )

            # Post-extraction validation
            self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
            all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings
