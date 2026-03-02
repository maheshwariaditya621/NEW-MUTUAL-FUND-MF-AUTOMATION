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
        self.column_mapping.update({
            "MARKET/ FAIR VALUE": "market_value_inr",
            "MARKET/FAIR VALUE": "market_value_inr",
            "INDUSTRY +/ RATING": "sector"
        })

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

            # Extract Total Net Assets (AUM) from the sheet's footer using df_raw
            raw_net_assets = None
            for idx, row in df_raw.iterrows():
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

            scheme_info = self.parse_verbose_scheme_name(full_scheme_name)
            
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
                            "sector": row.get("sector", None),
                            "total_net_assets": normalized_net_assets
                        }
                    )

            if not sheet_holdings and normalized_net_assets:
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

            # Post-extraction validation
            self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
            all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings
