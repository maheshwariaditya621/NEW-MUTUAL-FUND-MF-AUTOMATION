import pandas as pd
import re
from typing import List, Dict, Any, Optional
from src.extractors.common_extractor_v1 import CommonExtractorV1
from src.config import logger

class EdelweissExtractorV1(CommonExtractorV1):
    """
    Dedicated extractor for Edelweiss Mutual Fund.
    Excel Structure:
    - Header Row: Typically at index 5 (Row 6).
    - Scheme Name: Typically in Row 0 as "PORTFOLIO STATEMENT OF ... AS ON ...".
    - Units: Values are in Lakhs ("Rs. In Lacs").
    """

    def __init__(self):
        super().__init__(amc_slug="edelweiss", amc_name="Edelweiss Mutual Fund")

    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        Edelweiss headers are typically in Row 6 (index 5).
        """
        for i in range(min(len(df), 15)):
            row_str = " ".join([str(v).upper() for v in df.iloc[i].values if pd.notna(v)])
            if "ISIN" in row_str and any(k in row_str for k in ["QUANTITY", "MARKET/FAIR VALUE", "RATING/INDUSTRY"]):
                return i
        return 5 # Default fallback

    def _extract_scheme_name(self, df: pd.DataFrame, header_idx: int, sheet_name: str) -> str:
        """
        Extract and clean scheme name from Row 0.
        Example: "PORTFOLIO STATEMENT OF EDELWEISS ELSS TAX SAVER FUND AS ON DECEMBER 31, 2025"
        """
        if len(df) > 0:
            row0_vals = [str(v).strip() for v in df.iloc[0].values if pd.notna(v) and len(str(v).strip()) > 10]
            if row0_vals:
                name = row0_vals[0]
                # Remove "PORTFOLIO STATEMENT OF" (case insensitive)
                name = re.sub(r"(?i)^PORTFOLIO STATEMENT OF\s+", "", name).strip()
                # Remove "AS ON.*" (case insensitive)
                name = re.sub(r"(?i)\s+AS ON.*$", "", name).strip()
                # Clean multiple spaces
                name = re.sub(r"\s+", " ", name).strip()
                return name.title()
        
        # Fallback to sheet name cleaning if Row 0 fails
        return sheet_name.replace("EDEL Notes PM", "").strip().title()

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Specific mapping for Edelweiss columns.
        """
        col_map = {
            "ISIN": "isin",
            "NAME OF THE INSTRUMENT": "company_name",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE(RS. IN LACS)": "market_value_inr",
            "MARKET/FAIR VALUE(RS. IN LAKHS)": "market_value_inr",
            "% TO NET ASSETS": "percent_of_nav",
            "RATING/INDUSTRY": "sector"
        }
        
        new_cols = {}
        for col in df.columns:
            clean_col = str(col).strip().upper()
            if clean_col in col_map:
                new_cols[col] = col_map[clean_col]
        
        return df.rename(columns=new_cols)

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Edelweiss file: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            # Edelweiss often uses "EDEL Notes PM" prefixes
            if any(k in str(sheet_name).upper() for k in ["INDEX", "SUMMARY", "COMBINE INDEX"]):
                continue

            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_raw.empty:
                continue

            header_idx = self.find_header_row(df_raw)
            full_scheme_name = self._extract_scheme_name(df_raw, header_idx, str(sheet_name))
            
            # Re-read with actual headers
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            df = self._map_columns(df)

            if "isin" not in df.columns:
                continue

            equity_df = self.filter_equity_isins(df, "isin")
            if equity_df.empty:
                continue

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
                normalized_net_assets = self.normalize_currency(raw_net_assets, "LAKHS")

            equity_df = pd.DataFrame()
            if "isin" in df.columns:
                equity_df = self.filter_equity_isins(df, "isin")
            
            if not equity_df.empty:
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
                            "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), "LAKHS"),
                            "percent_of_nav": self.parse_percentage(row.get("percent_of_nav", 0)),
                            "sector": row.get("sector", None),
                            "total_net_assets": normalized_net_assets
                        }
                    )

                self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
                all_holdings.extend(sheet_holdings)
            elif normalized_net_assets:
                # Ghost Holding for Non-Equity funds
                all_holdings.append(
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

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings
