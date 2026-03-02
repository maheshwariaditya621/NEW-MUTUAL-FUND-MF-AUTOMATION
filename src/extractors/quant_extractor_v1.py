import pandas as pd
from typing import List, Dict, Any, Optional
from src.extractors.common_extractor_v1 import CommonExtractorV1
from src.config import logger

class QuantExtractorV1(CommonExtractorV1):
    """
    Dedicated extractor for Quant Mutual Fund.
    Excel Structure:
    - Header Row: Typically at index 7 (Row 8).
    - Scheme Name: Typically in Row 3 or 4.
    - Units: Values are in Lakhs.
    """

    def __init__(self):
        super().__init__(amc_slug="quant", amc_name="Quant Mutual Fund")

    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        Quant headers are typically in Row 8 (index 7).
        """
        for i in range(min(len(df), 15)):
            row = [str(v).upper().strip() for v in df.iloc[i].values if pd.notna(v)]
            if "ISIN" in row and any(k in row for k in ["NAME OF THE INSTRUMENT", "QUANTITY", "RATING"]):
                return i
        return 7 # Default fallback for Quant

    def _extract_scheme_name(self, df: pd.DataFrame, header_idx: int, sheet_name: str) -> str:
        """
        Extract scheme name from the top rows.
        Quant usually has:
        Row 1 or 2: Scheme Name
        """
        for i in range(min(header_idx, 6)):
            row_vals = [str(v).strip() for v in df.iloc[i].values if pd.notna(v) and len(str(v).strip()) > 5]
            for val in row_vals:
                v_up = val.upper()
                if "QUANT" in v_up and "MUTUAL FUND" not in v_up and "(FORMERLY" not in v_up:
                    return val.strip().title()
            
        return sheet_name.title()

    def parse_percentage(self, value: Any) -> float:
        """
        Quant percentages are literal (0.96 means 0.96%).
        Disable BaseExtractor's smart scaling.
        """
        return self.safe_float(value)

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Specific mapping for Quant columns.
        """
        col_map = {
            "ISIN": "isin",
            "NAME OF THE INSTRUMENT": "company_name",
            "QUANTITY": "quantity",
            "MARKET VALUE(RS.IN LAKHS)": "market_value_inr",
            "MARKET VALUE(RS. IN LAKHS)": "market_value_inr",
            "% TO NAV": "percent_of_nav",
            "INDUSTRY": "sector"
        }
        
        # Fuzzy match for headers
        new_cols = {}
        for col in df.columns:
            clean_col = str(col).strip().upper()
            if clean_col in col_map:
                new_cols[col] = col_map[clean_col]
        
        return df.rename(columns=new_cols)

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Quant file: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            if any(k in str(sheet_name).upper() for k in ["INDEX", "SUMMARY"]):
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
            
            # Quant is consistently in LAKHS
            value_unit = "LAKHS"
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

            df = self._map_columns(df)

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
                            "sector": row.get("sector", None),
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

            # Post-extraction validation
            self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
            all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings
