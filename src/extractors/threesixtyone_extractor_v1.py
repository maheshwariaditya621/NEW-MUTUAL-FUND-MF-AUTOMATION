import pandas as pd
import re
from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class ThreeSixtyOneExtractorV1(BaseExtractor):
    """
    Dedicated extractor for 360 ONE Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_name="360 ONE Mutual Fund", version="V1")
        self.header_keywords = ["INSTRUMENT", "ISIN", "QUANTITY"]

    def extract_scheme_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extracts scheme name from the first few rows.
        Usually in Row 0 or Row 1.
        """
        for i in range(min(5, len(df))):
            row_values = [str(val).strip() for val in df.iloc[i].values if not pd.isna(val)]
            row_text = " ".join(row_values)
            if row_text and not any(kw in row_text.upper() for kw in ["MONTHLY PORTFOLIO", "STATEMENT AS ON"]):
                # Clean up: remove text in parentheses (e.g. "(Formerly known as ...)")
                cleaned_text = re.sub(r'\(.*?\)', '', row_text).strip()
                cleaned_text = " ".join(cleaned_text.split())
                return self.parse_verbose_scheme_name(cleaned_text)
        
        return self.parse_verbose_scheme_name("Unknown 360 ONE Scheme")

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from 360 ONE file: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_raw.empty:
                continue

            header_idx = self.find_header_row(df_raw, self.header_keywords)
            if header_idx == -1:
                logger.warning(f"Header not found in sheet: {sheet_name}")
                continue

            scheme_info = self.extract_scheme_info(df_raw)
            
            # Read data
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            raw_columns = df.columns.tolist()
            
            # 360 ONE is typically in LAKHS
            value_unit = "LAKHS"
            
            # Map columns
            col_map = {
                "INSTRUMENT": "company_name",
                "ISIN": "isin",
                "QUANTITY": "quantity",
                "VALUE": "market_value_inr",
                "ROUNDED % TO NET ASSETS": "percent_of_nav",
                "INDUSTRY": "sector"
            }
            
            new_cols = {}
            for col in df.columns:
                col_upper = str(col).strip().upper()
                for pattern, canonical in col_map.items():
                    if pattern in col_upper:
                        new_cols[col] = canonical
                        break
            
            df = df.rename(columns=new_cols)

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
                            "sector": self.clean_company_name(row.get("sector", row.get("Industry", "N/A"))),
                            "total_net_assets": normalized_net_assets
                        }
                    )

            if not sheet_holdings and normalized_net_assets:
                # Ghost Holding for Non-Equity funds
                sheet_holdings.append({
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
                })

            if sheet_holdings:
                # Post-extraction validation
                self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
                all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} holdings from {file_path}")
        return all_holdings
