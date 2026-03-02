import pandas as pd
import re
from typing import List, Dict, Any, Optional
from src.extractors.common_extractor_v1 import CommonExtractorV1
from src.config import logger

class TataExtractorV1(CommonExtractorV1):
    """
    Dedicated extractor for Tata Mutual Fund.
    Excel Structure:
    - Header Row: index 11 (Row 12).
    - Scheme Name: Row 0, Column 1.
    - Units: Values are in Lakhs ("Rs. Lacs").
    """

    def __init__(self):
        super().__init__(amc_slug="tata", amc_name="Tata Mutual Fund")

    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        Tata headers are typically in Row 12 (index 11).
        """
        for i in range(min(len(df), 20)):
            row_str = " ".join([str(v).upper() for v in df.iloc[i].values if pd.notna(v)])
            if "ISIN" in row_str and any(k in row_str for k in ["QUANTITY", "MKT VAL", "INDUSTRY"]):
                return i
        return 11 # Default fallback

    def _extract_scheme_name(self, df: pd.DataFrame, header_idx: int, sheet_name: str) -> str:
        """
        Extract scheme name from Row 0, Column 1 and remove 'MF'.
        """
        if len(df) > 0 and df.shape[1] > 1:
            val = df.iloc[0, 1]
            if pd.notna(val) and len(str(val).strip()) > 5:
                name = str(val).strip()
                # Remove "MF" or "Mf" (case insensitive)
                name = re.sub(r"(?i)\bMF\b", "", name).strip()
                name = re.sub(r"\s+", " ", name)
                return name.title()
        
        fallback = sheet_name.replace("as on 31st ", "").strip().title()
        return re.sub(r"(?i)\bMF\b", "", fallback).strip()

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Specific mapping for Tata columns.
        """
        col_map = {
            "NAME OF THE INSTRUMENT": "company_name",
            "ISIN CODE": "isin",
            "INDUSTRY": "sector",
            "QUANTITY": "quantity",
            "MKT VAL": "market_value_inr",
            "% TO NAV": "percent_of_nav"
        }
        
        new_cols = {}
        for col in df.columns:
            clean_col = str(col).strip().upper()
            matched = False
            for target_key, canonical in col_map.items():
                if target_key in clean_col:
                    new_cols[col] = canonical
                    matched = True
                    break
        
        return df.rename(columns=new_cols)

    def validate_nav_completeness(self, holdings: List[Dict[str, Any]], scheme_name: str) -> bool:
        """
        Handle Hybrid/Arbitrage threshold for Tata as well.
        """
        if not holdings:
            return False
            
        total_nav_pct = sum(h.get('percent_of_nav', 0.0) for h in holdings)
        
        threshold = 90.0
        hybrid_keywords = ["ARBITRAGE", "BALANCED", "HYBRID", "SAVINGS", "MULTI ASSET", "INSURANCE", "CHILDREN", "CONSUMPTION"]
        if any(kw in scheme_name.upper() for kw in hybrid_keywords):
            threshold = 25.0
            logger.info(f"[{scheme_name}] Using HYBRID NAV threshold: {threshold}%")
            
        if total_nav_pct < threshold or total_nav_pct > 105.0:
             logger.error(f"[{scheme_name}] FAILED NAV GUARD: {total_nav_pct:.2f}% (Threshold {threshold}-105%)")
             return False
             
        return True

    def parse_percentage(self, value: Any) -> float:
        """
        Tata uses a 0-100 scale (e.g. 6.52 for 6.52%).
        We override to avoid the BaseExtractor's 'smart' scaling (which inflates < 1% values).
        """
        return self.safe_float(value)

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Tata file: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            if any(k in str(sheet_name).upper() for k in ["INDEX", "SUMMARY", "DIVIDEND", "RISK"]):
                continue

            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=15)
            if df_raw.empty:
                continue

            header_idx = self.find_header_row(df_raw)
            full_scheme_name = self._extract_scheme_name(df_raw, header_idx, str(sheet_name))
            
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            df = self._map_columns(df)
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
            
            sheet_holdings: List[Dict[str, Any]] = []

            if not equity_df.empty:
                for _, row in equity_df.iterrows():
                    mkt_val = self.normalize_currency(row.get("market_value_inr", 0), "LAKHS")
                    qty = self.normalize_currency(row.get("quantity", 0), "RUPEES")

                    # The DB has a CHECK constraint (market_value >= 0 and quantity >= 0).
                    # Arbitrage funds have negative rows (Shorts/Hedges) which we should skip
                    # to avoid netting out the gross exposure percentages.
                    if mkt_val < 0 or qty < 0:
                        logger.debug(f"[{full_scheme_name}] Skipping negative holding (Hedge): {row.get('company_name')} ({row.get('isin')})")
                        continue

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
                            "quantity": int(qty),
                            "market_value_inr": mkt_val,
                            "percent_of_nav": self.parse_percentage(row.get("percent_of_nav", 0)),
                            "sector": self.clean_company_name(row.get("sector", "N/A")),
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

            self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
            all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings
