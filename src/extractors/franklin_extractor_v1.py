import pandas as pd
import logging
import re
from typing import List, Dict, Any, Optional
from src.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

class FranklinExtractorV1(BaseExtractor):
    def __init__(self):
        super().__init__("Franklin Templeton", "V1")
        # Franklin using "ISIN Number" as primary header keyword
        self.header_keywords = ["ISIN NUMBER"]
        self.secondary_keywords = ["NAME OF THE INSTRUMENT", "MARKET VALUE"]
        self.units = "LAKHS"

    def find_header_row(self, df: pd.DataFrame, keywords: List[str] = None) -> int:
        """
        Franklin headers are typically at Row 3.
        """
        for i in range(min(15, len(df))):
            row_str = df.iloc[i].astype(str).str.upper().tolist()
            # Check for ISIN Number
            if any("ISIN NUMBER" in str(val) for val in row_str):
                return i
        return -1

    def parse_verbose_scheme_name(self, raw_text: str) -> Dict[str, Any]:
        """
        Franklin scheme names are in Row 0.
        Example: "Franklin India Liquid Fund"
        """
        clean_text = raw_text.strip()
        
        # Remove common footers/suffixes if present
        clean_text = re.sub(r'\(\s*formerly known as.*?\)', '', clean_text, flags=re.IGNORECASE).strip()
        clean_text = re.sub(r'\^', '', clean_text).strip()
        
        # Default classification (Franklin schemes usually don't have plan/option in Row 0)
        return {
            "scheme_name": clean_text,
            "scheme_description": clean_text,
            "plan_type": "Regular", # Default to Regular if not specified
            "option_type": "Growth",  # Default to Growth if not specified
            "is_reinvest": False
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        all_holdings = []
        
        with pd.ExcelFile(file_path) as xls:
            for sheet_name in xls.sheet_names:
                # Skip index or summaries if any
                if any(x in sheet_name.lower() for x in ['index', 'summary']):
                    continue
                
                logger.info(f"Processing sheet: {sheet_name}")
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                
                if df_raw.empty:
                    continue

                # Get Scheme Name from Row 0
                raw_scheme_text = str(df_raw.iloc[0, 0]).strip()
                scheme_info = self.parse_verbose_scheme_name(raw_scheme_text)
                
                header_idx = self.find_header_row(df_raw)
                if header_idx == -1:
                    logger.warning(f"Header not found in sheet: {sheet_name}")
                    continue

                # Read data starting from header
                df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
                
                # Normalize columns
                df = self._map_columns(df)
                
                if 'isin' not in df.columns:
                    logger.warning(f"ISIN column missing in sheet: {sheet_name}")
                    continue

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

                # Extraction loop
                sheet_holdings = []
                equity_rows = []
                for _, row in df.iterrows():
                    isin = str(row.get('isin', '')).strip()
                    if any(marker in isin.upper() for marker in ["TOTAL", "NET ASSETS", "GRAND TOTAL"]):
                        break
                    if self.is_valid_equity_isin(isin):
                        equity_rows.append(row)
                
                if equity_rows:
                    for row in equity_rows:
                        sheet_holdings.append({
                            "amc_name": self.amc_name,
                            "isin": str(row.get('isin', '')).strip(),
                            "company_name": str(row.get('company_name', row.get('instrument_name', ''))).strip(),
                            "quantity": self.safe_float(row.get('quantity', 0)),
                            "market_value_inr": self.safe_float(row.get('market_value_inr', 0)) * 100000.0,
                            "percent_of_nav": self.safe_float(row.get('percent_of_nav', 0)),
                            "sector": str(row.get('sector', row.get('industry', ''))).strip(),
                            "total_net_assets": normalized_net_assets,
                            **scheme_info
                        })
                elif normalized_net_assets:
                    # Ghost Holding for Non-Equity funds
                    sheet_holdings.append({
                        "amc_name": self.amc_name,
                        "isin": None,
                        "company_name": "N/A",
                        "quantity": 0,
                        "market_value_inr": 0,
                        "percent_of_nav": 0,
                        "sector": "N/A",
                        "total_net_assets": normalized_net_assets,
                        **scheme_info
                    })


                # Post-extraction validation
                if self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"]):
                    all_holdings.extend(sheet_holdings)
                else:
                    logger.error(f"Validation failed for scheme: {scheme_info['scheme_name']}")
        
        return all_holdings

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        new_cols = {}
        for col in df.columns:
            c = str(col).upper()
            if 'ISIN' in c:
                new_cols[col] = 'isin'
            elif 'NAME OF THE INSTRUMENT' in c or 'NAME OF INSTRUMENT' in c:
                new_cols[col] = 'company_name'
            elif 'QUANTITY' in c:
                new_cols[col] = 'quantity'
            elif 'MARKET VALUE' in c:
                new_cols[col] = 'market_value_inr'
            elif '% TO NET ASSETS' in c and 'DERIVATIVE' not in c:
                new_cols[col] = 'percent_of_nav'
            elif 'RATING' in c or 'INDUSTRY' in c or 'SECTOR' in c:
                new_cols[col] = 'sector'
        
        return df.rename(columns=new_cols)
