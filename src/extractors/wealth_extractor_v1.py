import pandas as pd
import re
from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class WealthExtractorV1(BaseExtractor):
    """
    Dedicated extractor for The Wealth Company.
    Schema:
    - Multi-sheet (Sheet Name ~ Scheme Name).
    - Scheme Name explicitly in Row 1 (Index 1), Column 0.
    - Header at Row 3 (Index 3).
    - No stop logic (consumes all valid ISIN rows).
    """

    def __init__(self):
        super().__init__(amc_name="The Wealth Company", version="V1")
        self.header_keywords = ["ISIN", "Name of Instrument"]

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        holdings = []
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                # Skip metadata/junk sheets if any (though inspection showed clean sheets)
                if "SHEET" in sheet_name.upper() and len(sheet_name) < 8:
                    continue

                logger.info(f"Processing sheet: {sheet_name}")
                # Read specific rows for Scheme Name and Header
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                
                if df_raw.shape[0] < 5:
                    continue

                # 1. Extract Scheme Name (Row 1, Col 0) -> Index 1
                try:
                    scheme_raw = str(df_raw.iloc[1, 0]).strip()
                    # Pattern: CODE-NAME (e.g. WCAR-THE WEALTH COMPANY ARBITRAGE FUND)
                    if "-" in scheme_raw:
                        scheme_clean = scheme_raw.split("-", 1)[1].strip()
                    else:
                        scheme_clean = scheme_raw
                    
                    # Fix truncated names
                    if "MULTIASSET ALLOC" in scheme_clean:
                        scheme_clean = scheme_clean.replace("MULTIASSET ALLOC", "MULTI ASSET ALLOCATION")

                    scheme_info = self.parse_verbose_scheme_name(scheme_clean)
                except Exception as e:
                    logger.warning(f"Could not extract scheme name from Row 1 in {sheet_name}: {e}")
                    # Fallback to sheet name
                    scheme_info = self.parse_verbose_scheme_name(sheet_name)

                # 2. Extract Data (Header at Row 3 -> Index 3)
                header_idx = 3
                headers = [str(h).strip() for h in df_raw.iloc[header_idx]]
                df = df_raw.iloc[header_idx + 1:].copy()
                df.columns = headers

                # Map columns
                col_map = {
                    "Name of Instrument": "company_name",
                    "ISIN": "isin",
                    "Quantity": "quantity",
                    "Market Value (In Rs. lakh)": "market_value_inr",
                    "% To Net Assets": "percent_to_nav",
                    "Rating/Industry": "sector"
                }

                final_map = {}
                for col in df.columns:
                    col_clean = str(col).strip()
                    if col_clean in col_map:
                        final_map[col] = col_map[col_clean]
                
                if 'isin' not in final_map.values():
                    logger.warning(f"ISIN column not found in sheet: {sheet_name}")
                    continue

                for _, row in df.iterrows():
                    raw_data = {final_map[c]: row[c] for c in final_map if c in df.columns}
                    
                    isin_str = str(raw_data.get('isin', '')).strip()
                    
                    # STRICT FILTERING: Only valid Equity ISINs
                    if not self.is_valid_equity_isin(isin_str):
                        continue

                    # Clean and parse
                    record = {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info['scheme_name'],
                        "scheme_description": scheme_info['description'],
                        "plan_type": scheme_info['plan_type'],
                        "option_type": scheme_info['option_type'],
                        "is_reinvest": scheme_info['is_reinvest'],
                        "isin": self.clean_isin(isin_str),
                        "company_name": self.clean_company_name(raw_data.get('company_name', 'N/A')),
                        "quantity": self.safe_float(raw_data.get('quantity')),
                        "market_value_inr": self.normalize_currency(raw_data.get('market_value_inr'), "LAKHS"),
                        "percent_to_nav": self.safe_float(raw_data.get('percent_to_nav', 0.0)),
                        "sector": self.clean_company_name(raw_data.get('sector', 'N/A'))
                    }
                    holdings.append(record)

        except Exception as e:
            logger.error(f"Error extracting Wealth Company file {file_path}: {e}")
            
        return holdings
