import pandas as pd
from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class WhiteOakExtractorV1(BaseExtractor):
    """
    Dedicated extractor for WhiteOak Mutual Fund.
    Schema:
    - Multi-sheet (Sheet represents a scheme).
    - Scheme Name in Row 0, Col 0.
    - Header in Row 3.
    - Data starts from Row 4.
    - Columns:
      - Col 1: Name of the Instrument
      - Col 3: ISIN
      - Col 5: Quantity
      - Col 6: Market/Fair Value (Rs. in Lakhs) -> needs x100,000 scaling
      - Col 7: % to Net Assets -> needs x100 scaling
    """

    def __init__(self):
        super().__init__(amc_name="WhiteOak Mutual Fund", version="V1")

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        holdings = []
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                # Skip temp/hidden sheets
                if "SHEET" in sheet_name.upper() and len(sheet_name) < 8:
                    continue

                logger.info(f"Processing sheet: {sheet_name}")
                
                # Check scheme name at Row 0, Col 1
                # Based on user feedback and inspection: row 0, col 1 contains the full name
                df_header_check = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=1)
                
                raw_scheme_name = ""
                if not df_header_check.empty and df_header_check.shape[1] > 1:
                    val = str(df_header_check.iloc[0, 1]).strip()
                    if val and val.lower() != 'nan':
                        raw_scheme_name = val

                if not raw_scheme_name:
                    logger.warning(f"Could not extract scheme name from Row 0, Col 1 in {sheet_name}. Fallback to sheet name.")
                    raw_scheme_name = sheet_name

                # Clean up if needed (e.g. remove extra spaces)
                scheme_info = self.parse_verbose_scheme_name(raw_scheme_name)

                scheme_info = self.parse_verbose_scheme_name(raw_scheme_name)

                # Read data assuming Header at Row 3 (Index 3)
                # We load the whole sheet to ensure we get all data
                df = pd.read_excel(xls, sheet_name=sheet_name, header=3)
                
                # Clean columns: remove NaNs, strip spaces
                df.columns = [str(c).strip() for c in df.columns]

                # Identify columns by index/name approximation since names might vary slightly
                # Based on inspection:
                # Col 1: Name
                # Col 3: ISIN
                # Col 5: Quantity
                # Col 6: Market Value
                # Col 7: % NAV
                
                # Verify indices boundaries
                if df.shape[1] < 8:
                    logger.warning(f"Sheet {sheet_name} has insufficient columns: {df.shape[1]}")
                    continue

                # Get column names by index
                col_name = df.columns[1]
                col_isin = df.columns[3]
                col_qty = df.columns[5]
                col_mkt = df.columns[6]
                col_pct = df.columns[7]
                col_rating = df.columns[4] if len(df.columns) > 4 else None

                for idx, row in df.iterrows():
                    isin_str = str(row[col_isin]).strip()
                    
                    # Strict Equity ISIN check
                    if not self.is_valid_equity_isin(isin_str):
                        continue
                    
                    company = str(row[col_name]).strip()
                    
                    # Quantity
                    qty_val = self.safe_float(row[col_qty])
                    
                    # Market Value (Lakhs -> INR)
                    mkt_val_lakhs = self.safe_float(row[col_mkt])
                    market_value_inr = mkt_val_lakhs * 100000.0
                    
                    # Percent (Decimal typically 0.0139 -> 1.39)
                    # Implementation plan noted 0.0139. safe_float returns the float.
                    # We need to multiply by 100 to get percentage points.
                    pct_raw = self.safe_float(row[col_pct])
                    percent_of_nav = pct_raw * 100.0

                    # Sector
                    sector = str(row[col_rating]).strip() if col_rating else "N/A"

                    record = {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info['scheme_name'],
                        "scheme_description": scheme_info['description'],
                        "plan_type": scheme_info['plan_type'],
                        "option_type": scheme_info['option_type'],
                        "is_reinvest": scheme_info['is_reinvest'],
                        "isin": self.clean_isin(isin_str),
                        "company_name": self.clean_company_name(company),
                        "quantity": qty_val,
                        "market_value_inr": market_value_inr,
                        "percent_of_nav": percent_of_nav,
                        "sector": self.clean_company_name(sector)
                    }
                    holdings.append(record)

        except Exception as e:
            logger.error(f"Error extracting WhiteOak file {file_path}: {e}")

        return holdings
