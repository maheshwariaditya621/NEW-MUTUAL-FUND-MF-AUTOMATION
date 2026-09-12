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

                # Read data assuming Header at Row 3 (Index 3)
                # We load the whole sheet to ensure we get all data
                df = pd.read_excel(xls, sheet_name=sheet_name, header=3)
                
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

                # Clean columns: remove NaNs, strip spaces
                df.columns = [str(c).strip() for c in df.columns]

                # Identify columns dynamically by header name with fallback to positional index
                col_name = None
                col_isin = None
                col_qty = None
                col_mkt = None
                col_pct = None
                col_rating = None

                for col in df.columns:
                    c_upper = str(col).upper()
                    if "ISIN" in c_upper and not col_isin:
                        col_isin = col
                    elif ("INSTRUMENT" in c_upper or "COMPANY" in c_upper or ("NAME" in c_upper and "UNNAMED" not in c_upper)) and not col_name:
                        col_name = col
                    elif ("QUANTITY" in c_upper or "QTY" in c_upper) and not col_qty:
                        col_qty = col
                    elif ("MARKET" in c_upper or "FAIR VALUE" in c_upper) and not col_mkt:
                        col_mkt = col
                    elif ("%" in c_upper or "NET ASSET" in c_upper or "NAV" in c_upper) and not col_pct:
                        col_pct = col
                    elif ("INDUSTRY" in c_upper or "RATING" in c_upper or "SECTOR" in c_upper) and not col_rating:
                        col_rating = col

                # Fallback to positional indices if headers could not be matched
                cols = list(df.columns)
                if not col_name and len(cols) > 1:
                    col_name = cols[1]
                if not col_isin and len(cols) > 3:
                    col_isin = cols[3]
                if not col_rating and len(cols) > 4:
                    col_rating = cols[4]
                if not col_qty and len(cols) > 5:
                    col_qty = cols[5]
                if not col_mkt and len(cols) > 6:
                    col_mkt = cols[6]
                if not col_pct and len(cols) > 7:
                    col_pct = cols[7]

                if not col_isin or not col_name:
                    logger.warning(f"Sheet {sheet_name} missing essential columns (ISIN={col_isin}, Name={col_name})")
                    continue


                sheet_holdings = []
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
                        "sector": self.clean_company_name(sector),
                        "total_net_assets": normalized_net_assets
                    }
                    sheet_holdings.append(record)

                if not sheet_holdings and normalized_net_assets:
                    # Ghost Holding for Non-Equity funds
                    record = {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info['scheme_name'],
                        "scheme_description": scheme_info['description'],
                        "plan_type": scheme_info['plan_type'],
                        "option_type": scheme_info['option_type'],
                        "is_reinvest": scheme_info['is_reinvest'],
                        "isin": None,
                        "company_name": "N/A",
                        "quantity": 0,
                        "market_value_inr": 0,
                        "percent_of_nav": 0,
                        "sector": "N/A",
                        "total_net_assets": normalized_net_assets
                    }
                    sheet_holdings.append(record)
                
                holdings.extend(sheet_holdings)

        except Exception as e:
            logger.error(f"Error extracting WhiteOak file {file_path}: {e}")

        return holdings
