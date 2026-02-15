from typing import Dict, List, Any, Optional
import pandas as pd
import re
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class GrowwExtractorV1(BaseExtractor):
    def __init__(self):
        super().__init__("Groww Mutual Fund", "V1")
        # Groww headers are consistently at Row 2 (0-indexed)
        # We'll use "ISIN" as the primary anchor
        self.header_keywords = ["ISIN"]
        self.secondary_keywords = ["Name of Instrument", "Quantity"]
        # DEBUG CHECK
        # raise Exception("I AM GROWW EXTRACTOR V1 - CONFIRMED")

    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        Groww files usually have header at row 2.
        We'll scan first 10 rows just to be safe.
        """
        for i, row in df.head(10).iterrows():
            row_str = " ".join([str(x).upper() for x in row.values if not pd.isna(x)])
            if "ISIN" in row_str and "NAME OF INSTRUMENT" in row_str:
                return i
        return -1

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Maps Groww specific columns to canonical names.
        """
        new_cols = {}
        for col in df.columns:
            c = str(col).upper().strip()
            if 'ISIN' == c:
                new_cols[col] = 'isin'
            elif 'NAME OF INSTRUMENT' in c:
                new_cols[col] = 'company_name'
            elif 'QUANTITY' in c:
                new_cols[col] = 'quantity'
            elif 'MARKET VALUE' in c and 'LAKH' in c:
                new_cols[col] = 'market_value_inr'
            elif '% TO NET ASSETS' in c:
                new_cols[col] = 'percent_of_nav'
            elif 'RATING' in c or 'INDUSTRY' in c or 'SECTOR' in c:
                new_cols[col] = 'sector'
                
        df = df.rename(columns=new_cols)
        return df

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        holdings = []
        try:
            xls = pd.ExcelFile(file_path)
            # Skip hidden/metadata sheets if any
            sheet_names = [s for s in xls.sheet_names if "METADATA" not in s.upper()]
            
            for sheet_name in sheet_names:
                try:
                    # 1. Parse Scheme Name from Metadata
                    # Usually in Row 0, Col 1. But sometimes shifted (e.g. Sheet , XB has it in Row 1)
                    # We'll scan first 5 rows
                    df_meta_rows = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=5)
                    raw_scheme_name = None
                    
                    for r_idx, row in df_meta_rows.iterrows():
                        # check col 1 first (most common)
                        if len(row) > 1:
                            val = str(row[1]).strip()
                            if val and val.upper() != 'NAN' and ('GROWW' in val.upper() or val.startswith('IB')):
                                raw_scheme_name = val
                                break
                        # check col 0 fallback
                        if len(row) > 0:
                            val = str(row[0]).strip()
                            if val and val.upper() != 'NAN' and ('GROWW' in val.upper() or val.startswith('IB')):
                                raw_scheme_name = val
                                break
                                
                    if not raw_scheme_name:
                        logger.warning(f"[{sheet_name}] Could not find scheme name in first 5 rows.")
                        continue

                    # Clean "IBXX-" prefix
                    scheme_name = re.sub(r'^IB\d+-', '', raw_scheme_name).strip()
                    
                    # Identify Plan/Option (Groww usually provides consolidated view, defaults to Regular/Growth if not specified)
                    # We'll infer from name or default
                    plan_type = "Regular" 
                    option_type = "Growth"
                    
                    # 2. Extract Data
                    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                    header_idx = self.find_header_row(df)
                    
                    if header_idx == -1:
                        logger.warning(f"[{sheet_name}] Header not found. Skipping.")
                        continue
                        
                    df_data = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
                    df_data = self._map_columns(df_data)
                    
                    required_cols = ['isin', 'market_value_inr', 'quantity']
                    if not all(col in df_data.columns for col in required_cols):
                        logger.warning(f"[{sheet_name}] Missing columns: {set(required_cols) - set(df_data.columns)}")
                        continue
                    
                    # DEBUG: Print df_data info
                    # print(f"Processing {sheet_name}. Data shape: {df_data.shape}. Type: {type(df_data)}")

                    # 3. Iterate rows
                    sheet_holdings = []
                    
                    # Explicitly convert to dict records to avoid iterrows issues if there's some underlying numpy oddity
                    records = df_data.to_dict('records')
                    
                    for idx, row in enumerate(records):
                        try:
                            # row is now a dict, so .get() should work. If row is a string, we know where it came from.
                            if not isinstance(row, dict):
                                logger.error(f"[{sheet_name}] Row {idx} is not a dict: {type(row)} - {row}")
                                continue

                            isin = str(row.get('isin', '')).strip()
                            
                            # Stop conditions
                            if any(marker in isin.upper() for marker in ["TOTAL", "NET ASSETS", "GRAND TOTAL", "SUB TOTAL"]):
                                break
                                
                            # Equity Filter
                            if not self.is_valid_equity_isin(isin):
                                continue
                                
                            # Basic holding data
                            holding = {
                                "amc_name": self.amc_name,
                                "isin": isin,
                                "company_name": str(row.get('company_name', '')).strip(),
                                "quantity": self.safe_float(row.get('quantity', 0)),
                                # Convert Lakhs to Rupees
                                "market_value_inr": self.safe_float(row.get('market_value_inr', 0)) * 100000,
                                "percent_of_nav": self.safe_float(row.get('percent_of_nav', 0)) * 100,
                                "sector": str(row.get('sector', '')).strip(),
                                
                                # Scheme Info
                                "scheme_name": scheme_name,
                                "scheme_description": raw_scheme_name,
                                "plan_type": plan_type,
                                "option_type": option_type,
                                "is_reinvest": False
                            }
                            sheet_holdings.append(holding)
                        except Exception as row_err:
                            logger.error(f"[{sheet_name}] Error processing row {idx}: {row_err}. Row data: {row}")
                            continue
                    
                    # 4. Sheet specific validation
                    self.validate_nav_completeness(sheet_holdings, sheet_name)
                    holdings.extend(sheet_holdings)
                    
                except Exception as e:
                    import traceback
                    logger.error(f"Error processing sheet {sheet_name}: {e}")
                    logger.error(traceback.format_exc())
                    continue
                    
            return holdings
            
        except Exception as e:
            logger.error(f"Failed to extract Groww file: {e}")
            return []
