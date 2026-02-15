from typing import Dict, List, Any, Optional
import pandas as pd
import re
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class HeliosExtractorV1(BaseExtractor):
    def __init__(self):
        super().__init__("Helios Mutual Fund", "V1")
        # Header is consistently at Row 5 (0-indexed)
        # We'll use "ISIN" as the primary anchor
        self.header_keywords = ["ISIN"]
        
    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        Helios files usually have header at row 5.
        We'll scan first 15 rows.
        """
        for i, row in df.head(15).iterrows():
            row_str = " ".join([str(x).upper() for x in row.values if not pd.isna(x)])
            if "ISIN" in row_str and "NAME OF THE INSTRUMENT" in row_str:
                return i
        return -1

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Maps Helios specific columns to canonical names.
        """
        new_cols = {}
        for col in df.columns:
            c = str(col).upper().strip()
            if 'ISIN' == c:
                new_cols[col] = 'isin'
            elif 'NAME OF THE INSTRUMENT' in c:
                new_cols[col] = 'company_name'
            elif 'QUANTITY' in c:
                new_cols[col] = 'quantity'
            elif 'MARKET VALUE' in c and 'LAKH' in c:
                new_cols[col] = 'market_value_inr'
            elif '% TO AUM' in c:
                new_cols[col] = 'percent_to_nav'
            elif 'RATING' in c or 'INDUSTRY' in c:
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
                    # 1. Parse Scheme Name from Row 2
                    # Format: "SCHEME NAME :  Helios Balanced Advantage Fund..."
                    df_meta = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=5)
                    raw_scheme_name = None
                    
                    # Usually in Row 2, Col 2 (index 2)
                    if len(df_meta) > 2 and len(df_meta.columns) > 2:
                        val = str(df_meta.iloc[2, 2]).strip()
                        if "SCHEME NAME" in val.upper():
                            # Check if name is in this cell or next
                            if len(val) < 20 and len(df_meta.columns) > 3:
                                raw_scheme_name = str(df_meta.iloc[2, 3]).strip()
                            else:
                                raw_scheme_name = val
                    
                    if not raw_scheme_name:
                        logger.warning(f"[{sheet_name}] Could not find scheme name in Row 2. Skipping valid extraction for this sheet might fail.")
                        # Fallback scan
                        for r_idx, row in df_meta.iterrows():
                            for c_idx, val in enumerate(row):
                                if "SCHEME NAME" in str(val).upper():
                                    raw_scheme_name = str(val).strip()
                                    break
                            if raw_scheme_name: break

                    if not raw_scheme_name:
                         logger.warning(f"[{sheet_name}] Scheme name NOT found. Skipping.")
                         continue

                    # Clean prefix "SCHEME NAME :"
                    scheme_name = re.sub(r'(?i)SCHEME NAME\s*:\s*', '', raw_scheme_name).strip()
                    # Remove anything in parenthesis usually (An open ended...)
                    scheme_name = re.sub(r'\(.*?\)', '', scheme_name, flags=re.DOTALL).strip()
                    
                    # Identify Plan/Option
                    # Helios usually consolidated. Defaulting.
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

                    # 3. Iterate rows
                    sheet_holdings = []
                    records = df_data.to_dict('records')
                    
                    for idx, row in enumerate(records):
                        try:
                            isin = str(row.get('isin', '')).strip()
                            
                            # Stop conditions
                            if any(marker in isin.upper() for marker in ["TOTAL", "NET ASSETS", "GRAND TOTAL"]):
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
                                "percent_to_nav": self.safe_float(row.get('percent_to_nav', 0)),
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
                            logger.error(f"[{sheet_name}] Error processing row {idx}: {row_err}")
                            continue
                    
                    # 4. Sheet specific validation
                    # Note: Helios likely small enough to use standard guard
                    self.validate_nav_completeness(sheet_holdings, sheet_name)
                    holdings.extend(sheet_holdings)
                    
                except Exception as e:
                    logger.error(f"Error processing sheet {sheet_name}: {e}")
                    continue
                    
            return holdings
            
        except Exception as e:
            logger.error(f"Failed to extract Helios file: {e}")
            return []
