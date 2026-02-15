from typing import Dict, List, Any, Optional
import pandas as pd
import re
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class ITIExtractorV1(BaseExtractor):
    def __init__(self):
        super().__init__("ITI Mutual Fund", "V1")
        self.header_keywords = ["ISIN", "NAME OF THE INSTRUMENT"]
        
    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        ITI files usually have header at row 5.
        """
        for i, row in df.head(15).iterrows():
            row_str = " ".join([str(x).upper() for x in row.values if not pd.isna(x)])
            if "ISIN" in row_str and "NAME OF THE INSTRUMENT" in row_str:
                return i
        return -1

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Maps ITI specific columns to canonical names.
        """
        new_cols = {}
        for col in df.columns:
            c = str(col).upper().strip().replace('\n', ' ')
            if 'ISIN' == c:
                new_cols[col] = 'isin'
            elif 'NAME OF THE INSTRUMENT' in c:
                new_cols[col] = 'company_name'
            elif 'QUANTITY' in c:
                new_cols[col] = 'quantity'
            elif 'MARKET/FAIR VALUE' in c and 'LAKH' in c:
                new_cols[col] = 'market_value_inr'
            elif '% TO NET' in c and 'ASSET' in c:
                new_cols[col] = 'percent_to_nav'
            elif 'INDUSTRY' in c or 'RATING' in c:
                new_cols[col] = 'sector'
                
        df = df.rename(columns=new_cols)
        return df

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        holdings = []
        try:
            xls = pd.ExcelFile(file_path)
            # ITI uses Index sheet for mapping, but we can iterate data sheets
            # Skip 'INDEX' and any other summary/metadata sheets
            sheet_names = [s for s in xls.sheet_names if s.upper() not in ["INDEX", "METADATA"]]
            
            for sheet_name in sheet_names:
                try:
                    # 1. Parse Scheme Name from Row 2, Col 1
                    df_meta = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=5)
                    raw_scheme_name = None
                    
                    if len(df_meta) > 2 and len(df_meta.columns) > 1:
                        raw_scheme_name = str(df_meta.iloc[2, 1]).strip()
                    
                    if not raw_scheme_name or raw_scheme_name == 'nan':
                        # Fallback: check Row 2, Col 0 or scan first 5 rows
                        for r_idx in range(5):
                            for c_idx in range(min(len(df_meta.columns), 3)):
                                val = str(df_meta.iloc[r_idx, c_idx]).strip()
                                if "ITI" in val.upper() and ("FUND" in val.upper() or "SCHEME" in val.upper()):
                                    raw_scheme_name = val
                                    break
                            if raw_scheme_name: break

                    if not raw_scheme_name or raw_scheme_name == 'nan':
                         logger.warning(f"[{sheet_name}] Scheme name NOT found. Skipping.")
                         continue

                    # Clean scheme name
                    scheme_name = raw_scheme_name.replace('\n', ' ')
                    scheme_name = re.sub(r'\s+', ' ', scheme_name).strip()
                    
                    # Identify Plan/Option
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
                                # Scale Decimal to Percentage (0.15 -> 15.0)
                                "percent_to_nav": self.safe_float(row.get('percent_to_nav', 0)) * 100,
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
                    self.validate_nav_completeness(sheet_holdings, sheet_name)
                    holdings.extend(sheet_holdings)
                    
                except Exception as e:
                    logger.error(f"Error processing sheet {sheet_name}: {e}")
                    continue
                    
            return holdings
            
        except Exception as e:
            logger.error(f"Failed to extract ITI file: {e}")
            return []
