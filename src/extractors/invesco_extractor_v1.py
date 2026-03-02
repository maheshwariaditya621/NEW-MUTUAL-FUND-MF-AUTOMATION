from typing import Dict, List, Any, Optional
import pandas as pd
import re
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class InvescoExtractorV1(BaseExtractor):
    def __init__(self):
        super().__init__("Invesco Mutual Fund", "V1")
        # Header is consistently at Row 4 (0-indexed)
        # We'll use "ISIN" as the primary anchor
        self.header_keywords = ["ISIN"]
        
    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        Invesco files usually have header at row 4.
        We'll scan first 15 rows.
        """
        for i, row in df.head(15).iterrows():
            row_str = " ".join([str(x).upper() for x in row.values if not pd.isna(x)])
            if "ISIN" in row_str and "NAME OF THE INSTRUMENT" in row_str:
                return i
        return -1

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Maps Invesco specific columns to canonical names.
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
            elif 'MARKET/FAIR VALUE' in c and 'LAKH' in c:
                new_cols[col] = 'market_value_inr'
            elif '% TO NET ASSETS' in c:
                new_cols[col] = 'percent_of_nav'
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
                    # Invesco puts scheme name in Row 2, Col 0 usually.
                    # It often contains newlines and descriptions in parens.
                    df_meta = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=5)
                    raw_scheme_name = None
                    
                    if len(df_meta) > 2:
                        # Try Col 0 first
                        if len(df_meta.columns) > 0:
                            val0 = str(df_meta.iloc[2, 0]).strip()
                            if val0 and "INVESCO" in val0.upper():
                                raw_scheme_name = val0
                        
                        # Try Col 1 if Col 0 failed
                        if not raw_scheme_name and len(df_meta.columns) > 1:
                            val1 = str(df_meta.iloc[2, 1]).strip()
                            if val1 and "INVESCO" in val1.upper():
                                raw_scheme_name = val1
                    
                    if not raw_scheme_name:
                         # Fallback scan
                        for r_idx, row in df_meta.iterrows():
                            for c_idx, val in enumerate(row):
                                if "INVESCO" in str(val).upper() and "SCHEME" in str(val).upper():
                                    raw_scheme_name = str(val).strip()
                                    break
                            if raw_scheme_name: break

                    if not raw_scheme_name:
                         logger.warning(f"[{sheet_name}] Scheme name NOT found. Skipping.")
                         continue

                    # Clean scheme name
                    # Remove "Scheme Name:" prefix if present
                    scheme_name = re.sub(r'(?i)SCHEME NAME\s*:\s*', '', raw_scheme_name).strip()
                    # Replace newlines with space
                    scheme_name = scheme_name.replace('\n', ' ')
                    
                    # Remove parenthesized description e.g. (An open ended...)
                    scheme_name = re.sub(r'\(.*?\)', '', scheme_name, flags=re.DOTALL).strip()
                    
                    # Remove " . A relatively high ..." pattern (Riskometer text often starts with dot)
                    # Pattern: dot, space, "A relatively", ... ending with ) or end of string
                    scheme_name = re.sub(r'\.\s+A\s+relatively\s+high.*?(?:\)|$)', '', scheme_name, flags=re.DOTALL | re.IGNORECASE).strip()

                    # Remove theme/strategy descriptions often found in Invesco names
                    # e.g. "theme following integration strategy)" or "(theme following...)"
                    # Catch trailing "theme following..." with optional closing paren
                    scheme_name = re.sub(r'(?i)\s+theme\s+following.*?(?:\)|$)', '', scheme_name).strip()
                    
                    # Extra cleanup of spaces
                    scheme_name = re.sub(r'\s+', ' ', scheme_name).strip()
                    
                    # Remove trailing dot or closing parenthesis if any (leftover artifacts)
                    scheme_name = scheme_name.rstrip('.)').strip()
                    
                    # Identify Plan/Option
                    # Invesco consolidated -> defaulting
                    plan_type = "Regular" 
                    option_type = "Growth"
                    
                    # 2. Extract Data
                    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                    header_idx = self.find_header_row(df)
                    
                    if header_idx == -1:
                        logger.warning(f"[{sheet_name}] Header not found. Skipping (likely summary sheet).")
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
                                "percent_of_nav": self.safe_float(row.get('percent_of_nav', 0)),
                                "sector": str(row.get('sector', '')).strip(),
                                
                                # Scheme Info
                                "scheme_name": scheme_name,
                                "scheme_description": raw_scheme_name,
                                "plan_type": plan_type,
                                "option_type": option_type,
                                "is_reinvest": False,
                                "total_net_assets": normalized_net_assets
                            }
                            sheet_holdings.append(holding)
                        except Exception as row_err:
                            logger.error(f"[{sheet_name}] Error processing row {idx}: {row_err}")
                            continue

                    if not sheet_holdings and normalized_net_assets:
                        # Ghost Holding for Non-Equity funds
                        holding = {
                            "amc_name": self.amc_name,
                            "scheme_name": scheme_name,
                            "scheme_description": raw_scheme_name,
                            "plan_type": plan_type,
                            "option_type": option_type,
                            "is_reinvest": False,
                            "isin": None,
                            "company_name": "N/A",
                            "quantity": 0,
                            "market_value_inr": 0,
                            "percent_of_nav": 0,
                            "sector": "N/A",
                            "total_net_assets": normalized_net_assets
                        }
                        sheet_holdings.append(holding)
                    
                    # 4. Sheet specific validation
                    self.validate_nav_completeness(sheet_holdings, scheme_name)
                    holdings.extend(sheet_holdings)
                    
                except Exception as e:
                    logger.error(f"Error processing sheet {sheet_name}: {e}")
                    continue
                    
            return holdings
            
        except Exception as e:
            logger.error(f"Failed to extract Invesco file: {e}")
            return []
