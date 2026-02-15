
import pandas as pd
import logging
import re
from typing import List, Dict, Any, Optional

from src.extractors.base_extractor import BaseExtractor
from src.config.constants import AMC_PPFAS

class PPFASExtractorV1(BaseExtractor):
    """
    Extractor for PPFAS Mutual Fund Excel files.
    """
    
    def __init__(self):
        # User requested AMC name change from 'PPFAS' to 'PARAG PARIKH'
        super().__init__(amc_name="PARAG PARIKH MUTUAL FUND", version="v1")
        self.logger = logging.getLogger(__name__)

    def standardize_columns(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Standardize column names based on mapping.
        """
        new_cols = {}
        for col in df.columns:
            col_str = str(col).strip()
            # Fuzzy match for complex headers
            for key, val in mapping.items():
                if key in col_str:
                    new_cols[col] = val
                    break
        return df.rename(columns=new_cols)

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extracts holdings from PPFAS Excel file.
        Each sheet corresponds to a scheme.
        """
        self.logger.info(f"[{self.amc_name}] Starting extraction from: {file_path}")
        
        all_holdings = []
        xls = pd.ExcelFile(file_path)
        
        for sheet_name in xls.sheet_names:
            try:
                # Basic skip logic for non-scheme sheets if any
                if "Report" not in sheet_name and "Fund" not in sheet_name:
                    continue

                self.logger.info(f"[{self.amc_name}] Processing sheet: {sheet_name}")
                
                # 1. Read Sheet Raw (No Header)
                df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                
                # 2. Extract Scheme Name
                # Usually in Row 0
                raw_scheme_name = self._extract_scheme_name(df_raw, sheet_name)
                clean_scheme_name = self._clean_scheme_name(raw_scheme_name)
                
                self.logger.info(f"[{self.amc_name}] Scheme Name: {clean_scheme_name}")

                # 3. Find Header
                # Search for "Name of the Instrument"
                required_cols_keywords = ["Name of the Instrument"]
                header_row_idx = self.find_header_row(df_raw, required_cols_keywords)
                
                if header_row_idx is None:
                    # Fallback to Row 3 (Index 3)
                    if len(df_raw) > 3:
                        header_row_idx = 3
                
                if header_row_idx is None:
                    self.logger.error(f"[{self.amc_name}] Header not found in sheet '{sheet_name}'. Skipping.")
                    continue

                # Set header
                df_raw.columns = df_raw.iloc[header_row_idx].fillna('')
                # Ensure unique columns if duplicates exist
                df_raw.columns = [str(c).strip() for c in df_raw.columns]
                
                df_data = df_raw.iloc[header_row_idx+1:].reset_index(drop=True)
                
                # Standardize Columns
                column_map = {
                    "Name of the Instrument": "security_name",
                    "ISIN": "isin",
                    "Industry / Rating": "sector",
                    "Quantity": "quantity",
                    "Market/Fair Value": "market_value", 
                    "% to Net": "percent_of_nav"
                }
                
                # Use fuzzy match for column mapping to handle watermarked/newlines
                df_std = self.standardize_columns(df_data, column_map)
                
                if "security_name" not in df_std.columns:
                     self.logger.error(f"[{self.amc_name}] 'security_name' column missing. Skipping.")
                     continue

                # 4. Iterate Rows (Custom Logic for PPFAS to handle Sub-Totals)
                # We do NOT use filter_equity_isins because it stops at "Sub Total", 
                # but PPFAS has "Equity -> Sub Total -> Arbitrage -> Sub Total".
                
                scheme_holdings_map = {} # ISIN -> Holding Dict (for aggregation)

                for idx, row in df_std.iterrows():
                    # Check for Global Termination
                    row_str = " ".join([str(v).upper() for v in row.values if not pd.isna(v)])
                    if "NET ASSETS" in row_str or "GRAND TOTAL" in row_str:
                        # Stop at Net Assets or Grand Total
                        break
                        
                    isin = str(row.get('isin', '')).strip()
                    
                    # Basic ISIN Validation (INE + 12 chars + '10' code)
                    # We manually apply the specific checks from BaseExtractor here
                    cleaned_isin = self.clean_isin(isin)
                    if not cleaned_isin: 
                        continue
                        
                    if not (len(cleaned_isin) == 12 and cleaned_isin.startswith("INE") and cleaned_isin[8:10] == "10"):
                        # Skip non-equity ISINs
                        continue

                    sec_name = str(row.get('security_name', '')).strip()
                    
                    try:
                        qty = self.safe_float(row.get('quantity'))
                        
                        # Market Value (Lakhs -> INR)
                        mv_lakhs = self.safe_float(row.get('market_value'))
                        mv_inr = mv_lakhs * 100000.0 if mv_lakhs is not None else 0.0
                        
                        # NAV Percentage (Handle decimal vs scaled)
                        nav_pct = self.parse_percentage(row.get('percent_of_nav'))
                        
                        sector = str(row.get('sector', '')).strip()
                        if not sector or sector.lower() == 'nan': sector = None
                        
                        # Parse scheme info for plan/option (default to Regular/Growth if not found)
                        scheme_info = self.parse_verbose_scheme_name(clean_scheme_name)

                        # AGGREGATION LOGIC
                        if cleaned_isin in scheme_holdings_map:
                            # Aggregate
                            existing = scheme_holdings_map[cleaned_isin]
                            existing['quantity'] += qty
                            existing['market_value_inr'] += mv_inr
                            existing['percent_of_nav'] += nav_pct
                        else:
                            # New Entry
                            holding = {
                                "amc_name": self.amc_name,
                                "scheme_name": clean_scheme_name,
                                "plan_type": scheme_info['plan_type'],
                                "option_type": scheme_info['option_type'],
                                "isin": cleaned_isin,
                                "company_name": self.clean_company_name(sec_name),
                                "quantity": qty,
                                "market_value_inr": mv_inr,
                                "percent_of_nav": nav_pct,
                                "sector": sector
                            }
                            scheme_holdings_map[cleaned_isin] = holding
                        
                    except Exception as e:
                        continue
                
                all_holdings.extend(list(scheme_holdings_map.values()))
                
            except Exception as e:
                self.logger.error(f"[{self.amc_name}] Error processing sheet '{sheet_name}': {e}")
                
        return all_holdings

    def _extract_scheme_name(self, df: pd.DataFrame, sheet_name: str) -> str:
        # Check first few rows/cols for "Fund"
        for r in range(min(5, len(df))):
            for c in range(min(5, df.shape[1])):
                val = str(df.iloc[r, c]).strip()
                if "Parag Parikh" in val and "Fund" in val:
                    return val
        return sheet_name

    def _clean_scheme_name(self, raw_name: str) -> str:
        """
        Clean and normalize scheme name to ensure it starts with 'PARAG PARIKH '.
        """
        if pd.isna(raw_name): return ""
        
        # 0. Global Encoding Fix (Inherited from Base)
        cleaned = self.fix_mojibake(str(raw_name).strip())
        
        # Remove parenthesized content only if it's metadata (e.g. "(An open ended...)")
        cleaned = re.sub(r'\(.*?\)', '', cleaned).strip()
        
        # Normalize spaces
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        cleaned_upper = cleaned.upper()
        
        # Prefix Logic
        base_prefix = "PARAG PARIKH "
        
        # Remove existing "PPFAS" or "PARAG PAREKH" variations to start fresh
        cleaned = re.sub(r'(?i)^PPFAS\s+MUTUAL\s+FUND\s*-\s*', '', cleaned).strip()
        cleaned = re.sub(r'(?i)^PPFAS\s+', '', cleaned).strip()
        
        # Check startups
        if cleaned.upper().startswith("PARAG PARIKH"):
             return cleaned # It's good
             
        if cleaned.upper().startswith("PARAG PAREKH"):
             # Fix spelling: Parekh -> Parikh
             return base_prefix + cleaned[12:].strip()
             
        # Else prepend
        return base_prefix + cleaned
