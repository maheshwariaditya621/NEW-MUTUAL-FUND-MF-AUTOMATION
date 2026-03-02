import pandas as pd
import re
from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class UTIExtractorV1(BaseExtractor):
    """
    Dedicated extractor for UTI Mutual Fund.
    Handles single-sheet structure where schemes are stacked vertically.
    """

    def __init__(self):
        super().__init__(amc_name="UTI Mutual Fund", version="V1")
        self.header_keywords = ["ISIN", "NAME OF THE INSTRUMENT"]

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        all_holdings = []
        try:
            xls = pd.ExcelFile(file_path)
            # UTI usually has one main sheet for everything
            sheet_name = [s for s in xls.sheet_names if "EXPOSURE" in s.upper()]
            if not sheet_name:
                sheet_name = [xls.sheet_names[0]]
            
            sheet_name = sheet_name[0]
            logger.info(f"Processing UTI sheet: {sheet_name}")
            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            
            if df_raw.empty:
                return []

            current_scheme_info = None
            header_idx = -1
            final_col_map = {}
            is_in_equity_section = False
            
            # Vertical scan
            for idx, row in df_raw.iterrows():
                row_vals = [str(val).strip() for val in row.values if not pd.isna(val)]
                row_text = " ".join(row_vals).upper()
                
                # Only look for SCHEME: in the first 5 columns to avoid garbage from far-right
                first_few_vals = [str(val).strip() for val in row.iloc[:5].values if not pd.isna(val)]
                row_text_limited = " ".join(first_few_vals).upper()

                # 1. Scheme Detection
                if "SCHEME:" in row_text_limited:
                    scheme_raw = row_text_limited.split("SCHEME:")[1].split("PROVISIONAL")[0].strip()
                    if scheme_raw:
                        # Normalize UTI - prefix to avoid parse_verbose_scheme_name splitting on it
                        scheme_clean = scheme_raw.replace("UTI - ", "UTI ").strip()
                        # Clean trailing dots and weird artifacts
                        scheme_clean = scheme_clean.rstrip(".").strip()
                        # Further cleaning for long junk names
                        if len(scheme_clean) > 200:
                             scheme_clean = scheme_clean[:200].strip()
                        
                        current_scheme_info = self.parse_verbose_scheme_name(scheme_clean)
                    header_idx = -1 # Reset header for new scheme
                    is_in_equity_section = False
                    logger.info(f"Detected UTI Scheme: {current_scheme_info['scheme_name']}")
                    continue

                # 2. Header Detection (within a scheme block)
                if current_scheme_info and header_idx == -1:
                    if all(kw.upper() in row_text for kw in self.header_keywords):
                        header_idx = idx
                        # Map columns
                        headers = [str(h).strip().upper().replace("\n", " ") for h in row.values]
                        final_col_map = {}
                        col_keywords = {
                            "NAME OF THE INSTRUMENT": "company_name",
                            "ISIN": "isin",
                            "RATING/INDUSTRY": "sector",
                            "QUANTITY": "quantity",
                            "MARKET-VALUE": "market_value_inr",
                            "% TO NAV": "percent_of_nav"
                        }
                        for i, h in enumerate(headers):
                            for kw, std in col_keywords.items():
                                if kw in h:
                                    final_col_map[i] = std
                                    break
                        continue

                # 3. Section Scanning
                if "EQUITY AND EQUITY RELATED" in row_text and "TOTAL" not in row_text:
                    is_in_equity_section = True
                    continue
                
                if "TOTAL: EQUITY AND EQUITY RELATED" in row_text:
                    is_in_equity_section = False
                    continue

                # 4. Data Extraction
                if current_scheme_info and header_idx != -1:
                    # 4a. Check for end of current scheme to capture AUM
                    if "TOTAL" in row_text or "GRAND TOTAL" in row_text or "NET ASSETS" in row_text:
                        candidates = []
                        for val in row.values:
                            f_val = self.safe_float(val)
                            if f_val is not None and f_val > 105:
                                candidates.append(f_val)
                        
                        if candidates:
                            raw_aum = candidates[-2] if len(candidates) > 1 and abs(candidates[-1]-100)<0.05 else candidates[-1]
                            norm_aum = self.normalize_currency(raw_aum, "LAKHS")
                            
                            # Update all holdings for this scheme with the found AUM
                            scheme_started_at = -1
                            for i in range(len(all_holdings)-1, -1, -1):
                                if all_holdings[i]["scheme_name"] == current_scheme_info["scheme_name"]:
                                    all_holdings[i]["total_net_assets"] = norm_aum
                                    scheme_started_at = i
                                else:
                                    break
                            
                            # If no equity holdings were found yet, add a Ghost Holding
                            if is_in_equity_section == False and scheme_started_at == -1:
                                record = {
                                    "amc_name": self.amc_name,
                                    "scheme_name": current_scheme_info['scheme_name'],
                                    "scheme_description": current_scheme_info['description'],
                                    "plan_type": current_scheme_info['plan_type'],
                                    "option_type": current_scheme_info['option_type'],
                                    "is_reinvest": current_scheme_info['is_reinvest'],
                                    "isin": None,
                                    "company_name": "N/A",
                                    "quantity": 0,
                                    "market_value_inr": 0,
                                    "percent_of_nav": 0,
                                    "sector": "N/A",
                                    "total_net_assets": norm_aum
                                }
                                all_holdings.append(record)
                        
                        # If it's a hard stop for the scheme, we could reset here, 
                        # but UTI often has nested totals, so we rely on "SCHEME:" for full reset.
                        if "GRAND TOTAL" in row_text or "NET ASSETS" in row_text:
                            is_in_equity_section = False

                    # 4b. Filter and extract equity rows
                    if is_in_equity_section:
                        # Skip sub-headers and totals inside the equity section
                        if any(kw in row_text for kw in ["TOTAL", "SUBTOTAL", "LISTED/AWAITING"]):
                            continue
                        
                        # Extract raw data
                        raw_record = {}
                        for col_idx, std_name in final_col_map.items():
                            raw_record[std_name] = row.iloc[col_idx]
                        
                        isin_str = str(raw_record.get('isin', '')).strip()
                        if not self.is_valid_equity_isin(isin_str):
                            continue

                        percent_val = self.safe_float(raw_record.get('percent_of_nav', 0.0))
                        market_val = self.normalize_currency(raw_record.get('market_value_inr'), "LAKHS")
                        
                        # Guard: Skip negative market values or percentages
                        if market_val < 0 or percent_val < 0:
                            continue

                        # Clean and parse
                        record = {
                            "amc_name": self.amc_name,
                            "scheme_name": current_scheme_info['scheme_name'],
                            "scheme_description": current_scheme_info['description'],
                            "plan_type": current_scheme_info['plan_type'],
                            "option_type": current_scheme_info['option_type'],
                            "is_reinvest": current_scheme_info['is_reinvest'],
                            "isin": self.clean_isin(isin_str),
                            "company_name": self.clean_company_name(raw_record.get('company_name', 'N/A')),
                            "quantity": self.safe_float(raw_record.get('quantity')),
                            "market_value_inr": market_val,
                            "percent_of_nav": percent_val,
                            "sector": self.clean_company_name(raw_record.get('sector', 'N/A')),
                            "total_net_assets": None # Will be filled by scan above
                        }
                        all_holdings.append(record)

        except Exception as e:
            logger.error(f"Error extracting UTI file {file_path}: {e}")
            
        return all_holdings
