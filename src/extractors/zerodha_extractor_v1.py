# src/extractors/zerodha_extractor_v1.py

from typing import Dict, Any, List
import pandas as pd
import re

from src.config import logger
from src.extractors.base_extractor import BaseExtractor


class ZerodhaExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Zerodha Mutual Fund.
    
    File structure (observed from Nov 2025 files):
    - Row 21: Column Headers (ISIN, Name of the Instrument, Quantity, etc.)
    - Market Value: In Rs. in Lakhs
    - % to NAV: In decimal format (e.g. 0.03028 -> 3.028%)
    """

    def __init__(self):
        super().__init__(amc_name="ZERODHA", version="v1")
        self.column_mapping = {
            "NAME OF THE INSTRUMENT": "company_name",
            "NAME OF INSTRUMENT": "company_name",
            "INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY": "sector",
            "QUANTITY": "quantity",
            "MARKET/ FAIR VALUE": "market_value_inr",
            "MARKET VALUE": "market_value_inr",
            "FAIR VALUE": "market_value_inr",
            "% TO NET ASSETS": "percent_of_nav",
            "NAV": "percent_of_nav",
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting Zerodha from: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings = []

        for sheet_name in xls.sheet_names:
            try:
                # 1. Read first few rows to find Full Scheme Name
                df_header = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=10)
                raw_scheme_name = sheet_name
                
                # Check first 5 rows for scheme name patterns
                for r in range(min(5, len(df_header))):
                    for c in range(min(10, len(df_header.columns))):
                        val = str(df_header.iloc[r, c])
                        if "ZERODHA" in val.upper():
                            # Pattern 1: Inside brackets (ZERODHA...)
                            matches = re.findall(r'\(([^)]+)\)', val)
                            for m in matches:
                                if "ZERODHA" in m.upper() and len(m) > 10:
                                    raw_scheme_name = m.strip()
                                    break
                            
                            if raw_scheme_name == sheet_name:
                                # Pattern 2: "PORTFOLIO OF [NAME] FOR [MONTH]"
                                match = re.search(r'PORTFOLIO OF (.*?) FOR', val, re.IGNORECASE)
                                if match:
                                    raw_scheme_name = match.group(1).strip()
                                elif "ZERODHA" in val.upper() and len(val) > 15:
                                    # Fallback: Just use the whole row if it mentions ZERODHA
                                    raw_scheme_name = val.strip()

                            if raw_scheme_name != sheet_name: break
                    if raw_scheme_name != sheet_name: break

                scheme_info = self.parse_verbose_scheme_name(raw_scheme_name)
                
                # Always apply Zerodha-specific cleaning if it looks like a header
                if "PORTFOLIO" in scheme_info["scheme_name"].upper() or "STATEMENT" in scheme_info["scheme_name"].upper():
                    # Surgical extraction of full fund name
                    # Clean up common header junk first
                    name = raw_scheme_name
                    name = re.sub(r'(?i).*?STATEMENTS?\s+OF\s+', '', name)
                    name = re.sub(r'(?i).*?PORTFOLIO\s+OF\s+', '', name)
                    name = re.sub(r'(?i).*?SCHEME\s+PORTFOLIO\s+OF\s+', '', name)
                    name = re.sub(r'(?i)\s+FOR\s+(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER).*', '', name)
                    
                    # Ensure name still starts with ZERODHA
                    if "ZERODHA" in name.upper():
                         scheme_info["scheme_name"] = name.strip()

                # Clean redundant AMC and formatting
                scheme_name = scheme_info["scheme_name"]
                if scheme_name.upper().startswith("ZERODHA"):
                    # Regularize "ZERODHA MUTUAL FUND - NAME" or similar
                    scheme_name = re.sub(r'(?i)^ZERODHA\s+MUTUAL\s+FUND\s*-?\s*', 'ZERODHA ', scheme_name)
                
                scheme_info["scheme_name"] = self.clean_company_name(scheme_name).strip()
                
                # Double name deduplication (e.g. "NAME NAME")
                words = scheme_info["scheme_name"].split()
                if len(words) >= 4 and words[:2] == words[2:4]:
                     scheme_info["scheme_name"] = " ".join(words[:2])
                elif len(words) == 2 and words[0] == words[1]:
                     scheme_info["scheme_name"] = words[0]
                
                # 2. Find header row
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=100)
                header_idx = self.find_header_row(df_raw, keywords=["ISIN", "INSTRUMENT", "QUANTITY", "INDUSTRY"])
                if header_idx == -1:
                    logger.debug(f"Sheet '{sheet_name}': Header not found, skipping.")
                    continue

                # 3. Process Data
                raw_headers = [str(h).replace('\n', ' ').strip().upper() for h in df_raw.iloc[header_idx].values]
                df_data = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx + 1, header=None)
                df_data.columns = raw_headers

                # Map columns
                mapped = {}
                for col in df_data.columns:
                    col_u = str(col).upper()
                    for pattern, canonical in self.column_mapping.items():
                        if pattern in col_u:
                            mapped[col] = canonical
                            break
                
                df_data = df_data.rename(columns=mapped)
                
                if "isin" not in df_data.columns:
                    continue

                # Filter for equity ISINs
                equity_df = self.filter_equity_isins(df_data, "isin")
                if equity_df.empty:
                    continue

                for _, row in equity_df.iterrows():
                    isin = str(row.get("isin", "")).strip()
                    name = str(row.get("company_name", "")).strip()
                    sector = str(row.get("sector", "N/A")).strip()
                    qty = self.safe_float(row.get("quantity", 0))
                    
                    # Zerodha market value is in Lakhs
                    mv_lakhs = self.safe_float(row.get("market_value_inr", 0))
                    market_value = self.normalize_currency(mv_lakhs, "LAKHS")
                    
                    # Zerodha % is often decimal (0.03 = 3%)
                    pct_val = self.safe_float(row.get("percent_of_nav", 0))
                    percent_of_nav = self.parse_percentage(pct_val)

                    all_holdings.append({
                        "amc_name": "ZERODHA",
                        "scheme_name": scheme_info["scheme_name"],
                        "scheme_description": sheet_name,
                        "plan_type": scheme_info["plan_type"],
                        "option_type": scheme_info["option_type"],
                        "is_reinvest": scheme_info["is_reinvest"],
                        "isin": isin,
                        "company_name": self.clean_company_name(name),
                        "quantity": qty,
                        "market_value_inr": market_value,
                        "percent_of_nav": percent_of_nav,
                        "sector": self.clean_company_name(sector)
                    })

            except Exception as e:
                logger.error(f"Error in Zerodha sheet {sheet_name}: {e}")
                continue

        logger.info(f"Total Zerodha holdings extracted: {len(all_holdings)}")
        return all_holdings
