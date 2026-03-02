import pandas as pd
import re
from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class TrustExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Trust Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_name="Trust Mutual Fund", version="V1")
        # STRICT RULE: Must find ISIN AND (Instrument OR Issuer OR Company).
        self.header_keywords = ["ISIN", "INSTRUMENT"]

    def parse_verbose_scheme_name(self, raw_name: str) -> Dict[str, Any]:
        """
        Trust specific parsing.
        Enforces 'TRUST ' prefix and replaces 'TRUSTMF' with 'TRUST'.
        """
        if pd.isna(raw_name): name = ""
        else: name = str(raw_name).strip()
        
        # Fix encoding issues
        name = self.fix_mojibake(name)
        
        # Replace TRUSTMF with TRUST (case insensitive)
        name = re.sub(r'(?i)TRUSTMF', 'TRUST', name)
        
        # Normalize spaces
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Enforce TRUST Prefix
        if not name.upper().startswith("TRUST "):
            name = "TRUST " + name
            
        return super().parse_verbose_scheme_name(name)

    def extract_scheme_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extracts scheme name from Row 3.
        """
        try:
            # Based on inspection, Row 3 (index 3) contains the scheme name
            row_3_values = [str(val).strip() for val in df.iloc[3].values if not pd.isna(val)]
            row_text = " ".join(row_3_values)
            if row_text:
                # TrustMF names often contain description after \n
                main_name = row_text.split('\n')[0].strip()
                return self.parse_verbose_scheme_name(main_name)
        except Exception as e:
            logger.warning(f"Error extracting scheme info from Row 3: {e}")
            
        return self.parse_verbose_scheme_name("Unknown Trust Scheme")

    def _extract_total_aum(self, df: pd.DataFrame, unit: str = "LAKHS") -> float:
        """Find Grand Total row and extract value from the Market Value column."""
        # Scan from bottom up
        for i in range(len(df)-1, -1, -1):
            row = df.iloc[i]
            row_text = ' '.join([str(v).upper() for v in row if pd.notna(v)])
            
            if "GRAND TOTAL" in row_text:
                # Based on observation, column 4 usually contains the Market Value (In Rs. lakh)
                # But it might be shifted. Let's find the numeric value.
                candidates = []
                for idx, val in enumerate(row):
                    f_val = self.safe_float(val)
                    # Filter out percentages (like 1.0 or 100.0)
                    if f_val is not None and f_val > 0 and abs(f_val - 1.0) > 0.001 and abs(f_val - 100.0) > 0.1:
                        candidates.append(f_val)
                
                if candidates:
                    # Usually the largest value in the row is the AUM (if multiple numbers exist)
                    return self.normalize_currency(max(candidates), unit)
        return 0.0

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        holdings = []
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                if "XDO MET" in sheet_name:
                    continue
                    
                logger.info(f"Processing sheet: {sheet_name}")
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                
                if df_raw.empty:
                    continue

                scheme_info = self.extract_scheme_info(df_raw)
                header_idx = self.find_header_row(df_raw, self.header_keywords)

                if header_idx == -1:
                    logger.warning(f"Header not found in sheet: {sheet_name}")
                    continue

                # Fetch Total AUM from the FULL dataframe (scanning bottom-up)
                normalized_net_assets = self._extract_total_aum(df_raw)
                if normalized_net_assets == 0:
                    normalized_net_assets = None

                # Prepare headers and data
                headers = [str(h).strip() for h in df_raw.iloc[header_idx]]
                df = df_raw.iloc[header_idx + 1:].copy()
                
                # Handle potential column count mismatch
                if len(headers) > df.shape[1]:
                    headers = headers[:df.shape[1]]
                elif len(headers) < df.shape[1]:
                    df = df.iloc[:, :len(headers)]
                
                df.columns = headers

                # Map columns
                col_map = {
                    "NAME OF INSTRUMENT": "company_name",
                    "ISIN": "isin",
                    "QUANTITY": "quantity",
                    "MARKET VALUE": "market_value_inr",
                    "% TO NET ASSETS": "percent_of_nav"
                }

                final_map = {}
                for col in df.columns:
                    col_upper = str(col).upper()
                    for key, val in col_map.items():
                        if key in col_upper:
                            final_map[col] = val
                            break

                # Ensure mandatory columns exist
                if 'isin' not in final_map.values():
                    logger.warning(f"ISIN column not found in sheet: {sheet_name}")
                    continue

                # Extract rows
                sheet_holdings = []
                for _, row in df.iterrows():
                    # Stop logic: row starts with "Subtotal" or "Total" or "GRAND TOTAL"
                    row_0_val = str(row.iloc[0]).upper().strip()
                    if any(kw in row_0_val for kw in ["SUBTOTAL", "TOTAL", "GRAND TOTAL"]):
                        break
                    
                    row_text = " ".join([str(v).upper() for v in row.values if not pd.isna(v)])
                    if any(kw in row_text for kw in ["NET ASSETS", "GRAND TOTAL"]):
                        break

                    raw_data = {final_map[c]: row[c] for c in final_map if c in df.columns}
                    
                    if not raw_data.get('isin') or pd.isna(raw_data['isin']):
                        continue

                    # Filter equity
                    if not self.is_valid_equity_isin(str(raw_data['isin'])):
                        continue

                    # Clean and parse
                    record = {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info['scheme_name'],
                        "scheme_description": scheme_info['description'],
                        "plan_type": scheme_info['plan_type'],
                        "option_type": scheme_info['option_type'],
                        "is_reinvest": scheme_info['is_reinvest'],
                        "isin": self.clean_isin(raw_data['isin']),
                        "company_name": self.clean_company_name(raw_data.get('company_name', 'N/A')),
                        "quantity": self.safe_float(raw_data.get('quantity')),
                        "market_value_inr": self.normalize_currency(raw_data.get('market_value_inr'), "LAKHS"),
                        "percent_of_nav": self.parse_percentage(raw_data.get('percent_of_nav', 0.0)),
                        "sector": self.clean_company_name(row.get('Rating/Industry', row.get('Rating', 'N/A'))),
                        "total_net_assets": normalized_net_assets
                    }
                    sheet_holdings.append(record)

                if not sheet_holdings and normalized_net_assets:
                    # Ghost Holding for Non-Equity funds
                    sheet_holdings.append({
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
                    })

                if sheet_holdings:
                    if self.validate_nav_completeness(sheet_holdings, scheme_info['scheme_name']):
                        holdings.extend(sheet_holdings)

        except Exception as e:
            logger.error(f"Error extracting Trust MF file {file_path}: {e}")
            
        return holdings
