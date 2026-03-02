import pandas as pd
import re
from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class TaurusExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Taurus Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_name="Taurus Mutual Fund", version="V1")
        self.header_keywords = ["INSTRUMENT", "ISIN", "QUANTITY"]

    def should_process_sheet(self, sheet_name: str) -> bool:
        """
        Target sheets ending with 'Report'.
        Skip performance sheets ending with '(1)'.
        """
        sheet_name_lower = sheet_name.lower()
        if "(1)" in sheet_name_lower:
            return False
        if "report" in sheet_name_lower:
            return True
        return False

    def extract_scheme_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extracts scheme name from Row 2.
        Example: Row 2 has "SCHEME NAME :" in one col and the name in the next.
        """
        for i in range(min(10, len(df))):
            row_values = [str(val).strip() for val in df.iloc[i].values if not pd.isna(val)]
            row_text = " ".join(row_values)
            if "SCHEME NAME :" in row_text.upper():
                # Extract everything after "SCHEME NAME :"
                match = re.search(r'SCHEME NAME\s*:\s*(.*)', row_text, re.IGNORECASE)
                if match:
                    raw_name = match.group(1).strip()
                    return self.parse_verbose_scheme_name(raw_name)
        
        return self.parse_verbose_scheme_name("Unknown Taurus Scheme")

    def _extract_total_aum(self, df: pd.DataFrame, unit: str = "LAKHS") -> float:
        """Find Grand Total or Net Assets row and extract value."""
        # Scan from bottom up
        for i in range(len(df)-1, -1, -1):
            row = df.iloc[i]
            row_text = ' '.join([str(v).upper() for v in row if pd.notna(v)])
            
            is_valid_marker = False
            if "GRAND TOTAL (AUM)" in row_text:
                is_valid_marker = True
            elif "GRAND TOTAL" in row_text:
                is_valid_marker = True
            elif "NET ASSETS" in row_text and "PER UNIT" not in row_text and "PERCENT" not in row_text:
                is_valid_marker = True
                
            if is_valid_marker:
                candidates = []
                for val in row:
                    f_val = self.safe_float(val)
                    # Filter out percentages (like 1.0 or 100.0)
                    if f_val is not None and f_val > 0 and abs(f_val - 1.0) > 0.001 and abs(f_val - 100.0) > 0.1:
                        candidates.append(f_val)
                
                if candidates:
                    # Usually the largest value in the row is the AUM
                    return self.normalize_currency(max(candidates), unit)
        return 0.0

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        holdings = []
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                if not self.should_process_sheet(sheet_name):
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
                    "INSTRUMENT": "company_name",
                    "ISIN": "isin",
                    "QUANTITY": "quantity",
                    "VALUE": "market_value_inr",
                    "AUM": "percent_of_nav",
                    "EQUITY": "company_name",
                    "ISSUER": "company_name"
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

                # Unit detection (Taurus uses Lakhs)
                unit_norm = "LAKHS"

                # Extract rows
                sheet_holdings = []
                for _, row in df.iterrows():
                    raw_data = {final_map[c]: row[c] for c in final_map if c in df.columns}
                    
                    isin_val = str(raw_data.get('isin', '')).strip()
                    if not isin_val or isin_val.lower() == 'nan':
                        continue

                    # Filter equity
                    if not self.is_valid_equity_isin(isin_val):
                        continue

                    # Clean and parse
                    record = {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info['scheme_name'],
                        "scheme_description": scheme_info['description'],
                        "plan_type": scheme_info['plan_type'],
                        "option_type": scheme_info['option_type'],
                        "is_reinvest": scheme_info['is_reinvest'],
                        "isin": self.clean_isin(isin_val),
                        "company_name": self.clean_company_name(raw_data.get('company_name', 'N/A')),
                        "quantity": self.safe_float(raw_data.get('quantity')),
                        "market_value_inr": self.normalize_currency(raw_data.get('market_value_inr'), unit_norm),
                        "percent_of_nav": self.safe_float(raw_data.get('percent_of_nav', 0.0)),
                        "sector": self.clean_company_name(row.get('Industry ^', 'N/A')),
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

                if self.validate_nav_completeness(sheet_holdings, scheme_info['scheme_name']):
                    holdings.extend(sheet_holdings)

        except Exception as e:
            logger.error(f"Error extracting Taurus file {file_path}: {e}")
            
        return holdings
