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
        for i in range(min(5, len(df))):
            row_values = [str(val).strip() for val in df.iloc[i].values if not pd.isna(val)]
            row_text = " ".join(row_values)
            if "SCHEME NAME :" in row_text.upper():
                # Extract everything after "SCHEME NAME :"
                match = re.search(r'SCHEME NAME\s*:\s*(.*)', row_text, re.IGNORECASE)
                if match:
                    raw_name = match.group(1).strip()
                    return self.parse_verbose_scheme_name(raw_name)
        
        return self.parse_verbose_scheme_name("Unknown Taurus Scheme")

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
                    "AUM": "percent_to_nav",
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
                unit_norm = "LAKHS" # Default for Taurus as seen in inspection

                # Extract rows
                sheet_holdings = []
                for _, row in df.iterrows():
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
                        "market_value_inr": self.normalize_currency(raw_data.get('market_value_inr'), unit_norm),
                        "percent_to_nav": self.safe_float(raw_data.get('percent_to_nav', 0.0)),
                        "sector": self.clean_company_name(row.get('Industry ^', 'N/A'))
                    }
                    sheet_holdings.append(record)

                if self.validate_nav_completeness(sheet_holdings, scheme_info['scheme_name']):
                    holdings.extend(sheet_holdings)

        except Exception as e:
            logger.error(f"Error extracting Taurus file {file_path}: {e}")
            
        return holdings
