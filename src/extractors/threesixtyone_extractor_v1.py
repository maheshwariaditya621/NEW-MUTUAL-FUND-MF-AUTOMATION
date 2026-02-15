import pandas as pd
import re
from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class ThreeSixtyOneExtractorV1(BaseExtractor):
    """
    Dedicated extractor for 360 ONE Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_name="360 ONE Mutual Fund", version="V1")
        self.header_keywords = ["INSTRUMENT", "ISIN", "QUANTITY"]

    def extract_scheme_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extracts scheme name from the first few rows.
        Usually in Row 0 or Row 1.
        """
        for i in range(min(5, len(df))):
            row_values = [str(val).strip() for val in df.iloc[i].values if not pd.isna(val)]
            row_text = " ".join(row_values)
            if row_text and not any(kw in row_text.upper() for kw in ["MONTHLY PORTFOLIO", "STATEMENT AS ON"]):
                # Clean up: remove text in parentheses (e.g. "(Formerly known as ...)")
                cleaned_text = re.sub(r'\(.*?\)', '', row_text).strip()
                cleaned_text = " ".join(cleaned_text.split())
                return self.parse_verbose_scheme_name(cleaned_text)
        
        return self.parse_verbose_scheme_name("Unknown 360 ONE Scheme")

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        holdings = []
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
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
                    "ROUNDED % TO NET ASSETS": "percent_of_nav"
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
                    # Stop logic
                    isin_val = str(row.get(next(k for k, v in final_map.items() if v == 'isin'), "")).strip().upper()
                    if any(kw in isin_val for kw in ["TOTAL", "SUB TOTAL"]):
                        break
                    
                    row_text = " ".join([str(v).upper() for v in row.values if not pd.isna(v)])
                    if any(kw in row_text for kw in ["TOTAL", "SUB TOTAL", "GRAND TOTAL", "NET ASSETS"]):
                        if "SUB TOTAL" not in row_text: # Keep processing if just a subtotal of a section, but usually 360one labels are final
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
                        "sector": self.clean_company_name(row.get('Industry', row.get('Industry / Rating', 'N/A')))
                    }
                    sheet_holdings.append(record)

                if sheet_holdings:
                    if self.validate_nav_completeness(sheet_holdings, scheme_info['scheme_name']):
                        holdings.extend(sheet_holdings)

        except Exception as e:
            logger.error(f"Error extracting 360 ONE file {file_path}: {e}")
            
        return holdings
