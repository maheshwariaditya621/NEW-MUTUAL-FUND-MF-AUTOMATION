import pandas as pd
from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class UnifiExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Unifi Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_name="Unifi Mutual Fund", version="V1")
        self.header_keywords = ["ISIN", "NAME OF INSTRUMENT"]

    def extract_scheme_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Robustly extracts scheme name by searching for the portfolio statement header.
        """
        try:
            # Search first 10 rows for "Portfolio Statement"
            for i in range(min(10, len(df))):
                row_values = [str(val).strip() for val in df.iloc[i].values if not pd.isna(val)]
                row_text = " ".join(row_values).upper()
                
                if "PORTFOLIO STATEMENT" in row_text:
                    # The scheme name is usually in the row IMMEDIATELY above
                    if i > 0:
                        prev_row_values = [str(val).strip() for val in df.iloc[i-1].values if not pd.isna(val)]
                        scheme_name_raw = " ".join(prev_row_values)
                        if scheme_name_raw and "UNIFI MUTUAL FUND" not in scheme_name_raw.upper():
                            return self.parse_verbose_scheme_name(scheme_name_raw.split('\n')[0].strip())
                        
                        # If row above is just AMC name, check two rows above
                        if i > 1:
                            prev_2_row_values = [str(val).strip() for val in df.iloc[i-2].values if not pd.isna(val)]
                            scheme_name_raw = " ".join(prev_2_row_values)
                            if scheme_name_raw:
                                return self.parse_verbose_scheme_name(scheme_name_raw.split('\n')[0].strip())

            # Fallback to Row 2 or 3 if search fails
            for idx in [1, 2]:
                row_values = [str(val).strip() for val in df.iloc[idx].values if not pd.isna(val)]
                row_text = " ".join(row_values)
                if row_text and "PORTFOLIO" not in row_text.upper():
                    return self.parse_verbose_scheme_name(row_text.split('\n')[0].strip())
                    
        except Exception as e:
            logger.warning(f"Error extracting Unifi scheme info: {e}")
            
        return self.parse_verbose_scheme_name("Unknown Unifi Scheme")

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        holdings = []
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                if "METADATA" in sheet_name.upper():
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
                    # Stop logic
                    row_text = " ".join([str(v).upper() for v in row.values if not pd.isna(v)])
                    if any(kw in row_text for kw in ["SUBTOTAL", "TOTAL", "NET ASSETS", "GRAND TOTAL"]):
                        # Usually Unifi has simple structure, Subtotal stops a section
                        # But we only care about equity rows anyway. 
                        # To be safe, if we hit a total row, we stop this block if it looks like a final total.
                        if "TOTAL" in row_text and any(v in row_text for v in ["NET ASSETS", "GRAND"]):
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
                        "sector": self.clean_company_name(row.get('Rating/Industry', 'N/A'))
                    }
                    sheet_holdings.append(record)

                if sheet_holdings:
                    if self.validate_nav_completeness(sheet_holdings, scheme_info['scheme_name']):
                        holdings.extend(sheet_holdings)

        except Exception as e:
            logger.error(f"Error extracting Unifi file {file_path}: {e}")
            
        return holdings
