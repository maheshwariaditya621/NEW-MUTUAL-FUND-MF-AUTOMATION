import pandas as pd
import re
from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class UnionExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Union Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_name="Union Mutual Fund", version="V1")
        self.header_keywords = ["ISIN", "Name of the Instrument"]

    def parse_verbose_scheme_name(self, raw_name: str) -> Dict[str, Any]:
        """
        Union specific parsing.
        Removes '(FORMERLY ...)' suffixes.
        """
        if pd.isna(raw_name): name = ""
        else: name = str(raw_name).strip()
        
        # Fix encoding issues
        name = self.fix_mojibake(name)
        
        # Remove (FORMERLY ...) suffix (case insensitive)
        # Handles nested parentheses like (FORMERLY UNION TAX SAVER (ELSS) FUND)
        name = re.sub(r'(?i)\(FORMERLY(?:[^()]*|\([^()]*\))*\)', '', name).strip()
        
        # Normalize spaces
        name = re.sub(r'\s+', ' ', name).strip()
        
        return super().parse_verbose_scheme_name(name)

    def extract_scheme_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extracts scheme name from Row 6.
        Expected pattern: MONTHLY PORTFOLIO STATEMENT OF [SCHEME NAME] AS ON [DATE]
        """
        try:
            # Row index 6 (Row 6 in Excel if 1-indexed and we assume no shift)
            # Let's look for "OF " and " AS ON" keywords inside row 6
            row_6_values = [str(val).strip() for val in df.iloc[6].values if not pd.isna(val)]
            row_text = " ".join(row_6_values).upper()
            
            if "STATEMENT OF" in row_text:
                scheme_part = row_text.split("STATEMENT OF")[1].split("AS ON")[0].strip()
                if scheme_part:
                    # Clean special characters like ^^, *
                    clean_name = re.sub(r'[\^\*\#]+', '', scheme_part).strip()
                    return self.parse_verbose_scheme_name(clean_name)
            
            # Fallback to Row 1-7 search if row 6 is empty or weird
            for i in range(1, 8):
                row_vals = [str(val).strip() for val in df.iloc[i].values if not pd.isna(val)]
                txt = " ".join(row_vals).upper()
                if "STATEMENT OF" in txt:
                    scheme_part = txt.split("STATEMENT OF")[1].split("AS ON")[0].strip()
                    clean_name = re.sub(r'[\^\*\#]+', '', scheme_part).strip()
                    return self.parse_verbose_scheme_name(clean_name)

        except Exception as e:
            logger.warning(f"Error extracting Union scheme info: {e}")
            
        return self.parse_verbose_scheme_name("Unknown Union Scheme")

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        holdings = []
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                if "METADATA" in sheet_name.upper():
                    continue
                    
                logger.info(f"Processing sheet: {sheet_name}")
                # Read without header first to find the header row
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                
                if df_raw.empty:
                    continue

                scheme_info = self.extract_scheme_info(df_raw)
                header_idx = self.find_header_row(df_raw, self.header_keywords)

                if header_idx == -1:
                    logger.warning(f"Header not found in Union sheet: {sheet_name}")
                    continue

                # Prepare headers and data
                headers = [str(h).strip() for h in df_raw.iloc[header_idx]]
                df = df_raw.iloc[header_idx + 1:].copy()
                
                # Column normalization
                if len(headers) > df.shape[1]:
                    headers = headers[:df.shape[1]]
                elif len(headers) < df.shape[1]:
                    df = df.iloc[:, :len(headers)]
                
                df.columns = headers

                # Map columns manually if needed, or use keyword matching
                col_map = {
                    "NAME OF THE INSTRUMENT": "company_name",
                    "ISIN": "isin",
                    "RATING / INDUSTRY": "sector",
                    "QUANTITY": "quantity",
                    "MARKET VALUE": "market_value_inr",
                    "% TO NAV": "percent_of_nav"
                }

                final_map = {}
                for col in df.columns:
                    col_upper = str(col).upper().replace("\n", " ")
                    for key, val in col_map.items():
                        if key in col_upper:
                            final_map[col] = val
                            break

                if 'isin' not in final_map.values():
                    logger.warning(f"ISIN column not found in Union sheet: {sheet_name}")
                    continue

                sheet_holdings = []
                for _, row in df.iterrows():
                    # Stop logic
                    row_text = " ".join([str(v).upper() for v in row.values if not pd.isna(v)])
                    if any(kw in row_text for kw in ["GRAND TOTAL", "NET ASSETS", "TOTAL (", "TOTAL "]):
                        # Avoid stopping on "SUB TOTAL"
                        if "TOTAL" in row_text and "SUB" not in row_text:
                            # Let's double check if it's a final total
                            if any(v in row_text for v in ["NET ASSETS", "100.00", "GRAND"]):
                                break

                    raw_data = {final_map[c]: row[c] for c in final_map if c in df.columns}
                    
                    if not raw_data.get('isin') or pd.isna(raw_data['isin']):
                        continue

                    # Union sometimes has multiple tables side by side or weird labels
                    # We only care about equity rows
                    isin_str = str(raw_data['isin']).strip()
                    if not self.is_valid_equity_isin(isin_str):
                        continue

                    # Clean and parse
                    record = {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info['scheme_name'],
                        "scheme_description": scheme_info['description'],
                        "plan_type": scheme_info['plan_type'],
                        "option_type": scheme_info['option_type'],
                        "is_reinvest": scheme_info['is_reinvest'],
                        "isin": self.clean_isin(isin_str),
                        "company_name": self.clean_company_name(raw_data.get('company_name', 'N/A')),
                        "quantity": self.safe_float(raw_data.get('quantity')),
                        "market_value_inr": self.normalize_currency(raw_data.get('market_value_inr'), "LAKHS"),
                        "percent_of_nav": self.safe_float(raw_data.get('percent_of_nav', 0.0)),
                        "sector": self.clean_company_name(raw_data.get('sector', 'N/A'))
                    }
                    sheet_holdings.append(record)

                if sheet_holdings:
                    if self.validate_nav_completeness(sheet_holdings, scheme_info['scheme_name']):
                        holdings.extend(sheet_holdings)

        except Exception as e:
            logger.error(f"Error extracting Union file {file_path}: {e}")
            
        return holdings
