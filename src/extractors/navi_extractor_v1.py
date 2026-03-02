from typing import Dict, Any, List
import pandas as pd
import re
from src.config import logger
from src.extractors.base_extractor import BaseExtractor

class NaviExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Navi Mutual Fund.
    File structure (observed from CONSOLIDATED_NAVI_2025_12.xlsx):
    - Row 4: Scheme Name (Col 0)
    - Row 6: Column Headers
    - Market Value: In Lacs
    - NAV %: Already in percentage format (e.g. 4.51)
    """

    def __init__(self):
        super().__init__(amc_name="Navi Mutual Fund", version="v1")
        # Direct column mapping based on observed headers
        self.column_mapping = {
            "NAME OF THE INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY/RATING": "sector",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE (RS. IN LACS)": "market_value_inr",
            "% TO NET ASSETS": "percent_of_nav"
        }

    def _extract_total_aum(self, df: pd.DataFrame, unit: str = "LAKHS") -> float:
        """Find Grand Total or Net Assets row and extract value."""
        # Scan from bottom up
        for i in range(len(df)-1, -1, -1):
            row = df.iloc[i]
            row_text = ' '.join([str(v).upper() for v in row if pd.notna(v)])
            
            is_valid_marker = False
            if "GRAND TOTAL" in row_text:
                is_valid_marker = True
            elif "NET ASSETS" in row_text and "PER UNIT" not in row_text and "PERCENT" not in row_text:
                is_valid_marker = True
                
            if is_valid_marker:
                candidates = []
                for val in row:
                    f_val = self.safe_float(val)
                    # Filter out percentages (like 1.0 or 100.0) usually found in the last column
                    if f_val is not None and f_val > 0 and abs(f_val - 1.0) > 0.001 and abs(f_val - 100.0) > 0.1:
                        candidates.append(f_val)
                
                if candidates:
                    return self.normalize_currency(max(candidates), unit)
        return 0.0

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting from Navi Mutual Fund: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        # Sheet patterns to skip (only non-portfolio sheets)
        summary_keywords = ["SUMMARY", "GENERAL INFORMATION", "CONTROL"]

        for sheet_name in xls.sheet_names:
            if any(k in sheet_name.upper() for k in summary_keywords):
                continue
            
            try:
                # Read enough rows to detect header and scheme name
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=100)
                if df_raw.empty:
                    continue

                # 1. Detect Header Row
                header_idx = self.find_header_row(df_raw, keywords=["ISIN", "NAME OF THE INSTRUMENT"])
                if header_idx == -1:
                    logger.warning(f"[{sheet_name}] Header not found. Skipping.")
                    continue

                # 2. Extract Scheme Name (Expected at Row 4, Col 0)
                raw_scheme_name = "N/A"
                for i in range(3, 7): # Scans rows 3-6
                    potential_name = str(df_raw.iloc[i, 0]).strip() if i < len(df_raw) else "nan"
                    if potential_name and potential_name.lower() != "nan" and len(potential_name) > 3:
                        # Skip generic AMC header
                        if "MUTUAL FUND" in potential_name.upper() and len(potential_name) < 25:
                            continue
                        raw_scheme_name = potential_name
                        break
                
                if raw_scheme_name == "N/A":
                    # Fallback to sheet name only if absolutely necessary
                    clean_sheet = re.sub(r'\s+\d+(st|nd|rd|th)?\s+\d+(st|nd|rd|th)?.*$', '', sheet_name).strip()
                    clean_sheet = clean_sheet.replace("Sheet1", "").strip()
                    raw_scheme_name = clean_sheet

                scheme_info = self.parse_verbose_scheme_name(raw_scheme_name)
                logger.info(f"Processing scheme: {scheme_info['scheme_name']} (from '{raw_scheme_name}')")
                
                # Fetch Total AUM from the full dataframe (scanning bottom-up)
                df_full = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                normalized_net_assets = self._extract_total_aum(df_full)
                if normalized_net_assets == 0:
                    normalized_net_assets = None
                
                # 3. Process Data
                # Correctly map columns based on header row
                headers = [str(h).strip().upper() for h in df_raw.iloc[header_idx]]
                df_data = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx + 1, header=None)
                df_data.columns = headers

                # Map columns to canonical names
                mapped_cols = {}
                for col in df_data.columns:
                    for pattern, canonical in self.column_mapping.items():
                        if pattern.upper() in str(col).upper():
                            mapped_cols[col] = canonical
                            break
                
                df_data = df_data.rename(columns=mapped_cols)
                
                if "isin" not in df_data.columns:
                    logger.warning(f"[{sheet_name}] 'isin' column not found after mapping. Skipping.")
                    continue

                sheet_holdings = []
                records = df_data.to_dict('records')

                for idx, row in enumerate(records):
                    try:
                        isin = str(row.get('isin', '')).strip()
                        
                        # Equity Filter
                        if not self.is_valid_equity_isin(isin):
                            continue
                            
                        # Basic holding data
                        name = str(row.get('company_name', '')).strip()
                        holding = {
                            "amc_name": self.amc_name,
                            "isin": self.clean_isin(isin),
                            "company_name": self.clean_company_name(name),
                            "quantity": self.safe_float(row.get('quantity', 0)),
                            # Convert Lacs (Lakhs) to Rupees
                            "market_value_inr": self.normalize_currency(row.get('market_value_inr', 0), "LAKHS"),
                            # Navi values are already percentages (e.g. 4.51)
                            "percent_of_nav": self.safe_float(row.get('percent_of_nav', 0)),
                            "sector": str(row.get('sector', '')).strip(),
                            
                            # Scheme Info
                            "scheme_name": scheme_info["scheme_name"],
                            "scheme_description": raw_scheme_name,
                            "plan_type": scheme_info["plan_type"],
                            "option_type": scheme_info["option_type"],
                            "is_reinvest": scheme_info["is_reinvest"],
                            "total_net_assets": normalized_net_assets
                        }
                        sheet_holdings.append(holding)
                    except Exception as row_err:
                        logger.error(f"[{sheet_name}] Error processing row {idx}: {row_err}")
                        continue
                
                if not sheet_holdings and normalized_net_assets:
                    # Ghost Holding for Non-Equity funds
                    sheet_holdings.append(
                        {
                            "amc_name": self.amc_name,
                            "scheme_name": scheme_info["scheme_name"],
                            "scheme_description": raw_scheme_name,
                            "plan_type": scheme_info["plan_type"],
                            "option_type": scheme_info["option_type"],
                            "is_reinvest": scheme_info["is_reinvest"],
                            "isin": None,
                            "company_name": "N/A",
                            "quantity": 0,
                            "market_value_inr": 0,
                            "percent_of_nav": 0,
                            "sector": "N/A",
                            "total_net_assets": normalized_net_assets
                        }
                    )
                
                # Validation
                if self.validate_nav_completeness(sheet_holdings, sheet_name):
                    all_holdings.extend(sheet_holdings)

            except Exception as sheet_err:
                logger.error(f"Error processing sheet {sheet_name}: {sheet_err}")
                continue

        logger.info(f"Total holdings extracted for Navi: {len(all_holdings)}")
        return all_holdings
