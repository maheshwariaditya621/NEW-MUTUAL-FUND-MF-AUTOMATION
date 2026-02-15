from typing import Dict, Any, List
import pandas as pd
from src.config import logger
from src.extractors.base_extractor import BaseExtractor

class SamcoExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Samco Mutual Fund.
    File structure (observed from CONSOLIDATED_SAMCO_2025_12.xlsx):
    - Row 0, Col 1: Scheme Name
    - Row 3: Column Headers
    - Market Value: In Rs. in Lakhs
    - NAV %: In decimal format (e.g. 0.0691 -> 6.91%)
    """

    def __init__(self):
        super().__init__(amc_name="Samco Mutual Fund", version="v1")
        # Direct column mapping based on observed headers
        self.column_mapping = {
            "NAME OF THE INSTRUMENT": "company_name",
            "NAME OF INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY": "sector",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE": "market_value_inr",
            "MARKET/ FAIR VALUE": "market_value_inr",
            "LAKHS": "market_value_inr",
            "% TO NET": "percent_to_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting from Samco Mutual Fund: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        # Sheet patterns to skip
        summary_keywords = ["SUMMARY", "INDEX", "CONTROL"]

        for sheet_name in xls.sheet_names:
            if any(k in sheet_name.upper() for k in summary_keywords):
                continue
            
            try:
                # Read enough rows to detect header and scheme name
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=100)
                if df_raw.empty:
                    continue

                # 1. Detect Header Row
                header_idx = self.find_header_row(df_raw, keywords=["ISIN", "INSTRUMENT", "QUANTITY"])
                if header_idx == -1:
                    logger.warning(f"[{sheet_name}] Header not found. Skipping.")
                    continue

                # 2. Extract Scheme Name (Expected at Row 0, Col 1)
                raw_scheme_name = "N/A"
                if len(df_raw) > 0 and len(df_raw.columns) > 1:
                    raw_scheme_name = str(df_raw.iloc[0, 1]).strip()
                
                # Fallback / Scan if Row 0 is empty
                if not raw_scheme_name or raw_scheme_name.lower() == "nan":
                    for i in range(5):
                        potential = str(df_raw.iloc[i, 1]).strip() if len(df_raw.columns) > 1 else str(df_raw.iloc[i, 0]).strip()
                        if potential and potential.lower() != "nan" and len(potential) > 5:
                            raw_scheme_name = potential
                            break

                if raw_scheme_name == "N/A":
                    raw_scheme_name = sheet_name

                scheme_info = self.parse_verbose_scheme_name(raw_scheme_name)
                logger.info(f"Processing scheme: {scheme_info['scheme_name']} (from '{raw_scheme_name}')")
                
                # 3. Process Data
                headers = [str(h).replace('\n', ' ').strip().upper() for h in df_raw.iloc[header_idx]]
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
                        
                        if not self.is_valid_equity_isin(isin):
                            continue
                            
                        name = str(row.get('company_name', '')).strip()
                        holding = {
                            "amc_name": self.amc_name,
                            "isin": self.clean_isin(isin),
                            "company_name": self.clean_company_name(name),
                            "quantity": self.safe_float(row.get('quantity', 0)),
                            # Convert Lakhs to Rupees
                            "market_value_inr": self.normalize_currency(row.get('market_value_inr', 0), "LAKHS"),
                            # Samco values are often decimals (e.g. 0.0691 -> 6.91%)
                            "percent_to_nav": self.parse_percentage(row.get('percent_to_nav', 0)),
                            "sector": str(row.get('sector', '')).strip(),
                            
                            # Scheme Info
                            "scheme_name": scheme_info["scheme_name"],
                            "scheme_description": raw_scheme_name,
                            "plan_type": scheme_info["plan_type"],
                            "option_type": scheme_info["option_type"],
                            "is_reinvest": scheme_info["is_reinvest"]
                        }
                        sheet_holdings.append(holding)
                    except Exception as row_err:
                        logger.error(f"[{sheet_name}] Error processing row {idx}: {row_err}")
                        continue
                
                # Validation
                if self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"]):
                    all_holdings.extend(sheet_holdings)

            except Exception as sheet_err:
                logger.error(f"Error processing sheet {sheet_name}: {sheet_err}")
                continue

        logger.info(f"Total holdings extracted for Samco: {len(all_holdings)}")
        return all_holdings
