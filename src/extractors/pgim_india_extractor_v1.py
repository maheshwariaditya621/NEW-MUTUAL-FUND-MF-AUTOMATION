from typing import Dict, Any, List
import pandas as pd
from src.config import logger
from src.extractors.base_extractor import BaseExtractor

class PGIMIndiaExtractorV1(BaseExtractor):
    """
    Dedicated extractor for PGIM India Mutual Fund.
    File structure (observed from CONSOLIDATED_PGIM_INDIA_2025_12.xlsx):
    - Row 0, Col 1: Scheme Name
    - Row 11: Column Headers
    - Market Value: In INR Lacs
    - NAV %: In raw percentage format (e.g. 7.73 -> 7.73%)
    """

    def __init__(self):
        super().__init__(amc_name="PGIM India Mutual Fund", version="v1")
        # Direct column mapping based on observed headers
        self.column_mapping = {
            "NAME OF INSTRUMENT": "company_name",
            "NAME OF  INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY": "sector",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE": "market_value_inr",
            "MARKET/ FAIR VALUE": "market_value_inr",
            "INR LACS": "market_value_inr",
            "% TO NET ASSETS": "percent_of_nav",
            "%  TO NET ASSETS": "percent_of_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting from PGIM India Mutual Fund: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        # Sheet patterns to skip
        summary_keywords = ["SUMMARY", "INDEX", "CONTROL"]

        for sheet_name in xls.sheet_names:
            if any(k in sheet_name.upper() for k in summary_keywords):
                continue
            
            try:
                # Read enough rows to detect header and scheme name
                # Increased nrows to 500 to catch bottom totals
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=500)
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
                
                # Extract Total Net Assets (AUM) from the sheet's footer using df_raw
                raw_net_assets = None
                for idx, row in df_raw.iterrows():
                    row_vals = [str(val).upper() if pd.notna(val) else "" for val in row.values]
                    row_text = " ".join(row_vals)
                    if "GRAND TOTAL" in row_text or "NET ASSETS" in row_text or "TOTAL AUM" in row_text:
                        candidates = []
                        for val in row.values:
                            f_val = self.safe_float(val)
                            if f_val is not None and f_val > 105: # Avoid 100.00%
                                candidates.append(f_val)
                        
                        if candidates:
                            # Usually the one near 100% or the largest one
                            if len(candidates) > 1:
                                 if abs(candidates[-1] - 100.0) < 0.05:
                                     raw_net_assets = candidates[-2]
                                 else:
                                     raw_net_assets = candidates[-1]
                            else:
                                raw_net_assets = candidates[0]
                            
                        if raw_net_assets:
                            break
                            
                normalized_net_assets = None
                if raw_net_assets:
                    normalized_net_assets = self.normalize_currency(raw_net_assets, "INR LACS")

                # 3. Process Data
                df_data = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
                
                # Use inherited _map_columns for robustness
                df_data = self._map_columns(df_data)
                
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
                            # Convert INR Lacs to Rupees
                            "market_value_inr": self.normalize_currency(row.get('market_value_inr', 0), "INR LACS"),
                            # PGIM values are already percentages (e.g. 7.73)
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
                    holding = {
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
                    sheet_holdings.append(holding)

                # Validation
                self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
                all_holdings.extend(sheet_holdings)

            except Exception as sheet_err:
                logger.error(f"Error processing sheet {sheet_name}: {sheet_err}")
                continue

        logger.info(f"Total holdings extracted for PGIM India: {len(all_holdings)}")
        return all_holdings
