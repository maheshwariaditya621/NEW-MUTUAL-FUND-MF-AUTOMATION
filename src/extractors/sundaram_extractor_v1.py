from typing import Dict, Any, List
import pandas as pd
from src.config import logger
from src.extractors.base_extractor import BaseExtractor

class SundaramExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Sundaram Mutual Fund.
    File structure (observed from CONSOLIDATED_SUNDARAM_2025_12.xlsx):
    - Row 1, Col 0: Scheme Name
    - Row 3: Column Headers
    - Market Value: In Rs. in Lacs
    - NAV %: In decimal format (e.g. 0.092233 -> 9.22%)
    """

    def __init__(self):
        super().__init__(amc_name="Sundaram Mutual Fund", version="v1")
        # Direct column mapping based on observed headers
        self.column_mapping = {
            "NAME OF THE INSTRUMENT": "company_name",
            "NAME OF INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY": "sector",
            "RATING": "sector",
            "QUANTITY": "quantity",
            "MKT VALUE": "market_value_inr",
            "MARKET VALUE": "market_value_inr",
            "LACS": "market_value_inr",
            "% OF NET ASSET": "percent_of_nav"
        }

    def _extract_total_aum(self, df: pd.DataFrame, unit: str = "INR LACS") -> float:
        """Find Grand Total or Net Assets row and extract value."""
        # Scan from bottom up
        for i in range(len(df)-1, -1, -1):
            row = df.iloc[i]
            row_vals = [str(val).upper() if pd.notna(val) else "" for val in row.values]
            row_text = " ".join(row_vals)
            
            is_valid_marker = False
            if "GRAND TOTAL" in row_text:
                is_valid_marker = True
            elif "NET ASSETS" in row_text and "PER UNIT" not in row_text and "PERCENT" not in row_text:
                is_valid_marker = True
                
            if is_valid_marker:
                candidates = []
                for val in row:
                    f_val = self.safe_float(val)
                    # Filter out percentages (like 1.0 or 100.0)
                    if f_val is not None and f_val > 105: # Value for Lacs usually > 100
                        candidates.append(f_val)
                
                if candidates:
                    # Usually the largest value in the row is the AUM (if multiple numbers exist)
                    return self.normalize_currency(max(candidates), unit)
        return 0.0

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting from Sundaram Mutual Fund: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            u_sheet = sheet_name.upper()
            if any(k in u_sheet for k in ["SUMMARY", "CONTROL", "DATA", "NAV"]):
                continue
            if u_sheet.endswith(" INDEX") or u_sheet == "INDEX":
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

                # 2. Extract Scheme Name (Expected at Row 1, Col 0)
                raw_scheme_name = "N/A"
                if len(df_raw) > 1:
                    raw_scheme_name = str(df_raw.iloc[1, 0]).strip()
                
                # Fallback / Scan if Row 1 is empty
                if not raw_scheme_name or raw_scheme_name.lower() == "nan":
                    for i in range(1, 6):
                        if i >= len(df_raw): break
                        potential = str(df_raw.iloc[i, 0]).strip()
                        if potential and potential.lower() != "nan" and len(potential) > 5 and "MUTUAL FUND" not in potential.upper():
                            raw_scheme_name = potential
                            break

                if raw_scheme_name == "N/A":
                    raw_scheme_name = sheet_name

                scheme_info = self.parse_verbose_scheme_name(raw_scheme_name)
                logger.info(f"Processing sheet: {sheet_name} -> {scheme_info['scheme_name']}")
                
                # 3. Process Data
                header_row = df_raw.iloc[header_idx]
                headers = [str(h).replace('\n', ' ').strip().upper() for h in header_row]
                
                df_data = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx + 1, header=None)
                
                # Handle column mismatch by slicing to common length
                min_cols = min(len(headers), len(df_data.columns))
                df_data = df_data.iloc[:, :min_cols]
                df_data.columns = headers[:min_cols]

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

                # Fetch Total AUM from the FULL dataframe (scanning bottom-up)
                df_full = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                normalized_net_assets = self._extract_total_aum(df_full)
                if normalized_net_assets == 0:
                    normalized_net_assets = None

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
                            # Convert Lakhs (Lacs) to Rupees
                            "market_value_inr": self.normalize_currency(row.get('market_value_inr', 0), "INR LACS"),
                            # Sundaram values are often decimals (e.g. 0.0922 -> 9.22%)
                            "percent_of_nav": self.parse_percentage(row.get('percent_of_nav', 0)),
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

        logger.info(f"Total holdings extracted for Sundaram: {len(all_holdings)}")
        return all_holdings
