import pandas as pd
import re
from typing import Dict, Any, List
from src.extractors.base_extractor import BaseExtractor
from src.config import logger
from src.config.constants import AMC_BAJAJ

class BajajExtractorV1(BaseExtractor):
    """
    Bajaj Mutual Fund Extractor Implementation (Version 1).
    Handles merged Excel files where each sheet is a scheme.
    """

    def __init__(self):
        super().__init__(amc_name=AMC_BAJAJ, version="v1")
        # Mapping of raw column substrings to canonical fields
        self.column_mapping = {
            "NAME OF ISSUER/INSTRUMENT": "company_name",
            "NAME OF THE INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY/RATING": "sector",
            "INDUSTRY": "sector",
            "RATING": "sector",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE": "market_value_inr",
            "MARKET VALUE": "market_value_inr",
            "% TO NET ASSETS": "percent_of_nav",
            "% TO NAV": "percent_of_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract data from all sheets in the Bajaj merged Excel file.
        """
        logger.info(f"Extracting data from Bajaj file: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        all_holdings = []

        for sheet_name in xls.sheet_names:
            logger.debug(f"Processing sheet: {sheet_name}")
            
            if self._should_skip_sheet(sheet_name):
                logger.debug(f"Skipping metadata sheet: {sheet_name}")
                continue
            
            # Read first 100 rows to find header and scheme name
            df_full = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            
            if df_full.empty:
                logger.warning(f"Sheet {sheet_name} is empty. Skipping.")
                continue

            # Find the header row index (usually Row 3)
            header_idx = self.find_header_row(df_full)
            if header_idx == -1:
                logger.debug(f"Header row not found in sheet: {sheet_name}. Skipping.")
                continue

            # Extract Scheme Name from Row 0
            scheme_name = self._extract_scheme_name(df_full, sheet_name)
            scheme_info = self.parse_verbose_scheme_name(scheme_name)
            
            # Process rows after header
            # Bajaj uses section headers like "Equity", "Debt Instruments", "Cash & Cash Equivalents"
            current_instrument_type = "Unknown"
            
            # Re-read with header=header_idx to get column names for mapping
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            df = self._map_columns(df)
            
            # Find the value unit (usually LAKHS for Bajaj)
            # Scan top rows for unit mentions if possible, else default to LAKHS
            value_unit = self.scan_sheet_for_global_units(df_full)
            if value_unit == "RUPEES": # BaseExtractor returns RUPEES as default if not found
                value_unit = "LAKHS"
                
            # Manual override if header specifically mentions Lakhs
            for col in df.columns:
                if col == "market_value_inr":
                    # Find the original raw column name
                    raw_col = self._get_raw_column_name(df_full, header_idx, df.columns.get_loc(col))
                    header_unit = self.detect_units(raw_col)
                    if header_unit != "RUPEES":
                        value_unit = header_unit
                        break

            logger.debug(f"Final Value Unit for {sheet_name}: {value_unit}")

            # Extract Total Net Assets (AUM) from the sheet's footer using df_full
            raw_net_assets = None
            for idx, row in df_full.iterrows():
                row_vals = [str(val).upper() if pd.notna(val) else "" for val in row.values]
                row_text = " ".join(row_vals)
                if "GRAND TOTAL" in row_text or "NET ASSETS" in row_text or "TOTAL AUM" in row_text:
                    candidates = []
                    for val in row.values:
                        f_val = self.safe_float(val)
                        if f_val is not None and f_val > 105: # Avoid 100.00%
                            candidates.append(f_val)
                    
                    if candidates:
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
                normalized_net_assets = self.normalize_currency(raw_net_assets, value_unit)

            # Iterate through rows and track section headers
            sheet_holdings = []
            for idx, row in df.iterrows():
                # Check if this is a section header (usually only first column has text, others are nan)
                row_list = row.tolist()
                non_nan_indices = [i for i, v in enumerate(row_list) if pd.notna(v) and str(v).strip() != '']
                
                # If only one column has data and it's a string, likely a section header or subtotal
                if len(non_nan_indices) == 1:
                    cell_val = str(row_list[non_nan_indices[0]]).strip()
                    # Check for section keywords
                    if any(kw in cell_val.upper() for kw in ["EQUITY", "DEBT", "CASH", "TREPS", "MONEY MARKET"]):
                        current_instrument_type = cell_val
                        logger.debug(f"Section detected: {current_instrument_type}")
                    continue
                
                # Stop if we hit Total Net Assets or Grand Total
                security_name = str(row.get("company_name", "")).strip()
                if "TOTAL" in security_name.upper() or "GRAND TOTAL" in security_name.upper():
                    # Check if it has a high % to NAV to be sure it's the final total
                    nav_pct = self.parse_percentage(row.get("percent_of_nav", 0))
                    if nav_pct > 95:
                        break
                    continue

                # Filter for Equity (ISIN-based)
                isin = str(row.get("isin", "")).strip()
                if self.is_valid_equity_isin(isin):
                    # It's an equity holding
                    holding = {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info["scheme_name"],
                        "scheme_description": scheme_info["description"],
                        "plan_type": scheme_info["plan_type"],
                        "option_type": scheme_info["option_type"],
                        "is_reinvest": scheme_info["is_reinvest"],
                        "isin": isin,
                        "company_name": security_name,
                        "quantity": int(self.normalize_currency(row.get("quantity", 0), "RUPEES")),
                        "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), value_unit),
                        "percent_of_nav": self.parse_percentage(row.get("percent_of_nav", 0)),
                        "sector": row.get("sector", None),
                        "total_net_assets": normalized_net_assets
                    }
                    sheet_holdings.append(holding)

            if not sheet_holdings and normalized_net_assets:
                # Ghost Holding for Non-Equity funds
                all_holdings.append({
                    "amc_name": self.amc_name,
                    "scheme_name": scheme_info["scheme_name"],
                    "scheme_description": scheme_info["description"],
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
                })
            else:
                all_holdings.extend(sheet_holdings)

        # Final sanity check / validation
        if all_holdings:
            # Group by scheme for validation
            from collections import defaultdict
            by_scheme = defaultdict(list)
            for h in all_holdings:
                scheme_key = f"{h['scheme_name']} - {h['plan_type']} - {h['option_type']}"
                by_scheme[scheme_key].append(h)
            
            # Validate each scheme
            for scheme_key, holdings in by_scheme.items():
                self.validate_nav_completeness(holdings, scheme_key)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings

    def _should_skip_sheet(self, sheet_name: str) -> bool:
        """Check if sheet should be skipped."""
        skip_keywords = ["index", "summary", "disclaimer", "contents", "glossary"]
        sheet_lower = sheet_name.lower()
        return any(kw in sheet_lower for kw in skip_keywords)

    def _extract_scheme_name(self, df_full: pd.DataFrame, sheet_name: str) -> str:
        """Extract scheme name from Row 0, Col 1 or fallback."""
        try:
            # Row 0, Col 1 is usually the second element in the first row
            if len(df_full.columns) > 1:
                cell_val = df_full.iloc[0, 1]
                if pd.notna(cell_val):
                    return str(cell_val).strip()
            
            # Fallback to Col 0 if Col 1 is nan
            cell_val = df_full.iloc[0, 0]
            if pd.notna(cell_val) and len(str(cell_val)) > 5:
                return str(cell_val).strip()

            return sheet_name
        except:
            return sheet_name

    def _get_raw_column_name(self, df_full: pd.DataFrame, header_idx: int, col_idx: int) -> str:
        """Get the original raw column name before mapping."""
        try:
            return str(df_full.iloc[header_idx, col_idx])
        except:
            return ""

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Maps raw columns to canonical names."""
        new_cols = {}
        for col in df.columns:
            col_upper = str(col).replace('\n', ' ').upper()
            col_upper = ' '.join(col_upper.split())
            
            for pattern, canonical in self.column_mapping.items():
                if pattern in col_upper:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)
