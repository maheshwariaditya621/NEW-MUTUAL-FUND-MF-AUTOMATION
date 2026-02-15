import pandas as pd
import re
from typing import Dict, Any, List
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

from src.config.constants import AMC_AXIS

class AxisExtractorV1(BaseExtractor):
    """
    Axis Mutual Fund Extractor Implementation (Version 1).
    Handles merged Excel files where each sheet is a scheme.
    """

    def __init__(self):
        super().__init__(amc_name=AMC_AXIS, version="v1")
        # Mapping of raw column substrings to canonical fields
        self.column_mapping = {
            "NAME OF THE INSTRUMENT": "company_name",
            "INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY": "sector",
            "RATING": "sector",  # For debt instruments
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE": "market_value_inr",
            "MARKET VALUE": "market_value_inr",
            "FAIR VALUE": "market_value_inr",
            "% TO NET ASSETS": "percent_of_nav",
            "% TO NAV": "percent_of_nav",
            "NAV": "percent_of_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract data from all sheets in the Axis merged Excel file.
        """
        logger.info(f"Extracting data from Axis file: {file_path}")
        
        # Use openpyxl engine for compatibility with merged excels
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        all_holdings = []

        for sheet_name in xls.sheet_names:
            logger.debug(f"Processing sheet: {sheet_name}")
            
            # Skip metadata sheets
            if self._should_skip_sheet(sheet_name):
                logger.debug(f"Skipping metadata sheet: {sheet_name}")
                continue
            
            # Read with header=None to find the real header row reliably
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            
            if df.empty:
                logger.warning(f"Sheet {sheet_name} is empty. Skipping.")
                continue

            # Find the header row index
            header_idx = self.find_header_row(df)
            if header_idx == -1:
                logger.debug(f"Header row not found in sheet: {sheet_name}. Likely not a portfolio sheet. Skipping.")
                continue

            # Re-read data with proper header
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            
            # Normalize column names
            raw_columns = df.columns.tolist()
            
            # Scan for global units before mapping
            global_unit = self.scan_sheet_for_global_units(df)
            
            df = self._map_columns(df)
            logger.debug(f"Mapped columns for {sheet_name}: {df.columns.tolist()}")
            
            # Find the unit for 'market_value_inr'
            # Priority: 1. Column Header | 2. Global Metadata | 3. Default (LAKHS for Axis)
            value_unit = global_unit if global_unit != "RUPEES" else "LAKHS"
            
            for col in raw_columns:
                # Check if this raw column was mapped to market_value_inr
                col_upper = str(col).upper()
                is_mv_col = False
                for pattern, canonical in self.column_mapping.items():
                    if pattern in col_upper and canonical == "market_value_inr":
                        is_mv_col = True
                        break
                
                if is_mv_col:
                    header_unit = self.detect_units(col)
                    logger.debug(f"Column '{col}' mapped to MV. Detected Unit: {header_unit}")
                    if header_unit != "RUPEES":
                        value_unit = header_unit
                        logger.info(f"Unit override from header: {value_unit} for {sheet_name}")
                        break
            
            logger.debug(f"Final Value Unit for {sheet_name}: {value_unit}")

            # Filter for Equity (Triple Filter)
            if "isin" not in df.columns:
                logger.debug(f"ISIN column missing in sheet {sheet_name} after mapping. Skipping.")
                continue
                
            equity_df = self.filter_equity_isins(df, "isin")
            
            if equity_df.empty:
                logger.debug(f"No equity holdings found in sheet {sheet_name}. Skipping.")
                continue
            
            # Extract Scheme Name
            scheme_name = self._extract_scheme_name(xls, sheet_name)
            
            # Parse Scheme Info
            # Special handling for Retirement Fund sub-plans
            if "Retirement Fund" in scheme_name:
                # Keep the full name including sub-plan (Aggressive, Conservative, Dynamic)
                # Example: "Axis Retirement Fund - Aggressive Plan" should stay as is
                scheme_info = {
                    "scheme_name": scheme_name,  # Keep full name
                    "description": "",
                    "plan_type": "Regular",
                    "option_type": "Growth",
                    "is_reinvest": False
                }
            else:
                # Use base parser for other schemes
                scheme_info = self.parse_verbose_scheme_name(scheme_name)
            
            # Build final records
            for _, row in equity_df.iterrows():
                holding = {
                    "amc_name": self.amc_name,
                    "scheme_name": scheme_info["scheme_name"],
                    "scheme_description": scheme_info["description"],
                    "plan_type": scheme_info["plan_type"],
                    "option_type": scheme_info["option_type"],
                    "is_reinvest": scheme_info["is_reinvest"],
                    "isin": row.get("isin"),
                    "company_name": self.clean_company_name(row.get("company_name")),
                    "quantity": int(self.normalize_currency(row.get("quantity", 0), "RUPEES")),
                    "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), value_unit),
                    "percent_of_nav": self.parse_percentage(row.get("percent_of_nav", 0)),
                    "sector": self.clean_company_name(row.get("sector", "N/A"))
                }
                all_holdings.append(holding)

        # Final sanity check for the entire file
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
        """Check if sheet should be skipped (metadata/summary sheets)."""
        skip_keywords = ["index", "summary", "disclaimer", "contents"]
        sheet_lower = sheet_name.lower()
        return any(kw in sheet_lower for kw in skip_keywords)

    def _extract_scheme_name(self, xls: pd.ExcelFile, sheet_name: str) -> str:
        """
        Extract scheme name from row 0, column 1 of the sheet.
        Falls back to sheet name if extraction fails.
        """
        try:
            # Read first row only
            df_header = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=1)
            
            # Scheme name is in row 0, column 1 (0-indexed)
            if len(df_header.columns) > 1:
                cell_value = df_header.iloc[0, 1]
                if pd.notna(cell_value):
                    cell_str = str(cell_value).strip()
                    # Validate it looks like a scheme name (not empty, reasonable length)
                    if len(cell_str) > 5:
                        logger.debug(f"Extracted scheme name from row 0, col 1: {cell_str}")
                        return self._clean_scheme_name(cell_str)
            
            # Fallback to sheet name
            logger.warning(f"Could not extract scheme name from row 0, col 1. Using sheet name: {sheet_name}")
            return self._clean_sheet_name(sheet_name)
            
        except Exception as e:
            logger.warning(f"Error extracting scheme name: {e}. Using sheet name.")
            return self._clean_sheet_name(sheet_name)

    def _clean_scheme_name(self, name: str) -> str:
        """Clean extracted scheme name."""
        # Remove "Axis Mutual Fund" prefix if present
        name = re.sub(r'^Axis\s+Mutual\s+Fund\s*[-:]?\s*', '', name, flags=re.IGNORECASE)
        
        # Encoding/Mojibake Fixes
        name = name.replace("â€™", "'").replace("’", "'").replace("‘", "'")
        name = name.replace("â€“", "-").replace("–", "-") # En-dash fixes
        
        # Remove extra whitespace
        name = re.sub(r'\s+', ' ', name).strip()
        return name

    def _clean_sheet_name(self, sheet_name: str) -> str:
        """
        Clean sheet name to extract scheme name.
        Example: "%20Portfolio %% AXISBCF" -> "Axis Bluechip Fund"
        """
        # Remove URL encoding and special characters
        name = sheet_name.replace("%20", " ").replace("%%", "").strip()
        # Remove "Portfolio" keyword
        name = re.sub(r'\bPortfolio\b', '', name, flags=re.IGNORECASE).strip()
        # Remove Axis code suffix (e.g., AXISBCF, AXIS112)
        name = re.sub(r'\s*AXIS[A-Z0-9]+\s*$', '', name, flags=re.IGNORECASE).strip()
        
        # If name is empty or too short, use original
        if len(name) < 3:
            name = sheet_name
        
        return name

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Maps raw columns to canonical names using fuzzy matching."""
        new_cols = {}
        for col in df.columns:
            # Normalize column name: remove newlines, extra whitespace
            col_normalized = str(col).replace('\n', ' ').replace('\r', ' ')
            col_normalized = ' '.join(col_normalized.split())  # Remove extra whitespace
            col_upper = col_normalized.upper()
            
            for pattern, canonical in self.column_mapping.items():
                if pattern in col_upper:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)
