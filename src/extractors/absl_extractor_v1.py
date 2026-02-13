import pandas as pd
import re
from typing import Dict, Any, List
from src.extractors.base_extractor import BaseExtractor
from src.config import logger
from src.config.constants import AMC_ABSL

class ABSLExtractorV1(BaseExtractor):
    """
    ABSL Mutual Fund Extractor Implementation (Version 1).
    Handles merged Excel files where each sheet is a scheme.
    """

    def __init__(self):
        super().__init__(amc_name=AMC_ABSL, version="v1")
        # Mapping of raw column substrings to canonical fields
        self.column_mapping = {
            "NAME OF THE INSTRUMENT": "company_name",
            "NAME OF INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY^ / RATING": "sector",
            "INDUSTRY": "sector",
            "RATING": "sector",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE": "market_value_inr",
            "MARKET VALUE": "market_value_inr",
            "LACS": "market_value_inr",
            "% TO NET ASSETS": "percent_of_nav",
            "% TO NAV": "percent_of_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract data from all sheets in the ABSL merged Excel file.
        """
        logger.info(f"Extracting data from ABSL file: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        all_holdings = []

        for sheet_name in xls.sheet_names:
            logger.debug(f"Processing sheet: {sheet_name}")
            
            if self._should_skip_sheet(sheet_name):
                logger.debug(f"Skipping metadata sheet: {sheet_name}")
                continue
            
            # Read first 100 rows to find header and scheme name
            df_full = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=100)
            
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
            
            # Parse Scheme Info
            # Special handling for Retirement Fund sub-plans to prevent name collisions
            if "RETIREMENT FUND" in scheme_name.upper():
                scheme_info = {
                    "scheme_name": scheme_name, # Keep full name with sub-plan
                    "description": "",
                    "plan_type": "Regular",
                    "option_type": "Growth",
                    "is_reinvest": False
                }
            else:
                scheme_info = self.parse_verbose_scheme_name(scheme_name)
            
            # Re-read with header=header_idx to get column names for mapping
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            df = self._map_columns(df)
            
            # Find the value unit
            # Priority: 1. Column Header | 2. Global Metadata | 3. Default (LAKHS for ABSL)
            value_unit = self.scan_sheet_for_global_units(df_full)
            if value_unit == "RUPEES":
                value_unit = "LAKHS"
                
            # Manual override if header specifically mentions Lacs/Lakhs
            for col in df.columns:
                if col == "market_value_inr":
                    # Get raw column name
                    raw_col = self._get_raw_column_name(df_full, header_idx, df.columns.get_loc(col))
                    header_unit = self.detect_units(raw_col)
                    if header_unit != "RUPEES":
                        value_unit = header_unit
                        break

            logger.debug(f"Final Value Unit for {sheet_name}: {value_unit}")

            # Filter for Equity (ISIN-based)
            if "isin" not in df.columns:
                logger.debug(f"ISIN column missing in sheet {sheet_name} after mapping. Skipping.")
                continue
                
            equity_df = self.filter_equity_isins(df, "isin")
            
            if equity_df.empty:
                logger.debug(f"No equity holdings found in sheet {sheet_name}. Skipping.")
                continue
            
            # Build final records
            for _, row in equity_df.iterrows():
                security_name = str(row.get("company_name", "")).strip()
                if not security_name or "TOTAL" in security_name.upper():
                    continue

                isin = str(row.get("isin", "")).strip()
                
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
                    "sector": row.get("sector", None)
                }
                all_holdings.append(holding)

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
            if len(df_full.columns) > 1:
                cell_val = df_full.iloc[0, 1]
                if pd.notna(cell_val):
                    name = str(cell_val).strip()
                    # Clean up "SEBI " prefix if present in sheet name but not in Row 0
                    return name
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
            col_upper = str(col).replace('\n', ' ').replace('_x000D_', ' ').upper()
            col_upper = ' '.join(col_upper.split())
            
            for pattern, canonical in self.column_mapping.items():
                if pattern in col_upper:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)
