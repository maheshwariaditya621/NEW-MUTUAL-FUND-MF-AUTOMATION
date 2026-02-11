import pandas as pd
from typing import Dict, Any, List
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class HDFCExtractorV1(BaseExtractor):
    """
    HDFC Mutual Fund Extractor Implementation (Version 1).
    Handles merged Excel files where each sheet is a scheme.
    """

    def __init__(self):
        super().__init__(amc_name="HDFC Mutual Fund", version="v1")
        # Mapping of raw column substrings to canonical fields
        self.column_mapping = {
            "ISIN": "isin",
            "COMPANY": "company_name",
            "INSTRUMENT": "company_name",
            "ISSUER": "company_name",
            "QUANTITY": "quantity",
            "UNITS": "quantity",
            "MARKET VALUE": "market_value_inr",
            "FAIR VALUE": "market_value_inr",
            "MARKET/ FAIR VALUE": "market_value_inr",
            "VALUE": "market_value_inr",
            "NAV": "percent_to_nav",
            "INDUSTRY": "sector",
            "SECTOR": "sector"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extracted data from all sheets in the HDFC merged Excel file.
        """
        logger.info(f"Extracting data from HDFC file: {file_path}")
        
        # We use openpyxl engine for compatibility with merged excels
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        all_holdings = []

        for sheet_name in xls.sheet_names:
            logger.debug(f"Processing sheet: {sheet_name}")
            # 1. Read with header=None to find the real header row reliably
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            
            if df.empty:
                logger.warning(f"Sheet {sheet_name} is empty. Skipping.")
                continue

            # Find the header row index
            header_idx = self.find_header_row(df)
            if header_idx == -1:
                logger.debug(f"Header row not found in sheet: {sheet_name}. Likely not a portfolio sheet. Skipping.")
                continue

            # 2. Re-read data with proper header - we skip header_idx rows and use the next one as header
            # Actually, pd.read_excel(..., skiprows=N) skips N rows and uses row N as header.
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            
            # 3. Normalize column names
            raw_columns = df.columns.tolist()
            # Scan for global units before mapping (since mapping might lose header info)
            global_unit = self.scan_sheet_for_global_units(df)
            
            df = self._map_columns(df)
            logger.debug(f"Mapped columns for {sheet_name}: {df.columns.tolist()}")
            
            # Find the unit for 'market_value_inr'
            # Priority: 1. Column Header | 2. Global Metadata | 3. Default (RUPEES)
            value_unit = global_unit
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

            # 4. Filter for Equity (Triple Filter)
            if "isin" not in df.columns:
                logger.debug(f"ISIN column missing in sheet {sheet_name} after mapping. Skipping.")
                continue
                
            equity_df = self.filter_equity_isins(df, "isin")
            
            # 5. Parse Scheme Info
            import re
            # Clean HDFC Suffix (e.g., "Arbitrage HDFCAR" -> "Arbitrage")
            clean_sheet_name = re.sub(r'\sHDFC[A-Z0-9]+$', '', sheet_name).strip()
            # Also handle cases like "Index HDFCNY" -> "Index"
            clean_sheet_name = re.sub(r'\sHD[A-Z0-9]+$', '', clean_sheet_name).strip()
            
            scheme_info = self.parse_verbose_scheme_name(clean_sheet_name)
            
            # 6. Build final records
            for _, row in equity_df.iterrows():
                holding = {
                    "amc_name": self.amc_name,
                    "scheme_name": scheme_info["scheme_name"],
                    "scheme_description": scheme_info["description"],
                    "plan_type": scheme_info["plan_type"],
                    "option_type": scheme_info["option_type"],
                    "is_reinvest": scheme_info["is_reinvest"],
                    "isin": row.get("isin"),
                    "company_name": row.get("company_name"),
                    "quantity": int(self.normalize_currency(row.get("quantity", 0), "RUPEES")),
                    "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), value_unit),
                    "percent_to_nav": self.safe_float(row.get("percent_to_nav", 0)),
                    "sector": row.get("sector", None)
                }
                all_holdings.append(holding)

        # Final sanity check for the entire sheet/scheme
        self.validate_nav_completeness(all_holdings) 

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Maps raw columns to canonical names using fuzzy matching."""
        new_cols = {}
        for col in df.columns:
            col_upper = str(col).upper()
            for pattern, canonical in self.column_mapping.items():
                if pattern in col_upper:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)
