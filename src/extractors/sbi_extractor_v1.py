import pandas as pd
from typing import Dict, Any, List
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class SBIExtractorV1(BaseExtractor):
    """
    SBI Mutual Fund Extractor Implementation (Version 1).
    Proves modularity without touching core code.
    """

    def __init__(self):
        super().__init__(amc_name="SBI Mutual Fund", version="v1")
        # SBI might use different names (hypothetical based on common patterns)
        self.column_mapping = {
            "ISIN": "isin",
            "NAME OF THE INSTRUMENT": "company_name",
            "NAME OF THE ISSUER": "company_name",
            "QUANTITY": "quantity",
            "MARKET VALUE": "market_value_inr",
            "PERCENTAGE TO NAV": "percent_to_nav",
            "INDUSTRY": "sector"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extracted data from all sheets in the SBI merged Excel file.
        """
        logger.info(f"Extracting data from SBI file: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        all_holdings = []

        for sheet_name in xls.sheet_names:
            logger.debug(f"Processing sheet: {sheet_name}")
            # Read with header=None to correctly identify the absolute header row index
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            
            if df.empty:
                continue

            # SBI might have different keywords or header depth
            header_idx = self.find_header_row(df, keywords=["ISIN", "INSTRUMENT"])
            if header_idx == -1:
                logger.error(f"Header row not found in SBI sheet: {sheet_name}")
                continue

            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            
            # Detect currency units
            # Priority: 1. Column Header | 2. Global Metadata | 3. Default (RUPEES)
            global_unit = self.scan_sheet_for_global_units(df)
            value_unit = global_unit
            
            for col in df.columns:
                if "VALUE" in str(col).upper():
                    header_unit = self.detect_units(col)
                    if header_unit != "RUPEES":
                        value_unit = header_unit
                        break

            df = self._map_columns(df)
            
            if "isin" not in df.columns:
                continue
                
            equity_df = self.filter_equity_isins(df, "isin")
            
            # Extract Scheme Name from Cell C3 (Row 2, Col 2 0-indexed) if available
            extracted_name = sheet_name
            try:
                # Re-read header part to get the name (df might be sliced or processed)
                # We can't rely on 'df' here because it skipped rows. 
                # But we can read just the top rows cheaply.
                header_df = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=5)
                # Check C3 (Row 2, Col 2)
                candidate = str(header_df.iloc[2, 2])
                if "SCHEME NAME" in candidate.upper():
                    # Case 1: Merged cell "SCHEME NAME : SBI Fund"
                    parts = candidate.split(":", 1)
                    if len(parts) > 1 and parts[1].strip():
                        extracted_name = parts[1].strip()
                    else:
                        # Case 2: Split cells C3="SCHEME NAME :", D3="SBI Fund"
                        # Check D3 (Row 2, Col 3)
                        extracted_name = str(header_df.iloc[2, 3]).strip()
                        
                    logger.debug(f"Found internal scheme name: {extracted_name}")
            except Exception as e:
                logger.debug(f"Could not extract internal scheme name: {e}")

            scheme_info = self.parse_verbose_scheme_name(extracted_name)
            
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
                    "percent_to_nav": float(row.get("percent_to_nav", 0)),
                    "sector": row.get("sector", None)
                }
                all_holdings.append(holding)

        return all_holdings

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        new_cols = {}
        for col in df.columns:
            col_upper = str(col).upper()
            for pattern, canonical in self.column_mapping.items():
                if pattern in col_upper:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)
