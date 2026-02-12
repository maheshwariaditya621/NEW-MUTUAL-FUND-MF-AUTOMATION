import pandas as pd
import re
from typing import Dict, Any, List
from src.extractors.base_extractor import BaseExtractor
from src.config import logger
from src.config.constants import AMC_ICICI

class ICICIExtractorV1(BaseExtractor):
    """
    ICICI Prudential Mutual Fund Extractor Implementation (Version 1).
    Handles merged Excel files with advanced safeguards.
    """

    def __init__(self):
        # Use string if constant not yet defined to avoid import errors
        super().__init__(amc_name="ICICI Prudential Mutual Fund", version="v1")
        
        self.column_mapping = {
            "COMPANY/ISSUER": "company_name",
            "INSTRUMENT NAME": "company_name",
            "NAME OF THE INSTRUMENT": "company_name",
            "NAME OF THE ISSUER": "company_name",
            "ISSUER NAME": "company_name",
            "COMPANY NAME": "company_name",
            "ISIN": "isin",
            "QUANTITY": "quantity",
            "EXPOSURE": "market_value_inr",
            "MARKET VALUE": "market_value_inr",
            "MARKET/FAIR VALUE": "market_value_inr",
            "% TO NAV": "percent_to_nav",
            "INDUSTRY": "sector",
            "RATING": "sector"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting ICICI data from: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        all_holdings = []

        for sheet_name in xls.sheet_names:
            logger.debug(f"Processing ICICI sheet: {sheet_name}")
            
            # 1. Read first 30 rows raw for header and metadata
            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=100)
            if df_raw.empty:
                continue

            # 2. Extract Scheme Metadata (Internal Name)
            scheme_metadata = self.extract_scheme_metadata(df_raw, sheet_name)
            
            # 3. Find Header
            header_idx = self.find_header_row(df_raw)
            if header_idx == -1:
                logger.debug(f"Header not found in sheet: {sheet_name}. Skipping.")
                continue

            # 4. Re-read/Slice data
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            
            # 5. Map Columns
            df = self._map_columns(df)
            
            if "isin" not in df.columns:
                logger.warning(f"ISIN column missing in {sheet_name} after mapping.")
                continue

            # 6. Unit Detection (Data-Driven)
            if "market_value_inr" in df.columns:
                value_unit = self.detect_currency_unit(df["market_value_inr"])
                logger.info(f"[{scheme_metadata['scheme_name']}] Detected Units: {value_unit}")
            else:
                value_unit = "RUPEES"

            # 7. Filter Equity (Includes Multi-Table Detection & ISIN Cleaning)
            equity_df = self.filter_equity_isins(df, "isin")
            
            # 8. Build Records
            sheet_holdings = []
            for _, row in equity_df.iterrows():
                # Note: clean_company_name is called here
                company_name = self.clean_company_name(row.get("company_name"))
                
                # percent_to_nav use parse_percentage
                raw_pct = row.get("percent_to_nav", 0)
                percent_to_nav = self.parse_percentage(raw_pct)
                
                holding = {
                    "amc_name": self.amc_name,
                    "scheme_name": scheme_metadata["scheme_name"],
                    "scheme_description": scheme_metadata["description"],
                    "plan_type": scheme_metadata["plan_type"],
                    "option_type": scheme_metadata["option_type"],
                    "is_reinvest": scheme_metadata["is_reinvest"],
                    "isin": row.get("isin"),
                    "company_name": company_name,
                    "quantity": self.safe_float(row.get("quantity", 0)),
                    "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), value_unit),
                    "percent_to_nav": percent_to_nav,
                    "sector": row.get("sector", None)
                }
                sheet_holdings.append(holding)

            # 9. Post-Extraction Validation (4 Golden Rules)
            is_valid = self.validate_nav_completeness(sheet_holdings, scheme_metadata["scheme_name"])
            if is_valid:
                all_holdings.extend(sheet_holdings)
            else:
                logger.warning(f"Sheet {sheet_name} failed validation. Dropping current scheme data.")

        return all_holdings

    def extract_scheme_metadata(self, df_raw: pd.DataFrame, sheet_name: str) -> Dict[str, Any]:
        """
        Extracts scheme name, plan, and option from the first 5 rows of the sheet.
        Falls back to sheet name if not found.
        """
        internal_name = sheet_name
        amc_upper = self.amc_name.upper()
        
        # Search for a cell containing "Fund" or "Series" or "Plan"
        for i in range(min(5, len(df_raw))):
            row_vals = [str(v).strip() for v in df_raw.iloc[i].values if not pd.isna(v) and len(str(v).strip()) > 10]
            for val in row_vals:
                val_upper = val.upper()
                # If it's the exact AMC name, skip it
                if val_upper == amc_upper or val_upper == (amc_upper + " MUTUAL FUND"):
                    continue
                # If it contains FUND or GROWTH or DIRECT or REGULAR, it's likely a scheme name
                if any(kw in val_upper for kw in ["FUND", "SERIES", "PLAN", "EQUITY", "TAX", "BLUECHIP"]):
                    internal_name = val
                    break
            else: continue
            break

        # Clean "ICICI Prudential" prefix if duplicated
        internal_name = internal_name.replace("ICICI Prudential Mutual Fund - ", "")
        internal_name = internal_name.replace("ICICI Prudential ", "")
        
        # Strip trailing sheet codes (e.g., "ICIPRU BLUCH")
        internal_name = re.sub(r'\s[A-Z0-9]{5,}$', '', internal_name).strip()

        return self.parse_verbose_scheme_name(internal_name)

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Strict mapping for ICICI to avoid collisions with 'Yield of the instrument'.
        """
        new_cols = {}
        for col in df.columns:
            col_upper = str(col).upper()
            
            # Explicitly skip columns that are clearly not what we want
            if "YIELD" in col_upper:
                continue
                
            for pattern, canonical in self.column_mapping.items():
                if pattern in col_upper:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)
