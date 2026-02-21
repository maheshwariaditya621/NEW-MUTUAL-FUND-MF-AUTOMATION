# src/extractors/abakkus_extractor_v1.py

from typing import Dict, Any, List
import pandas as pd
from src.config import logger
from src.extractors.base_extractor import BaseExtractor


class AbakkusExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Abakkus Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_name="ABAKKUS MUTUAL FUND", version="v1")
        self.column_mapping = {
            "ISIN": "isin",
            "NAME OF THE INSTRUMENT": "company_name",
            "COMPANY": "company_name",
            "QUANTITY": "quantity",
            "MARKET VALUE": "market_value_inr",
            "MARKET / FAIR VALUE": "market_value_inr",
            "MARKET/FAIR VALUE": "market_value_inr",
            "% TO NAV": "percent_of_nav",
            "% TO NET ASSET": "percent_of_nav",
            "INDUSTRY": "sector",
            "SECTOR": "sector",
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Abakkus file: {file_path}")

        xls = pd.ExcelFile(file_path, engine=None) 
        
        # 1. Build scheme mapping from 'Index' sheet if it exists
        scheme_mapping = {}
        if 'Index' in xls.sheet_names:
            try:
                index_df = pd.read_excel(xls, sheet_name='Index')
                # Looking for 'Short Name' and 'Scheme Name' columns
                # Let's clean column names for matching
                index_df.columns = [str(c).strip().upper() for c in index_df.columns]
                if 'SHORT NAME' in index_df.columns and 'SCHEME NAME' in index_df.columns:
                    for _, row in index_df.iterrows():
                        short = str(row['SHORT NAME']).strip()
                        full = str(row['SCHEME NAME']).strip()
                        if short and full:
                            scheme_mapping[short] = full
                    logger.debug(f"Loaded {len(scheme_mapping)} scheme mappings from Index sheet.")
            except Exception as e:
                logger.warning(f"Could not parse 'Index' sheet: {e}")

        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            if sheet_name == 'Index':
                continue
                
            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_raw.empty:
                continue

            header_idx = self.find_header_row(df_raw)
            if header_idx == -1:
                logger.debug(f"No header found in sheet: {sheet_name}")
                continue

            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            
            # Global Unit Detection - Use whole raw sheet to find units like "Lakhs"
            global_unit = self.scan_sheet_for_global_units(df_raw)
            logger.debug(f"Detected global unit for {sheet_name}: {global_unit}")
            
            # Map columns
            df = self._map_columns(df)

            if "isin" not in df.columns:
                continue

            # Filtering logic for Equity
            equity_df = self.filter_equity_isins(df, "isin")
            if equity_df.empty:
                logger.debug(f"No equity holdings found in sheet {sheet_name}")
                continue

            # Determine Scheme Name
            # Priority 1: Index mapping
            # Priority 2: Row 0 column 1
            # Priority 3: Sheet name
            scheme_name_raw = scheme_mapping.get(sheet_name)
            if not scheme_name_raw:
                # Try finding it in row 0-2 of column 1
                for r in range(min(3, len(df_raw))):
                    val = str(df_raw.iloc[r, 1]).strip()
                    if val and "Unnamed" not in val and len(val) > 10:
                        scheme_name_raw = val
                        break
            
            if not scheme_name_raw:
                scheme_name_raw = sheet_name

            scheme_info = self.parse_verbose_scheme_name(scheme_name_raw)
            
            sheet_holdings: List[Dict[str, Any]] = []

            for _, row in equity_df.iterrows():
                # Value Normalization
                mkt_val = self.normalize_currency(row.get("market_value_inr", 0), global_unit)
                
                # Percentage Parsing - Abakkus percentages are decimals (e.g. 0.0486 for 4.86%)
                # We multiply by 100 to store as 4.86 in the DB.
                raw_pct = self.safe_float(row.get("percent_of_nav", 0)) * 100.0
                
                sheet_holdings.append(
                    {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info["scheme_name"],
                        "scheme_description": scheme_info["description"],
                        "plan_type": scheme_info["plan_type"],
                        "option_type": scheme_info["option_type"],
                        "is_reinvest": scheme_info["is_reinvest"],
                        "isin": row.get("isin"),
                        "company_name": self.clean_company_name(row.get("company_name")),
                        "quantity": int(self.safe_float(row.get("quantity", 0))),
                        "market_value_inr": mkt_val,
                        "percent_of_nav": raw_pct, # Use raw float
                        "sector": self.clean_company_name(row.get("sector", "N/A")),
                    }
                )

            if sheet_holdings:
                # Validate and log
                aum_cr = sum(h["market_value_inr"] for h in sheet_holdings) / 10_000_000.0
                logger.info(f"Scheme: {scheme_info['scheme_name']} - Holdings: {len(sheet_holdings)} - Equity AUM: {aum_cr:.2f} Cr")
                all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        new_cols = {}
        for col in df.columns:
            # Normalize column name: uppercase and replace newlines/multiple spaces
            col_norm = str(col).upper().replace("\n", " ").strip()
            # Replace multiple spaces with single space
            import re
            col_norm = re.sub(r'\s+', ' ', col_norm)
            
            for pattern, canonical in self.column_mapping.items():
                if pattern.upper() in col_norm:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)
