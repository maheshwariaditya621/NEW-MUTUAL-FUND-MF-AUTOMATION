"""
HSBC Mutual Fund Extractor (Version 1)

Extracts equity holdings from HSBC merged Excel files.
Structure:
- Row 0: "HSBC Mutual Fund"
- Row 1: Scheme Name
- Row 3: Portfolio Date
- Header: ~Row 5 (Search for ISIN)
"""

import pandas as pd
import re
from typing import Dict, Any, List
from src.extractors.base_extractor import BaseExtractor
from src.config import logger
from src.config.constants import AMC_HSBC

class HSBCExtractorV1(BaseExtractor):
    """
    HSBC Mutual Fund Extractor.
    """

    def __init__(self):
        super().__init__(amc_name=AMC_HSBC, version="v1")
        
        # Column mapping based on analysis
        self.column_mapping = {
            "Name of the Instrument": "security_name",
            "ISIN": "isin",
            "Rating/Industries": "sector",
            "Quantity": "quantity",
            "Market Value": "market_value",  # Partial match suffices
            "Percentage to Net Assets": "percent_of_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"[HSBC] Starting extraction from: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        all_holdings = []
        schemes_processed = 0
        schemes_with_equity = 0

        for sheet_name in xls.sheet_names:
            try:
                # Read first 15 rows for metadata
                df_head = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=15)
                if df_head.empty:
                    continue

                # 1. Extract Scheme Name (Row 1, Index 1)
                try:
                    raw_scheme_name = str(df_head.iloc[1, 0]).strip()
                    if not raw_scheme_name or raw_scheme_name.lower() == 'nan':
                        raw_scheme_name = sheet_name
                except Exception:
                    raw_scheme_name = sheet_name

                # 2. Extract Date (Row 3, Index 3) - "Portfolio Statement as of December 31, 2025"
                # For now, we trust the file structure, but could add regex parsing if needed.
                
                # Clean Scheme Name
                # HSBC names are usually clean, but let's be safe
                cleaned_scheme_name = raw_scheme_name.replace("HSBC Mutual Fund", "").strip()
                
                scheme_info = self.parse_verbose_scheme_name(cleaned_scheme_name)
                
                # 3. Find Header
                header_idx = self.find_header_row(df_head)
                if header_idx == -1:
                    logger.debug(f"[HSBC] No header in {sheet_name}, skipping")
                    continue

                # 4. Read Data
                df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
                
                # 5. Map Columns
                # Fuzzy mapping for "Market Value (Rs in Lacs)"
                new_cols = {}
                for col in df.columns:
                    col_str = str(col)
                    # Handle newlines in header
                    col_clean = col_str.replace("\n", " ").replace("  ", " ").strip()
                    
                    for key, val in self.column_mapping.items():
                        if key in col_clean:
                            new_cols[col] = val
                            break
                
                df = df.rename(columns=new_cols)
                
                if "isin" not in df.columns:
                    continue

                # 6. Filter Equity
                equity_df = self.filter_equity_isins(df, "isin")
                
                if equity_df.empty:
                    schemes_processed += 1
                    continue

                # 7. Check for critical columns
                if "market_value" not in equity_df.columns or "percent_of_nav" not in equity_df.columns:
                    logger.warning(f"[HSBC] Missing MV or NAV column in {cleaned_scheme_name}")
                    schemes_processed += 1
                    continue

                # 8. Build Holdings
                sheet_holdings = []
                for _, row in equity_df.iterrows():
                    market_val = self.safe_float(row.get("market_value", 0))
                    nav_val = self.safe_float(row.get("percent_of_nav", 0))
                    
                    # Logic: 
                    # Market Value is in Lakhs -> * 100,000
                    # NAV is decimal (0.3628) -> * 100 -> 36.28%
                    
                    holding = {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info["scheme_name"],
                        "scheme_description": scheme_info["description"],
                        "plan_type": scheme_info["plan_type"],
                        "option_type": scheme_info["option_type"],
                        "is_reinvest": scheme_info["is_reinvest"],
                        "isin": self.clean_isin(row.get("isin")),
                        "company_name": self.clean_company_name(row.get("security_name")),
                        "quantity": int(self.safe_float(row.get("quantity", 0))),
                        "market_value_inr": market_val * 100_000,
                        "percent_of_nav": nav_val * 100,
                        "sector": row.get("sector", None)
                    }
                    sheet_holdings.append(holding)

                # 9. Validate (Log warning but always accept data)
                self.validate_nav_completeness(sheet_holdings, scheme_info['scheme_name'])
                all_holdings.extend(sheet_holdings)
                schemes_with_equity += 1
                logger.info(f"[HSBC] ✓ {scheme_info['scheme_name']}: {len(sheet_holdings)} holdings")
                
                schemes_processed += 1

            except Exception as e:
                logger.error(f"[HSBC] Error processing sheet {sheet_name}: {e}")
                continue

        logger.info(f"[HSBC] Extraction complete. Found {len(all_holdings)} holdings from {schemes_with_equity}/{schemes_processed} schemes.")
        return all_holdings
