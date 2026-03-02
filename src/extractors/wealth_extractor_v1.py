import pandas as pd
import re
from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger
from src.config.constants import AMC_WEALTH

class WealthExtractorV1(BaseExtractor):
    """
    Dedicated extractor for The Wealth Company.
    Schema:
    - Multi-sheet (Sheet Name ~ Scheme Name).
    - Scheme Name explicitly in Row 1 (Index 1), Column 0.
    - Header at Row 3 (Index 3).
    - No stop logic (consumes all valid ISIN rows).
    """

    def __init__(self):
        super().__init__(amc_name=AMC_WEALTH, version="V1")
        self.column_mapping = {
            "NAME OF INSTRUMENT": "company_name",
            "NAME OF THE INSTRUMENT": "company_name",
            "ISIN": "isin",
            "QUANTITY": "quantity",
            "MARKET VALUE": "market_value_inr",
            "FAIR VALUE": "market_value_inr",
            "% TO NET ASSETS": "percent_of_nav",
            "RATING/INDUSTRY": "sector",
            "INDUSTRY": "sector"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Wealth Company file: {file_path}")
        holdings = []
        
        try:
            xls = pd.ExcelFile(file_path, engine="openpyxl")
            for sheet_name in xls.sheet_names:
                if any(k in str(sheet_name).upper() for k in ["INDEX", "SUMMARY", "SHEET"]):
                    # Note: Previous check was "SHEET" in sheet_name.upper() and len < 8
                    # We'll stick to a more flexible one if needed, but usually index/summary are ignored.
                    if "SHEET" in str(sheet_name).upper() and len(str(sheet_name)) < 8:
                        continue

                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                if df_raw.empty:
                    continue

                # 1. Detect Header Row
                header_idx = self.find_header_row(df_raw, ["ISIN"])
                if header_idx == -1:
                    logger.warning(f"Header not found in sheet '{sheet_name}'. skipping.")
                    continue

                # 2. Extract Scheme Name (Usually 2 rows above header)
                scheme_raw = None
                for i in range(max(0, header_idx - 5), header_idx):
                    row_vals = [str(v).strip() for v in df_raw.iloc[i].values if pd.notna(v)]
                    row_text = " ".join(row_vals)
                    if "WEALTH COMPANY" in row_text.upper():
                        scheme_raw = row_text
                        break
                
                if not scheme_raw:
                    scheme_raw = str(sheet_name)
                
                # Clean Scheme Name
                if "-" in scheme_raw:
                    scheme_clean = scheme_raw.split("-", 1)[1].strip()
                else:
                    scheme_clean = scheme_raw
                
                # Fix specific truncations found in logs
                if "MULTIASSET ALLOC" in scheme_clean.upper():
                    scheme_clean = scheme_clean.upper().replace("MULTIASSET ALLOC", "MULTI ASSET ALLOCATION")

                scheme_info = self.parse_verbose_scheme_name(scheme_clean)

                # 3. Read Data
                # Use standard header-based DF
                df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
                raw_columns = df.columns.tolist()
                
                # Detect Units (Wealth is usually LAKHS based on headers)
                value_unit = self.scan_sheet_for_global_units(df_raw)
                # Resolve specifically if column header says Lakhs
                value_unit = self._resolve_value_unit(raw_columns, value_unit)

                df = self._map_columns(df)

                # 4. Extract Total AUM (Grand Total)
                raw_net_assets = None
                for idx, row in df_raw.iterrows():
                    row_vals = [str(val).upper() if pd.notna(val) else "" for val in row.values]
                    row_text = " ".join(row_vals)
                    # "Grand Total" is the reliable marker for Wealth Company
                    if "GRAND TOTAL" in row_text:
                        candidates = []
                        for val in row.values:
                            f_val = self.safe_float(val)
                            if f_val > 0 and abs(f_val - 1.0) > 0.001: # Avoid 1 (100%)
                                candidates.append(f_val)
                        
                        if candidates:
                            raw_net_assets = candidates[-1]
                            break
                
                normalized_net_assets = None
                if raw_net_assets:
                    normalized_net_assets = self.normalize_currency(raw_net_assets, value_unit)
                    logger.info(f"[{scheme_info['scheme_name']}] Extracted Total AUM: {normalized_net_assets / 10000000.0:.2f} Cr")

                # 5. Process Holdings
                raw_isin_count = len(df)
                equity_df = self.filter_equity_isins(df, "isin")
                equity_count = len(equity_df)
                logger.info(f"[{scheme_info['scheme_name']}] Raw rows: {raw_isin_count}, Equity rows: {equity_count}")
                sheet_holdings = []

                if not equity_df.empty:
                    for _, row in equity_df.iterrows():
                        sheet_holdings.append({
                            "amc_name": self.amc_name,
                            "scheme_name": scheme_info["scheme_name"],
                            "scheme_description": scheme_info["description"],
                            "plan_type": scheme_info["plan_type"],
                            "option_type": scheme_info["option_type"],
                            "is_reinvest": scheme_info["is_reinvest"],
                            "isin": row.get("isin"),
                            "company_name": self.clean_company_name(row.get("company_name")),
                            "quantity": self.safe_float(row.get("quantity")),
                            "market_value_inr": self.normalize_currency(row.get("market_value_inr"), value_unit),
                            "percent_of_nav": self.parse_percentage(row.get("percent_of_nav", 0.0)),
                            "sector": self.clean_company_name(row.get("sector", "N/A")),
                            "total_net_assets": normalized_net_assets
                        })
                
                # Ghost Holding if empty
                if not sheet_holdings and normalized_net_assets:
                    sheet_holdings.append({
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info["scheme_name"],
                        "scheme_description": scheme_info["description"],
                        "plan_type": scheme_info["plan_type"],
                        "option_type": scheme_info["option_type"],
                        "is_reinvest": scheme_info["is_reinvest"],
                        "isin": None,
                        "total_net_assets": normalized_net_assets,
                        "market_value_inr": 0,
                        "percent_of_nav": 0
                    })

                if sheet_holdings:
                    if not equity_df.empty:
                        self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
                    holdings.extend(sheet_holdings)

        except Exception as e:
            logger.error(f"Error in Wealth Company extractor: {e}")
            import traceback
            logger.error(traceback.format_exc())

        return holdings
