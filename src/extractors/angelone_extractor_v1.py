import pandas as pd
from typing import Dict, Any, List
from src.extractors.base_extractor import BaseExtractor
from src.config import logger
from src.config.constants import AMC_ANGELONE

class AngelOneExtractorV1(BaseExtractor):
    """
    Angel One Mutual Fund Extractor Implementation (Version 1).
    Handles merged Excel files with section-based equity extraction.
    """

    def __init__(self):
        super().__init__(amc_name=AMC_ANGELONE, version="v1")
        # Mapping of raw column substrings to canonical fields
        self.column_mapping = {
            "NAME OF INSTRUMENT": "company_name",
            "NAME OF THE INSTRUMENT": "company_name",
            "ISIN": "isin",
            "RATING/INDUSTRY": "sector",
            "INDUSTRY": "sector",
            "QUANTITY": "quantity",
            "MARKET VALUE": "market_value_inr",
            "IN RS. LAKH": "market_value_inr",
            "% TO NET ASSETS": "percent_of_nav",
            "% TO NAV": "percent_of_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract data from all sheets in the Angel One merged Excel file.
        Uses section-based detection for "Equity" sections and footer AUM scanning.
        """
        logger.info(f"Extracting data from Angel One file: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        all_holdings = []

        for sheet_name in xls.sheet_names:
            if self._should_skip_sheet(sheet_name):
                logger.debug(f"Skipping metadata sheet: {sheet_name}")
                continue
            
            # Read full raw sheet for footer detection and scheme name
            df_full = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_full.empty:
                continue

            # 1. Header Detection
            header_idx = self.find_header_row(df_full)
            if header_idx == -1:
                logger.debug(f"[{sheet_name}] Header not found. Skipping.")
                continue

            # 2. Scheme Name Detection & Normalization
            raw_scheme_name = self._extract_scheme_name(df_full, header_idx, sheet_name)
            scheme_info = self.parse_verbose_scheme_name(raw_scheme_name)
            
            # Read data using discovered header
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            df = self._map_columns(df)
            
            # 3. Total AUM Scanning from Footer
            raw_net_assets = None
            value_unit = "LAKHS" # Angel One is usually in LAKHS
            
            for idx, row in df_full.iterrows():
                row_vals = [str(val).upper() if pd.notna(val) else "" for val in row.values]
                row_text = " ".join(row_vals)
                if "GRAND TOTAL" in row_text or "NET ASSETS" in row_text:
                    candidates = []
                    for val in row.values:
                        f_val = self.safe_float(val)
                        if f_val is not None and f_val > 105: # Avoid 100.00%
                            candidates.append(f_val)
                    if candidates:
                         # Favor the one before 1.0 or 100.0 if present
                        if len(candidates) > 1:
                            if abs(candidates[-1] - 100.0) < 0.1 or abs(candidates[-1] - 1.0) < 0.01:
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

            # 4. Filter Equity Holdings
            equity_df = pd.DataFrame()
            if "isin" in df.columns:
                equity_df = self.filter_equity_isins(df, "isin")
            
            sheet_holdings = []
            if not equity_df.empty:
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
                        "sector": str(row.get("sector", None)) if pd.notna(row.get("sector")) else None,
                        "total_net_assets": normalized_net_assets
                    }
                    sheet_holdings.append(holding)

            # 5. Ghost Holding for Non-Equity
            if not sheet_holdings and normalized_net_assets:
                sheet_holdings.append({
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

            if sheet_holdings:
                self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"])
                all_holdings.extend(sheet_holdings)

        return all_holdings

    def _should_skip_sheet(self, sheet_name: str) -> bool:
        """Check if sheet should be skipped."""
        skip_keywords = ["summary", "disclaimer", "glossary", "contents"]
        sheet_lower = sheet_name.lower()
        if any(kw in sheet_lower for kw in skip_keywords):
            return True
        if sheet_lower == "sebi index" or sheet_lower == "index":
            return True
        return False

    def _extract_scheme_name(self, df_full: pd.DataFrame, header_idx: int, sheet_name: str) -> str:
        """Robust scheme name extraction with AO expansion."""
        for idx in range(min(header_idx, 20)):
            row = df_full.iloc[idx]
            for val in row:
                if pd.notna(val):
                    v_str = str(val).strip()
                    v_upper = v_str.upper()
                    if ("ANGEL ONE" in v_upper or v_upper.startswith("AO ")) and "PORTFOLIO" not in v_upper:
                        import re
                        # Clean fragments like "(R" at the end
                        v_str = re.sub(r'\s*\(R$', '', v_str)
                        # Expand AO to Angel One
                        if v_upper.startswith("AO "):
                            v_str = re.sub(r'^AO\s+', 'Angel One ', v_str, flags=re.IGNORECASE)
                        
                        # Robust Momentum Normalization
                        if "MOMENTUM" in v_upper:
                             v_str = v_str.replace("MKT", "Market").replace("Mkt", "Market")
                             v_str = v_str.replace("QLTY", "Quality").replace("Qlty", "Quality")
                             v_str = v_str.replace("INDX", "Index Fund").replace("Indx", "Index Fund")
                             # Ensure "Index Fund Index Fund" doesn't happen if it was already "Index" or "Index Fund"
                             v_str = v_str.replace("Index Fund Fund", "Index Fund").replace("Index Fund Index Fund", "Index Fund")
                        
                        return v_str
        
        # Fallback to normalized sheet name
        import re
        s_name = sheet_name.strip()
        if s_name.upper().startswith("AO "):
            s_name = re.sub(r'^AO\s+', 'Angel One ', s_name, flags=re.IGNORECASE)
        # Remove trailing fragment
        s_name = re.sub(r'\s*AO\d+$', '', s_name)
        return s_name

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
