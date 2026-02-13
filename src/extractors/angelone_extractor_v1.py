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
        Uses section-based detection for "Equity" sections.
        """
        logger.info(f"Extracting data from Angel One file: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        all_holdings = []

        for sheet_name in xls.sheet_names:
            if self._should_skip_sheet(sheet_name):
                logger.debug(f"Skipping metadata sheet: {sheet_name}")
                continue
            
            # Read first 100 rows for header/scheme detection
            df_full = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=100)
            
            if df_full.empty:
                continue

            # 1. Header Detection
            header_idx = self.find_header_row(df_full)
            if header_idx == -1:
                logger.debug(f"[{sheet_name}] Header not found. Skipping.")
                continue

            # 2. Scheme Name Detection (More robust search)
            scheme_name = self._extract_scheme_name(df_full, header_idx, sheet_name)
            scheme_info = self.parse_verbose_scheme_name(scheme_name)
            
            # 3. Read data
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            df = self._map_columns(df)
            
            if "isin" not in df.columns:
                logger.debug(f"[{sheet_name}] ISIN column not found. Skipping.")
                continue

            # 4. Section-Based Extraction (Equity & Equity Related)
            # We track the current section and only extract if it's "Equity" or if ISIN matches
            equity_holdings = []
            is_equity_section = False
            
            for _, row in df.iterrows():
                # Detect Section Header
                # Section headers usually have value in Col 0 and NaNs in others
                row_vals = [v for v in row if pd.notna(v)]
                if len(row_vals) == 1 and isinstance(row_vals[0], str):
                    section_text = row_vals[0].upper()
                    if "EQUITY" in section_text:
                        is_equity_section = True
                        logger.debug(f"[{sheet_name}] Entered Equity Section: {section_text}")
                    elif any(kw in section_text for kw in ["TOTAL", "NET ASSETS", "DEBT", "CASH", "MUTUAL FUND"]):
                        # Stop equity section if we hit totals or other major categories
                        is_equity_section = False
                    continue

                # Stop if TOTAL found (Be careful with "Adani Total Gas")
                security_name = str(row.get("company_name", "")).strip()
                security_name_upper = security_name.upper()
                
                if not security_name or security_name_upper in ["TOTAL", "SUBTOTAL", "GRAND TOTAL"]:
                    if is_equity_section and security_name_upper in ["TOTAL", "SUBTOTAL", "GRAND TOTAL"]:
                        is_equity_section = False # End of section
                    continue

                if security_name_upper.startswith("TOTAL ") or security_name_upper.startswith("SUBTOTAL "):
                     is_equity_section = False
                     continue

                # Fallback ISIN check (Equity or Preference Shares)
                isin = str(row.get("isin", "")).strip()
                is_valid_isin = self.is_valid_equity_isin(isin)
                
                # Minimum Data Check
                market_val = self.safe_float(row.get("market_value_inr", 0))
                qty = self.safe_float(row.get("quantity", 0))
                
                if is_valid_isin and security_name and security_name.lower() != 'nan' and (market_val > 0 or qty > 0):
                    # Capture holding
                    holding = {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info["scheme_name"],
                        "scheme_description": scheme_info["description"],
                        "plan_type": scheme_info["plan_type"],
                        "option_type": scheme_info["option_type"],
                        "is_reinvest": scheme_info["is_reinvest"],
                        "isin": isin,
                        "company_name": security_name,
                        "quantity": int(self.safe_float(row.get("quantity", 0))),
                        "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), "LAKHS"),
                        "percent_of_nav": self.parse_percentage(row.get("percent_of_nav", 0)),
                        "sector": str(row.get("sector", None)) if pd.notna(row.get("sector")) else None
                    }
                    equity_holdings.append(holding)

            if equity_holdings:
                logger.info(f"[{sheet_name}] Extracted {len(equity_holdings)} holdings for {scheme_info['scheme_name']}")
                all_holdings.extend(equity_holdings)
            else:
                logger.debug(f"[{sheet_name}] No equity holdings found.")

        return all_holdings

    def _should_skip_sheet(self, sheet_name: str) -> bool:
        """Check if sheet should be skipped. Be very careful with 'index'."""
        skip_keywords = ["summary", "disclaimer", "glossary", "contents"]
        sheet_lower = sheet_name.lower()
        if any(kw in sheet_lower for kw in skip_keywords):
            return True
        # Only skip 'index' if it looks like a TOC sheet (e.g., 'SEBI Index' or just 'Index')
        if sheet_lower == "sebi index" or sheet_lower == "index":
            return True
        return False

    def _extract_scheme_name(self, df_full: pd.DataFrame, header_idx: int, sheet_name: str) -> str:
        """More aggressive scheme name extraction."""
        # 1. Scan rows above header
        for idx in range(header_idx):
            row = df_full.iloc[idx]
            for val in row:
                if pd.notna(val):
                    v_str = str(val).strip()
                    # Look for "Angel One" or "AO " or starting with "Angel One"
                    v_upper = v_str.upper()
                    if ("ANGEL ONE" in v_upper or v_upper.startswith("AO ")) and "PORTFOLIO" not in v_upper:
                        return v_str
        
        # 2. Fallback to sheet name
        return sheet_name

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
