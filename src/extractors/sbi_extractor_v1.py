import pandas as pd
import re
from typing import Dict, Any, List
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

from src.config.constants import AMC_SBI

class SBIExtractorV1(BaseExtractor):
    """
    SBI Mutual Fund Extractor Implementation (Version 1).
    Proves modularity without touching core code.
    """

    def __init__(self):
        super().__init__(amc_name=AMC_SBI, version="v1")
        # SBI might use different names (hypothetical based on common patterns)
        self.column_mapping = {
            "ISIN": "isin",
            "NAME OF THE INSTRUMENT": "company_name",
            "NAME OF THE ISSUER": "company_name",
            "QUANTITY": "quantity",
            "MARKET VALUE": "market_value_inr",
            "PERCENTAGE TO NAV": "percent_of_nav",
            "% TO NAV": "percent_of_nav",
            "% TO AUM": "percent_of_nav",
            "% TO NET ASSETS": "percent_of_nav",
            "INDUSTRY": "sector",
            # New mappings from debug
            "EQUITY & EQUITY RELATED": "company_name" # Sometimes header is subsection
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
                # 1. Try to get internal name from Row 2 or Row 3, columns C or D
                header_df = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=5)
                candidate = str(header_df.iloc[2, 2]) # C3
                if "SCHEME NAME" in candidate.upper():
                    parts = candidate.split(":", 1)
                    if len(parts) > 1 and parts[1].strip():
                        extracted_name = parts[1].strip()
                    else:
                        extracted_name = str(header_df.iloc[2, 3]).strip()
                
                # 2. UNIQUENESS FIX: If sheet_name has a suffix like "SRBF AHP" or "SLTAF II", 
                # and the extracted_name is generic, append the suffix.
                sheet_suffix_match = re.search(r'31st\s+(.+)$', sheet_name)
                if sheet_suffix_match:
                    suffix = sheet_suffix_match.group(1).strip()
                    # If the suffix is not already in the name, and name is short/generic
                    if suffix not in extracted_name and (len(extracted_name) < 25 or "BENEFIT" in extracted_name.upper()):
                         extracted_name = f"{extracted_name} ({suffix})"
                         
                logger.debug(f"Found unique scheme name: {extracted_name}")
            except Exception as e:
                logger.debug(f"Could not extract unique name: {e}")

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
                    "company_name": self.clean_company_name(row.get("company_name")),
                    "quantity": int(self.normalize_currency(row.get("quantity", 0), "RUPEES")),
                    "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), value_unit),
                    "percent_of_nav": self._safe_float(row.get("percent_of_nav", 0)),
                    "sector": self.clean_company_name(row.get("sector", "N/A"))
                }
                all_holdings.append(holding)

        return all_holdings

    def parse_verbose_scheme_name(self, raw_name: str) -> Dict[str, Any]:
        """
        SBI specific parsing. Avoids over-aggressive splitting that merges 
        different series or sub-plans.
        Also enforces 'SBI ' prefix as per user request.
        """
        # 1. Cleanup & Prefix Enforcement
        if pd.isna(raw_name): name = ""
        else: name = str(raw_name).strip()
        
        # Fix encoding issues
        name = self.fix_mojibake(name)
        
        # Normalize spaces
        name = re.sub(r'\s+', ' ', name).strip()

        # Remove parenthetical codes (e.g. (SETFBSE1), (SBISENSE))
        # We target content that looks like a code or description in parens
        name = re.sub(r'\s*\(.*?\)', '', name).strip()
        
        # Enforce SBI Prefix
        if not name.upper().startswith("SBI "):
            name = "SBI " + name
            
        # Call base to get standard plan/option detection
        info = super().parse_verbose_scheme_name(name)
        
        # For SBI, if we have a hyphen, we might want to keep the suffix in the name 
        # if it's a "Series" or specific "Plan" that isn't just Regular/Direct.
        if " - " in name:
            parts = [p.strip() for p in name.split(" - ")]
            main_parts = []
            description_parts = []
            
            for p in parts:
                p_upper = p.upper()
                # If it's a standard Plan/Option keywords, it's description
                if any(kw in p_upper for kw in ["DIRECT", "REGULAR", "GROWTH", "IDCW", "DIVIDEND", "PAYOUT", "REINVEST"]):
                    description_parts.append(p)
                else:
                    # It's part of the unique scheme identity (e.g. Series VI, Investment Plan)
                    main_parts.append(p)
            
            if main_parts:
                info["scheme_name"] = " - ".join(main_parts)
                info["description"] = " - ".join(description_parts)
                
        return info

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        new_cols = {}
        for col in df.columns:
            col_upper = str(col).upper()
            for pattern, canonical in self.column_mapping.items():
                if pattern in col_upper:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)

    def _safe_float(self, val: Any) -> float:
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0
