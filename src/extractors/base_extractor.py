import abc
import pandas as pd
import re
from typing import Dict, Any, List, Optional, Tuple
from src.config import logger

class BaseExtractor(abc.ABC):
    """
    Abstract Base Class for all AMC-specific extractors.
    Provides standard utilities for ISIN filtering, currency normalization,
    and header detection.
    """

    def __init__(self, amc_name: str, version: str):
        self.amc_name = amc_name
        self.version = version

    @abc.abstractmethod
    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Main method to extract data from a merged Excel file.
        Returns a list of dictionaries following the Mandatory Schema.
        """
        pass

    def find_header_row(self, df: pd.DataFrame, keywords: List[str] = ["ISIN"]) -> int:
        """
        Detects the header row by searching for specific keywords.
        Searches within the first 20 rows.
        """
        for i in range(min(20, len(df))):
            # Check if any cell in this row (ignoring NaNs) matches our keywords
            row_values = [str(val).upper().strip() for val in df.iloc[i].values if not pd.isna(val)]
            for val in row_values:
                if any(kw.upper() in val for kw in keywords):
                    return i
        return -1

    def filter_equity_isins(self, df: pd.DataFrame, isin_col: str) -> pd.DataFrame:
        """
        Applies TRIPLE FILTER for Equity:
        1) ISIN starts with 'INE'
        2) Length = 12 characters
        3) Security code (pos 8-9) must be '10'
        """
        def is_equity(isin: str) -> bool:
            if not isinstance(isin, str):
                return False
            isin = isin.strip().upper()
            if len(isin) != 12:
                return False
            if not isin.startswith("INE"):
                return False
            # STRICT FILTER: Security code '10' for Equity Shares
            if isin[8:10] != "10":
                return False
            return True

        equity_mask = df[isin_col].apply(is_equity)
        filtered_df = df[equity_mask].copy()
        
        # Log filtered out rows for audit (lineage)
        filtered_out_count = len(df) - len(filtered_df)
        if filtered_out_count > 0:
            logger.debug(f"Filtered out {filtered_out_count} non-equity rows using Triple Filter.")
            
        return filtered_df

    def detect_units(self, text: str) -> str:
        """
        Detects units (RUPEES, LAKHS, CRORES) from a string (e.g., column header or sheet metadata).
        """
        s = str(text).upper()
        # Crores: CRORE, CR, CR.
        if re.search(r'\bCRORE\b|\bCR\b|\bCR\.', s):
            return "CRORES"
        # Lakhs: LAKH, LK, LAC, LC, LAKHS, LACS
        if re.search(r'\bLAKH\b|\bLK\b|\bLAC\b|\bLC\b|\bLAKHS\b|\bLACS\b|\bLK\.', s):
            return "LAKHS"
        return "RUPEES"

    def scan_sheet_for_global_units(self, df: pd.DataFrame) -> str:
        """
        Scans the first 15 rows of a sheet for a global unit indicator.
        Example: "All figures in Rs. Lakhs"
        """
        for i in range(min(15, len(df))):
            row_str = " ".join([str(val) for val in df.iloc[i].values if not pd.isna(val)])
            detected = self.detect_units(row_str)
            if detected != "RUPEES":
                logger.debug(f"Detected global unit '{detected}' in row {i}")
                return detected
        return "RUPEES"

    def normalize_currency(self, value: Any, source_unit: str = "RUPEES") -> float:
        """
        Normalizes currency values to base ₹ Rupees.
        source_unit: 'RUPEES', 'LAKHS', 'CRORES'
        """
        return self.safe_float(value) * (100_000 if source_unit == "LAKHS" else 10_000_000 if source_unit == "CRORES" else 1)

    def safe_float(self, value: Any) -> float:
        """Safely converts a value to float, handling commas, '@', and other artifacts."""
        try:
            if pd.isna(value) or value is None:
                return 0.0
            if isinstance(value, str):
                # Remove commas, currency symbols, and other common artifacts
                clean_val = re.sub(r'[^\d\.-]', '', value)
                if not clean_val or clean_val == '.':
                    return 0.0
                return float(clean_val)
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def parse_verbose_scheme_name(self, raw_name: str) -> Dict[str, Any]:
        """
        Splits verbose scheme name into name, plan, option, and reinvestment flag.
        Example: "HDFC Balanced Advantage Fund - Direct Plan - Growth Option"
        """
        name_clean = raw_name.replace("_", " ").strip()
        parts = name_clean.split(" - ", 1)
        
        main_name = parts[0].strip()
        description = parts[1].strip() if len(parts) > 1 else ""

        upper_name = main_name.upper()
        
        # 1. Plan Detection
        plan_type = "Regular"
        if "DIRECT" in upper_name:
            plan_type = "Direct"
            
        # 2. Option Detection
        option_type = "Growth"
        if any(kw in upper_name for kw in ["IDCW", "DIVIDEND", "PAYOUT", "INCOME"]):
            option_type = "IDCW"

        # 3. Reinvestment Flag
        is_reinvest = False
        if "REINVEST" in upper_name:
            is_reinvest = True

        return {
            "scheme_name": main_name,
            "description": description,
            "plan_type": plan_type,
            "option_type": option_type,
            "is_reinvest": is_reinvest
        }

    def validate_nav_completeness(self, holdings: List[Dict[str, Any]], reported_nav_pct: float = None) -> Tuple[bool, float]:
        """
        Sanity Check: Total % to NAV per scheme should be between 95% and 105%.
        Catches silent bad parsing or missing high-value rows.
        """
        if not holdings:
            return False, 0.0
            
        total_nav_pct = sum(h.get('percent_to_nav', 0.0) for h in holdings)
        
        # If we have a reported total from Excel, use that (e.g. 98.5%)
        # Otherwise, assume it should be near 100%
        target = reported_nav_pct if reported_nav_pct else 100.0
        
        is_valid = 95.0 <= total_nav_pct <= 105.0
        
        if not is_valid:
            logger.warning(f"NAV SANITY CHECK FAILED: Total NAV % is {total_nav_pct:.2f}% (Expected ~100%)")
            
        return is_valid, total_nav_pct

    def get_mandatory_columns(self) -> List[str]:
        """Returns the list of columns required for the production DB."""
        return [
            "amc_name", "scheme_name", "scheme_description", "isin", 
            "company_name", "quantity", "market_value_inr", 
            "percent_to_nav", "sector"
        ]
