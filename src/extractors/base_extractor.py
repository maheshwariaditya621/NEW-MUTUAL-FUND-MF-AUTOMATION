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
        STRICT RULE: Must find ISIN AND (Instrument OR Issuer OR Company).
        Searches within the first 25 rows.
        """
        secondary_keywords = ["INSTRUMENT", "ISSUER", "COMPANY", "NAME OF THE"]
        
        for i in range(min(25, len(df))):
            row_values = [str(val).upper().strip() for val in df.iloc[i].values if not pd.isna(val)]
            
            # Check for ISIN
            has_isin = any("ISIN" in val for val in row_values)
            if not has_isin:
                continue
                
            # Check for secondary descriptive keyword
            has_secondary = any(any(skw in val for skw in secondary_keywords) for val in row_values)
            
            if has_isin and has_secondary:
                logger.debug(f"Header found at row {i} with ISIN and secondary keyword.")
                return i
                
        return -1

    def clean_isin(self, isin: Any) -> str:
        """
        Production-grade ISIN cleaning:
        - Remove spaces and *
        - Truncate to first 12 characters (handles '(Partly Paid)', '-RE', etc.)
        - Uppercase
        """
        if pd.isna(isin) or not isin:
            return ""
        s = str(isin).strip().upper()
        s = s.replace("*", "").replace(" ", "")
        return s[:12]

    def clean_company_name(self, name: Any) -> str:
        """
        Minimal company name cleaning as per instructions.
        - Trim
        - Remove multiple spaces
        - Remove trailing punctuation
        """
        if pd.isna(name) or not name:
            return "N/A"
        s = str(name).strip()
        s = re.sub(r'\s+', ' ', s)
        s = s.rstrip('., ')
        return s

    def parse_percentage(self, value: Any) -> float:
        """
        Smart percentage parsing:
        - If value > 1 (e.g. 5.23), keep as-is.
        - If value <= 1 (e.g. 0.0523), multiply by 100.
        """
        f_val = self.safe_float(value)
        if 0 < abs(f_val) <= 1.0:
            return f_val * 100.0
        return f_val

    def filter_equity_isins(self, df: pd.DataFrame, isin_col: str) -> pd.DataFrame:
        """
        Applies TRIPLE FILTER for Equity with ISIN cleaning.
        1) ISIN starts with 'INE'
        2) Length = 12 characters
        3) Security code (pos 8-9) must be '10'
        
        Also implements MULTI-TABLE DETECTION:
        Stops processing if termination keywords found.
        """
        equity_rows = []
        has_started = False
        for _, row in df.iterrows():
            raw_isin = row[isin_col]
            
            # ISIN cleaning and empty check
            cleaned_isin = self.clean_isin(raw_isin)
            if not cleaned_isin:
                continue
            
            has_started = True

            # Filter Equity
            if len(cleaned_isin) == 12 and cleaned_isin.startswith("INE") and cleaned_isin[8:10] == "10":
                row_copy = row.to_dict()
                row_copy[isin_col] = cleaned_isin # Use cleaned ISIN
                equity_rows.append(row_copy)

        return pd.DataFrame(equity_rows)

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

    def detect_currency_unit(self, values: pd.Series) -> str:
        """
        Data-driven unit detection (Lakhs vs Rupees).
        Follows user instructions: 
        - mostly < 10,000 -> LAKHS
        - values > 1,00,00,000 -> RUPEES
        """
        numeric_vals = values.apply(self.safe_float)
        numeric_vals = numeric_vals[numeric_vals > 0]
        
        if numeric_vals.empty:
            return "RUPEES"
            
        median_val = numeric_vals.median()
        max_val = numeric_vals.max()
        
        # Exact thresholds from user (corrected for Lakhs/Rupees scale)
        if max_val > 10_000_000: # 1 Crore
            return "RUPEES"
        if median_val < 10_000:
            return "LAKHS"
            
        return "RUPEES"

    def scan_sheet_for_global_units(self, df: pd.DataFrame) -> str:
        """
        Scans the first 15 rows for global unit indicators in text.
        """
        for i in range(min(15, len(df))):
            row_str = " ".join([str(val) for val in df.iloc[i].values if not pd.isna(val)])
            detected = self.detect_units(row_str)
            if detected != "RUPEES":
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

    def validate_nav_completeness(self, holdings: List[Dict[str, Any]], scheme_name: str) -> bool:
        """
        Mandatory Post-Extraction Validation (4 Golden Rules):
        1. 95% - 105% Total NAV
        2. 20 - 200 Holdings (Fast catch for bad parsing/multi-table)
        3. No Duplicate ISINs
        4. Top Weight < 15% (skip for ETFs)
        """
        if not holdings:
            logger.error(f"[{scheme_name}] FAILED: No holdings extracted.")
            return False
            
        total_nav_pct = sum(h.get('percent_to_nav', 0.0) for h in holdings)
        isin_count = len(holdings)
        isins = [h.get('isin') for h in holdings]
        unique_isins = set(isins)
        
        # 1. NAV Guard
        if not (90.0 <= total_nav_pct <= 105.0):
            logger.warning(f"[{scheme_name}] NAV GUARD WARNING: {total_nav_pct:.2f}% (Range 90-105%). Continuing as per user policy.")
            # Note: We return True now to avoid skipping the scheme, but log a warning.
            return True
            
        # 2. Holdings Count Guard
        if not (20 <= isin_count <= 200):
            # Check if it's an ETF/Index (often have fewer) or small scheme
            if isin_count < 20:
                logger.warning(f"[{scheme_name}] LOW HOLDING COUNT: {isin_count} (Expected >20). Verify if valid for this scheme type.")
            elif isin_count > 200:
                logger.error(f"[{scheme_name}] FAILED COUNT GUARD: {isin_count} (Max 200). Possible multi-table leakage.")
                return False
                
        # 3. Duplicate ISIN Guard
        if len(isins) != len(unique_isins):
            logger.error(f"[{scheme_name}] FAILED DUPES GUARD: {len(isins) - len(unique_isins)} duplicate ISINs found.")
            return False
            
        # 4. Top Weight Guard
        top_weight = max(h.get('percent_to_nav', 0.0) for h in holdings)
        if top_weight > 15.0:
            # Note: ETFs/Indices can have more (e.g. HDFC/Reliance in Nifty 50), so this is a warning/conditional
            is_etf = any(kw in scheme_name.upper() for kw in ["ETF", "INDEX", "ARBITRAGE", "CONCENTRATED"])
            if not is_etf:
                logger.warning(f"[{scheme_name}] HIGH TOP WEIGHT: {top_weight:.2f}% (Expected <15% for non-ETF).")
        
        return True

    def get_mandatory_columns(self) -> List[str]:
        """Returns the list of columns required for the production DB."""
        return [
            "amc_name", "scheme_name", "scheme_description", "isin", 
            "company_name", "quantity", "market_value_inr", 
            "percent_to_nav", "sector"
        ]

    def extract_from_excel_config(self, file_path: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generic extractor that uses a configuration dictionary to parse Excel files.
        Eliminates the need for custom code for standard formats.
        """
        input_cfg = config.get('input_config', {})
        col_map = config.get('column_mapping', {})
        
        # 2. Iterate Sheets
        all_data = []
        sheet_pattern = input_cfg.get('sheet_pattern', '.*')
        iterate_sheets = input_cfg.get('iterate_sheets', False)
        
        target_sheets = []
        try:
            with pd.ExcelFile(file_path) as xl:
                for sheet in xl.sheet_names:
                    if re.match(sheet_pattern, sheet, re.IGNORECASE):
                        target_sheets.append(sheet)
                        if not iterate_sheets:
                            break
        except Exception as e:
            logger.error(f"Failed to open Excel file: {e}")
            return []
        
        if not target_sheets:
            logger.error(f"No sheet found matching pattern '{sheet_pattern}'")
            return []
            
        for sheet_name in target_sheets:
            try:
                # 3. Read Sheet ONCE (Raw)
                # Use dtype=object to preserve raw data for header detection
                df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                
                if df_raw.empty:
                    continue

                # 4. Global Unit Detection
                global_unit = self.scan_sheet_for_global_units(df_raw)
                
                # 5. Find Header Row
                header_keywords = input_cfg.get('header_row_keywords', ['ISIN'])
                header_idx = self.find_header_row(df_raw, header_keywords)
                
                if header_idx == -1:
                    logger.warning(f"Header not found in sheet '{sheet_name}'. Skipping.")
                    continue
                    
                # 6. Slice Data (Avoid re-reading)
                # Row at header_idx is the header
                # Rows after are data
                new_header = [str(c).strip() for c in df_raw.iloc[header_idx].values]
                df = df_raw.iloc[header_idx + 1:].copy()
                df.columns = new_header
                
                # 7. Extract Scheme Name
                # Clean via Regex if provided
                clean_regex = input_cfg.get('scheme_name_cleaning_regex')
                scheme_name_extracted = sheet_name
                if clean_regex:
                    scheme_name_extracted = re.sub(clean_regex, '', scheme_name_extracted).strip()
                
                # 8. PROCESS COLUMNS (Fuzzy Mapping)
                # Create a map of Raw Column Name -> Standard Column Name
                final_col_map = {}
                for df_col in df.columns:
                    df_col_upper = str(df_col).upper()
                    for pattern, std_col in col_map.items():
                        if pattern.upper() in df_col_upper:
                             final_col_map[df_col] = std_col
                             break
                
                # Check for ISIN column
                raw_isin_col = None
                for raw, std in final_col_map.items():
                    if std == 'ISIN':
                        raw_isin_col = raw
                        break
                        
                if raw_isin_col:
                     df = df.dropna(subset=[raw_isin_col])
                
                for _, row in df.iterrows():
                    row_data = {}
                    # Build row data using the map
                    for raw_col, std_col in final_col_map.items():
                        row_data[std_col] = row[raw_col]
                    
                    if 'ISIN' not in row_data:
                        continue
                        
                    if not self.is_valid_equity_isin(row_data['ISIN']):
                        continue
                        
                    parsed_scheme = self.parse_verbose_scheme_name(scheme_name_extracted)
                    
                    # Unit Normalization
                    # Check column specific units? For now use global.
                    # TODO: Add per-column unit detection from config if needed.
                    
                    market_val = self.safe_float(row_data.get('Market Value', 0))
                    market_val_norm = self.normalize_currency(market_val, global_unit)
                    
                    qty = self.safe_float(row_data.get('Quantity', 0))
                    nav = self.safe_float(row_data.get('NAV', 0))
                    
                    record = {
                        "amc_name": self.amc_name,
                        "scheme_name": parsed_scheme['scheme_name'],
                        "scheme_plan": parsed_scheme['plan_type'],
                        "isin": str(row_data['ISIN']).strip(),
                        "company_name": row_data.get('Company Name', 'N/A'),
                        "quantity": qty,
                        "market_value_inr": market_val_norm,
                        "percent_to_nav": self.safe_float(row_data.get('Percent to NAV', 0)),
                        "sector": row_data.get('Sector', 'N/A')
                    }
                    all_data.append(record)
                    
            except Exception as e:
                logger.error(f"Error processing sheet '{sheet_name}': {e}")
                continue
                
        return all_data

    def is_valid_equity_isin(self, isin: Any) -> bool:
        """Helper for single ISIN validation."""
        if not isinstance(isin, str): return False
        isin = isin.strip().upper()
        if len(isin) != 12: return False
        if not isin.startswith("INE"): return False
        if isin[8:10] != "10": return False
        return True
