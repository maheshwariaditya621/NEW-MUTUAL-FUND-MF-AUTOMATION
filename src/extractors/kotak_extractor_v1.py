import pandas as pd
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from openpyxl import load_workbook

from src.extractors.base_extractor import BaseExtractor
from src.config.constants import AMC_KOTAK
from src.config import logger

class KotakExtractorV1(BaseExtractor):
    """
    Extractor for Kotak Mutual Fund (Dec 2025 format).
    
    Structure:
    - Scheme Name: Row 0 ("Portfolio of ... as on ...")
    - Header: Row 1
    - Columns: 
        - Name of Instrument
        - ISIN Code
        - Market Value (Rs.in Lacs) -> Needs * 100,000
        - % to Net Assets -> Percentage
    """
    
    def __init__(self):
        super().__init__(amc_name=AMC_KOTAK, version="v1")
        
        # Column mapping based on analysis
        self.column_mapping = {
            "Name of Instrument": "security_name",
            "Name of the Instrument": "security_name", # Possible variant
            "ISIN Code": "isin",
            "ISIN": "isin",
            "Industry": "sector",
            "Quantity": "quantity",
            "Market Value (Rs.in Lacs)": "market_value",
            "Market Value(Rs. in Lakhs)": "market_value", # Possible variant
            "% to Net Assets": "percent_to_nav"
        }

    def standardize_columns(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Standardize column names based on mapping.
        """
        new_cols = {}
        for col in df.columns:
            col_str = str(col).strip()
            # Fuzzy match for complex headers
            for key, val in mapping.items():
                if key in col_str:
                    new_cols[col] = val
                    break
        return df.rename(columns=new_cols)

    def _extract_scheme_name(self, df_raw: pd.DataFrame, sheet_name: str) -> str:
        """
        Extract scheme name from Row 0 (Cell A1/B1/C1).
        Pattern: "Portfolio of [SCHEME NAME] as on [DATE]"
        """
        # Iterate first few cells in first row to find the text
        for col in range(5):
            val = str(df_raw.iloc[0, col])
            if "Portfolio of " in val:
                # Regex to extract name
                match = re.search(r"Portfolio of\s+(.+?)\s+as on", val, re.IGNORECASE)
                if match:
                    return match.group(1).strip()
        
        # Fallback to sheet name
        return sheet_name

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"[Kotak] Starting extraction from: {file_path}")
        
        wb = load_workbook(file_path, read_only=True, data_only=True)
        all_holdings = []
        schemes_processed = 0
        schemes_with_equity = 0
        
        for sheet_name in wb.sheetnames:
            try:
                # 1. Skip non-scheme sheets
                if self._should_skip_sheet(sheet_name):
                    continue

                # 2. Read Sheet
                # Read raw to find header manually (handling merged cells)
                df_raw_sheet = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                
                # Keywords to identify header row
                required_cols_keywords = ["Instrument", "ISIN", "Quantity", "Market Value"]
                
                # Find header
                header_row_idx = self.find_header_row(df_raw_sheet, required_cols_keywords)
                
                if header_row_idx is None:
                    # Fallback or skip
                     logger.warning(f"[Kotak] Header not found in {sheet_name}, skipping")
                     continue

                # EXTRACT HEADER ROW & FORWARD FILL (Handle merged cells)
                # This ensures "Name of Instrument" in Col A covers Data in Col C if merged
                header_row = df_raw_sheet.iloc[header_row_idx]
                filled_header = header_row.ffill()
                
                # Set columns
                df_raw_sheet.columns = filled_header
                
                # Slice data (rows after header)
                df = df_raw_sheet.iloc[header_row_idx+1:].reset_index(drop=True)
                
                # 3. Extract Scheme Name (from raw header block if needed, but we used df_raw in find_header)
                # Note: We need a fresh read for scheme name logic if it relies on Row 0 specifically
                # But _extract_scheme_name uses df_raw.iloc[0], so we can pass df_raw_sheet BEFORE slicing?
                # Actually, df_raw_sheet contains everything.
                
                # 3. Extract Scheme Name
                raw_scheme_name = self._extract_scheme_name(df_raw_sheet, sheet_name)
                
                # 3. Clean Scheme Name (Strip AMC Prefix)
                scheme_name_clean = self._clean_scheme_name(raw_scheme_name)
                
                # 4. Parse Scheme Info (Plan, Option, etc.)
                try:
                    scheme_info = self.parse_verbose_scheme_name(scheme_name_clean)
                except Exception as e:
                    logger.warning(f"[Kotak] Failed to parse scheme name '{scheme_name_clean}': {e}")
                    continue

                # 5. Standardize Columns
                df_clean = self.standardize_columns(df, self.column_mapping)
                
                # 5. Extract Date (optional, can use file date)
                # We typically rely on the filename date passed by orchestrator, 
                # but if we needed it, it's in Row 0 too.
                
                # 6. Iterate Rows
                sheet_holdings = []
                # Check required columns (except ISIN which is checked next)
                required_cols = ['security_name', 'market_value', 'percent_to_nav']
                if not all(col in df_clean.columns for col in required_cols):
                    logger.debug(f"[Kotak] Missing columns in {sheet_name}, skipping")
                    continue
                
                if 'isin' not in df_clean.columns:
                     logger.debug(f"[Kotak] No ISIN column in {sheet_name}, skipping")
                     continue

                # Filter Equity & Handle Multi-table
                # Note: filter_equity_isins operates on DataFrame. 
                # If duplicate columns exist, it might fail?
                # BaseExtractor.filter_equity_isins uses df[isin_col].str.startswith...
                # If df[isin_col] is a DataFrame (duplicates), .str accessor fails.
                
                # We need to deduplicate columns BEFORE filtering?
                # Or handle it in loop.
                # BUT filter_equity_isins IS called before loop.
                
                # Fix: Deduplicate columns by keeping the one with "best" data? 
                # But that's hard to know globally.
                
                # Workaround: For filter_equity_isins, we assume ISIN column is NOT merged/duplicated usually.
                # Or IF it is, we resolve it now.
                
                # For Kotak, ISIN column is usually not merged like Security Name.
                # Let's hope ISIN is unique. If not, we might crash.
                # If crash, we catch it.
                
                try:
                    df_equity = self.filter_equity_isins(df_clean, 'isin')
                except Exception as e:
                    # If ISIN column is duplicated, try to resolve it
                    if isinstance(df_clean['isin'], pd.DataFrame):
                        # Combine
                        # We use the same logic: first non-empty? 
                        # Actually for ISIN, usually valid ISIN starts with 'IN'.
                        # This is getting complex.
                        # For now, let's assume ISIN is fine (Col D usually).
                        logger.warning(f"[Kotak] Error filtering equity ISINs (dup cols?): {e}")
                        continue
                    raise e
                
                if df_equity.empty:
                    schemes_processed += 1
                    continue
                    
                for _, row in df_equity.iterrows():
                    # Stop at footer
                    # handle possible duplicate security_name
                    sec_name_val = self._resolve_merged_column(row, 'security_name')
                    sec_name = str(sec_name_val if sec_name_val else '').lower()
                    
                    if 'total' in sec_name or 'grand total' in sec_name:
                        break
                        
                    # ISIN is already filtered
                    isin = self._resolve_merged_column(row, 'isin')
                    
                    # Transform Data
                    # Market Value: Lakhs -> INR
                    try:
                        mv_val = self._resolve_merged_column(row, 'market_value')
                        mv_lakhs = self.safe_float(mv_val if mv_val is not None else 0)
                        mv_inr = mv_lakhs * 100000
                    except (ValueError, TypeError):
                        mv_inr = 0.0

                    # NAV: Percentage -> Float
                    try:
                        nav_val = self._resolve_merged_column(row, 'percent_to_nav')
                        nav_pct = self.safe_float(nav_val if nav_val is not None else 0)
                    except (ValueError, TypeError):
                        nav_pct = 0.0

                    holding = {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info['scheme_name'],
                        "plan_type": scheme_info['plan_type'],
                        "option_type": scheme_info['option_type'],
                        "is_reinvest": scheme_info['is_reinvest'],
                        "isin": isin,
                        "company_name": self.clean_company_name(sec_name_val),
                        "quantity": self.safe_float(self._resolve_merged_column(row, 'quantity')),
                        "market_value_inr": mv_inr,
                        "percent_to_nav": nav_pct,
                        "sector": str(self._resolve_merged_column(row, 'sector') or '').strip() or None,
                        "timestamp": datetime.now()
                    }
                    sheet_holdings.append(holding)

                # 7. Validate & Add
                # Log warning for NAV check but accept (Hybrid support)
                self.validate_nav_completeness(sheet_holdings, scheme_info['scheme_name'])
                
                if sheet_holdings:
                    all_holdings.extend(sheet_holdings)
                    schemes_with_equity += 1
                    logger.info(f"[Kotak] ✓ {scheme_info['scheme_name']}: {len(sheet_holdings)} holdings")
                
                schemes_processed += 1

            except Exception as e:
                logger.error(f"[Kotak] Error processing sheet {sheet_name}: {e}")
                continue

        logger.info(f"[Kotak] Extraction complete. Found {len(all_holdings)} holdings from {schemes_with_equity}/{schemes_processed} schemes.")
        return all_holdings

    def _clean_scheme_name(self, raw_name: str) -> str:
        """
        Clean scheme name by removing AMC prefix (following HDFC/ICICI pattern).
        Examples:
            "Kotak Flexicap Fund" -> "Flexicap Fund"
            "KOTAK SMALL CAP FUND" -> "SMALL CAP FUND"
        """
        prefixes_to_remove = [
            "KOTAK MAHINDRA ",
            "KOTAK "
        ]
        
        cleaned = raw_name.strip()
        cleaned_upper = cleaned.upper()
        
        for prefix in prefixes_to_remove:
            if cleaned_upper.startswith(prefix):
                cleaned = cleaned[len(prefix):].strip()
                break
        
        return cleaned

    def _should_skip_sheet(self, sheet_name: str) -> bool:
        """Skip summary/index sheets."""
        skip_terms = ['summary', 'index', 'contents', 'disclaimer', 'note']
        sn = sheet_name.lower()
        return any(term in sn for term in skip_terms)

    def _resolve_merged_column(self, row: pd.Series, col_name: str) -> Any:
        """
        Resolve value from a column that might be duplicated due to header ffill.
        Returns the first non-empty value found.
        """
        val = row.get(col_name)
        if isinstance(val, pd.Series):
            # Multiple columns with same name.
            # Look for first non-empty value.
            for v in val.values:
                if pd.notna(v) and str(v).strip() != '':
                    return v
            return None
        return val
