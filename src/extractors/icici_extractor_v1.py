"""
ICICI Prudential Mutual Fund Extractor (Version 1)

Extracts equity holdings from ICICI merged Excel files and outputs
data in canonical schema format.

ICICI File Structure:
- Row 0: "ICICI Prudential Mutual Fund"
- Row 1: Scheme name
- Row 2: Portfolio date (e.g., "Portfolio as on Dec 31,2025")
- Row 3: Header row
- Row 4+: Data rows (with section headers mixed in)

Key Characteristics:
- Market values in Lakhs (multiply by 100,000)
- NAV percentages in decimal form (multiply by 100)
- Section headers have no ISIN (filtered out automatically)
"""

import pandas as pd
import re
from typing import Dict, Any, List
from datetime import datetime
from src.extractors.base_extractor import BaseExtractor
from src.config import logger
from src.config.constants import AMC_ICICI


class ICICIExtractorV1(BaseExtractor):
    """ICICI Prudential extractor following canonical schema."""

    def __init__(self):
        super().__init__(amc_name=AMC_ICICI, version="v1")
        
        # Exact column name mapping (case-sensitive matching)
        self.column_mapping = {
            "Company/Issuer/Instrument Name": "security_name",
            "ISIN": "isin",
            "Industry/Rating": "sector",
            "Quantity": "quantity",
            "Exposure/Market Value(Rs.Lakh)": "market_value",
            "% to Nav": "percent_to_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        """Extract all equity holdings from ICICI merged file."""
        logger.info(f"[ICICI] Starting extraction from: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        all_holdings = []
        schemes_processed = 0
        schemes_with_equity = 0

        for sheet_name in xls.sheet_names:
            try:
                # Read first 30 rows for metadata
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=30)
                if df_raw.empty:
                    continue

                # Extract metadata
                raw_scheme_name = self._extract_scheme_name(df_raw, sheet_name)
                report_date = self._extract_report_date(df_raw, file_path)
                
                # Clean scheme name (remove AMC prefix)
                cleaned_scheme_name = self._clean_scheme_name(raw_scheme_name)
                
                # Parse scheme info (plan type, option type, etc.)
                scheme_info = self.parse_verbose_scheme_name(cleaned_scheme_name)
                
                # Find header row
                header_idx = self.find_header_row(df_raw)
                if header_idx == -1:
                    logger.debug(f"[ICICI] No header in {sheet_name}, skipping")
                    continue

                # Read full sheet with proper header
                df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
                
                # Map columns using exact names
                df = self._map_columns(df)
                
                if "isin" not in df.columns:
                    logger.warning(f"[ICICI] No ISIN column in {sheet_name}, skipping")
                    continue

                # Filter equity holdings (this also removes section headers)
                equity_df = self.filter_equity_isins(df, "isin")
                
                if equity_df.empty:
                    logger.debug(f"[ICICI] No equity in {sheet_name}")
                    schemes_processed += 1
                    continue
                
                
                # Build holdings
                sheet_holdings = []
                
                # DEBUG: Check if percent_to_nav column exists
                if 'percent_to_nav' not in equity_df.columns:
                    logger.error(f"[ICICI] {scheme_name}: percent_to_nav column missing after mapping!")
                    logger.error(f"[ICICI] Available columns: {equity_df.columns.tolist()}")
                    schemes_processed += 1
                    continue
                
                for _, row in equity_df.iterrows():
                    # Market value: convert from Lakhs to INR
                    market_value_lakhs = self.safe_float(row.get("market_value", 0))
                    market_value_inr = market_value_lakhs * 100_000
                    
                    # NAV percentage: convert from decimal to percentage
                    nav_decimal = self.safe_float(row.get("percent_to_nav", 0))
                    nav_pct = nav_decimal * 100  # 0.061137 -> 6.1137
                    
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
                        "market_value_inr": market_value_inr,
                        "percent_to_nav": nav_pct,
                        "sector": row.get("sector", None)
                    }
                    sheet_holdings.append(holding)

                # Validate
                logger.debug(f"[ICICI] {scheme_info['scheme_name']}: Built {len(sheet_holdings)} holdings before validation")
                if len(sheet_holdings) > 0:
                    total_nav_debug = sum(h['percent_to_nav'] for h in sheet_holdings)
                    logger.debug(f"[ICICI] {scheme_info['scheme_name']}: Total NAV = {total_nav_debug:.2f}%")
                
                if self.validate_nav_completeness(sheet_holdings, scheme_info['scheme_name']):
                    all_holdings.extend(sheet_holdings)
                    schemes_with_equity += 1
                    logger.info(f"[ICICI] ✓ {scheme_info['scheme_name']}: {len(sheet_holdings)} holdings")
                else:
                    logger.warning(f"[ICICI] ✗ {scheme_info['scheme_name']}: Failed validation")
                
                schemes_processed += 1
                
            except Exception as e:
                logger.error(f"[ICICI] Error in {sheet_name}: {e}")
                continue

        logger.info(f"[ICICI] Complete: {len(all_holdings)} holdings from {schemes_with_equity}/{schemes_processed} schemes")
        return all_holdings

    def _clean_scheme_name(self, raw_name: str) -> str:
        """
        Clean scheme name by removing AMC prefix (following HDFC pattern).
        Examples:
            "ICICI PRUDENTIAL HOUSING OPPORTUNITIES FUND" -> "HOUSING OPPORTUNITIES FUND"
            "Active Momentum Fund" -> "Active Momentum Fund" (unchanged)
        """
        # Remove common AMC prefixes
        prefixes_to_remove = [
            "ICICI PRUDENTIAL ",
            "ICICI Prudential ",
            "ICICI "
        ]
        
        cleaned = raw_name.strip()
        for prefix in prefixes_to_remove:
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix):]
                break
        
        return cleaned
    
    def _extract_scheme_name(self, df_raw: pd.DataFrame, sheet_name: str) -> str:
        """Extract and clean scheme name from row 1."""
        scheme_name = sheet_name
        
        # Row 1 has the full scheme name
        if len(df_raw) > 1:
            row1_vals = [str(v).strip() for v in df_raw.iloc[1].values if not pd.isna(v)]
            for val in row1_vals:
                if len(val) > 10 and "FUND" in val.upper():
                    scheme_name = val
                    break
        
        # Clean prefixes
        scheme_name = scheme_name.replace("ICICI Prudential Mutual Fund - ", "")
        scheme_name = scheme_name.replace("ICICI Prudential ", "")
        
        # Remove trailing codes (e.g., "MOMACT")
        scheme_name = re.sub(r'\s+[A-Z]{5,}$', '', scheme_name).strip()
        
        return scheme_name

    def _extract_report_date(self, df_raw: pd.DataFrame, file_path: str) -> str:
        """Extract report date from row 2 or filename."""
        # Try row 2
        if len(df_raw) > 2:
            row2_vals = [str(v).strip() for v in df_raw.iloc[2].values if not pd.isna(v)]
            for val in row2_vals:
                if "PORTFOLIO" in val.upper() or "AS ON" in val.upper():
                    # Parse "Dec 31,2025" or similar
                    date_match = re.search(r'(\w+)\s+(\d{1,2}),\s*(\d{4})', val)
                    if date_match:
                        month_str, day, year = date_match.groups()
                        try:
                            date_obj = datetime.strptime(f"{month_str} {day} {year}", "%b %d %Y")
                            return date_obj.strftime("%Y-%m-%d")
                        except ValueError:
                            pass
        
        # Fallback: filename "CONSOLIDATED_ICICI_2025_12.xlsx"
        filename_match = re.search(r'(\d{4})_(\d{2})', file_path)
        if filename_match:
            year, month = filename_match.groups()
            # Last day of month
            if month == "02":
                day = "28"
            elif month in ["04", "06", "09", "11"]:
                day = "30"
            else:
                day = "31"
            return f"{year}-{month}-{day}"
        
        return "2025-12-31"

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map ICICI column names to canonical names."""
        new_cols = {}
        for col in df.columns:
            # Exact match for known columns
            if col in self.column_mapping:
                new_cols[col] = self.column_mapping[col]
        
        return df.rename(columns=new_cols)
