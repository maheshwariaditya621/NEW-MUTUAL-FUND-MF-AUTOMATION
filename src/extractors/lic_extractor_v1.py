import pandas as pd
import re
from typing import List, Dict, Any, Optional
from src.extractors.common_extractor_v1 import CommonExtractorV1
from src.config import logger

class LICExtractorV1(CommonExtractorV1):
    """
    Dedicated extractor for LIC Mutual Fund.
    Excel Structure:
    - Header Row: index 3 (Row 4).
    - Scheme Name: Row 0, Column 1.
    - Units: Values are in Lakhs ("Rs. In Lacs").
    """

    def __init__(self):
        super().__init__(amc_slug="lic", amc_name="LIC Mutual Fund")

    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        LIC headers are typically in Row 4 (index 3).
        """
        for i in range(min(len(df), 10)):
            row_str = " ".join([str(v).upper() for v in df.iloc[i].values if pd.notna(v)])
            if "ISIN" in row_str and any(k in row_str for k in ["QUANTITY", "MARKET/FAIR VALUE", "INDUSTRY"]):
                return i
        return 3 # Default fallback

    def _extract_scheme_name(self, df: pd.DataFrame, header_idx: int, sheet_name: str) -> str:
        """
        Extract scheme name from Row 0, Column 1.
        """
        if len(df) > 0 and df.shape[1] > 1:
            val = df.iloc[0, 1]
            if pd.notna(val) and len(str(val).strip()) > 5:
                # Clean prefix/suffix
                name = str(val).strip()
                # Remove "MF" or "Mf" (case insensitive)
                name = re.sub(r"(?i)\bMF\b", "", name).strip()
                # Handle double spaces resulting from removal
                name = re.sub(r"\s+", " ", name)
                return name.title()
        
        fallback = sheet_name.replace("lic equity ", "").replace("lic debt ", "").strip().title()
        return re.sub(r"(?i)\bMF\b", "", fallback).strip()

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Specific mapping for LIC columns.
        """
        col_map = {
            "NAME OF THE INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY / RATING": "sector",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE (ROUNDED, RS. IN LACS)": "market_value_inr",
            "ROUNDED, % TO NET ASSETS": "percent_of_nav"
        }
        
        new_cols = {}
        for col in df.columns:
            clean_col = str(col).strip().upper()
            # Handle variations in name (e.g. multi-line headers)
            matched = False
            for target_key, canonical in col_map.items():
                if target_key in clean_col:
                    new_cols[col] = canonical
                    matched = True
                    break
        
        return df.rename(columns=new_cols)

    def _extract_total_aum(self, df: pd.DataFrame, unit: str = "LAKHS") -> float:
        """Find GRAND TOTAL or NET ASSETS row and extract value."""
        # Scan from bottom up
        for i in range(len(df)-1, -1, -1):
            row = df.iloc[i]
            row_text = ' '.join([str(v).upper() for v in row if pd.notna(v)])
            
            is_valid_marker = False
            if "GRAND TOTAL" in row_text:
                is_valid_marker = True
            elif "NET ASSETS" in row_text and "PER UNIT" not in row_text and "PERCENTAGE TO" not in row_text:
                is_valid_marker = True
                
            if is_valid_marker:
                candidates = []
                for val in row:
                    f_val = self.safe_float(val)
                    # Filter out percentages (like 1.0 or 100.0) usually found in the last column
                    if f_val > 0 and abs(f_val - 1.0) > 0.001 and abs(f_val - 100.0) > 0.1:
                        candidates.append(f_val)
                
                if candidates:
                    return self.normalize_currency(max(candidates), unit)
        return 0.0

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from LIC file: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            if any(k in str(sheet_name).upper() for k in ["INDEX", "SUMMARY", "COMBINE INDEX"]):
                continue

            # Read raw for header detection and scheme name
            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=10)
            if df_raw.empty:
                continue

            header_idx = self.find_header_row(df_raw)
            full_scheme_name = self._extract_scheme_name(df_raw, header_idx, str(sheet_name))
            
            # Read data with actual headers
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            df = self._map_columns(df)

            if "isin" not in df.columns:
                logger.warning(f"ISIN column not found in sheet {sheet_name}")
                continue

            equity_df = self.filter_equity_isins(df, "isin")
            
            # Fetch Total AUM from the full dataframe (scanning bottom-up)
            df_full = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            normalized_net_assets = self._extract_total_aum(df_full)
            if normalized_net_assets == 0:
                normalized_net_assets = None

            scheme_info = self.parse_verbose_scheme_name(full_scheme_name)

            sheet_holdings: List[Dict[str, Any]] = []
            if not equity_df.empty:
                for _, row in equity_df.iterrows():
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
                            "quantity": int(self.normalize_currency(row.get("quantity", 0), "RUPEES")),
                            "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), "LAKHS"),
                            "percent_of_nav": self.parse_percentage(row.get("percent_of_nav", 0)),
                            "sector": row.get("sector", None),
                            "total_net_assets": normalized_net_assets
                        }
                    )

            if not sheet_holdings and normalized_net_assets:
                # Ghost Holding for Non-Equity funds
                sheet_holdings.append(
                    {
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
                    }
                )

            # Deduplicate by summing values (if any duplicates exist in raw sheet)
            merged_holdings = self.merge_duplicate_isins(sheet_holdings)
            
            self.validate_nav_completeness(merged_holdings, scheme_info["scheme_name"])
            all_holdings.extend(merged_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings

    def validate_nav_completeness(self, holdings: List[Dict[str, Any]], scheme_name: str) -> bool:
        """
        LIC has many hybrid/arbitrage funds where equity is only 65-75%.
        We override to lower the threshold for these types.
        """
        if not holdings:
            logger.error(f"[{scheme_name}] FAILED: No holdings extracted.")
            return False
            
        total_nav_pct = sum(h.get('percent_of_nav', 0.0) for h in holdings)
        
        # Determine threshold based on scheme type
        threshold = 90.0
        hybrid_keywords = ["ARBITRAGE", "BALANCED", "HYBRID", "SAVINGS", "MULTI ASSET", "INSURANCE", "CHILDREN", "CONSUMPTION"]
        if any(kw in scheme_name.upper() for kw in hybrid_keywords):
            threshold = 25.0 # Lowered for hybrid/arbitrage
            logger.info(f"[{scheme_name}] Using HYBRID NAV threshold: {threshold}%")
        
        if total_nav_pct < threshold:
            logger.error(f"[{scheme_name}] FAILED NAV GUARD: {total_nav_pct:.2f}% (Threshold {threshold}%)")
            return False
            
        if total_nav_pct > 105.0:
            logger.error(f"[{scheme_name}] FAILED NAV GUARD: {total_nav_pct:.2f}% (Max 105%)")
            return False

        return True

    def merge_duplicate_isins(self, holdings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge holdings with same ISIN in the same scheme/sheet."""
        merged = {}
        for h in holdings:
            isin = h['isin']
            if isin in merged:
                merged[isin]['quantity'] += h['quantity']
                merged[isin]['market_value_inr'] += h['market_value_inr']
                merged[isin]['percent_of_nav'] = float(merged[isin]['percent_of_nav']) + float(h['percent_of_nav'])
            else:
                merged[isin] = h
        return list(merged.values())
