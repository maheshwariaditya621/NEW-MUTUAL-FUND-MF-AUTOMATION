from typing import List, Dict, Any
import pandas as pd
import re
from src.config import logger
from src.extractors.base_extractor import BaseExtractor

class BarodaExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Baroda BNP Paribas Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_name="Baroda BNP Paribas Mutual Fund", version="v1")
        self.column_mapping = {
            "ISIN": "isin",
            "NAME OF THE INSTRUMENT": "company_name",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE": "market_value_inr",
            "MARKET VALUE": "market_value_inr",
            "FAIR VALUE": "market_value_inr",
            "INDUSTRY / RATING": "sector",
            "INDUSTRY/RATING": "sector",
            "RATINGS / INDUSTRY": "sector",
            "% TO NET ASSETS": "percent_of_nav",
            "% TO NAV": "percent_of_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting data from Baroda file: {file_path}")
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            # Skip only real summary/index-of-sheets pages, not index FUNDS
            if any(kw in sheet_name.upper() for kw in ["SUMMARY", "INDEX OF"]) and "INDEX FUND" not in sheet_name.upper():
                if sheet_name.upper() == "BOBBNPMF INDEX":
                    continue

            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_raw.empty or len(df_raw) < 5:
                continue

            # 1. Extract Scheme Name
            # Baroda puts it in Row 0 or Row 1, Col 1
            scheme_name_raw = ""
            for r in [0, 1]:
                if len(df_raw) > r and df_raw.shape[1] > 1:
                    val = df_raw.iloc[r, 1]
                    if pd.notna(val) and len(str(val).strip()) > 5:
                        scheme_name_raw = str(val).strip()
                        break
            
            if not scheme_name_raw:
                scheme_name_raw = sheet_name.strip()

            scheme_info = self.parse_verbose_scheme_name(scheme_name_raw)

            # 2. Find Header
            header_idx = self.find_header_row(df_raw)
            if header_idx == -1:
                logger.warning(f"Could not find header for sheet: {sheet_name}")
                continue

            # 3. Read Data
            df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
            raw_columns = df.columns.tolist()
            global_unit = self.scan_sheet_for_global_units(df_raw.head(15))

            df = self._map_columns(df)
            if "isin" not in df.columns:
                continue

            value_unit = self._resolve_value_unit(raw_columns, global_unit)

            # Extract Total Net Assets (AUM) from the sheet's footer using df_raw
            raw_net_assets = None
            for idx, row in df_raw.iterrows():
                row_vals = [str(val).upper() if pd.notna(val) else "" for val in row.values]
                row_text = " ".join(row_vals)
                if "GRAND TOTAL" in row_text or "NET ASSETS" in row_text or "TOTAL AUM" in row_text:
                    candidates = []
                    for val in row.values:
                        f_val = self.safe_float(val)
                        if f_val is not None and f_val > 105: # Avoid 100.00%
                            candidates.append(f_val)
                    
                    if candidates:
                        if len(candidates) > 1:
                             if abs(candidates[-1] - 100.0) < 0.05:
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

            # Use filter_equity_isins from BaseExtractor
            equity_df = pd.DataFrame()
            if "isin" in df.columns:
                equity_df = self.filter_equity_isins(df, "isin")
            
            sheet_holdings: List[Dict[str, Any]] = []
            if not equity_df.empty:
                for _, row in equity_df.iterrows():
                    # Baroda percentage can be decimal (0.0691) or string with %
                    raw_nav = row.get("percent_of_nav", 0)
                    if isinstance(raw_nav, str):
                        raw_nav = raw_nav.replace("%", "").strip()
                    
                    nav_parsed = self.parse_percentage(raw_nav)

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
                            "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), value_unit),
                            "percent_of_nav": nav_parsed,
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

            # Use validate_nav_completeness (modified to allow high counts for indices)
            if self.validate_nav_completeness_custom(sheet_holdings, scheme_info["scheme_name"]):
                all_holdings.extend(sheet_holdings)

        logger.info(f"Successfully extracted {len(all_holdings)} equity holdings from {file_path}")
        return all_holdings

    def validate_nav_completeness_custom(self, holdings: List[Dict[str, Any]], scheme_name: str) -> bool:
        if not holdings: return False
        
        total_nav_pct = sum(h.get('percent_of_nav', 0.0) for h in holdings)
        isin_count = len(holdings)
        
        # User Policy: Warning only for low NAV
        if not (90.0 <= total_nav_pct <= 105.0):
            logger.warning(f"[{scheme_name}] NAV GUARD WARNING: {total_nav_pct:.2f}% (Range 90-105%).")

        # Large Count Guard
        max_limit = 200
        upper_name = scheme_name.upper()
        if any(kw in upper_name for kw in ["INDEX", "TOTAL MARKET", "SMALL CAP", "MID CAP", "FOCUSED"]):
            max_limit = 1000
            
        if isin_count > max_limit:
            logger.error(f"[{scheme_name}] FAILED COUNT GUARD: {isin_count} (Max {max_limit}).")
            return False
            
        return True

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        new_cols = {}
        for col in df.columns:
            # Normalize whitespace and collapse newlines
            col_norm = re.sub(r'\s+', ' ', str(col)).strip().upper()
            for pattern, canonical in self.column_mapping.items():
                if pattern in col_norm:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)

    def _resolve_value_unit(self, raw_columns: List[Any], default_unit: str) -> str:
        for col in raw_columns:
            col_upper = str(col).upper()
            if "MARKET" in col_upper and "VALUE" in col_upper:
                detected = self.detect_units(str(col))
                if detected != "RUPEES":
                    return detected
        return default_unit
