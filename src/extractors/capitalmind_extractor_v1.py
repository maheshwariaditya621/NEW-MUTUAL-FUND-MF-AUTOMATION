import pandas as pd
import logging
import re
from typing import List, Dict, Any, Optional
from src.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

class CapitalmindExtractorV1(BaseExtractor):
    def __init__(self):
        super().__init__(amc_name="Capitalmind Mutual Fund", version="V1")
        # Standard column mapping for Capitalmind
        self.column_mapping = {
            "NAME OF THE INSTRUMENT / ISSUER": "company_name",
            "NAME OF THE INSTRUMENT": "company_name",
            "ISIN": "isin",
            "RATING / INDUSTRY^": "sector",
            "RATING": "sector",
            "INDUSTRY": "sector",
            "QUANTITY": "quantity",
            "MARKET VALUE\n(RS. IN LAKHS)": "market_value_inr",
            "MARKET VALUE (RS. IN LAKHS)": "market_value_inr",
            "% TO NAV": "percent_of_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        all_holdings = []
        
        with pd.ExcelFile(file_path) as xls:
            for sheet_name in xls.sheet_names:
                if sheet_name.lower() == 'index':
                    continue
                
                logger.info(f"Processing sheet: {sheet_name}")
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                
                if df_raw.empty:
                    continue

                # 1. Header Detection (expecting Row 7)
                header_idx = self.find_header_row(df_raw, keywords=["ISIN"])
                if header_idx == -1:
                    logger.warning(f"Could not find header row in sheet '{sheet_name}'")
                    continue

                # 2. Extract global units
                global_unit = "LAKHS" # Capitalmind standard
                
                # Re-read with correct header
                df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
                raw_columns = df.columns.tolist()
                
                df = self._map_columns(df)
                
                if "isin" not in df.columns:
                    logger.warning(f"ISIN column not found in sheet '{sheet_name}' after mapping")
                    continue

                # 3. Equity Filter
                equity_df = self.filter_equity_isins(df, "isin")
                if equity_df.empty:
                    logger.info(f"[{sheet_name}] No equity holdings found. Proceeding for potential non-equity AUM.")

                # 4. Scheme Info - More robust detection
                raw_scheme_text = ""
                potential_rows = df_raw.head(15).values
                for r_idx, row_vals in enumerate(potential_rows):
                    row_str = " ".join([str(v) for v in row_vals if not pd.isna(v)])
                    if "SCHEME NAME" in row_str.upper():
                        # Use everything after "SCHEME NAME :"
                        if ":" in row_str:
                            raw_scheme_text = row_str.split(":", 1)[1].strip()
                        else:
                            # Fallback if colon is missing
                            raw_scheme_text = row_str.replace("SCHEME NAME", "", 1).strip()
                        break
                
                if not raw_scheme_text:
                    logger.warning(f"[{sheet_name}] Could not find SCHEME NAME row. Falling back to sheet name.")
                    raw_scheme_text = str(sheet_name)
                
                # Clean up verbose descriptions in parentheses (usually investment objective)
                # We use re.DOTALL (via flag) or [\s\S] to handle newlines inside the parentheses
                raw_scheme_text = re.sub(r'\s*\(.*?\)', '', raw_scheme_text, flags=re.DOTALL).strip()
                
                logger.info(f"[{sheet_name}] Detected Scheme: {raw_scheme_text}")
                scheme_info = self.parse_verbose_scheme_name(raw_scheme_text)
                
                # Resolve value unit
                value_unit = self._resolve_value_unit(raw_columns, global_unit)
                
                # 5. Extract Total AUM (Net Assets)
                total_aum = self._extract_total_aum(df_raw, value_unit)
                logger.info(f"[{sheet_name}] Extracted Total AUM: {total_aum:,.2f} INR")

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
                            "isin": self.clean_isin(row.get("isin")),
                            "company_name": self.clean_company_name(row.get("company_name")),
                            "quantity": int(self.normalize_currency(row.get("quantity", 0), "RUPEES")),
                            "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), value_unit),
                            "percent_of_nav": self.safe_float(row.get("percent_of_nav", 0)) * 100.0,
                            "sector": row.get("sector", "Other"),
                            "total_net_assets": total_aum
                        })

                # Always ensure at least one record exists to carry Total AUM
                if not sheet_holdings:
                    sheet_holdings.append({
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info["scheme_name"],
                        "scheme_description": scheme_info["description"],
                        "plan_type": scheme_info["plan_type"],
                        "option_type": scheme_info["option_type"],
                        "is_reinvest": scheme_info["is_reinvest"],
                        "isin": "IN9999999999",
                        "company_name": "NON-EQUITY ASSETS",
                        "quantity": 0,
                        "market_value_inr": 0,
                        "percent_of_nav": 0.0,
                        "sector": "Other",
                        "total_net_assets": total_aum
                    })
                
                sheet_total_nav = sum(h.get('percent_of_nav', 0.0) for h in sheet_holdings)
                logger.info(f"[{sheet_name}] Extracted {len(sheet_holdings)} holdings. Total NAV: {sheet_total_nav:.2f}%")
                
                # Validation bypass for ghost holdings (non-equity schemes)
                is_ghost = len(sheet_holdings) == 1 and sheet_holdings[0]["isin"] == "IN9999999999"
                if is_ghost or self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"]):
                    all_holdings.extend(sheet_holdings)
        
        return all_holdings

    def _extract_total_aum(self, df: pd.DataFrame, unit: str) -> float:
        """Find GRAND TOTAL or NET ASSETS row and extract value."""
        # 1. First priority: Look for GRAND TOTAL (usually the last numeric footer row)
        for i in range(len(df)-1, -1, -1):
            row = df.iloc[i]
            row_text = ' '.join([str(v).upper() for v in row if pd.notna(v)])
            if "GRAND TOTAL" in row_text:
                candidates = []
                for val in row:
                    f_val = self.safe_float(val)
                    if f_val > 0 and abs(f_val - 100.0) > 0.01:
                        candidates.append(f_val)
                if candidates:
                    return self.normalize_currency(candidates[0], unit)
        
        # 2. Fallback: Look for NET ASSETS
        for i in range(len(df)-1, -1, -1):
            row = df.iloc[i]
            row_text = ' '.join([str(v).upper() for v in row if pd.notna(v)])
            if "NET ASSETS" in row_text:
                # If it's a long description string with a colon, try to parse after colon
                full_text = ' '.join([str(v) for v in row if pd.notna(v)])
                if ":" in full_text:
                    parts = full_text.split(":")
                    for p in parts[1:]:
                        f_val = self.safe_float(p)
                        if f_val > 0 and abs(f_val - 100.0) > 0.01:
                            return self.normalize_currency(f_val, unit)
                
                # Otherwise look for a separate column
                for val in row:
                    f_val = self.safe_float(val)
                    if f_val > 0 and abs(f_val - 100.0) > 0.01:
                        return self.normalize_currency(f_val, unit)
                        
        return 0.0

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        new_cols = {}
        for col in df.columns:
            # Normalize whitespace: replace newlines/tabs with space
            col_norm = re.sub(r'\s+', ' ', str(col)).strip().upper()
            for pattern, canonical in self.column_mapping.items():
                pattern_norm = re.sub(r'\s+', ' ', pattern).upper()
                if pattern_norm in col_norm:
                    new_cols[col] = canonical
                    break
        return df.rename(columns=new_cols)

    def _resolve_value_unit(self, columns: List[str], default_unit: str) -> str:
        for col in columns:
            col_upper = str(col).upper()
            if "CRORE" in col_upper:
                return "CRORES"
            if "LAKH" in col_upper:
                return "LAKHS"
        return default_unit
