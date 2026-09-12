import pandas as pd
import logging
import re
from typing import List, Dict, Any, Optional
from src.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

class BOIExtractorV1(BaseExtractor):
    def __init__(self):
        super().__init__(amc_name="Bank of India Mutual Fund", version="V1")
        # Standard column mapping for BOI
        self.column_mapping = {
            "NAME OF THE INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY / RATING": "sector",
            "RATING": "sector",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE (RS. IN LACS)": "market_value_inr",
            "% TO NET ASSETS": "percent_of_nav"
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        all_holdings = []
        
        with pd.ExcelFile(file_path) as xls:
            # Build index map from Index sheet if available
            index_map = {}
            for s in xls.sheet_names:
                if s.strip().lower() == 'index':
                    try:
                        df_idx = pd.read_excel(xls, sheet_name=s, header=None)
                        for _, row in df_idx.iterrows():
                            code = str(row[0]).strip()
                            name = str(row[1]).strip() if pd.notna(row[1]) else ""
                            if code and name and name.lower() != "scheme names":
                                clean_name = re.sub(r'\(.*?\)', '', name, flags=re.DOTALL).strip()
                                clean_name = re.sub(r'\s+', ' ', clean_name)
                                index_map[code.upper()] = clean_name
                    except Exception as e:
                        logger.warning(f"Error parsing BOI Index sheet: {e}")
                    break

            for sheet_name in xls.sheet_names:
                if sheet_name.strip().lower() in ['index', 'f & o']:
                    continue
                
                logger.info(f"Processing sheet: {sheet_name}")
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                
                if df_raw.empty:
                    continue

                # 1. Header Detection (expecting Row 5)
                header_idx = self.find_header_row(df_raw, keywords=["ISIN"])
                if header_idx == -1:
                    logger.warning(f"Could not find header row in sheet '{sheet_name}'")
                    continue

                # 2. Extract global units (usually in Row 5 or headers)
                global_unit = "LAKHS" # BOI standard
                
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
                    continue

                # 4. Scheme Info
                # Priority: 1. Index Sheet mapping | 2. Row 1 Col 1 | 3. Row 0 Col 1 | 4. Sheet Name
                raw_scheme_text = index_map.get(sheet_name.strip().upper())
                if not raw_scheme_text:
                    for r_idx in [1, 0, 2]:
                        if df_raw.shape[0] > r_idx and df_raw.shape[1] > 1:
                            val = str(df_raw.iloc[r_idx, 1]).strip()
                            if val and val.lower() != 'nan' and not val.lower().startswith('name of mutual fund'):
                                raw_scheme_text = val
                                break
                if not raw_scheme_text:
                    raw_scheme_text = str(sheet_name)
                
                # Clean up parenthetical descriptions
                raw_scheme_text = re.sub(r'\(.*?\)', '', raw_scheme_text, flags=re.DOTALL).strip()
                raw_scheme_text = re.sub(r'\s+', ' ', raw_scheme_text)
                
                scheme_info = self.parse_verbose_scheme_name(raw_scheme_text)
                
                # Resolve value unit
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
                            "isin": row.get("isin"),
                            "company_name": self.clean_company_name(row.get("company_name")),
                            "quantity": int(self.normalize_currency(row.get("quantity", 0), "RUPEES")),
                            "market_value_inr": self.normalize_currency(row.get("market_value_inr", 0), value_unit),
                            "percent_of_nav": self.safe_float(row.get("percent_of_nav", 0)) * 100.0,
                            "sector": row.get("sector", "Other"),
                            "total_net_assets": normalized_net_assets
                        })
                
                if not sheet_holdings and normalized_net_assets:
                    # Ghost Holding for Non-Equity funds
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
                
                sheet_total_nav = sum(h.get('percent_of_nav', 0.0) for h in sheet_holdings)
                logger.info(f"[{sheet_name}] Extracted {len(sheet_holdings)} holdings. Total NAV: {sheet_total_nav:.2f}%")
                
                if self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"]):
                    all_holdings.extend(sheet_holdings)
        
        return all_holdings

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        new_cols = {}
        for col in df.columns:
            col_norm = str(col).strip().upper()
            for pattern, canonical in self.column_mapping.items():
                if pattern.upper() in col_norm:
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
