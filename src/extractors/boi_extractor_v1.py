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
            for sheet_name in xls.sheet_names:
                if sheet_name.lower() == 'index':
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
                # BOI usually has scheme name in Row 0
                raw_scheme_text = str(df_raw.iloc[0, 1]).strip() if df_raw.shape[1] > 1 else str(sheet_name)
                if pd.isna(raw_scheme_text) or raw_scheme_text.lower() == 'nan':
                    raw_scheme_text = str(sheet_name)
                
                # Clean up "Monthly Portfolio Statement" if it's in the text
                raw_scheme_text = re.sub(r'\(.*?\)', '', raw_scheme_text).strip()
                
                scheme_info = self.parse_verbose_scheme_name(raw_scheme_text)
                
                # Resolve value unit
                value_unit = self._resolve_value_unit(raw_columns, global_unit)
                
                sheet_holdings = []
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
