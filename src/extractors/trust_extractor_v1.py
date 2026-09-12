import pandas as pd
import re
from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger

class TrustExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Trust Mutual Fund.
    """

    def __init__(self):
        super().__init__(amc_name="Trust Mutual Fund", version="V1")
        # STRICT RULE: Must find ISIN AND (Instrument OR Issuer OR Company).
        self.header_keywords = ["ISIN", "INSTRUMENT"]

    def parse_verbose_scheme_name(self, raw_name: str) -> Dict[str, Any]:
        """
        Trust specific parsing.
        Enforces 'TRUST ' prefix and replaces 'TRUSTMF' with 'TRUST'.
        """
        if pd.isna(raw_name): name = ""
        else: name = str(raw_name).strip()
        
        # Fix encoding issues
        name = self.fix_mojibake(name)
        
        # Replace TRUSTMF with TRUST (case insensitive)
        name = re.sub(r'(?i)TRUSTMF', 'TRUST', name)
        
        # Normalize spaces
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Enforce TRUST Prefix
        if not name.upper().startswith("TRUST "):
            name = "TRUST " + name
            
        return super().parse_verbose_scheme_name(name)

    def extract_scheme_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extracts scheme name robustly, handling both old and new Trust formats:

        OLD FORMAT (Jan 2026 and earlier):
          Row 0: 'MONTHLY PORTFOLIO STATEMENT AS ON ...'
          Row 1: SEBI regulation text
          Row 3: Scheme name ('TRUSTMF Banking & PSU Fund...')
          Row 4: Header row

        NEW FORMAT (Apr 2026 onwards):
          Row 0: Sheet code ('TMFLIQ')
          Row 1: Scheme name ('TRUSTMF Liquid Fund')  ← moved here
          Row 4: 'Monthly Portfolio Statement as on...'
          Row 5: Header row

        Strategy: scan rows 0 → (header_idx - 1) for any cell containing
        'TRUST' (case-insensitive) that is not a boilerplate line.
        """
        # First find the header so we know how far to scan
        header_idx = self.find_header_row(df, self.header_keywords)
        scan_limit = header_idx if header_idx != -1 else min(10, len(df))

        # Boilerplate phrases to skip
        boilerplate_markers = [
            "MONTHLY PORTFOLIO", "PURSUANT TO", "SEBI", "REGULATION",
            "SECURITIES AND EXCHANGE", "PORTFOLIO STATEMENT",
        ]

        for i in range(scan_limit):
            row_vals = [
                str(v).strip()
                for v in df.iloc[i].values
                if pd.notna(v) and str(v).strip() and str(v).strip() != "nan"
            ]
            for val in row_vals:
                # Must reference TRUST and not be a boilerplate header
                if "TRUST" in val.upper() and len(val) > 5:
                    val_upper = val.upper()
                    if not any(bp in val_upper for bp in boilerplate_markers):
                        # Strip embedded newlines (cell may have name + description)
                        clean = val.split("\n")[0].strip()
                        return self.parse_verbose_scheme_name(clean)

        return self.parse_verbose_scheme_name("Unknown Trust Scheme")


    def _extract_total_aum(self, df: pd.DataFrame, unit: str = "LAKHS") -> float:
        """Find Grand Total row and extract value from the Market Value column."""
        for i in range(len(df)):
            row = df.iloc[i]
            for c in range(min(5, len(row))):
                val_str = str(row.iloc[c]).strip().upper().replace('_', ' ')
                if len(val_str) > 35:
                    continue
                if any(bad in val_str for bad in ["EXPOSURE", "PERCENTAGE", "HEDGED", "FUTURES", "OPTIONS", "PER UNIT", "AGGREGATE"]):
                    continue
                if "GRAND TOTAL" in val_str or val_str in ["NET ASSETS", "TOTAL NET ASSETS"]:
                    candidates = []
                    for val in row:
                        f_val = self.safe_float(val)
                        if f_val is not None and f_val > 0 and abs(f_val - 1.0) > 0.001 and abs(f_val - 100.0) > 0.1 and f_val < 20000000:
                            candidates.append(f_val)
                    if candidates:
                        return self.normalize_currency(candidates[0], unit)
        return 0.0

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        holdings = []
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                if "XDO MET" in sheet_name:
                    continue
                    
                logger.info(f"Processing Trust sheet: {sheet_name}")
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                
                if df_raw.empty:
                    continue

                scheme_info = self.extract_scheme_info(df_raw)
                header_idx = self.find_header_row(df_raw, self.header_keywords)

                if header_idx == -1:
                    logger.warning(f"Header not found in sheet: {sheet_name}")
                    continue

                # Fetch Total AUM from the FULL dataframe
                normalized_net_assets = self._extract_total_aum(df_raw)
                if normalized_net_assets == 0:
                    normalized_net_assets = None

                # Prepare headers and data (normalize all newlines and whitespace)
                headers = [re.sub(r'[\s\n\r]+', ' ', str(h)).strip().upper() for h in df_raw.iloc[header_idx]]
                df = df_raw.iloc[header_idx + 1:].copy()
                
                # Handle potential column count mismatch
                if len(headers) > df.shape[1]:
                    headers = headers[:df.shape[1]]
                elif len(headers) < df.shape[1]:
                    df = df.iloc[:, :len(headers)]
                
                df.columns = headers

                # Map columns
                col_map = {
                    "NAME OF THE INSTRUMENT": "company_name",
                    "NAME OF INSTRUMENT": "company_name",
                    "ISIN": "isin",
                    "QUANTITY": "quantity",
                    "MARKET/FAIR VALUE": "market_value_inr",
                    "MARKET VALUE": "market_value_inr",
                    "FAIR VALUE": "market_value_inr",
                    "% TO NET ASSETS": "percent_of_nav",
                    "% TO NAV": "percent_of_nav"
                }

                final_map = {}
                for col in df.columns:
                    col_upper = str(col).upper()
                    for key, val in col_map.items():
                        if key in col_upper:
                            final_map[col] = val
                            break

                # Ensure mandatory columns exist
                if 'isin' not in final_map.values():
                    logger.warning(f"ISIN column not found in sheet: {sheet_name}")
                    continue

                # Extract rows
                sheet_holdings = []
                for _, row in df.iterrows():
                    # Stop logic: row starts with "Subtotal" or "Total" or "GRAND TOTAL"
                    row_0_val = str(row.iloc[0]).upper().strip()
                    if any(kw in row_0_val for kw in ["SUBTOTAL", "TOTAL", "GRAND TOTAL"]):
                        break
                    
                    row_text = " ".join([str(v).upper() for v in row.values if not pd.isna(v)])
                    if any(kw in row_text for kw in ["NET ASSETS", "GRAND TOTAL"]):
                        break

                    raw_data = {final_map[c]: row[c] for c in final_map if c in df.columns}
                    
                    if not raw_data.get('isin') or pd.isna(raw_data['isin']):
                        continue

                    # Filter equity
                    if not self.is_valid_equity_isin(str(raw_data['isin'])):
                        continue

                    # Clean and parse
                    record = {
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info['scheme_name'],
                        "scheme_description": scheme_info['description'],
                        "plan_type": scheme_info['plan_type'],
                        "option_type": scheme_info['option_type'],
                        "is_reinvest": scheme_info['is_reinvest'],
                        "isin": self.clean_isin(raw_data['isin']),
                        "company_name": self.clean_company_name(raw_data.get('company_name', 'N/A')),
                        "quantity": self.safe_float(raw_data.get('quantity')),
                        "market_value_inr": self.normalize_currency(raw_data.get('market_value_inr'), "LAKHS"),
                        "percent_of_nav": self.parse_percentage(raw_data.get('percent_of_nav', 0.0)),
                        "sector": self.clean_company_name(row.get('Rating/Industry', row.get('Rating', 'N/A'))),
                        "total_net_assets": normalized_net_assets
                    }
                    sheet_holdings.append(record)

                if not sheet_holdings and normalized_net_assets:
                    # Ghost Holding for Non-Equity funds
                    sheet_holdings.append({
                        "amc_name": self.amc_name,
                        "scheme_name": scheme_info['scheme_name'],
                        "scheme_description": scheme_info['description'],
                        "plan_type": scheme_info['plan_type'],
                        "option_type": scheme_info['option_type'],
                        "is_reinvest": scheme_info['is_reinvest'],
                        "isin": None,
                        "company_name": "N/A",
                        "quantity": 0,
                        "market_value_inr": 0,
                        "percent_of_nav": 0,
                        "sector": "N/A",
                        "total_net_assets": normalized_net_assets
                    })

                if sheet_holdings:
                    if self.validate_nav_completeness(sheet_holdings, scheme_info['scheme_name']):
                        holdings.extend(sheet_holdings)

        except Exception as e:
            logger.error(f"Error extracting Trust MF file {file_path}: {e}")
            
        return holdings
