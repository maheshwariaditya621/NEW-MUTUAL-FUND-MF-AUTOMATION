from typing import Dict, Any, List
import re
import pandas as pd
from src.config import logger
from src.extractors.base_extractor import BaseExtractor

class SamcoExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Samco Mutual Fund.

    File structure variants:
    - v1 (up to Mar 2026): Row 0, Col 1 = Scheme Name
    - v2 (Apr 2026 onwards): Row 0, Col 1 = "SAMCO MUTUAL FUND" (AMC name),
      Row 2, Col 1 = "MONTHLY PORTFOLIO STATEMENT OF <FUND NAME> AS ON <DATE>"
    - Header: Row 4
    - Market Value: Rs. in Lakhs
    - NAV %: Decimal format (e.g. 0.0691 -> 6.91%)
    """

    # When Row 0, Col 1 equals this string, it is the AMC name — not a scheme name.
    _AMC_NAME_MARKERS = {"SAMCO MUTUAL FUND"}

    def __init__(self):
        super().__init__(amc_name="Samco Mutual Fund", version="v1")
        # Direct column mapping based on observed headers
        self.column_mapping = {
            "NAME OF THE INSTRUMENT": "company_name",
            "NAME OF INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY": "sector",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE": "market_value_inr",
            "MARKET/ FAIR VALUE": "market_value_inr",
            "LAKHS": "market_value_inr",
            "% TO NET": "percent_of_nav"
        }

    def _extract_total_aum(self, df: pd.DataFrame, unit: str = "LAKHS") -> float:
        """Find Grand Total or Net Assets row and extract value."""
        for i in range(len(df)):
            row = df.iloc[i]
            for c in range(min(5, len(row))):
                val_str = str(row.iloc[c]).strip().upper().replace('_', ' ')
                if len(val_str) > 35:
                    continue
                if any(bad in val_str for bad in ["EXPOSURE", "PERCENTAGE", "HEDGED", "FUTURES", "OPTIONS", "PER UNIT", "AGGREGATE"]):
                    continue
                if val_str in ["GRAND TOTAL", "NET ASSETS", "TOTAL NET ASSETS", "GRAND TOTAL (AUM)"] or val_str.startswith("GRAND TOTAL"):
                    candidates = []
                    for v in row:
                        f_val = self.safe_float(v)
                        if f_val is not None and f_val > 0 and abs(f_val - 1.0) > 0.001 and abs(f_val - 100.0) > 0.1 and f_val < 20000000:
                            candidates.append(f_val)
                    if candidates:
                        return self.normalize_currency(candidates[0], unit)
        return 0.0

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting from Samco Mutual Fund: {file_path}")
        
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        # Sheet patterns to skip
        summary_keywords = ["SUMMARY", "INDEX", "CONTROL"]

        for sheet_name in xls.sheet_names:
            if any(k in sheet_name.upper() for k in summary_keywords):
                continue
            
            try:
                # Read enough rows to detect header and scheme name
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=100)
                if df_raw.empty:
                    continue

                # 1. Detect Header Row
                header_idx = self.find_header_row(df_raw, keywords=["ISIN", "INSTRUMENT", "QUANTITY"])
                if header_idx == -1:
                    logger.warning(f"[{sheet_name}] Header not found. Skipping.")
                    continue

                # 2. Extract Scheme Name
                # v1 format (up to Mar 2026): Row 0, Col 1 = actual scheme name
                # v2 format (Apr 2026+):      Row 0, Col 1 = "SAMCO MUTUAL FUND" (AMC name)
                #                             Row 2, Col 1 = portfolio statement containing fund name
                raw_scheme_name = "N/A"
                if len(df_raw) > 0 and len(df_raw.columns) > 1:
                    raw_scheme_name = str(df_raw.iloc[0, 1]).strip()

                # Detect v2 format: Row 0 is just the AMC name, not a scheme name
                if raw_scheme_name.upper() in self._AMC_NAME_MARKERS or raw_scheme_name.lower() == "nan" or not raw_scheme_name:
                    # Try Row 2, Col 1 — contains portfolio statement in v2 format
                    parsed_from_statement = None
                    if len(df_raw) > 2 and len(df_raw.columns) > 1:
                        stmt_text = str(df_raw.iloc[2, 1]).strip()
                        # Pattern: "MONTHLY PORTFOLIO STATEMENT OF <FUND NAME> AS ON <DATE>"
                        match = re.search(
                            r'(?:MONTHLY\s+PORTFOLIO\s+STATEMENT\s+OF\s+)(.*?)(?:\s+AS\s+ON\b)',
                            stmt_text, re.IGNORECASE | re.DOTALL
                        )
                        if match:
                            # Normalise internal whitespace (the text can have double spaces)
                            parsed_from_statement = ' '.join(match.group(1).split())

                    if parsed_from_statement:
                        raw_scheme_name = parsed_from_statement
                        logger.debug(f"[{sheet_name}] v2 format detected. Scheme name parsed from portfolio statement: '{raw_scheme_name}'")
                    else:
                        # Last-resort: scan first 5 rows for any non-trivial text
                        for i in range(5):
                            potential = str(df_raw.iloc[i, 1]).strip() if len(df_raw.columns) > 1 else str(df_raw.iloc[i, 0]).strip()
                            if potential and potential.lower() != "nan" and len(potential) > 5 and potential.upper() not in self._AMC_NAME_MARKERS:
                                raw_scheme_name = potential
                                break

                if raw_scheme_name == "N/A" or raw_scheme_name.upper() in self._AMC_NAME_MARKERS:
                    raw_scheme_name = sheet_name
                    logger.warning(f"[{sheet_name}] Could not parse scheme name; falling back to sheet name.")

                scheme_info = self.parse_verbose_scheme_name(raw_scheme_name)
                logger.info(f"Processing scheme: {scheme_info['scheme_name']} (from '{raw_scheme_name}')")
                
                # Fetch Total AUM from the full dataframe (scanning bottom-up)
                df_full = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                normalized_net_assets = self._extract_total_aum(df_full)
                if normalized_net_assets == 0:
                    normalized_net_assets = None
                
                # 3. Process Data
                headers = [str(h).replace('\n', ' ').strip().upper() for h in df_raw.iloc[header_idx]]

                # Deduplicate headers to avoid silent column-drop in to_dict('records')
                seen_headers = {}
                deduped_headers = []
                for h in headers:
                    if h in seen_headers:
                        seen_headers[h] += 1
                        deduped_headers.append(f"{h}_{seen_headers[h]}")
                    else:
                        seen_headers[h] = 0
                        deduped_headers.append(h)
                headers = deduped_headers

                df_data = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx + 1, header=None)
                df_data.columns = headers

                # Map columns to canonical names
                mapped_cols = {}
                for col in df_data.columns:
                    for pattern, canonical in self.column_mapping.items():
                        if pattern.upper() in str(col).upper():
                            mapped_cols[col] = canonical
                            break

                df_data = df_data.rename(columns=mapped_cols)
                
                if "isin" not in df_data.columns:
                    logger.warning(f"[{sheet_name}] 'isin' column not found after mapping. Skipping.")
                    continue

                sheet_holdings = []
                records = df_data.to_dict('records')

                for idx, row in enumerate(records):
                    try:
                        isin = str(row.get('isin', '')).strip()
                        
                        if not self.is_valid_equity_isin(isin):
                            continue
                            
                        name = str(row.get('company_name', '')).strip()
                        holding = {
                            "amc_name": self.amc_name,
                            "isin": self.clean_isin(isin),
                            "company_name": self.clean_company_name(name),
                            "quantity": self.safe_float(row.get('quantity', 0)),
                            # Convert Lakhs to Rupees
                            "market_value_inr": self.normalize_currency(row.get('market_value_inr', 0), "LAKHS"),
                            # Samco values are often decimals (e.g. 0.0691 -> 6.91%)
                            "percent_of_nav": self.parse_percentage(row.get('percent_of_nav', 0)),
                            "sector": str(row.get('sector', '')).strip(),
                            
                            # Scheme Info
                            "scheme_name": scheme_info["scheme_name"],
                            "scheme_description": raw_scheme_name,
                            "plan_type": scheme_info["plan_type"],
                            "option_type": scheme_info["option_type"],
                            "is_reinvest": scheme_info["is_reinvest"],
                            "total_net_assets": normalized_net_assets
                        }
                        sheet_holdings.append(holding)
                    except Exception as row_err:
                        logger.error(f"[{sheet_name}] Error processing row {idx}: {row_err}")
                        continue
                
                if not sheet_holdings and normalized_net_assets:
                    # Ghost Holding for Non-Equity funds
                    sheet_holdings.append(
                        {
                            "amc_name": self.amc_name,
                            "scheme_name": scheme_info["scheme_name"],
                            "scheme_description": raw_scheme_name,
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
                
                # Validation
                if self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"]):
                    all_holdings.extend(sheet_holdings)

            except Exception as sheet_err:
                logger.error(f"Error processing sheet {sheet_name}: {sheet_err}")
                continue

        logger.info(f"Total holdings extracted for Samco: {len(all_holdings)}")
        return all_holdings
