from typing import Dict, Any, List

import pandas as pd

from src.config import logger
from src.extractors.base_extractor import BaseExtractor


class ShriramExtractorV1(BaseExtractor):
    """
    Dedicated extractor for Shriram Mutual Fund.

    File structure (CONSOLIDATED_SHRIRAM_YYYY_MM.xlsx):
    - One sheet per scheme; sheet name IS the scheme name
    - Header is detected dynamically (typically at row ~18)
    - Columns: Name of Instrument | ISIN | Industry/Rating | Quantity
               | Market Value (INR Lacs) | % to Net Assets | % Yield
    - Market Value: In INR Lakhs → normalised to base Rupees
    - % to Net Assets: Already in percentage form (e.g. 4.98 = 4.98%)
    """

    # Sheets to skip — purely debt / money-market / ETF sheets with no equity ISINs
    SKIP_KEYWORDS = ["OVERNIGHT", "LIQUID", "ETF", "SUMMARY", "INDEX"]

    def __init__(self):
        super().__init__(amc_name="Shriram Mutual Fund", version="v1")
        self.column_mapping = {
            "NAME OF  INSTRUMENT": "company_name",     # double-space variant in file
            "NAME OF INSTRUMENT": "company_name",
            "INSTRUMENT": "company_name",
            "ISIN": "isin",
            "INDUSTRY": "sector",
            "RATING": "sector",
            "QUANTITY": "quantity",
            "MARKET/FAIR VALUE": "market_value_inr",   # 'Market/Fair Value  (INR Lacs)'
            "MARKET/ FAIR VALUE": "market_value_inr",
            "MARKET VALUE": "market_value_inr",
            "FAIR VALUE": "market_value_inr",
            "INR LACS": "market_value_inr",            # fallback on unit hint in header
            "NET ASSETS": "percent_of_nav",            # '%  to Net Assets' (double space safe)
            "% TO NAV": "percent_of_nav",
        }

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        logger.info(f"Extracting from Shriram Mutual Fund: {file_path}")

        xls = pd.ExcelFile(file_path, engine="openpyxl")
        all_holdings: List[Dict[str, Any]] = []

        for sheet_name in xls.sheet_names:
            # Skip non-equity / utility sheets
            sn_upper = sheet_name.upper()
            if any(kw in sn_upper for kw in self.SKIP_KEYWORDS):
                logger.info(f"[{sheet_name}] Skipping (non-equity sheet)")
                continue

            try:
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=100)
                if df_raw.empty:
                    continue

                # ── 1. Detect Header Row ────────────────────────────────────────
                header_idx = self.find_header_row(df_raw, keywords=["ISIN", "INSTRUMENT", "NAME"])
                if header_idx == -1:
                    logger.warning(f"[{sheet_name}] Header not found — skipping.")
                    continue

                # ── 2. Scheme Name from cell (Row 0, Col 1) — full untruncated name ──────
                # Excel's 31-char sheet limit truncates names like "Shriram Aggressive Hybr".
                # The full name is always in cell (0, 1), e.g. "Shriram Aggressive Hybrid Fund".
                raw_scheme_name = sheet_name.strip()  # fallback
                try:
                    cell_val = str(df_raw.iloc[0, 1]).strip()
                    if cell_val and cell_val.lower() not in ("nan", "none", ""):
                        raw_scheme_name = cell_val
                except Exception:
                    pass
                scheme_info = self.parse_verbose_scheme_name(raw_scheme_name)
                logger.info(
                    f"Processing: {scheme_info['scheme_name']} "
                    f"({scheme_info['plan_type']} / {scheme_info['option_type']})"
                )

                # ── 3. Build data frame from header row onward ─────────────────
                raw_headers = [
                    str(h).replace("\n", " ").strip().upper()
                    for h in df_raw.iloc[header_idx].values
                ]
                df_data = pd.read_excel(
                    xls, sheet_name=sheet_name,
                    skiprows=header_idx + 1, header=None
                )
                df_data.columns = raw_headers

                # ── 4. Map columns to canonical names ─────────────────────────
                mapped = {}
                for col in df_data.columns:
                    col_u = str(col).upper()
                    for pattern, canonical in self.column_mapping.items():
                        if pattern.upper() in col_u:
                            mapped[col] = canonical
                            break

                df_data = df_data.rename(columns=mapped)

                if "isin" not in df_data.columns:
                    logger.warning(f"[{sheet_name}] 'isin' column not found after mapping — skipping.")
                    continue

                # ── 5. Filter for equity ISINs (triple-filter) ────────────────
                equity_df = self.filter_equity_isins(df_data, "isin")
                if equity_df.empty:
                    logger.info(f"[{sheet_name}] No equity ISINs found — skipping.")
                    continue

                # Extract Total AUM (Net Assets)
                normalized_net_assets = None
                for r_i in range(len(df_data)):
                    r_vals = df_data.iloc[r_i]
                    for c_i in range(min(5, len(r_vals))):
                        v_str = str(r_vals.iloc[c_i]).strip().upper().replace('_', ' ')
                        if len(v_str) > 35:
                            continue
                        if any(bad in v_str for bad in ["EXPOSURE", "PERCENTAGE", "HEDGED", "FUTURES", "OPTIONS", "PER UNIT", "AGGREGATE"]):
                            continue
                        if "GRAND TOTAL" in v_str or v_str in ["NET ASSETS", "TOTAL NET ASSETS"]:
                            candidates = []
                            for val in r_vals:
                                f_val = self.safe_float(val)
                                if f_val > 0 and abs(f_val - 1.0) > 0.001 and abs(f_val - 100.0) > 0.1 and f_val < 20000000:
                                    candidates.append(f_val)
                            if candidates:
                                normalized_net_assets = self.normalize_currency(candidates[0], "LAKHS")
                                break
                    if normalized_net_assets:
                        break

                # ── 6. Build holdings list ────────────────────────────────────
                sheet_holdings: List[Dict[str, Any]] = []
                for _, row in equity_df.iterrows():
                    try:
                        isin = str(row.get("isin", "")).strip()
                        name = str(row.get("company_name", "")).strip()
                        sector = str(row.get("sector", "")).strip()

                        # Market value is in INR Lakhs
                        raw_mv = row.get("market_value_inr", 0)
                        market_value = self.normalize_currency(raw_mv, "LAKHS")

                        # % NAV is already in percentage form — 0.98 means 0.98%, not 98%.
                        # Do NOT use parse_percentage() which would multiply ≤1.0 values by 100.
                        pct_nav = self.safe_float(row.get("percent_of_nav", 0))

                        qty = self.safe_float(row.get("quantity", 0))

                        holding = {
                            "amc_name": self.amc_name,
                            "scheme_name": scheme_info["scheme_name"],
                            "scheme_description": raw_scheme_name,
                            "plan_type": scheme_info["plan_type"],
                            "option_type": scheme_info["option_type"],
                            "is_reinvest": scheme_info["is_reinvest"],
                            "isin": self.clean_isin(isin),
                            "company_name": self.clean_company_name(name),
                            "quantity": qty,
                            "market_value_inr": market_value,
                            "percent_of_nav": pct_nav,
                            "sector": self.clean_company_name(sector) if sector else "N/A",
                            "total_net_assets": normalized_net_assets
                        }
                        sheet_holdings.append(holding)
                    except Exception as row_err:
                        logger.error(f"[{sheet_name}] Row error: {row_err}")
                        continue

                # ── 7. Validate and accumulate ────────────────────────────────
                if self.validate_nav_completeness(sheet_holdings, scheme_info["scheme_name"]):
                    all_holdings.extend(sheet_holdings)
                    logger.info(f"  ✓ {len(sheet_holdings)} equity holdings added")

            except Exception as sheet_err:
                logger.error(f"Error processing sheet '{sheet_name}': {sheet_err}")
                continue

        logger.info(f"Total holdings extracted for Shriram: {len(all_holdings)}")
        return all_holdings
