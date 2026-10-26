"""
Normalizer and Enrichment Engine for Bulk & Block Deals.

Handles:
1. Robust date and numeric parsing (handling Indian numbering, commas, and varied formats).
2. Investor categorization (Mutual Funds, FII/FPI, DII, Promoters, AIF, Other/HNIs).
3. Company Master resolution (linking via ISIN, NSE symbol, or BSE security code).
4. % of Equity calculations using shares_outstanding.
"""

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, Any, Tuple
from src.config import logger
from src.db.connection import get_connection


class DealNormalizer:
    """Normalizes and enriches raw deal records."""

    _company_cache: Optional[Dict[str, Dict[str, Any]]] = None

    # Known Institutional Keywords
    MF_KEYWORDS = [
        "MUTUAL FUND", " MF", "AMC", "TRUSTEE", "ASSET MANAGEMENT",
        "NIPPON INDIA", "HDFC MUTUAL", "ICICI PRUDENTIAL MUTUAL",
        "SBI MUTUAL", "KOTAK MAHINDRA MUTUAL", "AXIS MUTUAL", "UTI MUTUAL",
        "TATA MUTUAL", "DSP MUTUAL", "MIRAE ASSET", "MOTILAL OSWAL",
        "BANDHAN MUTUAL", "PARAG PARIKH", "PPFAS", "QUANT MUTUAL",
        "CANARA ROBECO", "EDELWEISS MUTUAL", "SUNDARAM MUTUAL", "WHITE OAK CAPITAL",
        "INVESCO MUTUAL", "FRANKLIN TEMPLETON", "HSBC MUTUAL", "MAHINDRA MANULIFE",
        "BARODA BNP", "UNION MUTUAL", "PGIM INDIA", "360 ONE", "GROWW MUTUAL"
    ]

    FII_KEYWORDS = [
        "GOVERNMENT PENSION FUND", "NORGES BANK", "GOLDMAN SACHS", "MORGAN STANLEY",
        "VANGUARD", "FIDELITY", "BLACKROCK", "SOCIETE GENERALE", "BNP PARIBAS",
        "CITIGROUP", "NOMURA", "BARCLAYS", "MACQUARIE", "UBS AG", "CREDIT SUISSE",
        "INVESTMENT AUTHORITY", "EMERGING MARKETS FUND", "FII", "FPI",
        "CAPITAL GROUP", "TEMPLETON GLOBAL", "WELLINGTON", "ABERDEEN",
        "SINGAPORE GOVERNMENT", "MONETARY AUTHORITY OF SINGAPORE", "KUWAIT INVESTMENT"
    ]

    DII_KEYWORDS = [
        "LIFE INSURANCE CORPORATION", "LIC OF INDIA", "GENERAL INSURANCE", "GIC",
        "HDFC LIFE", "ICICI PRUDENTIAL LIFE", "SBI LIFE", "MAX LIFE", "BAJAJ ALLIANZ",
        "TATA AIA", "KOTAK MAHINDRA LIFE", "NEW INDIA ASSURANCE", "ORIENTAL INSURANCE",
        "NATIONAL INSURANCE", "UNITED INDIA INSURANCE", "PENSION FUND"
    ]

    AIF_KEYWORDS = [
        "ALTERNATIVE INVESTMENT FUND", "AIF", "CAT III", "CAT II", "VENTURE CAPITAL",
        "GROWTH FUND", "OPPORTUNITIES FUND", "INDIA ADVANTAGE FUND"
    ]

    PROMOTER_KEYWORDS = [
        "PROMOTER", "PROMOTER GROUP", "HOLDINGS PRIVATE LIMITED", "FAMILY TRUST",
        "FOUNDATION", "INVESTMENTS PRIVATE LIMITED"
    ]

    @classmethod
    def parse_date(cls, raw_date: Any) -> Optional[date]:
        """Parse varied date formats reliably."""
        if not raw_date:
            return None
        if isinstance(raw_date, date):
            return raw_date
        if isinstance(raw_date, datetime):
            return raw_date.date()

        clean_str = str(raw_date).strip()
        # Common formats in Indian exchange files
        date_formats = [
            "%d-%b-%Y",      # 26-Oct-2026
            "%d-%B-%Y",      # 26-October-2026
            "%d/%m/%Y",      # 26/10/2026
            "%d-%m-%Y",      # 26-10-2026
            "%Y-%m-%d",      # 2026-10-26
            "%Y%m%d",        # 20261026
            "%d-%b-%y",      # 26-Oct-26
            "%d/%m/%y"       # 26/10/26
        ]

        for fmt in date_formats:
            try:
                return datetime.strptime(clean_str, fmt).date()
            except ValueError:
                continue

        logger.warning(f"Unable to parse date string: '{clean_str}'")
        return None

    @classmethod
    def parse_numeric(cls, raw_val: Any) -> Decimal:
        """Parse quantity or price strings containing commas, spaces, or currency symbols."""
        if raw_val is None:
            return Decimal("0.0")
        if isinstance(raw_val, (int, float, Decimal)):
            return Decimal(str(raw_val))

        clean_val = str(raw_val).replace(",", "").replace("₹", "").replace("Rs.", "").strip()
        try:
            return Decimal(clean_val)
        except InvalidOperation:
            # Try regex extraction
            match = re.search(r"[-+]?\d*\.?\d+", clean_val)
            if match:
                return Decimal(match.group(0))
            return Decimal("0.0")

    @classmethod
    def parse_action(cls, raw_side: str) -> str:
        """Normalize BUY/SELL side."""
        if not raw_side:
            return "BUY"
        clean = raw_side.strip().upper()
        if clean in ("B", "BUY", "PURCHASE"):
            return "BUY"
        if clean in ("S", "SELL", "SALE"):
            return "SELL"
        return "BUY"

    @classmethod
    def categorize_investor(cls, client_name: str) -> str:
        """Categorize client by institutional class."""
        if not client_name:
            return "OTHER"
        name_upper = client_name.strip().upper()

        for kw in cls.MF_KEYWORDS:
            if kw in name_upper:
                return "MUTUAL_FUND"

        for kw in cls.FII_KEYWORDS:
            if kw in name_upper:
                return "FII_FPI"

        for kw in cls.DII_KEYWORDS:
            if kw in name_upper:
                return "DII"

        for kw in cls.AIF_KEYWORDS:
            if kw in name_upper:
                return "AIF"

        for kw in cls.PROMOTER_KEYWORDS:
            if kw in name_upper:
                return "PROMOTER"

        return "OTHER"

    @classmethod
    def load_company_cache(cls, force_reload: bool = False) -> Dict[str, Dict[str, Any]]:
        """Pre-load company master into memory indexed by ISIN, symbol, and BSE code."""
        if cls._company_cache is not None and not force_reload:
            return cls._company_cache

        conn = get_connection()
        cache: Dict[str, Dict[str, Any]] = {
            "by_isin": {},
            "by_symbol": {},
            "by_bse_code": {}
        }
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT company_id, isin, company_name, exchange_symbol, nse_symbol, bse_code, shares_outstanding
                    FROM companies
                    """
                )
                for row in cur.fetchall():
                    cid, isin, cname, ex_sym, nse_sym, bse_code, shares = row
                    data = {
                        "company_id": cid,
                        "isin": isin.strip() if isin else None,
                        "company_name": cname,
                        "exchange_symbol": ex_sym.strip().upper() if ex_sym else None,
                        "nse_symbol": nse_sym.strip().upper() if nse_sym else None,
                        "bse_code": bse_code.strip() if bse_code else None,
                        "shares_outstanding": shares
                    }

                    if isin:
                        cache["by_isin"][isin.strip().upper()] = data
                    if ex_sym:
                        cache["by_symbol"][ex_sym.strip().upper()] = data
                    if nse_sym:
                        cache["by_symbol"][nse_sym.strip().upper()] = data
                    if bse_code:
                        cache["by_bse_code"][bse_code.strip()] = data

            cls._company_cache = cache
            logger.info(f"Loaded company master cache for deals ({len(cache['by_isin'])} companies)")
        except Exception as e:
            logger.error(f"Failed to load company cache: {e}")
            cls._company_cache = cache

        return cls._company_cache

    @classmethod
    def resolve_company(
        cls,
        symbol: Optional[str] = None,
        isin: Optional[str] = None,
        exchange: str = "NSE"
    ) -> Tuple[Optional[int], Optional[str], Optional[Decimal]]:
        """
        Resolves company_id, confirmed isin, and shares_outstanding from cache.
        Returns: (company_id, isin, shares_outstanding)
        """
        cache = cls.load_company_cache()
        matched: Optional[Dict[str, Any]] = None

        # 1. Match ISIN
        if isin and isin.strip().upper() in cache["by_isin"]:
            matched = cache["by_isin"][isin.strip().upper()]

        # 2. Match BSE Code if BSE
        if not matched and exchange == "BSE" and symbol:
            clean_sym = symbol.strip()
            if clean_sym in cache["by_bse_code"]:
                matched = cache["by_bse_code"][clean_sym]

        # 3. Match Ticker Symbol
        if not matched and symbol:
            clean_sym = symbol.strip().upper()
            if clean_sym in cache["by_symbol"]:
                matched = cache["by_symbol"][clean_sym]

        if matched:
            shares = matched.get("shares_outstanding")
            return (
                matched["company_id"],
                matched["isin"],
                Decimal(str(shares)) if shares else None
            )

        return (None, isin, None)
