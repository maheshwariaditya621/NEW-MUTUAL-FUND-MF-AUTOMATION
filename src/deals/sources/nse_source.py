"""
NSE Source Adapter for Bulk and Block Deals.

Supports:
1. Historical date-range queries via NSE JSON endpoints with session priming.
2. Daily snapshot archives (bulk.csv and block.csv) from nsearchives.
3. Full normalization, investor tagging, and company master linking.
"""

import csv
import io
import os
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Any
import requests

from src.config import logger
from src.deals.models import NormalizedDeal, SourceHealth
from src.deals.normalizer import DealNormalizer
from src.deals.sources.base import BaseDealSource

NSE_MAIN_URL = "https://www.nseindia.com/report-detail/display-bulk-and-block-deals"
NSE_HIST_DEALS_API = "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals"
NSE_BLOCK_API = "https://www.nseindia.com/api/block-deal"

NSE_DAILY_BULK_CSV = "https://nsearchives.nseindia.com/content/equities/bulk.csv"
NSE_DAILY_BLOCK_CSV = "https://nsearchives.nseindia.com/content/equities/block.csv"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/report-detail/display-bulk-and-block-deals",
}


class NSESource(BaseDealSource):
    """NSE Bulk and Block Deals Source."""

    def __init__(self, is_enabled: Optional[bool] = None):
        if is_enabled is None:
            env_val = os.getenv("ENABLE_NSE_COLLECTOR", "true").lower()
            self._is_enabled = env_val in ("1", "true", "yes")
        else:
            self._is_enabled = is_enabled

        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self._session_primed = False
        self._health = SourceHealth(
            source_name="NSE",
            is_enabled=self._is_enabled,
            status="IDLE"
        )

    @property
    def source_name(self) -> str:
        return "NSE"

    @property
    def is_enabled(self) -> bool:
        return self._is_enabled

    def get_health(self) -> SourceHealth:
        return self._health

    def _prime_session(self) -> bool:
        """Prime session cookies by requesting homepage."""
        try:
            resp = self.session.get(NSE_MAIN_URL, timeout=10)
            if resp.status_code == 200 and resp.cookies:
                self._session_primed = True
                return True
        except Exception as e:
            logger.warning(f"NSE session priming warning: {e}")
        return False

    def fetch_deals(
        self,
        from_date: date,
        to_date: date,
        deal_type: str = "ALL"
    ) -> List[NormalizedDeal]:
        """Fetch bulk and block deals from NSE."""
        if not self._is_enabled:
            logger.info("NSE collector is disabled via configuration.")
            return []

        self._health.status = "RUNNING"
        self._health.last_run_at = datetime.now()
        collected_deals: List[NormalizedDeal] = []

        try:
            types_to_fetch = ["BULK", "BLOCK"] if deal_type == "ALL" else [deal_type.upper()]

            for dtype in types_to_fetch:
                deals = self._fetch_by_type(dtype, from_date, to_date)
                collected_deals.extend(deals)

            self._health.status = "OK"
            self._health.last_success_at = datetime.now()
            self._health.records_count = len(collected_deals)
            self._health.last_error = None
            return collected_deals

        except Exception as e:
            logger.error(f"Error fetching NSE deals: {e}")
            self._health.status = "FAILED"
            self._health.last_error = str(e)
            return collected_deals

    def _fetch_by_type(self, deal_type: str, from_date: date, to_date: date) -> List[NormalizedDeal]:
        """Fetch either bulk or block deals across the date range."""
        collected: Dict[str, NormalizedDeal] = {}
        today = date.today()

        # 1. Daily CSV Archive (authoritative source for today's market session)
        if from_date <= today <= to_date or (to_date - from_date).days <= 3:
            logger.info(f"Checking NSE daily archive CSV for {deal_type} deals...")
            csv_deals = self._fetch_daily_csv(deal_type)
            for d in csv_deals:
                if from_date <= d.trade_date <= to_date:
                    collected[d.source_fingerprint] = d

        # 2. Historical JSON API
        if not self._session_primed:
            self._prime_session()

        # If range is within 14 days, iterate day-by-day because NSE's historical API truncates multi-day queries to the first day
        day_diff = (to_date - from_date).days
        if day_diff <= 14:
            curr = from_date
            while curr <= to_date:
                h_deals = self._fetch_historical_json(deal_type, curr, curr)
                for d in h_deals:
                    collected[d.source_fingerprint] = d
                curr += timedelta(days=1)
        else:
            h_deals = self._fetch_historical_json(deal_type, from_date, to_date)
            for d in h_deals:
                collected[d.source_fingerprint] = d

        return list(collected.values())

    def _fetch_historical_json(self, deal_type: str, from_date: date, to_date: date) -> List[NormalizedDeal]:
        """Query NSE historical API endpoint."""
        url = NSE_HIST_DEALS_API
        headers = dict(DEFAULT_HEADERS)
        headers["Accept"] = "application/json, text/plain, */*"

        from_str = from_date.strftime("%d-%m-%Y")
        to_str = to_date.strftime("%d-%m-%Y")
        opt_type = "bulk_deals" if deal_type == "BULK" else "block_deals"
        params = {"optionType": opt_type, "from": from_str, "to": to_str}

        try:
            resp = self.session.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                raw_rows = data.get("data", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                return self._parse_json_rows(raw_rows, deal_type)
        except Exception as e:
            logger.warning(f"NSE Historical {deal_type} API returned error: {e}")

        return []

    def _fetch_daily_csv(self, deal_type: str) -> List[NormalizedDeal]:
        """Fetch daily public CSV from archives."""
        csv_url = NSE_DAILY_BULK_CSV if deal_type == "BULK" else NSE_DAILY_BLOCK_CSV
        try:
            resp = self.session.get(csv_url, timeout=15)
            if resp.status_code == 200 and len(resp.text) > 50:
                return self._parse_csv_content(resp.text, deal_type)
        except Exception as e:
            logger.warning(f"NSE Daily {deal_type} CSV download warning: {e}")
        return []

    def _parse_json_rows(self, rows: List[Dict[str, Any]], deal_type: str) -> List[NormalizedDeal]:
        """Parse JSON response from NSE API."""
        results: List[NormalizedDeal] = []
        for row in rows:
            try:
                # Extract fields with fallback keys across NSE schema variations
                raw_date = row.get("BD_DT_DATE") or row.get("date") or row.get("tradeDate") or row.get("BD_DATE")
                trade_date = DealNormalizer.parse_date(raw_date)
                if not trade_date:
                    continue

                symbol = (row.get("BD_SYMBOL") or row.get("symbol") or row.get("securityCode") or "").strip().upper()
                sec_name = (row.get("BD_SCRIP_NAME") or row.get("BD_SECURITY_NAME") or row.get("securityName") or symbol).strip()
                client_name = (row.get("BD_CLIENT_NAME") or row.get("clientName") or "").strip()
                if not symbol or not client_name:
                    continue

                action = DealNormalizer.parse_action(row.get("BD_BUY_SELL") or row.get("buySell") or row.get("side") or "")
                quantity = int(DealNormalizer.parse_numeric(row.get("BD_QTY_TRD") or row.get("quantity") or row.get("qty") or 0))
                if quantity <= 0:
                    continue

                price = DealNormalizer.parse_numeric(
                    row.get("BD_TP_WATP") or row.get("BD_TP_PRICE") or row.get("tradePrice") or row.get("price") or 0
                )
                if price <= Decimal("0"):
                    continue

                val_inr = (price * Decimal(quantity)).quantize(Decimal("0.01"))
                isin = (row.get("BD_ISIN") or row.get("isin") or "").strip() or None

                # Resolve company & equity %
                company_id, resolved_isin, shares = DealNormalizer.resolve_company(symbol=symbol, isin=isin, exchange="NSE")
                pct_equity = None
                if shares and shares > Decimal("0"):
                    pct_equity = ((Decimal(quantity) / shares) * Decimal("100")).quantize(Decimal("0.0001"))

                inv_category = DealNormalizer.categorize_investor(client_name)

                deal = NormalizedDeal(
                    deal_type=deal_type,
                    exchange="NSE",
                    trade_date=trade_date,
                    symbol=symbol,
                    security_name=sec_name,
                    client_name=client_name,
                    investor_category=inv_category,
                    action=action,
                    quantity=quantity,
                    trade_price=price,
                    trade_value_inr=val_inr,
                    source_name=f"NSE_{deal_type}",
                    company_id=company_id,
                    isin=resolved_isin or isin,
                    pct_equity_traded=pct_equity
                )
                results.append(deal)
            except Exception as row_err:
                logger.warning(f"Error parsing NSE JSON deal row: {row_err}")
                continue

        return results

    def _parse_csv_content(self, csv_text: str, deal_type: str) -> List[NormalizedDeal]:
        """Parse standard NSE CSV content."""
        results: List[NormalizedDeal] = []
        reader = csv.reader(io.StringIO(csv_text))
        header: Optional[List[str]] = None

        for row in reader:
            if not row or len(row) < 5:
                continue
            clean_cells = [c.strip().upper() for c in row]

            # Detect Header
            if "SYMBOL" in clean_cells and ("CLIENT NAME" in clean_cells or "BUY/SELL" in clean_cells):
                header = clean_cells
                continue

            if not header:
                continue

            # Map columns by header
            try:
                row_dict = dict(zip(header, [c.strip() for c in row]))
                raw_date = row_dict.get("DATE")
                trade_date = DealNormalizer.parse_date(raw_date)
                if not trade_date:
                    continue

                symbol = row_dict.get("SYMBOL", "").strip().upper()
                sec_name = row_dict.get("SECURITY NAME") or row_dict.get("NAME OF THE COMPANY") or symbol
                client_name = row_dict.get("CLIENT NAME", "").strip()
                if not symbol or not client_name:
                    continue

                action = DealNormalizer.parse_action(row_dict.get("BUY/SELL") or row_dict.get("BUY / SELL") or "")
                quantity = int(DealNormalizer.parse_numeric(row_dict.get("QUANTITY TRADED") or row_dict.get("QUANTITY") or 0))
                if quantity <= 0:
                    continue

                price = DealNormalizer.parse_numeric(
                    row_dict.get("TRADE PRICE / WGHT. AVG. PRICE") or
                    row_dict.get("TRADE PRICE / WGT. AVG. PRICE") or
                    row_dict.get("TRADE PRICE") or
                    row_dict.get("PRICE") or 0
                )
                if price <= Decimal("0"):
                    continue

                val_inr = (price * Decimal(quantity)).quantize(Decimal("0.01"))

                company_id, resolved_isin, shares = DealNormalizer.resolve_company(symbol=symbol, exchange="NSE")
                pct_equity = None
                if shares and shares > Decimal("0"):
                    pct_equity = ((Decimal(quantity) / shares) * Decimal("100")).quantize(Decimal("0.0001"))

                inv_cat = DealNormalizer.categorize_investor(client_name)

                deal = NormalizedDeal(
                    deal_type=deal_type,
                    exchange="NSE",
                    trade_date=trade_date,
                    symbol=symbol,
                    security_name=sec_name,
                    client_name=client_name,
                    investor_category=inv_cat,
                    action=action,
                    quantity=quantity,
                    trade_price=price,
                    trade_value_inr=val_inr,
                    source_name=f"NSE_{deal_type}_CSV",
                    company_id=company_id,
                    isin=resolved_isin,
                    pct_equity_traded=pct_equity
                )
                results.append(deal)
            except Exception as e:
                logger.warning(f"Error parsing NSE CSV row: {e}")
                continue

        return results
