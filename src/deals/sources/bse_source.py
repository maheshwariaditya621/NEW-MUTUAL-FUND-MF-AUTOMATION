"""
BSE Source Adapter for Bulk and Block Deals.

Queries BSE India bulk and block deals endpoints with rate-limiting,
field normalization, company master resolution, and error recovery.
"""

import os
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any
import requests

from src.config import logger
from src.deals.models import NormalizedDeal, SourceHealth
from src.deals.normalizer import DealNormalizer
from src.deals.sources.base import BaseDealSource

BSE_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/BulkDeals/w"
BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}


class BSESource(BaseDealSource):
    """BSE Bulk and Block Deals Source."""

    def __init__(self, is_enabled: Optional[bool] = None):
        if is_enabled is None:
            env_val = os.getenv("ENABLE_BSE_COLLECTOR", "true").lower()
            self._is_enabled = env_val in ("1", "true", "yes")
        else:
            self._is_enabled = is_enabled

        self.session = requests.Session()
        self.session.headers.update(BSE_HEADERS)
        self._health = SourceHealth(
            source_name="BSE",
            is_enabled=self._is_enabled,
            status="IDLE"
        )

    @property
    def source_name(self) -> str:
        return "BSE"

    @property
    def is_enabled(self) -> bool:
        return self._is_enabled

    def get_health(self) -> SourceHealth:
        return self._health

    def fetch_deals(
        self,
        from_date: date,
        to_date: date,
        deal_type: str = "ALL"
    ) -> List[NormalizedDeal]:
        """Fetch bulk and block deals from BSE."""
        if not self._is_enabled:
            logger.info("BSE collector is disabled via configuration.")
            return []

        self._health.status = "RUNNING"
        self._health.last_run_at = datetime.now()
        collected_deals: List[NormalizedDeal] = []

        try:
            # Map deal type parameter for BSE
            types_to_fetch = ["Bulk", "Block"] if deal_type == "ALL" else [deal_type.capitalize()]

            for dtype in types_to_fetch:
                deals = self._fetch_bse_type(dtype, from_date, to_date)
                collected_deals.extend(deals)

            self._health.status = "OK"
            self._health.last_success_at = datetime.now()
            self._health.records_count = len(collected_deals)
            self._health.last_error = None
            return collected_deals

        except Exception as e:
            logger.error(f"Error fetching BSE deals: {e}")
            self._health.status = "FAILED"
            self._health.last_error = str(e)
            return collected_deals

    def _fetch_bse_type(self, bse_deal_type: str, from_date: date, to_date: date) -> List[NormalizedDeal]:
        """Query BSE endpoint for specific deal type."""
        params = {
            "fromdate": from_date.strftime("%d/%m/%Y"),
            "todate": to_date.strftime("%d/%m/%Y"),
            "deal_type": bse_deal_type,
            "segment": "Equity"
        }

        try:
            resp = self.session.get(BSE_API_URL, params=params, timeout=12)
            if resp.status_code == 200:
                data = resp.json()
                raw_rows = data.get("Table", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                return self._parse_bse_rows(raw_rows, bse_deal_type.upper())
        except Exception as e:
            logger.warning(f"BSE API returned error for {bse_deal_type}: {e}")

        return []

    def _parse_bse_rows(self, rows: List[Dict[str, Any]], deal_type: str) -> List[NormalizedDeal]:
        """Parse BSE raw rows."""
        results: List[NormalizedDeal] = []

        for row in rows:
            try:
                raw_date = row.get("DEAL_DATE") or row.get("DealDate") or row.get("DATE")
                trade_date = DealNormalizer.parse_date(raw_date)
                if not trade_date:
                    continue

                bse_code = str(row.get("SCRIP_CD") or row.get("ScripCode") or "").strip()
                sec_name = str(row.get("SCRIP_NAME") or row.get("ScripName") or bse_code).strip()
                client_name = str(row.get("CLIENT_NAME") or row.get("ClientName") or "").strip()
                if not bse_code or not client_name:
                    continue

                action = DealNormalizer.parse_action(str(row.get("BUY_SELL") or row.get("DealType") or ""))
                quantity = int(DealNormalizer.parse_numeric(row.get("QUANTITY") or row.get("Qty") or 0))
                if quantity <= 0:
                    continue

                price = DealNormalizer.parse_numeric(row.get("PRICE") or row.get("Rate") or 0)
                if price <= Decimal("0"):
                    continue

                val_inr = (price * Decimal(quantity)).quantize(Decimal("0.01"))

                # Resolve company by BSE Code
                company_id, resolved_isin, shares = DealNormalizer.resolve_company(symbol=bse_code, exchange="BSE")
                pct_equity = None
                if shares and shares > Decimal("0"):
                    pct_equity = ((Decimal(quantity) / shares) * Decimal("100")).quantize(Decimal("0.0001"))

                inv_cat = DealNormalizer.categorize_investor(client_name)

                deal = NormalizedDeal(
                    deal_type=deal_type,
                    exchange="BSE",
                    trade_date=trade_date,
                    symbol=bse_code,
                    security_name=sec_name,
                    client_name=client_name,
                    investor_category=inv_cat,
                    action=action,
                    quantity=quantity,
                    trade_price=price,
                    trade_value_inr=val_inr,
                    source_name=f"BSE_{deal_type}",
                    company_id=company_id,
                    isin=resolved_isin,
                    pct_equity_traded=pct_equity
                )
                results.append(deal)
            except Exception as e:
                logger.warning(f"Error parsing BSE row: {e}")
                continue

        return results
