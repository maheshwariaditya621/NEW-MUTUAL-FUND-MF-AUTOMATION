"""
Data models for Bulk & Block Deals module.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any
from decimal import Decimal
import hashlib


@dataclass
class NormalizedDeal:
    """Normalized deal record representation ready for persistence."""
    deal_type: str           # BULK or BLOCK
    exchange: str            # NSE or BSE
    trade_date: date         # Trade execution date
    symbol: str              # Ticker / Symbol
    security_name: str       # Company / Security Name
    client_name: str         # Buyer or Seller client name
    action: str              # BUY or SELL
    quantity: int            # Quantity traded
    trade_price: Decimal     # Price per share
    trade_value_inr: Decimal # Total value (quantity * price)
    source_name: str         # Source identifier (e.g. NSE_BULK, BSE_BLOCK)
    ingestion_mode: str = "AUTOMATED"
    company_id: Optional[int] = None
    isin: Optional[str] = None
    investor_category: str = "OTHER"
    pct_equity_traded: Optional[Decimal] = None
    source_record_id: Optional[str] = None

    @property
    def source_fingerprint(self) -> str:
        """
        Deterministic SHA-256 fingerprint for idempotency across repeated runs.
        Combines: deal_type, exchange, trade_date, symbol, client_name, action, quantity, trade_price.
        """
        raw_key = (
            f"{self.deal_type}|{self.exchange}|{self.trade_date.isoformat()}|"
            f"{self.symbol.strip().upper()}|{self.client_name.strip().upper()}|"
            f"{self.action.strip().upper()}|{self.quantity}|{float(self.trade_price):.4f}"
        )
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


@dataclass
class SourceHealth:
    source_name: str
    is_enabled: bool
    status: str = "IDLE"  # IDLE, RUNNING, OK, DEGRADED, FAILED
    last_run_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    records_count: int = 0
    last_error: Optional[str] = None


@dataclass
class IngestionResult:
    source_name: str
    deal_type: str
    exchange: str
    records_fetched: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    records_rejected: int = 0
    error_summary: Optional[str] = None
    duration_seconds: float = 0.0
