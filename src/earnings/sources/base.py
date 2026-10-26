"""
Base provider abstraction and data models for earnings sources.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import List, Optional, Dict, Any


@dataclass
class ScheduledEventRecord:
    company_id: int
    isin: str
    exchange_symbol: Optional[str]
    bse_code: Optional[str]
    meeting_date: date
    source_name: str
    source_record_id: str
    meeting_time: Optional[str] = None
    expected_results_date: Optional[date] = None
    financial_year: Optional[str] = None
    fiscal_period: Optional[str] = None
    raw_purpose: Optional[str] = None
    normalized_event_type: str = "BOARD_MEETING_RESULTS"
    event_status: str = "SCHEDULED"
    source_url: Optional[str] = None
    verification_status: str = "VERIFIED_EXCHANGE"
    consensus_eps: Optional[float] = None
    consensus_eps_high: Optional[float] = None
    consensus_eps_low: Optional[float] = None
    consensus_revenue_inr: Optional[float] = None
    raw_payload: Optional[Dict[str, Any]] = None


@dataclass
class ResultsFilingRecord:
    company_id: int
    exchange: str  # 'NSE', 'BSE', 'BOTH'
    source_filing_id: str
    filing_title: str
    dissemination_timestamp: Optional[datetime] = None
    submission_timestamp: Optional[datetime] = None
    financial_year: Optional[str] = None
    fiscal_period: Optional[str] = None
    result_type: str = "UNKNOWN"  # 'STANDALONE', 'CONSOLIDATED', 'BOTH', 'UNKNOWN'
    audit_status: str = "UNKNOWN"  # 'AUDITED', 'UNAUDITED', 'LIMITED_REVIEW', 'UNKNOWN'
    filing_url: Optional[str] = None
    attachment_url: Optional[str] = None
    attachment_filename: Optional[str] = None
    is_revision: bool = False
    revision_type: Optional[str] = None
    verification_status: str = "VERIFIED_EXCHANGE"
    raw_metadata: Optional[Dict[str, Any]] = None


@dataclass
class SourceHealth:
    source_name: str
    is_enabled: bool
    status: str  # 'IDLE', 'HEALTHY', 'DEGRADED', 'ERROR'
    last_poll_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    total_events_fetched: int = 0
    total_filings_fetched: int = 0
    last_error: Optional[str] = None


class BaseEarningsSource(ABC):
    """Abstract interface for all earnings data providers."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique identifier for this source."""
        pass

    @property
    @abstractmethod
    def is_enabled(self) -> bool:
        """Whether this provider is currently enabled."""
        pass

    @abstractmethod
    def fetch_scheduled_events(self, lookahead_days: int = 30) -> List[ScheduledEventRecord]:
        """Fetch forward-looking scheduled board meetings / expected results dates."""
        pass

    @abstractmethod
    def fetch_results_filings(self, lookback_days: int = 3) -> List[ResultsFilingRecord]:
        """Fetch actual published financial results filings."""
        pass

    @abstractmethod
    def get_health(self) -> SourceHealth:
        """Report current provider health metrics."""
        pass
