"""
Base Source Provider Interface for Bulk and Block Deals.
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import List, Optional
from src.deals.models import NormalizedDeal, SourceHealth


class BaseDealSource(ABC):
    """Abstract base class for all exchange/file deal collectors."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Name of this source (e.g. NSE, BSE, MANUAL_CSV)."""
        pass

    @property
    @abstractmethod
    def is_enabled(self) -> bool:
        """Whether this source is currently active."""
        pass

    @abstractmethod
    def fetch_deals(
        self,
        from_date: date,
        to_date: date,
        deal_type: str = "ALL"  # BULK, BLOCK, or ALL
    ) -> List[NormalizedDeal]:
        """Fetch deals for date range and normalize to NormalizedDeal objects."""
        pass

    @abstractmethod
    def get_health(self) -> SourceHealth:
        """Return the current operational status of the source."""
        pass
