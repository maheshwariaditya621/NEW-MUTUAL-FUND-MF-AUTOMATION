"""
Pydantic models for scheme portfolio endpoints.
"""

from typing import List, Optional
from pydantic import BaseModel, Field
from decimal import Decimal


class SchemeSearchResult(BaseModel):
    """Scheme search result model."""
    scheme_id: int = Field(..., description="Internal scheme ID")
    scheme_name: str = Field(..., description="Full scheme name")
    amc_name: str = Field(..., description="AMC name")
    plan_type: str = Field(..., description="Direct or Regular")
    option_type: str = Field(..., description="Growth, Dividend, or IDCW")
    category: Optional[str] = Field(None, description="Scheme category (e.g., Equity: Multi Cap)")


class MonthlyHoldingSnapshot(BaseModel):
    """Holdings data for a specific month."""
    month: str = Field(..., description="Month in YYYY-MM format")
    aum_cr: Decimal = Field(..., description="Assets Under Management in crores")
    percent_to_aum: Decimal = Field(..., description="% of AUM for this holding")
    num_shares: Optional[int] = Field(None, description="Number of shares held. None if AMC data not uploaded.")
    is_adjusted: bool = Field(False, description="Whether share count was adjusted for corporate actions")


class PortfolioHolding(BaseModel):
    """Individual stock holding across multiple months."""
    entity_id: Optional[int] = Field(None, description="Stable Corporate Entity ID")
    isin: str = Field(..., description="Company ISIN (latest/primary)")
    company_name: str = Field(..., description="Company name")
    sector: Optional[str] = Field(None, description="Sector classification")
    market_cap: Optional[Decimal] = Field(None, description="Market cap in crores")
    mcap_type: Optional[str] = Field(None, description="Market cap type (Large Cap, etc.)")
    monthly_data: List[MonthlyHoldingSnapshot] = Field(..., description="Monthly snapshots (last N months)")


class SchemePortfolioSummary(BaseModel):
    """Complete portfolio summary for a scheme."""
    scheme_id: int = Field(..., description="Internal scheme ID")
    scheme_name: str = Field(..., description="Full scheme name")
    amc_name: str = Field(..., description="AMC name")
    plan_type: str = Field(..., description="Direct or Regular")
    option_type: str = Field(..., description="Growth, Dividend, or IDCW")
    category: Optional[str] = Field(None, description="Scheme category")
    
    # Monthly AUM data
    monthly_aum: List[dict] = Field(..., description="Monthly AUM data")
    
    # Holdings
    holdings: List[PortfolioHolding] = Field(..., description="Stock holdings with monthly comparison")
    total_holdings: int = Field(..., description="Total number of unique stocks")


class SchemeSearchResponse(BaseModel):
    """Response for scheme search endpoint."""
    query: str = Field(..., description="Search query")
    results: List[SchemeSearchResult] = Field(..., description="Matching schemes")
    total_results: int = Field(..., description="Total number of results")
