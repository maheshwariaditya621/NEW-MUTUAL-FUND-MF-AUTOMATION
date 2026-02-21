"""
Pydantic models for stock holdings endpoints.
"""

from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import date, datetime
from decimal import Decimal


class CompanySearchResult(BaseModel):
    """Company search result model."""
    isin: str = Field(..., description="12-character ISIN code")
    company_name: str = Field(..., description="Company name")
    sector: Optional[str] = Field(None, description="Sector classification")
    nse_symbol: Optional[str] = Field(None, description="NSE trading symbol")
    bse_code: Optional[str] = Field(None, description="BSE code")


class MonthlyHoldingData(BaseModel):
    """Monthly holding data for a specific month."""
    month: str = Field(..., description="Month in YYYY-MM format")
    total_shares: int = Field(..., description="Total number of shares held")
    num_funds: int = Field(..., description="Number of funds holding this stock")
    trend: Optional[str] = Field(None, description="Trend indicator: up/down/same")
    is_adjusted: bool = Field(False, description="Whether the quantity was adjusted for corporate actions")


class HistoricalHolding(BaseModel):
    """Holding data for a specific month within a scheme."""
    month: str = Field(..., description="Month in YYYY-MM format")
    num_shares: int = Field(..., description="Number of shares held")
    percent_to_aum: Decimal = Field(..., description="Percentage of AUM")
    trend: Optional[str] = Field(None, description="Trend indicator: up/down/same")
    is_adjusted: bool = Field(False, description="Whether the quantity was adjusted for corporate actions")


class SchemeHolding(BaseModel):
    """Individual scheme holding details with history."""
    scheme_name: str = Field(..., description="Full scheme name")
    amc_name: str = Field(..., description="AMC name")
    plan_type: str = Field(..., description="Direct or Regular")
    option_type: str = Field(..., description="Growth, Dividend, or IDCW")
    aum_cr: Decimal = Field(..., description="Latest Assets Under Management in crores")
    
    # Historical data (last N months)
    history: List[HistoricalHolding] = Field(..., description="Monthly holding history for this scheme")


class StockHoldingsSummary(BaseModel):
    """Summary of stock holdings across all mutual funds."""
    isin: str = Field(..., description="12-character ISIN code")
    company_name: str = Field(..., description="Company name")
    sector: Optional[str] = Field(None, description="Sector classification")
    market_cap: Optional[Decimal] = Field(None, description="Market capitalization in INR Crores")
    mcap_type: Optional[str] = Field(None, description="Market cap category (Large/Mid/Small Cap)")
    mcap_updated_at: Optional[datetime] = Field(None, description="Timestamp when market cap was last updated")
    
    # Current month data
    as_of_date: str = Field(..., description="Latest data month (YYYY-MM)")
    total_shares: int = Field(..., description="Total shares held in current month")
    total_funds: int = Field(..., description="Number of funds holding in current month")
    
    # Historical trend (last 4 months)
    monthly_trend: List[MonthlyHoldingData] = Field(..., description="Monthly holding trend")
    
    # Detailed holdings
    holdings: List[SchemeHolding] = Field(..., description="Detailed scheme-wise holdings")


class StockSearchResponse(BaseModel):
    """Response for stock search endpoint."""
    query: str = Field(..., description="Search query")
    results: List[CompanySearchResult] = Field(..., description="Matching companies")
    total_results: int = Field(..., description="Total number of results")
