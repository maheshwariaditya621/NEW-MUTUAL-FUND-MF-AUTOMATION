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
    ownership_percent: Optional[float] = Field(None, description="Percentage of company shares held by mutual funds in this month")
    month_change: Optional[int] = Field(None, description="Absolute change in shares from previous month")
    percent_change: Optional[float] = Field(None, description="Percentage change in shares from previous month")


class HistoricalHolding(BaseModel):
    """Holding data for a specific month within a scheme."""
    month: str = Field(..., description="Month in YYYY-MM format")
    num_shares: Optional[int] = Field(None, description="Number of shares held. None if AMC data not uploaded for this month.")
    percent_to_aum: Optional[Decimal] = Field(None, description="Percentage of AUM")
    ownership_percent: Optional[float] = Field(None, description="Percentage of company shares held by this scheme in this month")
    trend: Optional[str] = Field(None, description="Trend indicator: up/down/same")
    is_adjusted: bool = Field(False, description="Whether the quantity was adjusted for corporate actions")
    month_change: Optional[int] = Field(None, description="Absolute change in shares from previous month")
    percent_change: Optional[float] = Field(None, description="Percentage change in shares from previous month")


class SchemeHolding(BaseModel):
    """Individual scheme holding details with history."""
    scheme_name: str = Field(..., description="Full scheme name")
    amc_name: str = Field(..., description="AMC name")
    plan_type: str = Field(..., description="Direct or Regular")
    option_type: str = Field(..., description="Growth, Dividend, or IDCW")
    equity_aum_cr: Decimal = Field(..., description="Latest Equity Assets Under Management in crores")
    total_aum_cr: Decimal = Field(..., description="Latest Total Assets Under Management in crores (including Debt/Liquid)")
    
    # Historical data (last N months)
    history: List[HistoricalHolding] = Field(..., description="Monthly holding history for this scheme")


class StockHoldingsSummary(BaseModel):
    """Summary of stock holdings across all mutual funds."""
    isin: str = Field(..., description="12-character ISIN code")
    company_name: str = Field(..., description="Company name")
    sector: Optional[str] = Field(None, description="Sector classification")
    market_cap: Optional[Decimal] = Field(None, description="Market capitalization in INR Crores")
    live_price: Optional[float] = Field(None, description="Current live price (LTP) of the stock")
    mcap_type: Optional[str] = Field(None, description="Market cap category (Large/Mid/Small Cap)")
    mcap_updated_at: Optional[datetime] = Field(None, description="Timestamp when market cap was last updated")
    shares_outstanding: Optional[int] = Field(None, description="Total shares outstanding for the company")
    shares_last_updated_at: Optional[datetime] = Field(None, description="Last known share count update time")
    
    # Current month data
    as_of_date: str = Field(..., description="Latest data month (YYYY-MM)")
    total_shares: int = Field(..., description="Total shares held in current month")
    total_funds: int = Field(..., description="Number of funds holding in current month")
    ownership_percent: Optional[float] = Field(None, description="Percentage of company shares held by mutual funds")
    
    # Historical trend (last 4 months)
    monthly_trend: List[MonthlyHoldingData] = Field(..., description="Monthly holding trend")
    
    # Detailed holdings
    holdings: List[SchemeHolding] = Field(..., description="Detailed scheme-wise holdings")

    # Data completeness warning (present when latest month has incomplete AMC data)
    data_warning: Optional[dict] = Field(None, description="Warning when latest period has partial AMC data")


class StockSearchResponse(BaseModel):
    """Response for stock search endpoint."""
    query: str = Field(..., description="Search query")
    results: List[CompanySearchResult] = Field(..., description="Matching companies")
    total_results: int = Field(..., description="Total number of results")


class BulkPriceRequest(BaseModel):
    """Request model for bulk price fetching."""
    isins: List[str] = Field(..., description="List of 12-character ISIN codes")
