"""
Pydantic models for mutual fund insights endpoints.
"""

from typing import List, Optional
from pydantic import BaseModel, Field
from decimal import Decimal


class StockActivityItem(BaseModel):
    """Represents a stock's net activity (buying/selling) for a month."""
    isin: str = Field(..., description="12-character ISIN code")
    company_name: str = Field(..., description="Company name")
    sector: Optional[str] = Field(None, description="Sector classification")
    classification: Optional[str] = Field(None, description="Market cap category (Large/Mid/Small Cap)")
    market_cap: Optional[Decimal] = Field(None, description="Actual Market Cap in INR Crores")
    nse_symbol: Optional[str] = Field(None, description="NSE trading symbol")
    
    # Activity metrics
    net_qty_bought: int = Field(..., description="Net change in total shares held by all mutual funds")
    total_qty_curr: int = Field(..., description="Total shares held in the latest month")
    total_qty_prev: int = Field(..., description="Total shares held in the previous month")
    
    buy_value_crore: Decimal = Field(..., description="Approximate buy/sell value in INR Crores")
    
    # Fund participation
    num_funds_curr: int = Field(..., description="Number of funds holding this stock in the latest month")
    num_funds_prev: int = Field(..., description="Number of funds holding this stock in the previous month")
    net_fund_entrants: int = Field(..., description="Net change in number of funds holding this stock")


class StockActivityResponse(BaseModel):
    """Response for stock activity insights (Top Buys/Sells)."""
    month: str = Field(..., description="Latest month for activity calculation (MMM-YY)")
    prev_month: str = Field(..., description="Previous month for comparison (MMM-YY)")
    results: List[StockActivityItem] = Field(..., description="List of stocks with highest activity")
    total_results: int = Field(..., description="Total count of results")
    activity_type: str = Field(..., description="'buying' or 'selling'")
    data_warning: Optional[dict] = Field(None, description="Warning when data is based on a partial/incomplete period")
