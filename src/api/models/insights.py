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
    ownership_change_percent: Optional[float] = Field(None, description="% of total company outstanding shares bought/sold")
    holding_change_percent: Optional[float] = Field(None, description="% increase/decrease in existing mutual fund holdings")


class StockActivityResponse(BaseModel):
    """Response for stock activity insights (Top Buys/Sells)."""
    month: str = Field(..., description="Latest month for activity calculation (MMM-YY)")
    prev_month: str = Field(..., description="Previous month for comparison (MMM-YY)")
    results: List[StockActivityItem] = Field(..., description="List of stocks with highest activity")
    total_results: int = Field(..., description="Total count of results")
    activity_type: str = Field(..., description="'buying', 'selling', 'entrants', or 'exits'")
    data_warning: Optional[dict] = Field(None, description="Warning when data is based on a partial/incomplete period")


class SectorShiftItem(BaseModel):
    """Represents institutional shift across an industry sector between two periods."""
    sector: str = Field(..., description="Sector name")
    base_shares: int = Field(..., description="Total shares held in base period")
    target_shares: int = Field(..., description="Total shares held in target period")
    net_shares: int = Field(..., description="Net share delta")
    percent_change: float = Field(..., description="Percentage shift in sector holding")


class TimeMachineSummary(BaseModel):
    """High level executive summary of institutional changes between two time periods."""
    base_period: str = Field(..., description="Base/Origin month (MMM-YY)")
    target_period: str = Field(..., description="Target/Destination month (MMM-YY)")
    months_gap: int = Field(..., description="Number of months between base and target")
    total_shares_base: int = Field(..., description="Total industry equity shares held in base period")
    total_shares_target: int = Field(..., description="Total industry equity shares held in target period")
    net_shares_delta: int = Field(..., description="Net change in aggregate shares")
    net_shares_delta_pct: float = Field(..., description="Percentage change in aggregate shares")
    accumulated_stocks_count: int = Field(..., description="Number of stocks with net accumulation")
    distributed_stocks_count: int = Field(..., description="Number of stocks with net distribution")
    new_entrants_count: int = Field(..., description="Number of brand new additions")
    complete_exits_count: int = Field(..., description="Number of completely liquidated holdings")


class TimeMachineResponse(BaseModel):
    """Full Time Machine comparison response between two arbitrary historical periods."""
    summary: TimeMachineSummary
    top_accumulations: List[StockActivityItem]
    top_distributions: List[StockActivityItem]
    new_entrants: List[StockActivityItem]
    complete_exits: List[StockActivityItem]
    sector_shifts: List[SectorShiftItem]
    available_periods: List[str]
