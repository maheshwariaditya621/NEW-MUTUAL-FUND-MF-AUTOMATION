"""
Pydantic models for AMC (Asset Management Company) explorer endpoints.
"""

from typing import List, Optional
from pydantic import BaseModel, Field
from decimal import Decimal


class AMCTopHolding(BaseModel):
    """Simplified holding info for AMC summary cards."""
    company_name: str = Field(..., description="Company name")
    percent_of_amc_equity: Decimal = Field(..., description="Percentage of total AMC equity AUM")


class AMCSummary(BaseModel):
    """Summary of an AMC for the gallery view."""
    amc_id: int = Field(..., description="Internal AMC ID")
    amc_name: str = Field(..., description="AMC Name")
    equity_aum_cr: Decimal = Field(..., description="Total Equity Assets Under Management across all schemes in crores")
    total_aum_cr: Decimal = Field(..., description="Total Assets Under Management across all schemes in crores (including non-equity)")
    scheme_count: int = Field(..., description="Number of managed schemes")
    top_holdings: List[AMCTopHolding] = Field(default_factory=list, description="Top 3 stock holdings for this AMC")


class AMCSchemeItem(BaseModel):
    """Individual scheme belonging to an AMC."""
    scheme_id: int = Field(..., description="Internal scheme ID")
    scheme_name: str = Field(..., description="Full scheme name")
    plan_type: str = Field(..., description="Direct or Regular")
    option_type: str = Field(..., description="Growth, Dividend, or IDCW")
    category: Optional[str] = Field(None, description="Scheme category")
    equity_aum_cr: Decimal = Field(..., description="Latest recorded Equity AUM in crores")
    total_aum_cr: Decimal = Field(..., description="Latest recorded Total AUM in crores")


class AMCDetail(BaseModel):
    """Comprehensive details for a specific AMC."""
    amc_id: int = Field(..., description="Internal AMC ID")
    amc_name: str = Field(..., description="AMC Name")
    equity_aum_cr: Decimal = Field(..., description="Total Equity AUM in crores")
    total_aum_cr: Decimal = Field(..., description="Total AUM in crores")
    scheme_count: int = Field(..., description="Total number of schemes")
    schemes: List[AMCSchemeItem] = Field(..., description="List of all schemes managed by this AMC")


class AMCListResponse(BaseModel):
    """Response model for the AMC explorer listing."""
    amcs: List[AMCSummary] = Field(..., description="List of AMC summaries")
    total_count: int = Field(..., description="Total number of AMCs")
    last_updated_month: str = Field(..., description="The month the data corresponds to (MMM-YY)")
