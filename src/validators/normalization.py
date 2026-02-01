"""
Data normalization functions.

Implements data cleaning and normalization according to Canonical Data Contract v1.0.
"""

import re
from typing import Optional, Union
from decimal import Decimal

from src.utils.numbers import lakhs_to_inr, crores_to_inr, scale_percent, round_to_precision
from src.utils.isin import format_isin


class AmbiguousDataError(Exception):
    """Raised when data is ambiguous and cannot be safely normalized."""
    pass


def clean_scheme_name(raw_name: str) -> str:
    """
    Clean scheme name by removing plan/option/date suffixes.
    
    Removes common suffixes like:
    - "- Direct Plan"
    - "- Growth Option"
    - "(Direct)"
    - "Jan 2025"
    - Etc.
    
    Args:
        raw_name: Raw scheme name
        
    Returns:
        Cleaned canonical scheme name
        
    Examples:
        >>> clean_scheme_name("HDFC Equity Fund - Direct Plan - Growth Option")
        "HDFC Equity Fund"
        >>> clean_scheme_name("ICICI Bluechip Fund (Direct) - Growth")
        "ICICI Bluechip Fund"
    """
    name = raw_name.strip()
    
    # Patterns to remove (in order)
    patterns = [
        r'\s*-\s*Direct\s*Plan\s*',
        r'\s*-\s*Regular\s*Plan\s*',
        r'\s*-\s*Growth\s*Option\s*',
        r'\s*-\s*Dividend\s*Option\s*',
        r'\s*-\s*IDCW\s*Option\s*',
        r'\s*-\s*Direct\s*',
        r'\s*-\s*Regular\s*',
        r'\s*-\s*Growth\s*',
        r'\s*-\s*Dividend\s*',
        r'\s*-\s*IDCW\s*',
        r'\s*\(Direct\)\s*',
        r'\s*\(Regular\)\s*',
        r'\s*\(Growth\)\s*',
        r'\s*\(Dividend\)\s*',
        r'\s*\(IDCW\)\s*',
        r'\s*-\s*[A-Z][a-z]{2,8}\s+\d{4}\s*',  # Month Year (e.g., "Jan 2025")
    ]
    
    for pattern in patterns:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    
    # Clean up extra whitespace and hyphens
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(r'\s*-\s*$', '', name)
    name = name.strip()
    
    return name


def normalize_plan_type(raw_plan: str) -> str:
    """
    Normalize plan type to canonical form.
    
    Args:
        raw_plan: Raw plan type string
        
    Returns:
        Normalized plan type ("Direct" or "Regular")
        
    Raises:
        ValueError: If plan type cannot be determined
        
    Examples:
        >>> normalize_plan_type("direct")
        "Direct"
        >>> normalize_plan_type("REGULAR PLAN")
        "Regular"
    """
    plan = raw_plan.strip().lower()
    
    if 'direct' in plan:
        return "Direct"
    elif 'regular' in plan:
        return "Regular"
    else:
        raise ValueError(f"Cannot determine plan type from: '{raw_plan}'")


def normalize_option_type(raw_option: str) -> str:
    """
    Normalize option type to canonical form.
    
    Args:
        raw_option: Raw option type string
        
    Returns:
        Normalized option type ("Growth", "Dividend", or "IDCW")
        
    Raises:
        ValueError: If option type cannot be determined
        
    Examples:
        >>> normalize_option_type("growth")
        "Growth"
        >>> normalize_option_type("DIVIDEND OPTION")
        "Dividend"
        >>> normalize_option_type("idcw")
        "IDCW"
    """
    option = raw_option.strip().lower()
    
    if 'growth' in option:
        return "Growth"
    elif 'idcw' in option:
        return "IDCW"
    elif 'dividend' in option or 'div' in option:
        return "Dividend"
    else:
        raise ValueError(f"Cannot determine option type from: '{raw_option}'")


def detect_and_convert_units(
    value: Union[int, float, Decimal],
    column_header: Optional[str] = None,
    hint: Optional[str] = None
) -> Decimal:
    """
    Detect units and convert to INR.
    
    Looks for hints in column header or explicit hint parameter.
    
    Args:
        value: Numeric value
        column_header: Optional column header (may contain unit hints)
        hint: Optional explicit unit hint ("lakhs", "crores", "inr")
        
    Returns:
        Value in INR
        
    Raises:
        AmbiguousDataError: If units cannot be determined
        
    Examples:
        >>> detect_and_convert_units(10, column_header="Market Value (Lakhs)")
        Decimal('1000000')
        >>> detect_and_convert_units(1.5, hint="crores")
        Decimal('15000000')
    """
    value_decimal = Decimal(str(value))
    
    # Determine unit from hint or column header
    unit = None
    
    if hint:
        unit = hint.lower().strip()
    elif column_header:
        header_lower = column_header.lower()
        if 'lakh' in header_lower or 'lac' in header_lower:
            unit = 'lakhs'
        elif 'crore' in header_lower or 'cr' in header_lower:
            unit = 'crores'
        elif 'inr' in header_lower or '₹' in header_lower or 'rupee' in header_lower:
            unit = 'inr'
    
    # Convert based on unit
    if unit == 'lakhs':
        return lakhs_to_inr(value_decimal)
    elif unit == 'crores':
        return crores_to_inr(value_decimal)
    elif unit == 'inr':
        return value_decimal
    else:
        # Cannot determine unit - this is ambiguous
        raise AmbiguousDataError(
            f"Cannot determine units for value {value}. "
            f"Column header: '{column_header}', Hint: '{hint}'. "
            f"Please specify units explicitly."
        )


def detect_and_scale_percent(
    value: Union[int, float, Decimal],
    column_header: Optional[str] = None
) -> Decimal:
    """
    Detect percentage scale and convert to 0-100 range.
    
    Heuristic: If value <= 1, assume 0-1 scale, otherwise assume 0-100 scale.
    
    Args:
        value: Percentage value
        column_header: Optional column header
        
    Returns:
        Percentage in 0-100 scale
        
    Examples:
        >>> detect_and_scale_percent(0.0306)
        Decimal('3.06')
        >>> detect_and_scale_percent(3.06)
        Decimal('3.06')
    """
    value_decimal = Decimal(str(value))
    
    # Heuristic: if value <= 1, assume 0-1 scale
    if value_decimal <= Decimal('1'):
        return scale_percent(value_decimal, from_scale="0-1")
    else:
        return scale_percent(value_decimal, from_scale="0-100")


def normalize_isin(raw_isin: str) -> str:
    """
    Normalize ISIN (trim, uppercase).
    
    Args:
        raw_isin: Raw ISIN string
        
    Returns:
        Normalized ISIN
    """
    return format_isin(raw_isin)


def normalize_company_name(raw_name: str) -> str:
    """
    Normalize company name.
    
    Args:
        raw_name: Raw company name
        
    Returns:
        Normalized company name
    """
    # Basic normalization: trim, collapse whitespace
    name = raw_name.strip()
    name = re.sub(r'\s+', ' ', name)
    
    return name


def round_market_value(value: Decimal) -> Decimal:
    """
    Round market value to schema precision (2 decimal places).
    
    Args:
        value: Market value
        
    Returns:
        Rounded value
    """
    return round_to_precision(value, decimals=2)


def round_percent_of_nav(value: Decimal) -> Decimal:
    """
    Round percent of NAV to schema precision (4 decimal places).
    
    Args:
        value: Percent of NAV
        
    Returns:
        Rounded value
    """
    return round_to_precision(value, decimals=4)
