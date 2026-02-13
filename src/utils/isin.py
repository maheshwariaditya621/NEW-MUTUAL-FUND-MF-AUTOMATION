"""
ISIN validation utilities.

Implements ISIN validation according to Canonical Data Contract v1.0.
"""

import re
from typing import Optional


# ISIN regex pattern for Indian equity securities
# Format: INE[5 alphanumeric][security code "10"][2 check digits]
# Example: INE040A01034 = INE + 040A0 + 10 + 34
EQUITY_ISIN_PATTERN = re.compile(r'^INE[A-Z0-9]{5}10[A-Z0-9]{2}$')


def is_valid_equity_isin(isin: str) -> bool:
    """
    Validate if ISIN is a valid Indian equity ISIN.
    
    Rules (from Canonical Data Contract v1.0):
    - Must be exactly 12 characters
    - Must start with "INE"
    - Characters 4-8: alphanumeric (company code, 5 chars)
    - Characters 9-10: must be "10" (equity security code)
    - Characters 11-12: check digits (2 chars, alphanumeric)
    
    Args:
        isin: ISIN code to validate
        
    Returns:
        True if valid equity ISIN, False otherwise
        
    Examples:
        >>> is_valid_equity_isin("INE002A01018")
        True
        >>> is_valid_equity_isin("INE002A01201")  # Debt (security code "01")
        False
        >>> is_valid_equity_isin("INE002A0101")  # Too short
        False
    """
    if not isin or not isinstance(isin, str):
        return False
    
    return bool(EQUITY_ISIN_PATTERN.match(isin))


def format_isin(isin: str) -> str:
    """
    Format ISIN by trimming whitespace and converting to uppercase.
    
    Args:
        isin: Raw ISIN string
        
    Returns:
        Formatted ISIN
        
    Examples:
        >>> format_isin(" ine002a01018 ")
        "INE002A01018"
    """
    if not isin:
        return ""
    
    return isin.strip().upper()


def validate_and_format_isin(isin: str) -> str:
    """
    Validate and format ISIN in one step.
    
    Args:
        isin: Raw ISIN string
        
    Returns:
        Formatted ISIN
        
    Raises:
        ValueError: If ISIN is invalid
    """
    formatted = format_isin(isin)
    
    if not is_valid_equity_isin(formatted):
        raise ValueError(
            f"Invalid ISIN: '{isin}'. Must be 12-character Indian equity ISIN "
            f"(format: INE[6 chars]10[1 char])"
        )
    
    return formatted
