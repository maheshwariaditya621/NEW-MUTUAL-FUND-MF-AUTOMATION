"""
Date utilities for period management.

Provides date calculation and parsing functions.
"""

from datetime import date, datetime
from calendar import monthrange
from typing import Union


def get_period_end_date(year: int, month: int) -> date:
    """
    Get the last day of a given month.
    
    Args:
        year: 4-digit year
        month: Month number (1-12)
        
    Returns:
        Last day of the month as date object
        
    Examples:
        >>> get_period_end_date(2025, 1)
        date(2025, 1, 31)
        >>> get_period_end_date(2025, 2)
        date(2025, 2, 28)
        >>> get_period_end_date(2024, 2)  # Leap year
        date(2024, 2, 29)
    """
    last_day = monthrange(year, month)[1]
    return date(year, month, last_day)


def parse_date(date_string: Union[str, date, datetime]) -> date:
    """
    Parse date from various formats.
    
    Args:
        date_string: Date in string, date, or datetime format
        
    Returns:
        date object
        
    Raises:
        ValueError: If date format is not recognized
        
    Examples:
        >>> parse_date("2025-01-31")
        date(2025, 1, 31)
        >>> parse_date("31/01/2025")
        date(2025, 1, 31)
    """
    if isinstance(date_string, date):
        return date_string
    
    if isinstance(date_string, datetime):
        return date_string.date()
    
    if not isinstance(date_string, str):
        raise ValueError(f"Invalid date type: {type(date_string)}")
    
    # Try common date formats
    formats = [
        "%Y-%m-%d",      # 2025-01-31
        "%d/%m/%Y",      # 31/01/2025
        "%d-%m-%Y",      # 31-01-2025
        "%Y/%m/%d",      # 2025/01/31
        "%d %b %Y",      # 31 Jan 2025
        "%d %B %Y",      # 31 January 2025
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_string.strip(), fmt).date()
        except ValueError:
            continue
    
    raise ValueError(f"Could not parse date: '{date_string}'")


def format_period(year: int, month: int) -> str:
    """
    Format period as human-readable string.
    
    Args:
        year: 4-digit year
        month: Month number (1-12)
        
    Returns:
        Formatted period string
        
    Examples:
        >>> format_period(2025, 1)
        "January 2025"
    """
    month_names = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    
    return f"{month_names[month - 1]} {year}"
