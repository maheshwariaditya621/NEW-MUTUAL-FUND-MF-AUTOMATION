"""
Numeric conversion and validation utilities.

Provides functions for unit conversion and numeric validation.
"""

from typing import Union, Optional
from decimal import Decimal, ROUND_HALF_UP


def lakhs_to_inr(value: Union[int, float, Decimal]) -> Decimal:
    """
    Convert lakhs to INR.
    
    Args:
        value: Value in lakhs
        
    Returns:
        Value in INR
        
    Examples:
        >>> lakhs_to_inr(10)
        Decimal('1000000')
        >>> lakhs_to_inr(2.5)
        Decimal('250000')
    """
    return Decimal(str(value)) * Decimal('100000')


def crores_to_inr(value: Union[int, float, Decimal]) -> Decimal:
    """
    Convert crores to INR.
    
    Args:
        value: Value in crores
        
    Returns:
        Value in INR
        
    Examples:
        >>> crores_to_inr(1)
        Decimal('10000000')
        >>> crores_to_inr(0.5)
        Decimal('5000000')
    """
    return Decimal(str(value)) * Decimal('10000000')


def scale_percent(value: Union[int, float, Decimal], from_scale: str = "0-1") -> Decimal:
    """
    Scale percentage to 0-100 range.
    
    Args:
        value: Percentage value
        from_scale: Source scale ("0-1" or "0-100")
        
    Returns:
        Percentage in 0-100 scale
        
    Examples:
        >>> scale_percent(0.0306, "0-1")
        Decimal('3.06')
        >>> scale_percent(3.06, "0-100")
        Decimal('3.06')
    """
    value_decimal = Decimal(str(value))
    
    if from_scale == "0-1":
        return value_decimal * Decimal('100')
    elif from_scale == "0-100":
        return value_decimal
    else:
        raise ValueError(f"Invalid scale: {from_scale}")


def round_to_precision(value: Union[int, float, Decimal], decimals: int) -> Decimal:
    """
    Round value to specified decimal places.
    
    Args:
        value: Value to round
        decimals: Number of decimal places
        
    Returns:
        Rounded value
        
    Examples:
        >>> round_to_precision(123.456789, 2)
        Decimal('123.46')
        >>> round_to_precision(123.456789, 4)
        Decimal('123.4568')
    """
    value_decimal = Decimal(str(value))
    quantizer = Decimal('0.1') ** decimals
    return value_decimal.quantize(quantizer, rounding=ROUND_HALF_UP)


def validate_non_negative(value: Union[int, float, Decimal], field_name: str) -> None:
    """
    Validate that a value is non-negative.
    
    Args:
        value: Value to validate
        field_name: Name of field (for error message)
        
    Raises:
        ValueError: If value is negative
    """
    if value < 0:
        raise ValueError(f"{field_name} cannot be negative: {value}")


def validate_percent_range(value: Union[int, float, Decimal], field_name: str) -> None:
    """
    Validate that a percentage is in valid range (0-100).
    
    Args:
        value: Percentage value
        field_name: Name of field (for error message)
        
    Raises:
        ValueError: If value is out of range
    """
    if value < 0 or value > 100:
        raise ValueError(f"{field_name} must be between 0 and 100: {value}")


def safe_decimal(value: Union[int, float, str, Decimal, None], default: Decimal = Decimal('0')) -> Decimal:
    """
    Safely convert value to Decimal.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Decimal value
    """
    if value is None:
        return default
    
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return default
