"""
Canonical Data Contract v1.0 validators.

Implements strict validation rules from the Canonical Data Contract.
"""

from typing import List, Dict, Any, Optional
from decimal import Decimal

from src.utils.isin import is_valid_equity_isin
from src.utils.numbers import validate_non_negative, validate_percent_range


class ValidationError(Exception):
    """Raised when data fails Canonical Data Contract validation."""
    pass


def validate_mandatory_fields(data: Dict[str, Any], required_fields: List[str]) -> None:
    """
    Validate that all mandatory fields are present and non-empty.
    
    Args:
        data: Data dictionary
        required_fields: List of required field names
        
    Raises:
        ValidationError: If any mandatory field is missing or empty
    """
    missing_fields = []
    empty_fields = []
    
    for field in required_fields:
        if field not in data:
            missing_fields.append(field)
        elif data[field] is None or (isinstance(data[field], str) and not data[field].strip()):
            empty_fields.append(field)
    
    if missing_fields:
        raise ValidationError(f"Missing mandatory fields: {', '.join(missing_fields)}")
    
    if empty_fields:
        raise ValidationError(f"Empty mandatory fields: {', '.join(empty_fields)}")


def validate_isin(isin: str) -> None:
    """
    Validate ISIN according to Canonical Data Contract v1.0.
    
    Args:
        isin: ISIN code
        
    Raises:
        ValidationError: If ISIN is invalid
    """
    if not is_valid_equity_isin(isin):
        raise ValidationError(
            f"Invalid ISIN: '{isin}'. Must be 12-character Indian equity ISIN "
            f"(format: INE[6 chars]10[1 char])"
        )


def validate_plan_type(plan_type: str) -> None:
    """
    Validate plan type.
    
    Args:
        plan_type: Plan type string
        
    Raises:
        ValidationError: If plan type is invalid
    """
    valid_plans = ["Direct", "Regular"]
    
    if plan_type not in valid_plans:
        raise ValidationError(
            f"Invalid plan_type: '{plan_type}'. Must be one of: {', '.join(valid_plans)}"
        )


def validate_option_type(option_type: str) -> None:
    """
    Validate option type.
    
    Args:
        option_type: Option type string
        
    Raises:
        ValidationError: If option type is invalid
    """
    valid_options = ["Growth", "Dividend", "IDCW"]
    
    if option_type not in valid_options:
        raise ValidationError(
            f"Invalid option_type: '{option_type}'. Must be one of: {', '.join(valid_options)}"
        )


def validate_numeric_ranges(
    quantity: int,
    market_value_inr: Decimal,
    percent_of_nav: Decimal
) -> None:
    """
    Validate numeric ranges according to schema constraints.
    
    Args:
        quantity: Number of shares
        market_value_inr: Market value in INR
        percent_of_nav: Percentage of NAV (0-100 scale)
        
    Raises:
        ValidationError: If any value is out of valid range
    """
    try:
        # Quantity can be 0 (exited positions)
        validate_non_negative(quantity, "quantity")
        
        # Market value can be 0 (exited positions)
        validate_non_negative(market_value_inr, "market_value_inr")
        
        # Percent of NAV can be 0 (exited positions or rounding)
        validate_percent_range(percent_of_nav, "percent_of_nav")
        
    except ValueError as e:
        raise ValidationError(str(e))


def validate_no_duplicates(holdings: List[Dict[str, Any]]) -> None:
    """
    Validate that there are no duplicate ISINs in holdings.
    
    Args:
        holdings: List of holding dictionaries with 'isin' key
        
    Raises:
        ValidationError: If duplicate ISINs found
    """
    isins = [h['isin'] for h in holdings]
    duplicates = [isin for isin in set(isins) if isins.count(isin) > 1]
    
    if duplicates:
        raise ValidationError(
            f"Duplicate ISINs found in holdings: {', '.join(duplicates)}"
        )


def validate_percent_sum(
    holdings: List[Dict[str, Any]],
    tolerance: Decimal = Decimal('5.0')
) -> Optional[str]:
    """
    Validate that sum of percent_of_nav is approximately 100%.
    
    This is a WARNING, not an error. Returns warning message if sum is off.
    
    Args:
        holdings: List of holding dictionaries with 'percent_of_nav' key
        tolerance: Acceptable deviation from 100% (default: 5%)
        
    Returns:
        Warning message if sum is off, None otherwise
    """
    total_percent = sum(Decimal(str(h['percent_of_nav'])) for h in holdings)
    
    if abs(total_percent - Decimal('100')) > tolerance:
        return (
            f"Percent of NAV sum mismatch: {total_percent:.2f}% "
            f"(expected ~100%, tolerance ±{tolerance}%)"
        )
    
    return None


def validate_scheme_metadata(
    amc_name: str,
    scheme_name: str,
    plan_type: str,
    option_type: str
) -> None:
    """
    Validate scheme metadata.
    
    Args:
        amc_name: AMC name
        scheme_name: Scheme name
        plan_type: Plan type
        option_type: Option type
        
    Raises:
        ValidationError: If any field is invalid
    """
    # Check mandatory fields
    validate_mandatory_fields(
        {
            'amc_name': amc_name,
            'scheme_name': scheme_name,
            'plan_type': plan_type,
            'option_type': option_type,
        },
        ['amc_name', 'scheme_name', 'plan_type', 'option_type']
    )
    
    # Validate plan and option types
    validate_plan_type(plan_type)
    validate_option_type(option_type)


def validate_holding(holding: Dict[str, Any]) -> None:
    """
    Validate a single holding record.
    
    Args:
        holding: Holding dictionary with required fields
        
    Raises:
        ValidationError: If holding is invalid
    """
    # Check mandatory fields
    required_fields = ['isin', 'company_name', 'quantity', 'market_value_inr', 'percent_of_nav']
    validate_mandatory_fields(holding, required_fields)
    
    # Validate ISIN
    validate_isin(holding['isin'])
    
    # Validate numeric ranges
    validate_numeric_ranges(
        quantity=int(holding['quantity']),
        market_value_inr=Decimal(str(holding['market_value_inr'])),
        percent_of_nav=Decimal(str(holding['percent_of_nav']))
    )


def validate_holdings_list(holdings: List[Dict[str, Any]]) -> Optional[str]:
    """
    Validate entire holdings list.
    
    Args:
        holdings: List of holding dictionaries
        
    Returns:
        Warning message if any, None otherwise
        
    Raises:
        ValidationError: If validation fails
    """
    if not holdings:
        raise ValidationError("Holdings list is empty")
    
    # Validate each holding
    for i, holding in enumerate(holdings):
        try:
            validate_holding(holding)
        except ValidationError as e:
            raise ValidationError(f"Holding #{i+1} invalid: {str(e)}")
    
    # Check for duplicates
    validate_no_duplicates(holdings)
    
    # Check percent sum (warning only)
    warning = validate_percent_sum(holdings)
    
    return warning
