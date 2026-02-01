"""
Equity holdings loader for PostgreSQL.

Implements transaction-safe loading of equity holdings data.
"""

from typing import List, Dict, Any
from decimal import Decimal

from src.db import (
    transactional,
    upsert_amc,
    upsert_scheme,
    upsert_period,
    upsert_company,
    create_snapshot,
    insert_holdings,
    check_snapshot_exists,
)
from src.validators import (
    ValidationError,
    validate_scheme_metadata,
    validate_holdings_list,
    normalize_isin,
    normalize_company_name,
    round_market_value,
    round_percent_of_nav,
)
from src.utils import get_period_end_date
from src.config import logger


class EquityHoldingsLoader:
    """
    Loader for equity holdings data.
    
    Handles validation, normalization, and transaction-safe loading to PostgreSQL.
    """
    
    def __init__(self):
        """Initialize loader."""
        pass
    
    @transactional
    def load_scheme_month(
        self,
        amc_name: str,
        scheme_name: str,
        plan_type: str,
        option_type: str,
        year: int,
        month: int,
        holdings_data: List[Dict[str, Any]],
        scheme_category: str = None,
        scheme_code: str = None
    ) -> int:
        """
        Load equity holdings for a scheme-month combination.
        
        This method is wrapped in @transactional decorator, so:
        - All operations happen in ONE transaction
        - Commits on success
        - Rolls back on ANY error
        
        Args:
            amc_name: Canonical AMC name
            scheme_name: Canonical scheme name (without plan/option suffixes)
            plan_type: "Direct" or "Regular"
            option_type: "Growth", "Dividend", or "IDCW"
            year: 4-digit year
            month: Month number (1-12)
            holdings_data: List of holding dictionaries with keys:
                - isin: str
                - company_name: str
                - quantity: int
                - market_value_inr: float/Decimal
                - percent_of_nav: float/Decimal
                - exchange_symbol: str (optional)
                - sector: str (optional)
                - industry: str (optional)
            scheme_category: Optional scheme category
            scheme_code: Optional AMC-specific scheme code
            
        Returns:
            snapshot_id
            
        Raises:
            ValidationError: If data validation fails
            psycopg2.Error: If database operation fails
        """
        logger.info(f"Loading {amc_name} - {scheme_name} - {plan_type} - {option_type} ({year}-{month:02d})")
        
        # STEP 1: Validate scheme metadata
        logger.info("Validating scheme metadata")
        validate_scheme_metadata(amc_name, scheme_name, plan_type, option_type)
        
        # STEP 2: Validate holdings
        logger.info(f"Validating {len(holdings_data)} holdings")
        warning = validate_holdings_list(holdings_data)
        
        if warning:
            logger.warning(warning)
        
        logger.success("All validations passed")
        
        # STEP 3: Normalize and prepare data
        logger.info("Normalizing data")
        
        # Normalize holdings
        normalized_holdings = []
        for holding in holdings_data:
            normalized_holdings.append({
                'isin': normalize_isin(holding['isin']),
                'company_name': normalize_company_name(holding['company_name']),
                'quantity': int(holding['quantity']),
                'market_value_inr': round_market_value(Decimal(str(holding['market_value_inr']))),
                'percent_of_nav': round_percent_of_nav(Decimal(str(holding['percent_of_nav']))),
                'exchange_symbol': holding.get('exchange_symbol'),
                'sector': holding.get('sector'),
                'industry': holding.get('industry'),
            })
        
        logger.success("Data normalized")
        
        # STEP 4: Upsert master data
        logger.info("Upserting master data")
        
        # Upsert AMC
        amc_id = upsert_amc(amc_name)
        
        # Upsert Scheme
        scheme_id = upsert_scheme(
            amc_id=amc_id,
            scheme_name=scheme_name,
            plan_type=plan_type,
            option_type=option_type,
            scheme_category=scheme_category,
            scheme_code=scheme_code
        )
        
        # Upsert Period
        period_end_date = get_period_end_date(year, month)
        period_id = upsert_period(year, month, period_end_date)
        
        # Check if snapshot already exists
        if check_snapshot_exists(scheme_id, period_id):
            raise ValidationError(
                f"Snapshot already exists for scheme_id={scheme_id}, period_id={period_id}. "
                f"Cannot load duplicate data."
            )
        
        # Upsert Companies
        company_map = {}  # isin -> company_id
        for holding in normalized_holdings:
            company_id = upsert_company(
                isin=holding['isin'],
                company_name=holding['company_name'],
                exchange_symbol=holding['exchange_symbol'],
                sector=holding['sector'],
                industry=holding['industry']
            )
            company_map[holding['isin']] = company_id
        
        logger.success("Master data upserted")
        
        # STEP 5: Calculate snapshot metadata
        total_holdings = len(normalized_holdings)
        total_value_inr = sum(h['market_value_inr'] for h in normalized_holdings)
        holdings_count = len(set(h['isin'] for h in normalized_holdings))  # Distinct companies
        
        # STEP 6: Create snapshot
        logger.info("Creating snapshot")
        snapshot_id = create_snapshot(
            scheme_id=scheme_id,
            period_id=period_id,
            total_holdings=total_holdings,
            total_value_inr=float(total_value_inr),
            holdings_count=holdings_count
        )
        
        # STEP 7: Insert holdings
        logger.info(f"Inserting {total_holdings} holdings")
        
        holdings_to_insert = [
            {
                'company_id': company_map[h['isin']],
                'quantity': h['quantity'],
                'market_value_inr': float(h['market_value_inr']),
                'percent_of_nav': float(h['percent_of_nav']),
            }
            for h in normalized_holdings
        ]
        
        insert_holdings(snapshot_id, holdings_to_insert)
        
        logger.success(f"✅ Pipeline completed successfully (snapshot_id={snapshot_id})")
        
        return snapshot_id
