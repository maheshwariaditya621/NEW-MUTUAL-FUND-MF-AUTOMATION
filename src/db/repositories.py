"""
Data access layer for PostgreSQL database.

Provides idempotent insert functions for master data and transactional inserts for holdings.
"""

from typing import Optional, Dict, Any, List
from datetime import date

from src.db.connection import get_cursor
from src.config import logger


def upsert_amc(amc_name: str) -> int:
    """
    Insert or get existing AMC.
    
    Args:
        amc_name: Canonical AMC name
        
    Returns:
        amc_id
        
    Raises:
        psycopg2.Error: If database operation fails
    """
    cursor = get_cursor()
    
    # Try to insert, return existing if conflict
    cursor.execute(
        """
        INSERT INTO amcs (amc_name)
        VALUES (%s)
        ON CONFLICT (amc_name) DO UPDATE SET amc_name = EXCLUDED.amc_name
        RETURNING amc_id
        """,
        (amc_name,)
    )
    
    amc_id = cursor.fetchone()[0]
    logger.debug(f"AMC upserted: {amc_name} (amc_id={amc_id})")
    
    return amc_id


def upsert_scheme(
    amc_id: int,
    scheme_name: str,
    plan_type: str,
    option_type: str,
    scheme_category: Optional[str] = None,
    scheme_code: Optional[str] = None
) -> int:
    """
    Insert or get existing scheme.
    
    Args:
        amc_id: AMC ID
        scheme_name: Canonical scheme name (without plan/option suffixes)
        plan_type: "Direct" or "Regular"
        option_type: "Growth", "Dividend", or "IDCW"
        scheme_category: Optional scheme category
        scheme_code: Optional AMC-specific scheme code
        
    Returns:
        scheme_id
        
    Raises:
        psycopg2.Error: If database operation fails
    """
    cursor = get_cursor()
    
    cursor.execute(
        """
        INSERT INTO schemes (amc_id, scheme_name, plan_type, option_type, scheme_category, scheme_code)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (amc_id, scheme_name, plan_type, option_type) 
        DO UPDATE SET 
            scheme_category = COALESCE(EXCLUDED.scheme_category, schemes.scheme_category),
            scheme_code = COALESCE(EXCLUDED.scheme_code, schemes.scheme_code),
            updated_at = CURRENT_TIMESTAMP
        RETURNING scheme_id
        """,
        (amc_id, scheme_name, plan_type, option_type, scheme_category, scheme_code)
    )
    
    scheme_id = cursor.fetchone()[0]
    logger.debug(f"Scheme upserted: {scheme_name} - {plan_type} - {option_type} (scheme_id={scheme_id})")
    
    return scheme_id


def upsert_period(year: int, month: int, period_end_date: date) -> int:
    """
    Insert or get existing period.
    
    Args:
        year: 4-digit year (2020-2100)
        month: Month number (1-12)
        period_end_date: Last day of the month
        
    Returns:
        period_id
        
    Raises:
        psycopg2.Error: If database operation fails
    """
    cursor = get_cursor()
    
    cursor.execute(
        """
        INSERT INTO periods (year, month, period_end_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (year, month) DO UPDATE SET period_end_date = EXCLUDED.period_end_date
        RETURNING period_id
        """,
        (year, month, period_end_date)
    )
    
    period_id = cursor.fetchone()[0]
    logger.debug(f"Period upserted: {year}-{month:02d} (period_id={period_id})")
    
    return period_id


def upsert_company(
    isin: str,
    company_name: str,
    exchange_symbol: Optional[str] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None
) -> int:
    """
    Insert or update company.
    
    Args:
        isin: 12-character ISIN code (equity only)
        company_name: Official company name
        exchange_symbol: Optional exchange symbol
        sector: Optional sector
        industry: Optional industry
        
    Returns:
        company_id
        
    Raises:
        psycopg2.Error: If database operation fails
    """
    cursor = get_cursor()
    
    cursor.execute(
        """
        INSERT INTO companies (isin, company_name, exchange_symbol, sector, industry)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (isin) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            exchange_symbol = COALESCE(EXCLUDED.exchange_symbol, companies.exchange_symbol),
            sector = COALESCE(EXCLUDED.sector, companies.sector),
            industry = COALESCE(EXCLUDED.industry, companies.industry),
            updated_at = CURRENT_TIMESTAMP
        RETURNING company_id
        """,
        (isin, company_name, exchange_symbol, sector, industry)
    )
    
    company_id = cursor.fetchone()[0]
    logger.debug(f"Company upserted: {company_name} ({isin}) (company_id={company_id})")
    
    return company_id


def create_snapshot(
    scheme_id: int,
    period_id: int,
    total_holdings: int,
    total_value_inr: float,
    holdings_count: int
) -> int:
    """
    Create a new scheme snapshot.
    
    NOTE: This will fail if snapshot already exists for this scheme-period combination
    due to UNIQUE constraint. This is intentional to prevent duplicate data.
    
    Args:
        scheme_id: Scheme ID
        period_id: Period ID
        total_holdings: Total number of holdings rows
        total_value_inr: Total portfolio value in INR
        holdings_count: Count of distinct companies
        
    Returns:
        snapshot_id
        
    Raises:
        psycopg2.IntegrityError: If snapshot already exists
        psycopg2.Error: If database operation fails
    """
    cursor = get_cursor()
    
    cursor.execute(
        """
        INSERT INTO scheme_snapshots (scheme_id, period_id, total_holdings, total_value_inr, holdings_count)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING snapshot_id
        """,
        (scheme_id, period_id, total_holdings, total_value_inr, holdings_count)
    )
    
    snapshot_id = cursor.fetchone()[0]
    logger.success(f"Snapshot created (snapshot_id={snapshot_id})")
    
    return snapshot_id


def insert_holdings(snapshot_id: int, holdings: List[Dict[str, Any]]) -> None:
    """
    Insert all equity holdings for a snapshot.
    
    Args:
        snapshot_id: Snapshot ID
        holdings: List of holding dictionaries with keys:
            - company_id: int
            - quantity: int
            - market_value_inr: float
            - percent_of_nav: float
            
    Raises:
        psycopg2.Error: If database operation fails
    """
    cursor = get_cursor()
    
    # Bulk insert using executemany
    cursor.executemany(
        """
        INSERT INTO equity_holdings (snapshot_id, company_id, quantity, market_value_inr, percent_of_nav)
        VALUES (%s, %s, %s, %s, %s)
        """,
        [
            (
                snapshot_id,
                holding['company_id'],
                holding['quantity'],
                holding['market_value_inr'],
                holding['percent_of_nav']
            )
            for holding in holdings
        ]
    )
    
    logger.success(f"{len(holdings)} holdings inserted for snapshot_id={snapshot_id}")


def check_snapshot_exists(scheme_id: int, period_id: int) -> bool:
    """
    Check if a snapshot already exists for a scheme-period combination.
    
    Args:
        scheme_id: Scheme ID
        period_id: Period ID
        
    Returns:
        True if snapshot exists, False otherwise
    """
    cursor = get_cursor()
    
    cursor.execute(
        """
        SELECT EXISTS(
            SELECT 1 FROM scheme_snapshots
            WHERE scheme_id = %s AND period_id = %s
        )
        """,
        (scheme_id, period_id)
    )
    
    return cursor.fetchone()[0]
