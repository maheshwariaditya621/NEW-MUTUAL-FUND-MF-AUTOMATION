"""
Data access layer for PostgreSQL database.

Provides idempotent insert functions for master data and transactional inserts for holdings.
"""

from typing import Optional, Dict, Any, List
from datetime import date

from src.db.connection import get_cursor
from src.config import logger


def upsert_isin_master(
    isin: str,
    canonical_name: str,
    nse_symbol: Optional[str] = None,
    bse_code: Optional[str] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None
) -> str:
    """
    Insert or update the ISIN Master (Source of Truth).
    """
    cursor = get_cursor()
    cursor.execute(
        """
        INSERT INTO isin_master (isin, canonical_name, nse_symbol, bse_code, sector, industry)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (isin) DO UPDATE SET 
            canonical_name = EXCLUDED.canonical_name,
            nse_symbol = COALESCE(EXCLUDED.nse_symbol, isin_master.nse_symbol),
            bse_code = COALESCE(EXCLUDED.bse_code, isin_master.bse_code),
            sector = COALESCE(EXCLUDED.sector, isin_master.sector),
            industry = COALESCE(EXCLUDED.industry, isin_master.industry),
            updated_at = CURRENT_TIMESTAMP
        RETURNING isin
        """,
        (isin, canonical_name, nse_symbol, bse_code, sector, industry)
    )
    return cursor.fetchone()[0]

def get_isin_master_details(isin: str) -> Optional[Dict[str, Any]]:
    """
    Lookup details from ISIN Master.
    """
    cursor = get_cursor()
    cursor.execute(
        "SELECT canonical_name, nse_symbol, bse_code, sector, industry FROM isin_master WHERE isin = %s",
        (isin,)
    )
    row = cursor.fetchone()
    if row:
        return {
            "canonical_name": row[0],
            "nse_symbol": row[1],
            "bse_code": row[2],
            "sector": row[3],
            "industry": row[4]
        }
    return None

def get_canonical_sector(raw_sector_name: Any) -> str:
    """
    Resolves a raw AMC sector name to a canonical one.
    Fallback: returns the cleaned raw name if no mapping found.
    """
    if not raw_sector_name or pd.isna(raw_sector_name) if 'pd' in globals() else not isinstance(raw_sector_name, str):
        if not isinstance(raw_sector_name, str):
            # Handle NaN or None
            if raw_sector_name is None: return "Unknown"
            try:
                import pandas as pd
                if pd.isna(raw_sector_name): return "Unknown"
            except ImportError:
                pass
            return "Unknown"
        
    cursor = get_cursor()
    cursor.execute(
        "SELECT canonical_sector FROM sector_master WHERE raw_sector_name = %s",
        (str(raw_sector_name).upper().strip(),)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    
    # Optional: Logic to auto-seed or just return cleaned name
    return str(raw_sector_name).upper().strip()

def upsert_company_master(
    isin: str,
    canonical_name: str,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    is_listed: bool = True
) -> int:
    """
    Insert or update the Analytical Entity (Company Master).
    """
    cursor = get_cursor()
    
    # 1. Resolve canonical sector if provided
    canonical_sector = get_canonical_sector(sector) if sector else None

    # 2. Upsert with Date Tracking
    cursor.execute(
        """
        INSERT INTO company_master (
            isin, canonical_name, sector, industry, is_listed, 
            first_seen_date, last_seen_date
        )
        VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_DATE)
        ON CONFLICT (isin) DO UPDATE SET 
            canonical_name = EXCLUDED.canonical_name,
            sector = COALESCE(EXCLUDED.sector, company_master.sector),
            industry = COALESCE(EXCLUDED.industry, company_master.industry),
            last_seen_date = CURRENT_DATE,
            updated_at = CURRENT_TIMESTAMP
        RETURNING company_id
        """,
        (isin, canonical_name, canonical_sector, industry, is_listed)
    )
    return cursor.fetchone()[0]

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
    is_reinvest: bool = False,
    scheme_category: Optional[str] = None,
    scheme_code: Optional[str] = None
) -> int:
    """
    Insert or get existing scheme with granular plan/option split.
    """
    cursor = get_cursor()
    
    cursor.execute(
        """
        INSERT INTO schemes (amc_id, scheme_name, plan_type, option_type, is_reinvest, scheme_category, scheme_code)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (amc_id, scheme_name, plan_type, option_type, is_reinvest) 
        DO UPDATE SET 
            is_reinvest = EXCLUDED.is_reinvest,
            scheme_category = COALESCE(EXCLUDED.scheme_category, schemes.scheme_category),
            scheme_code = COALESCE(EXCLUDED.scheme_code, schemes.scheme_code),
            updated_at = CURRENT_TIMESTAMP
        RETURNING scheme_id
        """,
        (amc_id, scheme_name.upper(), plan_type, option_type, is_reinvest, scheme_category, scheme_code)
    )
    
    scheme_id = cursor.fetchone()[0]
    logger.debug(f"Scheme upserted: {scheme_name} ({plan_type}/{option_type})")
    
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

def check_period_locked(year: int, month: int) -> bool:
    """
    Check if a period is marked as FINAL.
    """
    cursor = get_cursor()
    cursor.execute(
        "SELECT period_status FROM periods WHERE year = %s AND month = %s",
        (year, month)
    )
    row = cursor.fetchone()
    if row and row[0] == 'FINAL':
        return True
    return False


def upsert_company(
    isin: str,
    company_name: str,
    exchange_symbol: Optional[str] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    nse_symbol: Optional[str] = None,
    bse_code: Optional[str] = None
) -> int:
    """
    Insert or update company with support for exchange symbols and sectors.
    """
    cursor = get_cursor()
    
    cursor.execute(
        """
        INSERT INTO companies (isin, company_name, exchange_symbol, sector, industry, nse_symbol, bse_code)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (isin) DO UPDATE SET 
            company_name = EXCLUDED.company_name,
            exchange_symbol = COALESCE(EXCLUDED.exchange_symbol, companies.exchange_symbol),
            sector = COALESCE(EXCLUDED.sector, companies.sector),
            industry = COALESCE(EXCLUDED.industry, companies.industry),
            nse_symbol = COALESCE(EXCLUDED.nse_symbol, companies.nse_symbol),
            bse_code = COALESCE(EXCLUDED.bse_code, companies.bse_code),
            updated_at = CURRENT_TIMESTAMP
        RETURNING company_id
        """,
        (isin, company_name, exchange_symbol, sector, industry, nse_symbol, bse_code)
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

def record_extraction_run(
    amc_id: int,
    period_id: int,
    file_name: str,
    file_hash: str,
    extractor_version: str,
    header_fingerprint: str,
    rows_read: int,
    rows_inserted: int,
    rows_filtered: int,
    total_value: float,
    processing_time_seconds: float,
    status: str,
    error_log: Optional[str] = None,
    git_commit_hash: Optional[str] = None
) -> int:
    """
    Log an extraction run with full financial lineage.
    """
    cursor = get_cursor()
    cursor.execute(
        """
        INSERT INTO extraction_runs (
            amc_id, period_id, file_name, file_hash, extractor_version,
            header_fingerprint, rows_read, rows_inserted, rows_filtered, 
            total_value, processing_time_seconds, status, error_log,
            git_commit_hash
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING run_id
        """,
        (amc_id, period_id, file_name, file_hash, extractor_version, 
         header_fingerprint, rows_read, rows_inserted, rows_filtered, 
         total_value, processing_time_seconds, status, error_log,
         git_commit_hash)
    )
    run_id = cursor.fetchone()[0]
    logger.info(f"Extraction run recorded: {status} (run_id={run_id})")
    return run_id


def get_isin_details(isin: str) -> Optional[Dict[str, Any]]:
    """
    Fetch canonical details for an ISIN.
    Priority: isin_master -> companies (already exists)
    """
    # 1. Try isin_master
    master = get_isin_master_details(isin)
    if master:
        return master
        
    # 2. Fallback to existing company record
    cursor = get_cursor()
    cursor.execute(
        "SELECT company_name, sector, industry, nse_symbol, bse_code FROM companies WHERE isin = %s",
        (isin,)
    )
    row = cursor.fetchone()
    if row:
        return {
            "canonical_name": row[0],
            "sector": row[1],
            "industry": row[2],
            "nse_symbol": row[3],
            "bse_code": row[4]
        }
    return None

def check_file_already_extracted(file_hash: str) -> bool:
    """
    Check if this exact file (by hash) has already been successfully extracted.
    """
    cursor = get_cursor()
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM extraction_runs WHERE file_hash = %s AND status = 'SUCCESS')",
        (file_hash,)
    )
    return cursor.fetchone()[0]

def upsert_scheme_master(
    raw_sheet_name: str,
    canonical_name: str,
    plan_type: str,
    option_type: str
) -> None:
    """
    Map a raw Excel sheet name to canonical scheme attributes.
    """
    cursor = get_cursor()

def delete_extraction_run_and_holdings(amc_id: int, period_id: int) -> int:
    """
    Rollback: Delete all holdings and snapshots for an AMC and period,
    plus the extraction run record.
    Returns number of holdings deleted.
    """
    cursor = get_cursor()
    
    # 1. Get snapshots
    cursor.execute(
        "SELECT snapshot_id FROM scheme_snapshots WHERE scheme_id IN (SELECT scheme_id FROM schemes WHERE amc_id = %s) AND period_id = %s",
        (amc_id, period_id)
    )
    snapshot_ids = [r[0] for r in cursor.fetchall()]
    
    deleted_count = 0
    if snapshot_ids:
        # 2. Delete holdings
        cursor.execute(
            "DELETE FROM equity_holdings WHERE snapshot_id = ANY(%s)",
            (snapshot_ids,)
        )
        deleted_count = cursor.rowcount
        
        # 3. Delete snapshots
        cursor.execute(
            "DELETE FROM scheme_snapshots WHERE snapshot_id = ANY(%s)",
            (snapshot_ids,)
        )
    
    # 4. Delete extraction run
    cursor.execute(
        "DELETE FROM extraction_runs WHERE amc_id = %s AND period_id = %s",
        (amc_id, period_id)
    )
    
    logger.warning(f"ROLLBACK EXECUTED for amc_id={amc_id}, period_id={period_id}. Deleted {deleted_count} holdings.")
    return deleted_count

def upsert_nav_entries(nav_data: List[Dict[str, Any]]):
    """
    Bulk upsert NAV entries into nav_history.
    Enforces Inception Date Guard and Historical Year Locks.
    """
    from psycopg2.extras import execute_values
    from .connection import get_connection
    
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Fetch current AMFI code -> (scheme_id, inception_date) mapping
    cur.execute("SELECT amfi_code, scheme_id, inception_date FROM schemes WHERE amfi_code IS NOT NULL")
    scheme_meta = {row[0]: {"id": row[1], "inception": row[2]} for row in cur.fetchall()}
    
    # 2. Fetch locked years
    cur.execute("SELECT lock_year FROM nav_history_locks WHERE is_locked = TRUE")
    locked_years = {row[0] for row in cur.fetchall()}
    
    query = """
        INSERT INTO nav_history (
            scheme_code, isin_growth, isin_div_payout, 
            isin_div_reinv, scheme_name, nav_value, nav_date,
            plan_type, option_type, is_reinvest, scheme_id
        )
        VALUES %s
        ON CONFLICT (scheme_code, nav_date) DO UPDATE 
        SET 
            nav_value = EXCLUDED.nav_value,
            scheme_name = EXCLUDED.scheme_name,
            isin_growth = COALESCE(EXCLUDED.isin_growth, nav_history.isin_growth),
            plan_type = EXCLUDED.plan_type,
            option_type = EXCLUDED.option_type,
            is_reinvest = EXCLUDED.is_reinvest,
            scheme_id = COALESCE(EXCLUDED.scheme_id, nav_history.scheme_id)
    """
    
    # Filter data based on guards
    filtered_values = []
    skipped_inception = 0
    skipped_lock = 0
    
    for item in nav_data:
        code = item['scheme_code']
        n_date = item['nav_date']
        meta = scheme_meta.get(code)
        
        # Guard 1: Inception Date
        if meta and meta['inception'] and n_date < meta['inception']:
            skipped_inception += 1
            continue
            
        # Guard 2: Year Lock
        if n_date.year in locked_years:
            skipped_lock += 1
            continue
            
        filtered_values.append((
            code, 
            item['isin_growth'], 
            item.get('isin_div_payout'), 
            item.get('isin_div_reinv'),
            item['scheme_name'],
            item['nav_value'],
            n_date,
            item['plan_type'],
            item['option_type'],
            item['is_reinvest'],
            meta['id'] if meta else None
        ))
    
    if filtered_values:
        execute_values(cur, query, filtered_values)
        conn.commit()
        
    logger.info(f"Ingested {len(filtered_values)} NAV entries. "
                f"Skipped: {skipped_inception} (pre-inception), {skipped_lock} (year locked).")
