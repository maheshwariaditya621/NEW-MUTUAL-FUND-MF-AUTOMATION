"""
Data access layer for PostgreSQL database.

Provides idempotent insert functions for master data and transactional inserts for holdings.
"""

from typing import Optional, Dict, Any, List
from datetime import date

from src.db.connection import get_cursor, get_connection
from src.config import logger
from rapidfuzz import fuzz


def upsert_isin_master(
    isin: str,
    canonical_name: str,
    nse_symbol: Optional[str] = None,
    bse_code: Optional[str] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    entity_id: Optional[int] = None,
    period_id: Optional[int] = None
) -> str:
    """
    Insert or update the ISIN Master (Source of Truth) with Entity and Period tracking.
    """
    cursor = get_cursor()
    cursor.execute(
        """
        INSERT INTO isin_master (
            isin, canonical_name, nse_symbol, bse_code, sector, industry, 
            entity_id, first_seen_period_id, last_seen_period_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (isin) DO UPDATE SET 
            canonical_name = EXCLUDED.canonical_name,
            nse_symbol = COALESCE(EXCLUDED.nse_symbol, isin_master.nse_symbol),
            bse_code = COALESCE(EXCLUDED.bse_code, isin_master.bse_code),
            sector = COALESCE(EXCLUDED.sector, isin_master.sector),
            industry = COALESCE(EXCLUDED.industry, isin_master.industry),
            entity_id = COALESCE(EXCLUDED.entity_id, isin_master.entity_id),
            last_seen_period_id = EXCLUDED.last_seen_period_id,
            updated_at = CURRENT_TIMESTAMP
        RETURNING isin
        """,
        (isin, canonical_name, nse_symbol, bse_code, sector, industry, entity_id, period_id, period_id)
    )
    return cursor.fetchone()[0]

def get_isin_master_details(isin: str) -> Optional[Dict[str, Any]]:
    """
    Lookup details from ISIN Master, including the mapped entity_id.
    """
    cursor = get_cursor()
    cursor.execute(
        "SELECT canonical_name, nse_symbol, bse_code, sector, industry, entity_id FROM isin_master WHERE isin = %s",
        (isin,)
    )
    row = cursor.fetchone()
    if row:
        return {
            "canonical_name": row[0],
            "nse_symbol": row[1],
            "bse_code": row[2],
            "sector": row[3],
            "industry": row[4],
            "entity_id": row[5]
        }
    return None

def find_entity_by_name(canonical_name: str) -> Optional[int]:
    """Tier 2: Exact Name Match"""
    cursor = get_cursor()
    cursor.execute(
        "SELECT entity_id FROM corporate_entities WHERE UPPER(canonical_name) = %s AND is_active = TRUE",
        (canonical_name.upper(),)
    )
    row = cursor.fetchone()
    if row: return row[0]
    
    # Tier 2.5: Reverse Match (If the DB name is a single word like 'RELIANCE' 
    # and it is contained in the input name 'RELIANCE INDUSTRIES LTD')
    cursor.execute(
        """
        SELECT entity_id FROM corporate_entities 
        WHERE %s ILIKE '%%' || canonical_name || '%%' 
        AND LENGTH(canonical_name) >= 5
        AND is_active = TRUE
        """,
        (canonical_name.upper(),)
    )
    row = cursor.fetchone()
    return row[0] if row else None

def find_entity_by_symbol(name: str) -> Optional[int]:
    """Tier 3: Symbol containment Match"""
    # Check if any group_symbol is contained as a word within the company name
    cursor = get_cursor()
    cursor.execute(
        """
        SELECT entity_id FROM corporate_entities 
        WHERE %s ~* ('\y' || group_symbol || '\y')
        AND group_symbol IS NOT NULL
        AND is_active = TRUE
        LIMIT 1
        """,
        (name,)
    )
    row = cursor.fetchone()
    return row[0] if row else None

def fuzzy_search_entity(name: str, threshold: float = 0.6) -> Optional[int]:
    """Tier 4: Fuzzy Match fallback using pg_trgm (Lowered to 0.6 for name variations)"""
    cursor = get_cursor()
    # Ensure threshold set for session
    cursor.execute("SET pg_trgm.similarity_threshold = %s", (threshold,))
    cursor.execute(
        """
        SELECT entity_id, similarity(canonical_name, %s) as sml
        FROM corporate_entities 
        WHERE canonical_name %% %s AND is_active = TRUE
        ORDER BY sml DESC LIMIT 1
        """,
        (name, name)
    )
    row = cursor.fetchone()
    if row:
        logger.info(f"[FUZZY RESOLUTION] Matched '{name}' to entity_id {row[0]} (Similarity: {row[1]:.2f})")
        return row[0]
    return None

def create_corporate_entity(canonical_name: str, symbol: Optional[str] = None, sector: Optional[str] = None) -> int:
    """Create a new logical business identity."""
    cursor = get_cursor()
    cursor.execute(
        """
        INSERT INTO corporate_entities (canonical_name, group_symbol, sector)
        VALUES (%s, %s, %s)
        RETURNING entity_id
        """,
        (canonical_name, symbol, sector)
    )
    return cursor.fetchone()[0]

def log_resolution_audit(isin: str, company_name: str, resolved_entity_id: int, tier: str, details: Optional[str] = None):
    """Log the decision process for security resolution to the database."""
    cursor = get_cursor()
    cursor.execute(
        """
        INSERT INTO resolution_audit (isin, raw_name, resolved_entity_id, resolution_tier, details)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (isin, company_name, resolved_entity_id, tier, details)
    )
    logger.info(f"[RESOLUTION AUDIT] ISIN: {isin} | Name: {company_name} | Resolved ID: {resolved_entity_id} via {tier}")

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
    Integrates 3-tier string resolution:
    1. Explicit Mapping -> 2. Exact Match -> 3. Fuzzy Auto-Map (>95%)
    """
    cursor = get_cursor()
    original_name = scheme_name.upper().strip()
    final_name = original_name
    
    # Tier 0: Scheme Aliases (New Resolution Engine - Fast Path)
    cursor.execute(
        """
        SELECT canonical_scheme_id 
        FROM scheme_aliases 
        WHERE amc_id = %s AND alias_name = %s AND plan_type = %s AND option_type = %s AND is_reinvest = %s
        """,
        (amc_id, original_name, plan_type, option_type, is_reinvest)
    )
    alias = cursor.fetchone()
    if alias:
        logger.info(f"Resolved '{original_name}' via ALIAS to scheme_id {alias[0]}")
        return alias[0]

    # Tier 1: Legacy Mapping Validation (scheme_name_mappings)
    cursor.execute(
        "SELECT canonical_name FROM scheme_name_mappings WHERE amc_id = %s AND source_name = %s LIMIT 1",
        (amc_id, original_name)
    )
    mapping = cursor.fetchone()
    
    if mapping:
        final_name = mapping[0]
    else:
        # Tier 2: Exact Match check
        cursor.execute("SELECT 1 FROM schemes WHERE amc_id = %s AND scheme_name = %s LIMIT 1", (amc_id, original_name))
        is_exact = cursor.fetchone()
        
        if not is_exact:
            # Tier 3: Fuzzy Auto-mapping (for minor typos > 95%)
            cursor.execute("SELECT DISTINCT scheme_name FROM schemes WHERE amc_id = %s", (amc_id,))
            existing_schemes = [row[0] for row in cursor.fetchall()]
            
            if existing_schemes:
                from rapidfuzz import fuzz
                best_match = None
                best_score = 0
                for es in existing_schemes:
                    score = fuzz.token_set_ratio(original_name, es)
                    if score > 95 and score > best_score:
                        best_score = score
                        best_match = es
                        
                if best_match:
                    logger.info(f"Auto-mapping typo scheme '{original_name}' to '{best_match}' (Similarity: {best_score}%)")
                    final_name = best_match
                    # Persist the mapping so it doesn't need to fuzzy match again
                    cursor.execute(
                        "INSERT INTO scheme_name_mappings (amc_id, source_name, canonical_name) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (amc_id, original_name, best_match)
                    )

    # Proceed with canonical UPSERT
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
        (amc_id, final_name, plan_type, option_type, is_reinvest, scheme_category, scheme_code)
    )
    
    scheme_id = cursor.fetchone()[0]
    logger.debug(f"Scheme upserted: {final_name} ({plan_type}/{option_type})")
    
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

def get_previous_period_id(period_id: int) -> Optional[int]:
    """
    Find the period_id immediately preceding the given one.
    """
    cursor = get_cursor()
    cursor.execute(
        """
        SELECT period_id FROM periods 
        WHERE period_end_date < (SELECT period_end_date FROM periods WHERE period_id = %s)
        ORDER BY period_end_date DESC LIMIT 1
        """,
        (period_id,)
    )
    row = cursor.fetchone()
    return row[0] if row else None

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
    bse_code: Optional[str] = None,
    entity_id: Optional[int] = None
) -> int:
    """
    Insert or update company with support for exchange symbols, sectors, and entity linking.
    """
    cursor = get_cursor()
    
    cursor.execute(
        """
        INSERT INTO companies (isin, company_name, exchange_symbol, sector, industry, nse_symbol, bse_code, entity_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (isin) DO UPDATE SET 
            company_name = EXCLUDED.company_name,
            exchange_symbol = COALESCE(EXCLUDED.exchange_symbol, companies.exchange_symbol),
            sector = COALESCE(EXCLUDED.sector, companies.sector),
            industry = COALESCE(EXCLUDED.industry, companies.industry),
            nse_symbol = COALESCE(EXCLUDED.nse_symbol, companies.nse_symbol),
            bse_code = COALESCE(EXCLUDED.bse_code, companies.bse_code),
            entity_id = COALESCE(EXCLUDED.entity_id, companies.entity_id),
            updated_at = CURRENT_TIMESTAMP
        RETURNING company_id
        """,
        (isin, company_name, exchange_symbol, sector, industry, nse_symbol, bse_code, entity_id)
    )
    
    company_id = cursor.fetchone()[0]
    logger.debug(f"Company upserted: {company_name} ({isin}) (company_id={company_id})")
    
    return company_id


def create_snapshot(
    scheme_id: int,
    period_id: int,
    total_holdings: int,
    total_value_inr: float,
    holdings_count: int,
    total_net_assets_inr: Optional[float] = None
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
        total_net_assets_inr: Total AUM (Grand Total) extracted from the AMC Excel footer
        
    Returns:
        snapshot_id
        
    Raises:
        psycopg2.IntegrityError: If snapshot already exists
        psycopg2.Error: If database operation fails
    """
    cursor = get_cursor()
    
    cursor.execute(
        """
        INSERT INTO scheme_snapshots (scheme_id, period_id, total_holdings, total_value_inr, holdings_count, total_net_assets_inr)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING snapshot_id
        """,
        (scheme_id, period_id, total_holdings, total_value_inr, holdings_count, total_net_assets_inr)
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
        "SELECT company_name, sector, industry, nse_symbol, bse_code, entity_id FROM companies WHERE isin = %s",
        (isin,)
    )
    row = cursor.fetchone()
    if row:
        return {
            "canonical_name": row[0],
            "sector": row[1],
            "industry": row[2],
            "nse_symbol": row[3],
            "bse_code": row[4],
            "entity_id": row[5]
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
        
def get_product_type(name: str) -> str:
    """Detects if a scheme is an ETF, Index Fund, or Standard Mutual Fund."""
    name_upper = name.upper()
    if " ETF" in name_upper or "ETF " in name_upper or name_upper.endswith("ETF"):
        return "ETF"
    if any(x in name_upper for x in ["INDEX FUND", "INDX FUND", "INDX"]):
        return "INDEX"
    return "STANDARD"

def find_potential_scheme_renames(amc_id: int, new_name: str, plan: str, opt: str, re: bool, period_id: int):
    """
    Runs the 4-Layer Filter to find if a new scheme name is a rename of an existing one.
    This is triggered during PortfolioLoader.load if a name is not found.
    """
    cursor = get_cursor()
    
    # 1. Get previous period
    cursor.execute("SELECT period_id FROM periods WHERE period_end_date < (SELECT period_end_date FROM periods WHERE period_id = %s) ORDER BY period_end_date DESC LIMIT 1", (period_id,))
    res = cursor.fetchone()
    if not res: return None
    prev_period_id = res[0]

    # 2. Find "Missing" schemes (at same AMC/Plan/Opt/Re) that had data last month but NOT this month
    cursor.execute("""
        SELECT s.scheme_id, s.scheme_name
        FROM schemes s
        JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
        WHERE s.amc_id = %s AND s.plan_type = %s AND s.option_type = %s AND s.is_reinvest = %s
          AND ss.period_id = %s
          AND NOT EXISTS (
              SELECT 1 FROM scheme_snapshots ss2 
              WHERE ss2.scheme_id = s.scheme_id AND ss2.period_id = %s
          )
    """, (amc_id, plan, opt, re, prev_period_id, period_id))
    candidates = cursor.fetchall()
    
    if not candidates:
        return None

    # 3. Apply 4-Layer Filter
    new_type = get_product_type(new_name)
    amc_clean = "" # We could fetch it but for speed let's just do direct comparison
    best_match = None
    
    for old_id, old_name in candidates:
        # Layer 0: Product Type
        if new_type != get_product_type(old_name):
            continue
            
        # Layer 1: Fuzzy Text
        text_score = max(fuzz.token_sort_ratio(new_name, old_name), fuzz.token_set_ratio(new_name, old_name))
        if text_score < 50:
            continue
            
        # Layer 2 & 3: Portfolio Overlap
        # (Since we are in the middle of loading, the NEW scheme doesn't have a snapshot yet)
        # We need to compare the CURRENT holdings list with the PREVIOUS snapshot of the candidate
        # This will be handled in the PortfolioLoader which has the 'holdings' data
        return {
            "old_id": old_id,
            "old_name": old_name,
            "text_score": text_score,
            "product_type": new_type
        }
    
    return None

def record_pending_merge(amc_id: int, new_name: str, old_id: int, plan: str, opt: str, re: bool, score: float, method: str, metadata: dict):
    """Stores a potential scheme merge for user approval in Admin Vault."""
    import json
    cursor = get_cursor()
    cursor.execute(
        """
        INSERT INTO pending_scheme_merges (
            amc_id, new_scheme_name, old_scheme_id, plan_type, option_type, 
            is_reinvest, confidence_score, detection_method, metadata, status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING')
        ON CONFLICT (amc_id, new_scheme_name, plan_type, option_type, is_reinvest) 
        DO UPDATE SET 
            confidence_score = EXCLUDED.confidence_score,
            metadata = EXCLUDED.metadata,
            updated_at = CURRENT_TIMESTAMP
        """,
        (amc_id, new_name, old_id, plan, opt, re, score, method, json.dumps(metadata))
    )
    logger.warning(f"[QUARANTINE] Detected potential rename: '{new_name}' matches ID {old_id} ({score*100:.1f}%)")

def record_notification(level: str, category: str, content: str, error_details: str = None):
    """Records a system notification for the Admin Vault and potential Telegram sync."""
    cursor = get_cursor()
    cursor.execute(
        """
        INSERT INTO notification_logs (level, category, content, error_details)
        VALUES (%s, %s, %s, %s)
        """,
        (level, category, content, error_details)
    )
    logger.info(f"[NOTIF] {level} | {category} | {content[:50]}...")
