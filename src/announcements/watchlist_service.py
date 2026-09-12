"""
Master Watchlist Service.

Provides helper methods to fetch, cache, and manage the Master Office Watchlist
companies (50-250 stocks) monitored by the corporate announcement collectors.
"""

from typing import List, Dict, Any, Optional
import psycopg2.extras

from src.db.connection import get_connection
from src.config import logger


class WatchlistService:
    """Service to query and manage the active Master Office Watchlist."""

    @staticmethod
    def get_active_watchlist() -> List[Dict[str, Any]]:
        """
        Fetch all active companies in the Master Office Watchlist with their
        ISIN, NSE symbol, and BSE scrip code.
        """
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 
                        mw.master_id,
                        mw.company_id,
                        mw.notes,
                        mw.is_active,
                        mw.created_at,
                        c.isin,
                        c.company_name,
                        c.exchange_symbol AS nse_symbol,
                        c.bse_code,
                        c.sector,
                        c.industry
                    FROM master_watchlist mw
                    JOIN companies c ON mw.company_id = c.company_id
                    WHERE mw.is_active = TRUE
                    ORDER BY c.company_name ASC
                    """
                )
                return [dict(r) for r in cur.fetchall()]
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to fetch active master watchlist: {e}")
            raise

    @staticmethod
    def add_to_master_watchlist(company_id: int, notes: Optional[str] = None, user_id: Optional[int] = None) -> Dict[str, Any]:
        """Add a company to the Master Office Watchlist."""
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO master_watchlist (company_id, notes, is_active, added_by, updated_at)
                    VALUES (%s, %s, TRUE, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (company_id) DO UPDATE SET
                        is_active = TRUE,
                        notes = COALESCE(EXCLUDED.notes, master_watchlist.notes),
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING master_id, company_id, notes, is_active, created_at, updated_at
                    """,
                    (company_id, notes, user_id)
                )
                row = cur.fetchone()
            conn.commit()
            return dict(row)
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to add company {company_id} to master watchlist: {e}")
            raise

    @staticmethod
    def remove_from_master_watchlist(company_id: int) -> bool:
        """Deactivate or remove a company from the Master Office Watchlist."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE master_watchlist
                    SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
                    WHERE company_id = %s
                    """,
                    (company_id,)
                )
                rows_affected = cur.rowcount
            conn.commit()
            return rows_affected > 0
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to remove company {company_id} from master watchlist: {e}")
            raise

    @staticmethod
    def search_companies(query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search available companies to add to master watchlist."""
        conn = get_connection()
        clean_q = f"%{query.strip()}%"
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 
                        c.company_id,
                        c.isin,
                        c.company_name,
                        c.exchange_symbol AS nse_symbol,
                        c.bse_code,
                        c.sector,
                        c.industry,
                        EXISTS(
                            SELECT 1 FROM master_watchlist mw 
                            WHERE mw.company_id = c.company_id AND mw.is_active = TRUE
                        ) AS in_master_watchlist
                    FROM companies c
                    WHERE c.company_name ILIKE %s 
                       OR c.exchange_symbol ILIKE %s 
                       OR c.bse_code ILIKE %s 
                       OR c.isin ILIKE %s
                    ORDER BY in_master_watchlist DESC, c.company_name ASC
                    LIMIT %s
                    """,
                    (clean_q, clean_q, clean_q, clean_q, limit)
                )
                return [dict(r) for r in cur.fetchall()]
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to search companies: {e}")
            raise
