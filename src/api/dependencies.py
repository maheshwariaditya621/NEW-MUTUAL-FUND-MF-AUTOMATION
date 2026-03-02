"""
Dependency injection for FastAPI endpoints.

Provides reusable dependencies for database connections and common utilities.
"""

from typing import Generator
from psycopg2.extensions import connection, cursor
from src.db.connection import get_pool_connection, release_pool_connection
from fastapi import Header


def get_db_connection() -> Generator[connection, None, None]:
    """
    Dependency for database connection using a connection pool.
    
    Yields:
        PostgreSQL connection object
    """
    conn = get_pool_connection()
    try:
        yield conn
    finally:
        release_pool_connection(conn)


def get_db_cursor() -> Generator[cursor, None, None]:
    """
    Dependency for database cursor with transaction management and pooling.
    
    Yields:
        PostgreSQL cursor object
    """
    conn = get_pool_connection()
    cur = conn.cursor()
    try:
        yield cur
        # Commit if no exception occurred
        conn.commit()
    except Exception as e:
        # Rollback on error
        if conn and not conn.closed:
            conn.rollback()
        raise
    finally:
        cur.close()
        release_pool_connection(conn)

def verify_admin(x_admin_secret: str = Header(None)) -> bool:
    """
    Dependency to verify admin secret key.
    """
    from src.config.settings import ADMIN_SECRET_KEY
    from fastapi import HTTPException
    
    if not x_admin_secret or x_admin_secret != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid Admin Secret")
    return True
