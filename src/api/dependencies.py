"""
Dependency injection for FastAPI endpoints.

Provides reusable dependencies for database connections and common utilities.
"""

from typing import Generator
from psycopg2.extensions import connection, cursor
from src.db.connection import get_connection, get_cursor


def get_db_connection() -> Generator[connection, None, None]:
    """
    Dependency for database connection.
    
    Yields:
        PostgreSQL connection object
    """
    conn = get_connection()
    try:
        yield conn
    finally:
        # Connection is managed globally, don't close here
        pass


def get_db_cursor() -> Generator[cursor, None, None]:
    """
    Dependency for database cursor with transaction management.
    
    Yields:
        PostgreSQL cursor object
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        yield cur
        # Commit if no exception occurred
        conn.commit()
    except Exception as e:
        # Rollback on error
        conn.rollback()
        raise
    finally:
        cur.close()
