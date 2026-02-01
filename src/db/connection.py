"""
PostgreSQL database connection management.

Provides connection and cursor management for the application.
"""

import psycopg2
from psycopg2.extensions import connection, cursor
from typing import Optional

from src.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, logger


_connection: Optional[connection] = None


def get_connection() -> connection:
    """
    Get or create a PostgreSQL database connection.
    
    Returns:
        PostgreSQL connection object with autocommit=False
        
    Raises:
        RuntimeError: If database credentials are missing
        psycopg2.Error: If connection fails
    """
    global _connection
    
    # Validate credentials at connection time (not import time)
    if not DB_PASSWORD:
        raise RuntimeError(
            "Database credentials missing. Please configure DB_PASSWORD in .env file. "
            "See .env.example for template."
        )
    
    if _connection is None or _connection.closed:
        try:
            _connection = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                # CRITICAL: autocommit=False for transaction safety
                options='-c client_encoding=UTF8'
            )
            _connection.autocommit = False
            logger.info(f"Connected to PostgreSQL database: {DB_NAME}")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    return _connection


def get_cursor() -> cursor:
    """
    Get a cursor from the current connection.
    
    Returns:
        PostgreSQL cursor object
        
    Raises:
        psycopg2.Error: If cursor creation fails
    """
    conn = get_connection()
    return conn.cursor()


def close_connection():
    """Close the database connection if open."""
    global _connection
    
    if _connection and not _connection.closed:
        _connection.close()
        logger.info("Database connection closed")
        _connection = None
