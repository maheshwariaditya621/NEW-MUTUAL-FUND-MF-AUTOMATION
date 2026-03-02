"""
PostgreSQL database connection management.

Provides connection and cursor management for the application.
"""

import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2 import pool
from typing import Optional
import threading

from src.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, logger


_connection: Optional[connection] = None
_pool: Optional[pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


def get_connection() -> connection:
    """
    Get or create a PostgreSQL database connection (Legacy/Global).
    
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
            logger.info(f"Connected to PostgreSQL database (Global): {DB_NAME}")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    return _connection


def get_pool() -> pool.ThreadedConnectionPool:
    """Get or initialize the threaded connection pool."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                try:
                    _pool = pool.ThreadedConnectionPool(
                        minconn=1,
                        maxconn=20,
                        host=DB_HOST,
                        port=DB_PORT,
                        database=DB_NAME,
                        user=DB_USER,
                        password=DB_PASSWORD,
                        options='-c client_encoding=UTF8'
                    )
                    logger.info(f"Initialized PostgreSQL connection pool: {DB_NAME}")
                except psycopg2.Error as e:
                    logger.error(f"Failed to initialize connection pool: {e}")
                    raise
    return _pool


def get_pool_connection() -> connection:
    """Get a connection from the pool."""
    pool = get_pool()
    conn = pool.getconn()
    conn.autocommit = False
    return conn


def release_pool_connection(conn: connection):
    """Return a connection to the pool."""
    if _pool and conn:
        _pool.putconn(conn)


def get_cursor() -> cursor:
    """
    Get a cursor from the current legacy connection.
    
    Returns:
        PostgreSQL cursor object
        
    Raises:
        psycopg2.Error: If cursor creation fails
    """
    conn = get_connection()
    return conn.cursor()


def close_connection():
    """Close the legacy connection and pool if open."""
    global _connection, _pool
    
    if _connection and not _connection.closed:
        _connection.close()
        logger.info("Global database connection closed")
        _connection = None

    if _pool:
        _pool.closeall()
        logger.info("Database connection pool closed")
        _pool = None
