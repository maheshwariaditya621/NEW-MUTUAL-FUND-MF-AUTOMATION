"""
Transaction management for database operations.

Provides transaction wrapper for safe database operations.
"""

import functools
from typing import Callable, Any

from src.db.connection import get_connection
from src.config import logger


def transactional(func: Callable) -> Callable:
    """
    Decorator that wraps a function in a database transaction.
    
    - Begins transaction automatically
    - Commits on success
    - Rolls back on ANY error
    - Logs rollback clearly
    
    Usage:
        @transactional
        def load_data(...):
            # Your database operations here
            pass
    
    Args:
        func: Function to wrap in transaction
        
    Returns:
        Wrapped function
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        conn = get_connection()
        
        try:
            logger.info(f"Starting transaction for {func.__name__}")
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Commit transaction
            conn.commit()
            logger.success(f"Transaction committed successfully for {func.__name__}")
            
            return result
            
        except Exception as e:
            # Rollback transaction
            conn.rollback()
            logger.rollback(f"Transaction rolled back for {func.__name__}: {str(e)}")
            raise
    
    return wrapper


class TransactionContext:
    """
    Context manager for explicit transaction control.
    
    Usage:
        with TransactionContext() as conn:
            # Your database operations
            cursor = conn.cursor()
            cursor.execute(...)
    """
    
    def __init__(self):
        self.conn = None
    
    def __enter__(self):
        self.conn = get_connection()
        logger.info("Transaction started")
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # No exception - commit
            self.conn.commit()
            logger.success("Transaction committed successfully")
        else:
            # Exception occurred - rollback
            self.conn.rollback()
            logger.rollback(f"Transaction rolled back: {exc_val}")
        
        return False  # Re-raise exception if any
