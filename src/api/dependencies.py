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

from fastapi.security import OAuth2PasswordBearer
from fastapi import Depends, HTTPException, status
from datetime import datetime, timezone
from src.api.utils.auth_utils import decode_access_token

# Token URL must match the login endpoint
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

async def get_current_user(
    token: str = Depends(oauth2_scheme),
    cur = Depends(get_db_cursor)
) -> dict:
    """
    Dependency to get the currently authenticated user from a JWT token.
    Checks for status, account expiry, and brute-force locks.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    payload = decode_access_token(token)
    if payload is None:
        raise credentials_exception
        
    username: str = payload.get("sub")
    if username is None:
        raise credentials_exception
        
    # Fetch user from database with new security fields
    cur.execute(
        """SELECT id, username, email, role, is_active, created_at, last_login, 
                  expires_at, permissions, locked_until 
           FROM users WHERE username = %s""",
        (username,)
    )
    user = cur.fetchone()
    
    if user is None:
        raise credentials_exception
        
    # 1. Manual Inactive Check
    if not user[4]: # is_active
        raise HTTPException(status_code=403, detail="Your account has been deactivated. Please contact admin.")
        
    # 2. Expiry Check
    expires_at = user[7]
    if expires_at and datetime.now(timezone.utc) > expires_at:
        raise HTTPException(
            status_code=403, 
            detail="Your access period has ended. Please contact the administrator to renew your access."
        )

    # 3. Lock Check (Brute-force protection)
    locked_until = user[9]
    if locked_until and datetime.now(timezone.utc) < locked_until:
        raise HTTPException(
            status_code=403,
            detail=f"Account temporarily locked due to multiple failed attempts. Try again after {locked_until.strftime('%H:%M:%S UTC')}."
        )
        
    return {
        "id": user[0],
        "username": user[1],
        "email": user[2],
        "role": user[3],
        "is_active": user[4],
        "created_at": user[5],
        "last_login": user[6],
        "expires_at": user[7],
        "permissions": user[8] or [],
        "locked_until": user[9]
    }

def require_permission(permission: str):
    """
    Dependency factor to require a specific permission for an endpoint.
    Admin role or 'all' permission bypasses specific checks.
    """
    async def permission_checker(current_user: dict = Depends(get_current_user)):
        perms = current_user.get("permissions", [])
        role = current_user.get("role")
        
        if role == 'admin' or "all" in perms or permission in perms:
            return True
            
        raise HTTPException(
            status_code=403, 
            detail=f"Access Denied: You do not have permission to access {permission}."
        )
    return permission_checker
