"""
Health check endpoints.
"""

from fastapi import APIRouter, HTTPException, Depends
from psycopg2.extensions import connection

from src.api.models.responses import HealthResponse
from src.api.dependencies import get_db_connection

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Basic health check endpoint.
    
    Returns:
        Health status of the API
    """
    return HealthResponse(
        status="healthy",
        database="not_checked",
        message="API is running"
    )


@router.get("/health/db", response_model=HealthResponse)
async def database_health_check(conn: connection = Depends(get_db_connection)):
    """
    Database health check endpoint.
    
    Verifies database connectivity.
    
    Returns:
        Health status including database connection
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        
        return HealthResponse(
            status="healthy",
            database="connected",
            message="Database connection successful"
        )
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Database connection failed: {str(e)}"
        )
