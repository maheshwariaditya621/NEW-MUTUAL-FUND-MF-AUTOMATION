"""
Development server launcher for FastAPI application.

Run this script to start the API server with hot-reload enabled.
"""

import uvicorn
from src.config import logger

if __name__ == "__main__":
    logger.info("🚀 Starting Mutual Fund Analytics API...")
    logger.info("📖 API Documentation: http://localhost:8000/docs")
    logger.info("🔄 Hot reload enabled for development")
    
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
