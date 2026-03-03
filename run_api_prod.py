"""
Production server launcher for FastAPI application.

This script runs the API without hot-reload, which is critical for 
low CPU usage on EC2 instances with many data files.
"""

import uvicorn
from src.config import logger

if __name__ == "__main__":
    logger.info("🚀 Starting Mutual Fund Analytics API (PRODUCTION MODE)...")
    logger.info("⚠️  Hot reload is DISABLED for performance.")
    
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # This MUST be False on EC2
        workers=4,      # Run multiple workers for better throughput on multi-core
        log_level="info"
    )
