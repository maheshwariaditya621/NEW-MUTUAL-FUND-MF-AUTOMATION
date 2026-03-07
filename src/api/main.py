"""
FastAPI application for Mutual Fund Portfolio Analytics.

Provides REST API endpoints for querying mutual fund holdings data.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.config import logger

# Import routers
from src.api.routers import health, stocks, schemes, chatbot, insights, admin, amcs, auth

# Create FastAPI app
app = FastAPI(
    title="Mutual Fund Portfolio Analytics API",
    description="REST API for querying mutual fund holdings and portfolio data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS configuration: Allow all for dev LAN access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, prefix="/api/v1", tags=["Health"])
app.include_router(stocks.router, prefix="/api/v1/stocks", tags=["Stocks"])
app.include_router(schemes.router, prefix="/api/v1/schemes", tags=["Schemes"])
app.include_router(chatbot.router, prefix="/api/v1", tags=["Chatbot"])
app.include_router(insights.router, prefix="/api/v1/insights", tags=["Insights"])
app.include_router(admin.router, prefix="/api/v1/admin", tags=["Admin"])
app.include_router(amcs.router, prefix="/api/v1/amcs", tags=["AMCs"])
app.include_router(auth.router, prefix="/api/v1/auth", tags=["Authentication"])


@app.on_event("startup")
async def startup_event():
    """Initialize resources on startup."""
    logger.info("🚀 Mutual Fund Analytics API starting up...")
    logger.info("📊 API Documentation available at: http://localhost:8000/docs")
    
    # Send Telegram notification on startup
    try:
        from src.alerts.telegram_notifier import get_notifier
        notifier = get_notifier()
        notifier.alert("🟢 <b>MF Analytics API is UP and Running</b>\nSite: avfincorp.com\nStatus: Healthy")
    except Exception as e:
        logger.error(f"Failed to send startup notification: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on shutdown."""
    from src.db.connection import close_connection
    close_connection()
    logger.info("👋 API shutting down...")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Mutual Fund Portfolio Analytics API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/v1/health"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)
