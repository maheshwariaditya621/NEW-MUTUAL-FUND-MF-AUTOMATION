"""
FastAPI application for Mutual Fund Portfolio Analytics.

Provides REST API endpoints for querying mutual fund holdings data.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.config import logger

# Import routers
from src.api.routers import health, stocks, schemes, chatbot

# Create FastAPI app
app = FastAPI(
    title="Mutual Fund Portfolio Analytics API",
    description="REST API for querying mutual fund holdings and portfolio data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS configuration for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # React dev server
        "http://localhost:5173",  # Vite dev server
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, prefix="/api/v1", tags=["Health"])
app.include_router(stocks.router, prefix="/api/v1/stocks", tags=["Stocks"])
app.include_router(schemes.router, prefix="/api/v1/schemes", tags=["Schemes"])
app.include_router(chatbot.router, prefix="/api/v1", tags=["Chatbot"])


@app.on_event("startup")
async def startup_event():
    """Initialize resources on startup."""
    logger.info("🚀 Mutual Fund Analytics API starting up...")
    logger.info("📊 API Documentation available at: http://localhost:8000/docs")


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
