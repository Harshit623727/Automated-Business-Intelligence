"""
FastAPI Backend for Automated BI System
Main application entry point with API endpoints
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from datetime import datetime
import logging
import os

from app.api.endpoints import router as api_router
from app.core.config import settings
from app.utils.logger import setup_logging, get_logger
from app.db.database import create_tables, test_connection

# Setup logging
setup_logging(level=settings.LOG_LEVEL)
logger = get_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Automated Business Intelligence Platform - Upload data, get automated KPIs and AI insights",
    version=settings.VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    contact={
        "name": "Automated BI",
        "email": "support@automatedbi.com",
    },
    license_info={
        "name": "MIT",
    }
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router, prefix=settings.API_V1_STR)

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "service": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "status": "operational",
        "timestamp": datetime.now().isoformat(),
        "environment": settings.ENVIRONMENT,
        "endpoints": {
            "documentation": "/docs",
            "health": "/health",
            "upload": f"{settings.API_V1_STR}/upload",
            "metrics": f"{settings.API_V1_STR}/metrics/{{dataset_id}}",
            "insights": f"{settings.API_V1_STR}/insights/{{dataset_id}}",
            "datasets": f"{settings.API_V1_STR}/datasets"
        }
    }

@app.get("/health")
async def health_check():
    """Simplified health check for load balancers"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "environment": settings.ENVIRONMENT
    }

@app.on_event("startup")
async def startup_event():
    """
    Run on application startup
    Moved database connection test here to prevent deployment failures
    from temporary database unavailability
    """
    logger.info(f"Starting {settings.PROJECT_NAME} v{settings.VERSION} in {settings.ENVIRONMENT} mode")
    
    # Ensure required directories exist
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    # Create database tables
    try:
        create_tables()
        logger.info("✅ Database tables ready")
    except Exception as e:
        logger.error(f"❌ Failed to create tables: {e}")
        # In production, we might want to crash if DB is unavailable
        if settings.ENVIRONMENT == "production":
            logger.critical("Cannot start in production without database")
            raise e
    
    # Test database connection (moved from import time)
    if not test_connection():
        logger.warning("Database connection test failed at startup")
        if settings.ENVIRONMENT == "production":
            logger.critical("Production environment requires database connection")
            raise Exception("Database connection failed")
    else:
        logger.info("✅ Database connection verified")
    
    logger.info("Application startup complete")

@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown"""
    logger.info("Shutting down application")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=settings.ENVIRONMENT == "development",
        log_level="info"
    )