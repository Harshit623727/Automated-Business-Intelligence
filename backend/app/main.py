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
        "timestamp": datetime.now().isoformat()
    }

@app.on_event("startup")
async def startup_event():
    """Run on application startup"""
    logger.info(f"Starting {settings.PROJECT_NAME} v{settings.VERSION}")
    
    # Ensure required directories exist
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
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
        reload=True,
        log_level="info"
    )