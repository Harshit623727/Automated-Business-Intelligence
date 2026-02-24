"""
Database Configuration for Automated BI System
SQLite for development, PostgreSQL for production
"""
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from pathlib import Path
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)

# =====================================================================
# CRITICAL: Database URL Resolution
# =====================================================================
def get_database_url() -> str:
    """
    Get the appropriate database URL based on environment
    - Production: Use PostgreSQL from environment (Render provides this)
    - Development: Use SQLite local file
    
    Returns:
        str: Database connection URL
    """
    # First priority: Environment variable (Render PostgreSQL)
    env_db_url = os.getenv("DATABASE_URL")
    if env_db_url:
        # Render provides PostgreSQL URLs that might need modification
        db_url = env_db_url
        
        # Fix for Render PostgreSQL URLs (they start with postgres:// but SQLAlchemy needs postgresql://)
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
            logger.info("Converted postgres:// to postgresql:// for SQLAlchemy")
        
        logger.info(f"Using PostgreSQL database from environment")
        return db_url
    
    # Second priority: Settings (from .env or config)
    if settings.DATABASE_URL:
        logger.info(f"Using database from settings: {settings.DATABASE_URL.split('://')[0]}://...")
        return settings.DATABASE_URL
    
    # Final fallback: SQLite for development
    data_dir = Path(__file__).parent.parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    db_path = data_dir / "automated_bi.db"
    
    logger.warning(
        f"No DATABASE_URL found in environment. Using SQLite fallback: {db_path}\n"
        "This is fine for development but WILL NOT WORK in production!"
    )
    
    return f"sqlite:///{db_path}"

# =====================================================================
# Database Engine Creation
# =====================================================================
DATABASE_URL = get_database_url()
IS_SQLITE = DATABASE_URL.startswith("sqlite")
IS_POSTGRESQL = DATABASE_URL.startswith("postgresql")

# Create engine with appropriate settings
if IS_SQLITE:
    # SQLite specific configuration
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},  # SQLite needs this for multiple threads
        pool_pre_ping=True,
        echo=settings.ENVIRONMENT == "development"  # Log SQL in development
    )
    logger.info("✅ SQLite engine created for development")
    
elif IS_POSTGRESQL:
    # PostgreSQL specific configuration
    engine = create_engine(
        DATABASE_URL,
        pool_size=5,  # Number of connections to maintain
        max_overflow=10,  # Maximum overflow connections
        pool_pre_ping=True,  # Verify connections before using
        pool_recycle=3600,  # Recycle connections after 1 hour
        echo=settings.ENVIRONMENT == "development"
    )
    logger.info("✅ PostgreSQL engine created for production")
    
else:
    # Other databases (MySQL, etc.) - use generic configuration
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        echo=settings.ENVIRONMENT == "development"
    )
    logger.info(f"✅ Database engine created for {DATABASE_URL.split('://')[0]}")

# =====================================================================
# Session Factory
# =====================================================================
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# =====================================================================
# Base Class for Models
# =====================================================================
Base = declarative_base()

# =====================================================================
# Dependency for FastAPI
# =====================================================================
def get_db():
    """
    Dependency function to get database session
    Usage: db: Session = Depends(get_db)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =====================================================================
# Table Creation
# =====================================================================
def create_tables():
    """Create all database tables"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Database tables created/verified")
    except Exception as e:
        logger.error(f"❌ Failed to create tables: {e}")
        if IS_POSTGRESQL:
            logger.error(
                "PostgreSQL connection failed. Check:\n"
                "1. DATABASE_URL is correct in environment\n"
                "2. Database server is running\n"
                "3. Network allows connection"
            )
        raise

# =====================================================================
# Test Connection - FIXED for SQLAlchemy 2.0
# =====================================================================
def test_connection():
    """Test database connection"""
    try:
        with engine.connect() as conn:
            if IS_POSTGRESQL:
                # FIXED: Using text() for raw SQL in SQLAlchemy 2.0
                result = conn.execute(text("SELECT version()")).fetchone()
                logger.info(f"✅ Connected to PostgreSQL: {result[0][:50]}...")
            else:
                # FIXED: Using text() for raw SQL
                result = conn.execute(text("SELECT sqlite_version()")).fetchone()
                logger.info(f"✅ Connected to SQLite version: {result[0]}")
        return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False

# =====================================================================
# IMPORTANT: test_connection() is NOT called at import time
# It will be called in main.py startup event instead
# This prevents deployment failures from temporary DB unavailability
# =====================================================================