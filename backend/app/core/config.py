"""
Configuration Management for Automated BI System
Loads and validates environment variables
"""
import os
from typing import Optional, List
from pydantic import validator
from pydantic_settings import BaseSettings  # ✅ FIXED IMPORT
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # API Configuration
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Automated BI API"
    VERSION: str = "1.0.0"
    
    # Database
    DATABASE_URL: Optional[str] = None
    
    # OpenAI
    OPENAI_API_KEY: Optional[str] = None
    
    # Security
    SECRET_KEY: str = "development-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # CORS
    BACKEND_CORS_ORIGINS: List[str] = [
        "http://localhost:8501",
        "http://localhost:8000",
        "https://*.streamlit.app",
        "https://*.onrender.com",
        "https://*.railway.app"
    ]
    
    # Logging
    LOG_LEVEL: str = "INFO"
    
    # File Upload
    MAX_UPLOAD_SIZE: int = 100 * 1024 * 1024  # 100MB
    ALLOWED_EXTENSIONS: List[str] = [".csv", ".xlsx", ".xls"]
    
    # Environment
    ENVIRONMENT: str = "development"
    
    @validator("DATABASE_URL", pre=True)
    def validate_database_url(cls, v: str, values: dict) -> str:
        """Validate database URL and set defaults based on environment"""
        if v:
            return v
        
        environment = values.get("ENVIRONMENT", "development")
        if environment == "production":
            raise ValueError(
                "DATABASE_URL must be set in production environment. "
                "Check Render dashboard for PostgreSQL connection string."
            )
        
        # Development fallback - SQLite
        import os
        from pathlib import Path
        
        data_dir = Path(__file__).parent.parent.parent / "data"
        data_dir.mkdir(exist_ok=True)
        
        db_path = data_dir / "automated_bi.db"
        return f"sqlite:///{db_path}"
    
    @validator("ENVIRONMENT")
    def detect_environment(cls, v: str) -> str:
        """Auto-detect production environment"""
        if os.getenv("RENDER") or os.getenv("RAILWAY_STATIC_URL"):
            return "production"
        return v or "development"
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()