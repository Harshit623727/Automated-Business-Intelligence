"""
Configuration Management for Automated BI System
Loads and validates environment variables
"""
import os
from typing import Optional, List
from pydantic import BaseSettings, validator
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # API Configuration
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Automated BI API"
    VERSION: str = "1.0.0"
    
    # Database
    DATABASE_URL: str = "sqlite:///./data/automated_bi.db"
    
    # OpenAI
    OPENAI_API_KEY: Optional[str] = None
    
    # Security
    SECRET_KEY: str = "development-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # CORS
    BACKEND_CORS_ORIGINS: List[str] = [
        "http://localhost:8501",  # Streamlit local
        "http://localhost:8000",  # FastAPI local
        "https://*.streamlit.app",  # Streamlit Cloud
        "https://*.onrender.com",  # Render
        "https://*.railway.app"    # Railway
    ]
    
    # Logging
    LOG_LEVEL: str = "INFO"
    
    # File Upload
    MAX_UPLOAD_SIZE: int = 100 * 1024 * 1024  # 100MB
    ALLOWED_EXTENSIONS: List[str] = [".csv", ".xlsx", ".xls"]
    
    @validator("DATABASE_URL", pre=True)
    def validate_database_url(cls, v: str) -> str:
        """Ensure database URL is properly formatted"""
        if v.startswith("sqlite"):
            # Ensure directory exists
            db_path = v.replace("sqlite:///", "")
            os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        return v
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()