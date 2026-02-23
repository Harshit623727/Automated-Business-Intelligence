"""
SQLAlchemy Models for Automated BI System
Database schema definition
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, JSON, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.db.database import Base
import uuid

def generate_uuid():
    """Generate UUID for primary key"""
    return str(uuid.uuid4())

class Dataset(Base):
    """Stores uploaded dataset metadata"""
    __tablename__ = "datasets"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    filename = Column(String, nullable=False)
    original_rows = Column(Integer, nullable=False)
    cleaned_rows = Column(Integer, nullable=False)
    uploaded_at = Column(DateTime(timezone=True), server_default=func.now())
    file_type = Column(String)
    cleaning_report = Column(JSON)
    
    # Relationships
    metrics = relationship("DatasetMetrics", back_populates="dataset", cascade="all, delete-orphan")
    insights = relationship("Insights", back_populates="dataset", cascade="all, delete-orphan")

class DatasetMetrics(Base):
    """Stores calculated metrics for datasets"""
    __tablename__ = "dataset_metrics"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dataset_id = Column(String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    calculated_at = Column(DateTime(timezone=True), server_default=func.now())
    metrics_summary = Column(JSON)
    metrics_details = Column(JSON)
    health_scores = Column(JSON)
    
    # Relationship
    dataset = relationship("Dataset", back_populates="metrics")

class Insights(Base):
    """Stores AI-generated insights"""
    __tablename__ = "insights"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dataset_id = Column(String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    generated_at = Column(DateTime(timezone=True), server_default=func.now())
    executive_summary = Column(Text)
    key_insights = Column(JSON)
    recommendations = Column(JSON)
    risk_warnings = Column(JSON)
    growth_opportunities = Column(JSON)
    ai_model_used = Column(String)
    confidence_score = Column(Float)
    
    # Relationship
    dataset = relationship("Dataset", back_populates="insights")

class APILog(Base):
    """Logs API requests for monitoring and debugging"""
    __tablename__ = "api_logs"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    endpoint = Column(String)
    method = Column(String)
    status_code = Column(Integer)
    response_time_ms = Column(Float)
    user_agent = Column(String)
    ip_address = Column(String)