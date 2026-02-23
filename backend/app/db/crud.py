"""
CRUD Operations for Automated BI System
Database create, read, update, delete operations
"""
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime
from app.db import models

# ----------------------------------------------------------------------
# Dataset Operations
# ----------------------------------------------------------------------

def create_dataset(db: Session, dataset_data: Dict[str, Any]) -> models.Dataset:
    """Create a new dataset record"""
    dataset = models.Dataset(
        id=dataset_data.get("dataset_id"),
        filename=dataset_data["filename"],
        original_rows=dataset_data["original_rows"],
        cleaned_rows=dataset_data["cleaned_rows"],
        file_type=dataset_data.get("file_type"),
        cleaning_report=dataset_data.get("cleaning_report")
    )
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    return dataset

def get_dataset(db: Session, dataset_id: str) -> Optional[models.Dataset]:
    """Get dataset by ID"""
    return db.query(models.Dataset).filter(models.Dataset.id == dataset_id).first()

def get_datasets(db: Session, skip: int = 0, limit: int = 100) -> List[models.Dataset]:
    """List all datasets with pagination"""
    return db.query(models.Dataset).order_by(
        models.Dataset.uploaded_at.desc()
    ).offset(skip).limit(limit).all()

def delete_dataset(db: Session, dataset_id: str) -> bool:
    """Delete dataset and all related records"""
    dataset = get_dataset(db, dataset_id)
    if dataset:
        db.delete(dataset)
        db.commit()
        return True
    return False

# ----------------------------------------------------------------------
# Metrics Operations
# ----------------------------------------------------------------------

def create_metrics(db: Session, metrics_data: Dict[str, Any]) -> models.DatasetMetrics:
    """Save calculated metrics"""
    metrics = models.DatasetMetrics(
        dataset_id=metrics_data["dataset_id"],
        metrics_summary=metrics_data.get("metrics", {}).get("summary", {}),
        metrics_details=metrics_data.get("metrics", {}),
        health_scores=metrics_data.get("metrics", {}).get("health_scores", {})
    )
    db.add(metrics)
    db.commit()
    db.refresh(metrics)
    return metrics

def get_latest_metrics(db: Session, dataset_id: str) -> Optional[models.DatasetMetrics]:
    """Get most recent metrics for a dataset"""
    return db.query(models.DatasetMetrics).filter(
        models.DatasetMetrics.dataset_id == dataset_id
    ).order_by(
        models.DatasetMetrics.calculated_at.desc()
    ).first()

def get_all_metrics(db: Session, dataset_id: str) -> List[models.DatasetMetrics]:
    """Get all metrics for a dataset"""
    return db.query(models.DatasetMetrics).filter(
        models.DatasetMetrics.dataset_id == dataset_id
    ).order_by(
        models.DatasetMetrics.calculated_at.desc()
    ).all()

# ----------------------------------------------------------------------
# Insights Operations
# ----------------------------------------------------------------------

def create_insights(db: Session, insights_data: Dict[str, Any]) -> models.Insights:
    """Save AI-generated insights"""
    insights = models.Insights(
        dataset_id=insights_data["dataset_id"],
        executive_summary=insights_data.get("insights", {}).get("executive_summary", ""),
        key_insights=insights_data.get("insights", {}).get("key_insights", []),
        recommendations=insights_data.get("insights", {}).get("top_recommendations", []),
        risk_warnings=insights_data.get("insights", {}).get("risk_warnings", []),
        growth_opportunities=insights_data.get("insights", {}).get("growth_opportunities", []),
        ai_model_used = "openai" if insights_data.get("insights", {}).get("ai_enabled") else "mock",
        confidence_score=0.85  # Could be calculated from insights
    )
    db.add(insights)
    db.commit()
    db.refresh(insights)
    return insights

def get_latest_insights(db: Session, dataset_id: str) -> Optional[models.Insights]:
    """Get most recent insights for a dataset"""
    return db.query(models.Insights).filter(
        models.Insights.dataset_id == dataset_id
    ).order_by(
        models.Insights.generated_at.desc()
    ).first()

# ----------------------------------------------------------------------
# Logging Operations
# ----------------------------------------------------------------------

def create_api_log(db: Session, log_data: Dict[str, Any]) -> models.APILog:
    """Log an API request"""
    log = models.APILog(
        endpoint=log_data.get("endpoint"),
        method=log_data.get("method"),
        status_code=log_data.get("status_code"),
        response_time_ms=log_data.get("response_time_ms"),
        user_agent=log_data.get("user_agent"),
        ip_address=log_data.get("ip_address")
    )
    db.add(log)
    db.commit()
    db.refresh(log)
    return log